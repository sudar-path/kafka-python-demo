#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Integration tests -- these hit a real broker.

These carry the claims the unit tests structurally cannot. test_reset_offsets.py
patches compute_diff and apply_seek, so it proves the operator's control flow and
nothing about Kafka: `test_dry_run_commits_nothing` asserts a patched function
was not called, which would still pass if the real one committed on every call.
"the committed offsets on the broker are identical afterwards" is only observable
against a broker, and it is the property the approver of a promotion is relying
on.

TEST-002: a test that creates a topic must delete it. Do NOT copy the teardown
style from the neighbouring operator and trigger tests: they create topics and
abandon them, and on a shared broker the leftovers accumulate until runs start
interfering with each other. Those tests only stay green because the leaked
topics happen to be idempotent against their own leftovers. That is luck.
"""
from __future__ import annotations

import time
from uuid import uuid4

import pytest
from confluent_kafka import OFFSET_INVALID, Consumer, Producer, TopicPartition

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.providers.apache.kafka.operators.reset_offsets import (
    ResetConsumerGroupOffsetsOperator,
)

# Upstream's provider tests hardcode this address and it is Breeze's, not the
# host's. int-tests.yml preflights it and INT/STAG carry the PLAINTEXT_TEST
# listener that advertises it. PROD deliberately does not.
BOOTSTRAP = "broker:29092"
CONN_ID = "kafka_default"

PARTITIONS = 3
PER_PARTITION = 6  # so LATEST is 6 on every partition and the arithmetic is obvious


@pytest.fixture(autouse=True)
def kafka_connection(create_connection_without_db):
    """The connection the operator's hook resolves.

    `create_connection_without_db` is the upstream fixture for this; writing to
    the metadata DB would make these tests depend on a migrated Airflow
    instance, which the integration runner does not have.
    """
    create_connection_without_db(
        Connection(
            conn_id=CONN_ID,
            uri=(
                f"kafka://{BOOTSTRAP}"
                f"?bootstrap.servers={BOOTSTRAP}"
                # NOT upstream's socket.timeout.ms=10. That is tuned for a
                # producer that is expected to fail fast; an AdminClient
                # metadata round trip against a cold broker routinely exceeds
                # 10ms and the tests flake with a timeout that looks like a bug
                # in the operator.
                "&socket.timeout.ms=10000"
                "&group.id=operator.reset.test.integration.admin"
            ),
        )
    )


@pytest.fixture
def hook():
    return KafkaAdminClientHook(kafka_config_id=CONN_ID)


@pytest.fixture
def topic(hook):
    """Unique name plus guaranteed teardown.

    The uuid suffix is what makes concurrent runs safe; the `yield` is what
    makes cleanup run even when the test fails. A `delete_topic` at the end of
    the test body only cleans up on the happy path, which is precisely when
    cleanup matters least.
    """
    name = f"operator.reset.test.integration.{uuid4().hex[:8]}"
    hook.create_topic(topics=[(name, PARTITIONS, 1)])
    # create_topic returns before the metadata has propagated to the client that
    # will produce to it. Producing into the gap auto-creates a 1-partition
    # topic under the same name on brokers with auto-create enabled, and the
    # test then fails on a missing partition 1 for reasons invisible in the log.
    _await_partitions(name)
    yield name
    hook.delete_topic(topics=[name])


def _await_partitions(topic: str, expected: int = PARTITIONS, timeout: float = 30.0) -> None:
    admin = KafkaAdminClientHook(kafka_config_id=CONN_ID).get_conn
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        meta = admin.list_topics(timeout=10)
        found = meta.topics.get(topic)
        if found is not None and not found.error and len(found.partitions) == expected:
            return
        time.sleep(0.5)
    raise AssertionError(f"topic {topic!r} did not reach {expected} partitions in {timeout}s")


def _produce(topic: str, per_partition: int = PER_PARTITION) -> None:
    """Fill every partition explicitly.

    Partition is passed rather than left to the partitioner: the seek arithmetic
    is asserted per partition, and a partitioner that happens to hash every key
    to partition 0 would leave two partitions empty and the assertions vacuous.
    """
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for partition in range(PARTITIONS):
        for i in range(per_partition):
            producer.produce(topic, value=f"p{partition}-m{i}".encode(), partition=partition)
    assert producer.flush(30) == 0, "producer did not drain; broker slow or unreachable"


def _consumer(group: str, **extra) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            **extra,
        }
    )


def _commit(topic: str, group: str, offsets: dict[int, int]) -> None:
    """Set a group's committed position directly.

    Deliberately not "consume until done, then commit": poll-based consumption
    gives no guarantee about which partitions were reached before the loop ends,
    so the starting state would vary run to run. The operator does not care how
    the offsets got there.
    """
    consumer = _consumer(group)
    try:
        consumer.commit(
            offsets=[TopicPartition(topic, p, o) for p, o in sorted(offsets.items())],
            asynchronous=False,
        )
    finally:
        consumer.close()


def _committed(topic: str, group: str) -> dict[int, int]:
    consumer = _consumer(group)
    try:
        found = consumer.committed(
            [TopicPartition(topic, p) for p in range(PARTITIONS)], timeout=30
        )
        return {tp.partition: tp.offset for tp in found if tp.offset != OFFSET_INVALID}
    finally:
        consumer.close()


def _group_for(topic: str) -> str:
    return f"{topic}.group"


@pytest.mark.integration("kafka")
class TestResetConsumerGroupOffsetsIntegration:
    def test_seek_to_offset_moves_committed_position(self, topic):
        group = _group_for(topic)
        _produce(topic)
        _commit(topic, group, {p: PER_PARTITION for p in range(PARTITIONS)})
        assert _committed(topic, group) == {p: PER_PARTITION for p in range(PARTITIONS)}

        result = ResetConsumerGroupOffsetsOperator(
            task_id="rewind",
            group_id=group,
            topics=[topic],
            to_offset=2,
            dry_run=False,
            kafka_config_id=CONN_ID,
        ).execute(context={})

        assert _committed(topic, group) == {p: 2 for p in range(PARTITIONS)}
        assert result["dry_run"] is False
        assert sorted(result["committed"][topic]) == list(range(PARTITIONS))

        # The report the approver reads has to describe what actually happened,
        # not just be well formed. A diff that says "rewind by 4" while the
        # broker moved somewhere else is the failure mode worth catching.
        diff = result["diffs"][0]
        assert {p["partition"]: p["proposed"] for p in diff["partitions"]} == {
            p: 2 for p in range(PARTITIONS)
        }
        assert all(p["direction"] == "rewind" for p in diff["partitions"])
        assert all(p["delta"] == 2 - PER_PARTITION for p in diff["partitions"])

    def test_dry_run_leaves_committed_offsets_untouched(self, topic):
        """Read the committed offsets, run with dry_run=True, read again, assert
        equal. Against a live broker, because that is the only place the
        property is real -- a mocked assertion here would prove nothing."""
        group = _group_for(topic)
        _produce(topic)
        _commit(topic, group, {p: PER_PARTITION for p in range(PARTITIONS)})
        before = _committed(topic, group)

        result = ResetConsumerGroupOffsetsOperator(
            task_id="preview",
            group_id=group,
            topics=[topic],
            shift_by=-4,
            dry_run=True,
            kafka_config_id=CONN_ID,
        ).execute(context={})

        assert _committed(topic, group) == before, "dry_run=True moved the broker"
        assert result["dry_run"] is True
        assert result["committed"] == {}

        # Paired with the assertion above on purpose. "Nothing moved" is also
        # true of an operator that computed nothing at all, so the test has to
        # show it produced a real, non-trivial plan and still withheld it.
        proposed = {p["partition"]: p["proposed"] for p in result["diffs"][0]["partitions"]}
        assert proposed == {p: PER_PARTITION - 4 for p in range(PARTITIONS)}

    def test_refuses_a_group_with_live_members(self, topic):
        """Kafka rejects the commit anyway; the point is failing early with a
        message that names the problem instead of surfacing a broker error."""
        group = _group_for(topic)
        _produce(topic)
        _commit(topic, group, {p: PER_PARTITION for p in range(PARTITIONS)})
        before = _committed(topic, group)

        live = _consumer(group, **{"client.id": "integration-live-consumer"})
        try:
            live.subscribe([topic])
            # Poll until the rebalance completes. Without this the group is
            # still Empty when the operator looks, the pre-flight passes, and
            # the test proves nothing while appearing to.
            deadline = time.monotonic() + 60
            while not live.assignment() and time.monotonic() < deadline:
                live.poll(1.0)
            assert live.assignment(), "consumer never joined the group; cannot test the guard"

            with pytest.raises(AirflowException, match="live member"):
                ResetConsumerGroupOffsetsOperator(
                    task_id="rewind",
                    group_id=group,
                    topics=[topic],
                    to_offset=0,
                    dry_run=False,
                    kafka_config_id=CONN_ID,
                ).execute(context={})
        finally:
            live.close()

        assert _committed(topic, group) == before, "refused, but the offsets moved anyway"
