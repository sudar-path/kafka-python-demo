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

TEST-002: a test that creates a topic must delete it. Do NOT copy the
teardown style from neighbouring operator tests that create topics and
abandon them.
"""
from __future__ import annotations

import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer

from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.providers.apache.kafka.operators.reset_offsets import (
    ResetConsumerGroupOffsetsOperator,
)
from tools.apply_offsets import read_committed


@pytest.fixture
def hook():
    return KafkaAdminClientHook(kafka_config_id="kafka_default")


@pytest.fixture
def bootstrap(hook):
    return hook.get_connection("kafka_default").extra_dejson["bootstrap.servers"]


@pytest.fixture
def topic(hook):
    """Unique name plus guaranteed teardown.

    The uuid suffix is what makes concurrent runs safe; the `yield` is
    what makes cleanup run even when the test fails.
    """
    name = f"operator.reset.test.integration.{uuid4().hex[:8]}"
    hook.create_topic(topics=[(name, 3, 1)])
    yield name
    hook.delete_topic(topics=[name])


def _seed_committed_offsets(bootstrap: str, topic: str, group: str, n_messages: int = 6) -> None:
    producer = Producer({"bootstrap.servers": bootstrap})
    for i in range(n_messages):
        producer.produce(topic, str(i).encode())
    producer.flush(10)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    seen = 0
    deadline = time.time() + 20
    try:
        while seen < n_messages and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            seen += 1
        if seen == 0:
            raise RuntimeError(f"seeded no messages on {topic!r}")
        consumer.commit(asynchronous=False)
    finally:
        consumer.close()


class TestResetConsumerGroupOffsetsOperatorIntegration:
    def test_dry_run_does_not_mutate_committed_offsets(self, hook, bootstrap, topic):
        group = f"operator.reset.test.group.{uuid4().hex[:8]}"
        _seed_committed_offsets(bootstrap, topic, group)
        admin = hook.get_conn
        before = read_committed(admin, group, topic, timeout=10.0)
        assert before

        ResetConsumerGroupOffsetsOperator(
            task_id="preview",
            group_id=group,
            topics=[topic],
            to_timestamp="1970-01-01T00:00:00+00:00",
            dry_run=True,
        ).execute({})

        after = read_committed(admin, group, topic, timeout=10.0)
        assert after == before

    def test_timestamp_reset_applies_when_not_dry_run(self, hook, bootstrap, topic):
        group = f"operator.reset.test.group.{uuid4().hex[:8]}"
        _seed_committed_offsets(bootstrap, topic, group)
        admin = hook.get_conn
        before = read_committed(admin, group, topic, timeout=10.0)
        assert before

        ResetConsumerGroupOffsetsOperator(
            task_id="apply",
            group_id=group,
            topics=[topic],
            to_timestamp="1970-01-01T00:00:00+00:00",
            dry_run=False,
        ).execute({})

        after = read_committed(admin, group, topic, timeout=10.0)
        assert after != before
