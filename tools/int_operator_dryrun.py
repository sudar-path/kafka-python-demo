#!/usr/bin/env python3
"""Run ResetConsumerGroupOffsetsOperator against the INT broker, dry_run=True.

This is the claim the unit tests cannot make. `test_dry_run_commits_nothing`
asserts that a patched apply_seek was not called -- which proves the operator's
control flow and nothing about Kafka. The property that matters to whoever
approves the change is "the committed offsets on the broker are identical
afterwards", and that is only observable against a real broker.

So: read the committed offsets, run the operator, read them again, diff.

Runs ON the INT VM (it talks to localhost:9092), not from a laptop:
    PYTHONPATH=~/crsr-demo AIRFLOW_HOME=~/airflow-home \
      ~/airflow/.venv/bin/python tools/int_operator_dryrun.py

NEVER sets dry_run=False. Committing offsets is apply_offsets.py's job, driven
by the promotion workflow after a human approves the artifact. A script that
can mutate a broker should not be one SSH command away from a chat window.
"""
from __future__ import annotations

import json
import os
import sys

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")
GROUP = os.environ.get("GROUP", "payments-reconciler")
TOPIC = os.environ.get("TOPIC", "payments.events")

# The connection the hook resolves. Set before importing airflow so the env
# backend picks it up.
os.environ.setdefault(
    "AIRFLOW_CONN_KAFKA_DEFAULT",
    json.dumps({"conn_type": "kafka", "extra": {"bootstrap.servers": BOOTSTRAP}}),
)

from confluent_kafka import OFFSET_INVALID, ConsumerGroupTopicPartitions  # noqa: E402
from confluent_kafka.admin import AdminClient  # noqa: E402

from airflow.providers.apache.kafka.operators.reset_offsets import (  # noqa: E402
    ResetConsumerGroupOffsetsOperator,
)


def committed(admin: AdminClient) -> dict[int, int]:
    futures = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(GROUP)], request_timeout=10
    )
    result = next(iter(futures.values())).result()
    return {
        tp.partition: tp.offset
        for tp in result.topic_partitions
        if tp.topic == TOPIC and tp.offset != OFFSET_INVALID
    }


def main() -> int:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    before = committed(admin)
    print(f"committed BEFORE: {before}")
    if not before:
        print(f"group {GROUP!r} has no committed offsets on {TOPIC!r}; nothing to show")
        return 2

    mode = sys.argv[1] if len(sys.argv) > 1 else "shift_by"
    kwargs: dict = {"shift_by": -10}
    if mode == "to_timestamp":
        # The backfill case: what a DAG author writes as
        # to_timestamp="{{ data_interval_start }}". Passed as a rendered value
        # here because there is no scheduler in this harness.
        kwargs = {"to_timestamp": os.environ.get("TS", "2026-08-15T00:00:00+00:00")}
    elif mode == "to_offset":
        kwargs = {"to_offset": 0}

    op = ResetConsumerGroupOffsetsOperator(
        task_id="rewind",
        group_id=GROUP,
        topics=[TOPIC],
        dry_run=True,  # never flip this here -- see the module docstring
        **kwargs,
    )
    print(f"\nrunning execute(dry_run=True) with {kwargs}\n")
    result = op.execute(context={})

    after = committed(admin)
    print(f"\ncommitted AFTER : {after}")

    if before != after:
        print("\nFAIL: dry_run=True CHANGED committed offsets")
        return 1

    diff = result["diffs"][0]
    print("\nunchanged -- dry run held. proposed moves it would have made:")
    for p in diff["partitions"]:
        print(
            f"  p{p['partition']}: {p['current']} -> {p['proposed']} "
            f"(delta {p['delta']}, {p['direction']}, lag {p['lag_before']} -> {p['lag_after']})"
        )
    for w in diff.get("warnings", []):
        print(f"  warning: {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
