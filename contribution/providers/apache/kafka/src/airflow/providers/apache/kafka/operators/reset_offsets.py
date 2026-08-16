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
from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook

# PACKAGING SEAM -- read this before upstreaming.
#
# The seek arithmetic lives in tools/offset_diff.py because the CLI dry run and
# this operator must produce byte-identical previews: the artifact a human
# approves is the artifact the apply step consumes, and a second implementation
# here is how the two silently diverge. That is the right call for this repo and
# the wrong shape for a PR -- `tools/` is not importable from an installed
# provider.
#
# Upstreaming moves both functions into
# airflow/providers/apache/kafka/hooks/offsets.py and leaves tools/ importing
# THEM, not the reverse. Doing it in that order keeps one code path throughout;
# doing it by copy-paste creates the divergence this note exists to prevent.
from tools.apply_offsets import apply_seek
from tools.offset_diff import _parse_timestamp, compute_diff, render_table

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ResetConsumerGroupOffsetsOperator(BaseOperator):
    """Move a Kafka consumer group's committed offsets to a chosen position.

    Fills the gap the provider leaves: the existing operators can only move a
    group forward, and none of them can tell you where it currently is. With
    this, a Kafka-backed pipeline becomes backfillable from Airflow::

        ResetConsumerGroupOffsetsOperator(
            task_id="rewind",
            group_id="etl_orders",
            topics=["orders"],
            to_timestamp="{{ data_interval_start }}",
        )

    :param group_id: consumer group to move. Must be ``Empty`` -- Kafka refuses
        the commit if any member is live, and so does this operator, up front,
        with a clearer message.
    :param topics: topics whose partitions are moved.
    :param to_timestamp: seek to the first offset at or after this time. Epoch
        millis or ISO-8601 **with a timezone**. Templated -- this is the
        parameter that makes backfill work.
    :param to_offset: seek every partition to this absolute offset.
    :param shift_by: move each partition by this delta, clamped to the
        watermarks.
    :param partitions: restrict to these partitions. Default: every partition
        the topic has, including ones the group has never committed to.
    :param dry_run: compute and log the diff, commit nothing.
    :param kafka_config_id: the Airflow connection to use.
    :param timeout: per-request broker timeout, seconds.
    """

    # PROV-003. Every parameter a DAG author might reasonably template must be
    # listed. Omitting one does not raise -- it silently passes the literal
    # "{{ ... }}" string through, which is worse than a failure.
    template_fields: Sequence[str] = (
        "group_id",
        "topics",
        "to_timestamp",
        "to_offset",
        "shift_by",
        "kafka_config_id",
    )
    ui_color = "#e8f5e9"

    def __init__(
        self,
        *,
        group_id: str,
        topics: Sequence[str],
        to_timestamp: str | None = None,
        to_offset: int | None = None,
        shift_by: int | None = None,
        partitions: list[int] | None = None,
        dry_run: bool = False,
        kafka_config_id: str = "kafka_default",
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.group_id = group_id
        self.topics = topics
        self.to_timestamp = to_timestamp
        self.to_offset = to_offset
        self.shift_by = shift_by
        self.partitions = partitions
        self.dry_run = dry_run
        self.kafka_config_id = kafka_config_id
        self.timeout = timeout

    def _resolve_mode(self) -> tuple[str, int]:
        """Pick the seek mode and coerce its value.

        Called from execute(), not __init__(): templated fields are not rendered
        at construction time, so a check there would inspect the literal
        "{{ ... }}" string and reject a valid DAG.
        """
        chosen = [
            (name, value)
            for name, value in (
                ("to_timestamp", self.to_timestamp),
                ("to_offset", self.to_offset),
                ("shift_by", self.shift_by),
            )
            if value is not None
        ]
        if len(chosen) != 1:
            got = ", ".join(name for name, _ in chosen) or "none"
            raise ValueError(
                f"exactly one of to_timestamp, to_offset, shift_by is required (got: {got})"
            )

        mode, raw = chosen[0]
        if mode == "to_timestamp":
            # Templating renders to a string; a hand-written DAG may pass an
            # int. _parse_timestamp refuses a naive datetime rather than
            # guessing a zone -- an off-by-hours seek is silent and expensive.
            return mode, _parse_timestamp(str(raw))
        return mode, int(raw)

    def _require_empty_group(self, admin: Any) -> None:
        """Refuse a group that still has live members.

        Kafka rejects the commit anyway. The value here is the error message:
        the broker's is a bare GROUP_SUBSCRIBED_TO_TOPIC / non-empty state code,
        and the actual fix -- pause the consumers -- is not in it.
        """
        future = next(iter(admin.describe_consumer_groups([self.group_id]).values()))
        members = getattr(future.result(), "members", [])
        if members:
            ids = ", ".join(sorted(getattr(m, "client_id", "?") for m in members)) or "?"
            raise AirflowException(
                f"consumer group {self.group_id!r} has {len(members)} live member(s) "
                f"({ids}); Kafka will not accept an offset commit for an active "
                f"group. Stop the consumers, then re-run."
            )

    def execute(self, context: Context) -> dict[str, Any]:
        mode, value = self._resolve_mode()

        hook = KafkaAdminClientHook(kafka_config_id=self.kafka_config_id)
        admin = hook.get_conn
        bootstrap = str(hook.get_connection(self.kafka_config_id).extra_dejson.get(
            "bootstrap.servers", ""
        ))

        self._require_empty_group(admin)

        diffs: list[dict[str, Any]] = []
        for topic in self.topics:
            # compute_diff() is a CLI function and reports fatal input errors by
            # calling sys.exit(). Inside a worker that would tear the process
            # down past Airflow's error handling: the task ends up "failed" with
            # no message, no traceback, and nothing in the task log to act on.
            # Translate it here. Removing this once the code moves into the
            # provider package is part of the upstreaming work noted at the top.
            try:
                diff = compute_diff(
                    admin,
                    group=self.group_id,
                    topic=topic,
                    mode=mode,
                    value=value,
                    value_raw=str(self.to_timestamp) if mode == "to_timestamp" else None,
                    partitions=self.partitions,
                    bootstrap=bootstrap,
                    timeout=self.timeout,
                )
            except SystemExit as exc:
                raise AirflowException(
                    f"offset diff failed for topic {topic!r} (group {self.group_id!r}): {exc}"
                ) from exc

            self.log.info("offset diff for %s:\n%s", topic, render_table(diff))
            for warning in diff.warnings:
                self.log.warning("%s: %s", topic, warning)
            diffs.append(asdict(diff))

        if self.dry_run:
            self.log.info(
                "dry_run=True: %d topic(s) diffed, nothing committed", len(diffs)
            )
            return {"dry_run": True, "committed": {}, "diffs": diffs}

        committed: dict[str, list[int]] = {}
        for diff in diffs:
            # force=False on purpose. apply_seek re-reads the broker and refuses
            # if the group moved since the diff was computed -- here that window
            # is milliseconds, but the check costs one request and the failure
            # it prevents is overwriting offsets nobody reviewed.
            try:
                written = apply_seek(admin, diff, force=False, timeout=self.timeout)
            except RuntimeError as exc:
                raise AirflowException(str(exc)) from exc
            committed[diff["topic"]] = [tp.partition for tp in written]
            self.log.info(
                "committed %d partition(s) on %s", len(written), diff["topic"]
            )

        return {"dry_run": False, "committed": committed, "diffs": diffs}
