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

import datetime as dt
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook

from tools.apply_offsets import apply_seek
from tools.offset_diff import compute_diff, render_table

if TYPE_CHECKING:
    from airflow.utils.context import Context


def _to_epoch_millis(value: Any) -> int:
    """Parse a target time to Kafka epoch millis.

    Accepts epoch millis (int or digit string) or an ISO-8601 timestamp.
    Naive datetimes are rejected: a missing timezone would silently seek
    to the wrong place.
    """
    if isinstance(value, bool):
        raise ValueError("to_timestamp must be epoch millis or an ISO-8601 timestamp")
    if isinstance(value, int):
        return value
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            raise ValueError(
                "to_timestamp has no timezone. Use an aware datetime "
                "(e.g. UTC) -- a naive local time would silently seek "
                "to the wrong place on a runner in another zone."
            )
        return int(value.timestamp() * 1000)

    raw = str(value).strip()
    if not raw:
        raise ValueError("to_timestamp is required")
    if raw.isdigit():
        return int(raw)
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            f"to_timestamp {value!r} is neither epoch millis nor ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError(
            f"to_timestamp {value!r} has no timezone. Use e.g. "
            f"'{value}Z' or '{value}+00:00'."
        )
    return int(parsed.timestamp() * 1000)


class ResetConsumerGroupOffsetsOperator(BaseOperator):
    """Preview or apply a consumer-group reset to a point in time.

        ResetConsumerGroupOffsetsOperator(
            task_id="rewind",
            group_id="etl_orders",
            topics=["orders"],
            to_timestamp="{{ data_interval_start }}",
            dry_run=True,
        )

    :param group_id: consumer group whose committed offsets are reset.
    :param topics: topic name or list of topic names to reset.
    :param to_timestamp: target time as epoch millis or ISO-8601 with a
        timezone. Templatable so a DAG author can pass
        ``{{ data_interval_start }}``.
    :param dry_run: when True (the default), compute and log the preview
        without writing offsets. When False, apply the computed seek.
    :param kafka_config_id: the Airflow connection to use.
    """

    template_fields: Sequence[str] = (
        "group_id",
        "topics",
        "to_timestamp",
        "dry_run",
        "kafka_config_id",
    )

    def __init__(
        self,
        *,
        group_id: str,
        topics: str | Sequence[str],
        to_timestamp: Any,
        dry_run: bool = True,
        kafka_config_id: str = "kafka_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.group_id = group_id
        self.topics = topics
        self.to_timestamp = to_timestamp
        self.dry_run = dry_run
        self.kafka_config_id = kafka_config_id

    def _topic_list(self) -> list[str]:
        if isinstance(self.topics, str):
            return [self.topics]
        return list(self.topics)

    def execute(self, context: Context) -> dict[str, Any]:
        # Validate in execute(), not __init__(): templated fields are not
        # rendered yet at construction time, so a check there would inspect
        # the literal "{{ ... }}" string and reject a valid DAG.
        if not self.group_id:
            raise ValueError("group_id is required")
        topic_list = self._topic_list()
        if not topic_list:
            raise ValueError("topics is required")
        millis = _to_epoch_millis(self.to_timestamp)

        hook = KafkaAdminClientHook(kafka_config_id=self.kafka_config_id)
        admin = hook.get_conn()

        previews: list[dict[str, Any]] = []
        for topic in topic_list:
            diff = compute_diff(
                admin,
                group=self.group_id,
                topic=topic,
                mode="to_timestamp",
                value=millis,
                value_raw=str(self.to_timestamp),
            )
            self.log.info("%s", render_table(diff))
            if not self.dry_run:
                apply_seek(admin, asdict(diff))
            previews.append(
                {
                    "topic": diff.topic,
                    "messages_replayed": diff.totals["messages_replayed"],
                    "messages_skipped": diff.totals["messages_skipped"],
                    "no_change": diff.no_change,
                }
            )

        return {
            "group_id": self.group_id,
            "to_timestamp": self.to_timestamp,
            "dry_run": self.dry_run,
            "previews": previews,
        }
