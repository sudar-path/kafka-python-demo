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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook

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
    :param to_timestamp: seek to the first offset at or after this time.
        Templated -- this is the parameter that makes backfill work.
    :param to_offset: seek every partition to this absolute offset.
    :param shift_by: move each partition by this delta, clamped to the
        watermarks.
    :param dry_run: compute and log the diff, commit nothing.
    :param kafka_config_id: the Airflow connection to use.
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
        dry_run: bool = False,
        kafka_config_id: str = "kafka_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.group_id = group_id
        self.topics = topics
        self.to_timestamp = to_timestamp
        self.to_offset = to_offset
        self.shift_by = shift_by
        self.dry_run = dry_run
        self.kafka_config_id = kafka_config_id

    def execute(self, context: Context) -> dict[str, Any]:
        # Validate in execute(), not __init__(): templated fields are not
        # rendered yet at construction time, so a check there would inspect the
        # literal "{{ ... }}" string and reject a perfectly valid DAG.
        modes = [m for m in (self.to_timestamp, self.to_offset, self.shift_by) if m is not None]
        if len(modes) != 1:
            raise ValueError("exactly one of to_timestamp, to_offset, shift_by is required")

        hook = KafkaAdminClientHook(kafka_config_id=self.kafka_config_id)

        # Share one code path with the CLI dry run (tools/offset_diff.py) so the
        # preview a human approves is the same computation this operator runs.
        # Reimplementing it here is how the two quietly diverge.
        raise NotImplementedError("call compute_diff(), then commit unless self.dry_run")
