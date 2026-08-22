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
"""Unit tests. TEST-001: this file must live at the mirrored path

    src/airflow/providers/apache/kafka/operators/reset_offsets.py
    -> tests/unit/apache/kafka/operators/test_reset_offsets.py
"""
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.apache.kafka.operators.reset_offsets import (
    ResetConsumerGroupOffsetsOperator,
    _to_epoch_millis,
)
from tools.offset_diff import OffsetDiff, PartitionDiff


def _sample_diff(topic: str = "orders") -> OffsetDiff:
    return OffsetDiff(
        schema_version=1,
        generated_at="2026-08-22T00:00:00+00:00",
        group="etl_orders",
        topic=topic,
        bootstrap="",
        mode={"kind": "to_timestamp", "value": 0, "value_raw": "1970-01-01T00:00:00+00:00"},
        partitions=[
            PartitionDiff(
                partition=0,
                earliest=0,
                latest=10,
                current=10,
                proposed=0,
                delta=-10,
                lag_before=0,
                lag_after=10,
                direction="rewind",
            )
        ],
        totals={
            "messages_replayed": 10,
            "messages_skipped": 0,
            "lag_before": 0,
            "lag_after": 10,
        },
    )


@pytest.fixture
def operator():
    return ResetConsumerGroupOffsetsOperator(
        task_id="rewind",
        group_id="etl_orders",
        topics=["orders"],
        to_timestamp="2026-08-15T00:00:00+00:00",
    )


class TestResetConsumerGroupOffsetsOperator:
    def test_constructs_with_required_args(self, operator):
        assert operator.group_id == "etl_orders"
        assert operator.topics == ["orders"]
        assert operator.to_timestamp == "2026-08-15T00:00:00+00:00"
        assert operator.dry_run is True
        assert operator.kafka_config_id == "kafka_default"

    def test_template_fields_cover_templatable_args(self, operator):
        """PROV-003 as a unit test as well as a lint rule.

        The lint rule proves ``template_fields`` exists. This proves the
        right things are IN it -- specifically every constructor argument
        a DAG author might template. Leave ``to_timestamp`` out and
        ``{{ data_interval_start }}`` arrives as a literal string.
        """
        for field in ("group_id", "topics", "to_timestamp", "dry_run", "kafka_config_id"):
            assert field in operator.template_fields

    def test_execute_goes_through_the_hook(self, operator):
        """Patch the hook where the operator imports it, not at the hooks
        package. A patch one module off never intercepts the call.
        """
        with mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.KafkaAdminClientHook"
        ) as hook_cls, mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.compute_diff",
            return_value=_sample_diff(),
        ), mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.apply_seek"
        ):
            operator.execute({})
        hook_cls.assert_called_once_with(kafka_config_id="kafka_default")
        hook_cls.return_value.get_conn.assert_called_once()

    def test_dry_run_does_not_apply(self, operator):
        with mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.KafkaAdminClientHook"
        ), mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.compute_diff",
            return_value=_sample_diff(),
        ) as compute, mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.apply_seek"
        ) as apply:
            result = operator.execute({})
        compute.assert_called_once()
        assert compute.call_args.kwargs["mode"] == "to_timestamp"
        apply.assert_not_called()
        assert result["dry_run"] is True

    def test_apply_when_dry_run_is_false(self, operator):
        operator.dry_run = False
        diff = _sample_diff()
        with mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.KafkaAdminClientHook"
        ) as hook_cls, mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.compute_diff",
            return_value=diff,
        ), mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.apply_seek"
        ) as apply:
            operator.execute({})
        apply.assert_called_once()
        admin = hook_cls.return_value.get_conn.return_value
        assert apply.call_args.args[0] is admin
        assert apply.call_args.args[1]["topic"] == "orders"
        assert apply.call_args.args[1]["group"] == "etl_orders"


class TestToEpochMillis:
    def test_iso_timestamp(self):
        assert _to_epoch_millis("2026-08-15T00:00:00+00:00") == 1786752000000

    def test_epoch_millis_int(self):
        assert _to_epoch_millis(1786752000000) == 1786752000000

    def test_rejects_naive_datetime_string(self):
        with pytest.raises(ValueError, match="timezone"):
            _to_epoch_millis("2026-08-15T00:00:00")
