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

One directory off and pytest never collects it. The suite stays green and the
code is untested, which is the worst outcome available.
"""
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.apache.kafka.operators.reset_offsets import (
    ResetConsumerGroupOffsetsOperator,
)


@pytest.fixture
def operator():
    return ResetConsumerGroupOffsetsOperator(
        task_id="rewind",
        group_id="etl_orders",
        topics=["orders"],
        to_offset=100,
    )


class TestResetConsumerGroupOffsets:
    def test_exactly_one_mode_required(self):
        """Zero modes and two modes are both errors, and both are easy to write."""
        op = ResetConsumerGroupOffsetsOperator(
            task_id="t", group_id="g", topics=["x"]
        )
        with pytest.raises(ValueError, match="exactly one of"):
            op.execute({})

        op = ResetConsumerGroupOffsetsOperator(
            task_id="t", group_id="g", topics=["x"], to_offset=1, shift_by=1
        )
        with pytest.raises(ValueError, match="exactly one of"):
            op.execute({})

    def test_template_fields_cover_every_seek_mode(self, operator):
        """PROV-003 as a unit test as well as a lint rule.

        The lint rule proves `template_fields` exists. This proves the right
        things are IN it -- specifically to_timestamp, without which
        `{{ data_interval_start }}` arrives as a literal string and the backfill
        story silently does not work.
        """
        for field in ("group_id", "topics", "to_timestamp", "to_offset", "shift_by"):
            assert field in operator.template_fields

    def test_dry_run_commits_nothing(self, operator):
        """The safety property, asserted in code rather than in a docstring.

        This test exists because this project already shipped a tool documented
        as read-only that created topics on a live broker. A mock that records
        calls is the cheapest possible guard against repeating that.
        """
        operator.dry_run = True
        with mock.patch(
            "airflow.providers.apache.kafka.operators.reset_offsets.KafkaAdminClientHook"
        ) as hook_cls:
            admin = hook_cls.return_value.get_conn
            operator.execute({})
        assert not any(
            "commit" in str(call) or "alter" in str(call)
            for call in admin.mock_calls
        ), "dry_run must not issue a mutating call"
