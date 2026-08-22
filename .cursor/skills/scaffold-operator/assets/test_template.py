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

    src/airflow/providers/apache/kafka/operators/example_operator.py
    -> tests/unit/apache/kafka/operators/test_example_operator.py

One directory off and pytest never collects it. The suite stays green and the
code is untested, which is the worst outcome available.
"""
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.apache.kafka.operators.example_operator import (
    ExampleOperator,
)


@pytest.fixture
def operator():
    return ExampleOperator(
        task_id="example",
        resource_id="demo-resource",
        target="demo-target",
    )


class TestExampleOperator:
    def test_constructs_with_required_args(self, operator):
        assert operator.resource_id == "demo-resource"
        assert operator.target == "demo-target"

    def test_template_fields_cover_templatable_args(self, operator):
        """PROV-003 as a unit test as well as a lint rule.

        The lint rule proves ``template_fields`` exists. This proves the right
        things are IN it -- specifically every constructor argument a DAG
        author might template. Leave one out and ``{{ ds }}`` arrives as a
        literal string; nothing errors, and the task does the wrong thing.
        """
        for field in ("resource_id", "target", "kafka_config_id"):
            assert field in operator.template_fields

    def test_execute_goes_through_the_hook(self, operator):
        """Patch the hook where the operator imports it, not at the hooks
        package. A patch one module off never intercepts the call.
        """
        with mock.patch(
            "airflow.providers.apache.kafka.operators.example_operator.KafkaAdminClientHook"
        ) as hook_cls:
            with pytest.raises(NotImplementedError):
                operator.execute({})
        hook_cls.assert_called_once_with(kafka_config_id="kafka_default")
