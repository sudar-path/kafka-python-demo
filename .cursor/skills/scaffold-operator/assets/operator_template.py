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


class ExampleOperator(BaseOperator):
    """Skeleton for a Kafka provider operator.

    Replace this class, its parameters, and ``execute()`` with the behaviour
    the issue asks for. The surrounding structure -- licence header, future
    annotations, ``template_fields``, constructor, ``super()`` -- is what the
    static rules check.

        ExampleOperator(
            task_id="example",
            resource_id="demo-resource",
            target="{{ ds }}",
        )

    :param resource_id: identifier the operator acts on.
    :param target: value a DAG author might template.
    :param kafka_config_id: the Airflow connection to use.
    """

    # PROV-003. Every parameter a DAG author might reasonably template must be
    # listed. Omitting one does not raise -- it silently passes the literal
    # "{{ ... }}" string through, which is worse than a failure.
    template_fields: Sequence[str] = (
        "resource_id",
        "target",
        "kafka_config_id",
    )
    ui_color = "#e8f5e9"

    def __init__(
        self,
        *,
        resource_id: str,
        target: str,
        kafka_config_id: str = "kafka_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_id = resource_id
        self.target = target
        self.kafka_config_id = kafka_config_id

    def execute(self, context: Context) -> dict[str, Any]:
        # Validate in execute(), not __init__(): templated fields are not
        # rendered yet at construction time, so a check there would inspect the
        # literal "{{ ... }}" string and reject a perfectly valid DAG.
        hook = KafkaAdminClientHook(kafka_config_id=self.kafka_config_id)
        raise NotImplementedError("implement the operator body using hook")
