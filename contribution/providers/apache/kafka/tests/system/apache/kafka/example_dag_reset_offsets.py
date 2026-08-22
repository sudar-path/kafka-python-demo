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

import datetime

from airflow import DAG
from airflow.providers.apache.kafka.operators.reset_offsets import (
    ResetConsumerGroupOffsetsOperator,
)

with DAG(
    dag_id="example_kafka_reset_offsets",
    start_date=datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
    schedule=None,
    catchup=False,
    tags=["example", "kafka"],
) as dag:
    ResetConsumerGroupOffsetsOperator(
        task_id="preview_reset_to_interval_start",
        group_id="example_group",
        topics=["example_topic"],
        to_timestamp="{{ data_interval_start }}",
        dry_run=True,
    )
