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
"""System test / example DAG for ResetConsumerGroupOffsetsOperator.

Touchpoint 7. It is also the only place the operator's central claim is
actually exercised end to end.

The premise of the whole change is that a Kafka pipeline becomes backfillable
because you can write ``to_timestamp="{{ data_interval_start }}"`` and let
Airflow render the window. Nothing else in the test suite proves that renders:
the unit tests construct the operator directly and the integration tests pass a
literal timestamp, so both would still pass if `to_timestamp` had been left out
of ``template_fields`` and the operator received the string
``"{{ data_interval_start }}"`` verbatim. Only a real DagRun renders templates,
and STAG is the only environment here running a real Airflow.

``dry_run=True`` is not a placeholder to be edited out. An example DAG is the
thing people copy, and the copy inherits whatever this says. A version that
committed offsets by default would be a data-loss footgun distributed as
documentation. Flipping it is a deliberate act, done in the copy, after reading
the diff the dry run prints.
"""

from __future__ import annotations

import datetime
import os

from airflow.providers.apache.kafka.operators.reset_offsets import (
    ResetConsumerGroupOffsetsOperator,
)
from airflow.sdk import DAG

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_kafka_reset_offsets"

GROUP_ID = os.environ.get("KAFKA_RESET_GROUP", "payments-reconciler")
TOPIC = os.environ.get("KAFKA_RESET_TOPIC", "payments.events")

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "kafka"],
) as dag:
    # [START howto_operator_reset_consumer_group_offsets]
    preview_rewind_to_interval_start = ResetConsumerGroupOffsetsOperator(
        task_id="preview_rewind_to_interval_start",
        kafka_config_id="kafka_default",
        group_id=GROUP_ID,
        topics=[TOPIC],
        # The point of the operator. Rendered per run, so a cleared task or a
        # backfill rewinds to that run's own window rather than to a fixed
        # wall-clock time baked in when the DAG was written.
        to_timestamp="{{ data_interval_start }}",
        dry_run=True,
    )
    # [END howto_operator_reset_consumer_group_offsets]


# UPSTREAMING DELTA -- upstream example DAGs import this unconditionally:
#
#     from tests_common.test_utils.system_tests import get_test_run
#     test_run = get_test_run(dag)
#
# which is correct there, because those DAGs are only ever parsed inside the
# airflow repo where tests_common is importable. This one is also deployed into
# a real Airflow on STAG to prove the templating renders, and that image has no
# tests_common -- an unguarded import makes the DAG un-parseable and the DagBag
# reports an import error instead of a test result.
#
# Guarded rather than dropped, so `pytest tests/system` still picks it up
# upstream. Worth flagging in the PR description; a reviewer should see this.
try:
    from tests_common.test_utils.system_tests import get_test_run
except ImportError:
    pass
else:
    test_run = get_test_run(dag)
