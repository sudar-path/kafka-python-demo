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
"""Integration tests -- these hit a real broker.

TEST-002: a test that creates a topic must delete it. Do NOT copy the teardown
style from the neighbouring operator and trigger tests: they create topics and
abandon them, and on a shared broker the leftovers accumulate until runs start
interfering with each other. Those tests only stay green because the leaked
topics happen to be idempotent against their own leftovers. That is luck.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook


@pytest.fixture
def hook():
    return KafkaAdminClientHook(kafka_config_id="kafka_default")


@pytest.fixture
def topic(hook):
    """Unique name plus guaranteed teardown.

    The uuid suffix is what makes concurrent runs safe; the `yield` is what
    makes cleanup run even when the test fails. A `delete_topic` at the end of
    the test body only cleans up on the happy path, which is precisely when
    cleanup matters least.
    """
    name = f"operator.reset.test.integration.{uuid4().hex[:8]}"
    hook.create_topic(topics=[(name, 3, 1)])
    yield name
    hook.delete_topic(topics=[name])


class TestResetConsumerGroupOffsetsIntegration:
    def test_seek_to_offset_moves_committed_position(self, hook, topic):
        # Arrange: produce, consume, commit -- then assert the group moved.
        raise NotImplementedError

    def test_dry_run_leaves_committed_offsets_untouched(self, hook, topic):
        """Read the committed offsets, run with dry_run=True, read again, assert
        equal. Against a live broker, because that is the only place the
        property is real -- a mocked assertion here would prove nothing."""
        raise NotImplementedError

    def test_refuses_a_group_with_live_members(self, hook, topic):
        """Kafka rejects the commit anyway; the point is failing early with a
        message that names the problem instead of surfacing a broker error."""
        raise NotImplementedError
