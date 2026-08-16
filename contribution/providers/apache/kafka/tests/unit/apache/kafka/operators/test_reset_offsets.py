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
"""Unit tests for ResetConsumerGroupOffsetsOperator.

Scope: the operator's control flow -- mode validation, the empty-group
pre-flight, and whether dry_run actually withholds the commit. NOT the seek
arithmetic, which is covered against a real broker by the tools/ suite; asserting
it a second time here against a fake would only prove the fake agrees with
itself.

`compute_diff` and `apply_seek` are patched in the operator's namespace rather
than mocked at the confluent_kafka layer. Faking an AdminClient well enough to
drive the real compute_diff means reimplementing broker semantics in the test,
and the fake's bugs then read as passing tests.

Requires airflow + confluent-kafka importable. Runs in CI and on the INT VM;
does not run on a laptop without the provider installed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.kafka.operators import reset_offsets as mod
from airflow.providers.apache.kafka.operators.reset_offsets import (
    ResetConsumerGroupOffsetsOperator,
)

GROUP = "etl_orders"
TOPIC = "orders"


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class _Member:
    client_id: str


@dataclass
class _GroupDescription:
    members: list[_Member] = field(default_factory=list)


class _Future:
    def __init__(self, value: Any) -> None:
        self._value = value

    def result(self) -> Any:
        return self._value


class FakeAdmin:
    """Only the surface the operator itself touches."""

    def __init__(self, members: list[str] | None = None) -> None:
        self.members = [_Member(c) for c in (members or [])]

    def describe_consumer_groups(self, groups: list[str]) -> dict[str, _Future]:
        return {g: _Future(_GroupDescription(self.members)) for g in groups}


class FakeHook:
    def __init__(self, admin: FakeAdmin) -> None:
        self.get_conn = admin

    def get_connection(self, conn_id: str) -> Any:
        return type("Conn", (), {"extra_dejson": {"bootstrap.servers": "broker:9092"}})()


@dataclass
class FakeDiff:
    """Shaped like offset_diff.OffsetDiff for asdict()/render_table()."""

    topic: str = TOPIC
    group: str = GROUP
    warnings: list[str] = field(default_factory=list)
    partitions: list[dict] = field(default_factory=list)


@pytest.fixture
def admin() -> FakeAdmin:
    return FakeAdmin()


@pytest.fixture
def wired(monkeypatch, admin):
    """Patch the operator's collaborators; record every apply_seek call."""
    applied: list[dict] = []

    monkeypatch.setattr(mod, "KafkaAdminClientHook", lambda **kw: FakeHook(admin))
    monkeypatch.setattr(mod, "compute_diff", lambda *a, **kw: FakeDiff(topic=kw["topic"]))
    monkeypatch.setattr(mod, "render_table", lambda diff: "<table>")
    monkeypatch.setattr(
        mod, "apply_seek", lambda a, diff, **kw: applied.append(diff) or []
    )
    return applied


def build(**overrides: Any) -> ResetConsumerGroupOffsetsOperator:
    kwargs: dict[str, Any] = {
        "task_id": "reset",
        "group_id": GROUP,
        "topics": [TOPIC],
        "to_offset": 100,
    }
    kwargs.update(overrides)
    return ResetConsumerGroupOffsetsOperator(**kwargs)


# --------------------------------------------------------------------------
# Mode validation
# --------------------------------------------------------------------------


class TestModeValidation:
    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({}, id="none"),
            pytest.param({"to_offset": 1, "shift_by": 2}, id="two"),
            pytest.param(
                {"to_offset": 1, "shift_by": 2, "to_timestamp": "2026-01-01T00:00:00Z"},
                id="three",
            ),
        ],
    )
    def test_exactly_one_mode_required(self, kwargs):
        # Merged, not passed alongside: build() defaults to_offset=100, and
        # `build(to_offset=None, **kwargs)` is a TypeError the moment kwargs
        # also names to_offset. Clearing the default has to happen first.
        op = build(**{"to_offset": None, **kwargs})
        with pytest.raises(ValueError, match="exactly one of"):
            op._resolve_mode()

    def test_validation_happens_in_execute_not_init(self):
        """Templated fields are unrendered at construction.

        Constructing with an unrendered Jinja string in every mode field must
        not raise -- at that point they are all non-None strings, and a check in
        __init__ would reject a DAG that is perfectly valid once rendered.
        """
        build(to_offset="{{ params.offset }}", shift_by="{{ params.shift }}")

    def test_shift_by_zero_is_a_mode_not_an_absence(self):
        """0 is falsy and a legitimate no-op seek. `if self.shift_by` would
        silently treat it as unset and then fail with 'exactly one of'."""
        mode, value = build(to_offset=None, shift_by=0)._resolve_mode()
        assert (mode, value) == ("shift_by", 0)

    def test_naive_timestamp_is_refused(self):
        """A naive local time seeks to the wrong place on a runner in another
        timezone, and does it silently. offset_diff refuses rather than guess."""
        op = build(to_offset=None, to_timestamp="2026-01-01T00:00:00")
        with pytest.raises(SystemExit):
            op._resolve_mode()

    def test_iso_timestamp_becomes_epoch_millis(self):
        mode, value = build(
            to_offset=None, to_timestamp="2026-01-01T00:00:00+00:00"
        )._resolve_mode()
        assert mode == "to_timestamp"
        assert value == 1767225600000


# --------------------------------------------------------------------------
# Contract with the templating layer
# --------------------------------------------------------------------------


def test_template_fields_cover_every_seek_mode():
    """PROV-003, as a test rather than a lint pass.

    A seek parameter missing from template_fields does not raise -- it passes
    the literal "{{ data_interval_start }}" through to the broker. This is
    derived from the signature, so a mode added later fails here without anyone
    remembering to update the list.
    """
    import inspect

    signature = inspect.signature(ResetConsumerGroupOffsetsOperator.__init__)
    seek_params = {
        name
        for name in signature.parameters
        if name.startswith(("to_", "shift_")) or name in {"group_id", "topics"}
    }
    missing = seek_params - set(ResetConsumerGroupOffsetsOperator.template_fields)
    assert not missing, f"not templated, will pass Jinja through literally: {sorted(missing)}"


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------


class TestExecute:
    def test_dry_run_commits_nothing(self, wired):
        result = build(dry_run=True).execute(context={})
        assert wired == [], "dry_run reached apply_seek"
        assert result["dry_run"] is True
        assert result["committed"] == {}
        assert len(result["diffs"]) == 1

    def test_non_dry_run_commits_every_topic(self, wired):
        result = build(topics=["orders", "returns"]).execute(context={})
        assert [d["topic"] for d in wired] == ["orders", "returns"]
        assert result["dry_run"] is False
        assert set(result["committed"]) == {"orders", "returns"}

    def test_live_members_refused_before_any_diff(self, monkeypatch, wired):
        """The pre-flight must run before compute_diff, not after: the point is
        a message naming the fix, and a broker error thrown mid-loop buries it."""
        monkeypatch.setattr(
            mod, "KafkaAdminClientHook", lambda **kw: FakeHook(FakeAdmin(["consumer-1"]))
        )
        seen: list[str] = []
        monkeypatch.setattr(
            mod, "compute_diff", lambda *a, **kw: seen.append(kw["topic"]) or FakeDiff()
        )

        with pytest.raises(AirflowException, match="live member"):
            build().execute(context={})
        assert seen == [], "diffed a topic before checking the group was empty"

    def test_cli_exit_becomes_airflow_exception(self, monkeypatch, wired):
        """compute_diff is a CLI function that reports fatal errors with
        sys.exit(). Unhandled, that kills the worker past Airflow's error
        handling and the task fails with an empty log."""

        def _exits(*a, **kw):
            raise SystemExit("topic 'orders' not found on broker:9092")

        monkeypatch.setattr(mod, "compute_diff", _exits)
        with pytest.raises(AirflowException, match="not found"):
            build().execute(context={})

    def test_stale_approval_surfaces_as_airflow_exception(self, monkeypatch, wired):
        """apply_seek raises RuntimeError when the group moved since the diff.
        That is 'the approval is stale', not a transient failure -- it must not
        be retried into overwriting unreviewed offsets."""

        def _drifted(*a, **kw):
            raise RuntimeError("group 'etl_orders' moved since the dry run was approved")

        monkeypatch.setattr(mod, "apply_seek", _drifted)
        with pytest.raises(AirflowException, match="moved since"):
            build().execute(context={})
