#!/usr/bin/env python3
"""Proof that the seek arithmetic is right, against a fake broker.

Run:  python tools/test_offset_diff.py             (stdlib unittest, no pytest)

compute_diff() is the most consequential function in this repo and until now it
had zero tests. Everything downstream trusts it: the table a human reads before
approving, the policy check that decides whether the promotion is allowed, the
artifact apply_offsets.py replays onto a production broker, and the operator's
dry run. A wrong number here is wrong in all four places and looks authoritative
in every one of them.

The integration tests cover it against a real broker, but only through the happy
paths a broker will actually produce on demand. The cases worth the most here are
the ones that are awkward to arrange for real and easy to get wrong in code:

  * a partition the group has never committed to
  * a timestamp older than every message in the partition
  * a proposed offset outside [earliest, latest]
  * replayed vs skipped totals when partitions move in opposite directions

The fake is deliberately dumb -- it returns what it was constructed with and
records what it was asked. It is not a Kafka simulator, and where a behaviour
depends on real broker semantics the assertion belongs in the integration suite
instead.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import OFFSET_INVALID, TopicPartition  # noqa: E402

from tools.offset_diff import _parse_timestamp, compute_diff  # noqa: E402

GROUP = "etl_orders"
TOPIC = "orders"


# ---------------------------------------------------------------------------
# Fake broker
# ---------------------------------------------------------------------------


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self):
        return self._value


class _Offset:
    def __init__(self, offset):
        self.offset = offset


class _TopicMeta:
    def __init__(self, partitions, error=None):
        self.partitions = {p: object() for p in partitions}
        self.error = error


class _ClusterMeta:
    def __init__(self, topics):
        self.topics = topics


class _GroupOffsets:
    def __init__(self, topic_partitions):
        self.topic_partitions = topic_partitions


class FakeAdmin:
    """Records every call so the tests can assert on how it was used, not only
    on what came back. The auto-create regression below is only detectable that
    way."""

    def __init__(self, *, committed, earliest, latest, at_timestamp=None, topics=None):
        self.committed = committed
        self.earliest = earliest
        self.latest = latest
        self.at_timestamp = at_timestamp or {}
        self._topics = topics if topics is not None else {TOPIC: _TopicMeta(earliest)}
        self.list_topics_calls: list[dict] = []

    def list_consumer_group_offsets(self, requests, request_timeout=None):
        tps = [
            TopicPartition(TOPIC, p, o if o is not None else OFFSET_INVALID)
            for p, o in sorted(self.committed.items())
        ]
        # A partition on another topic, always. _committed() filters by topic and
        # a regression that drops the filter would otherwise go unnoticed.
        tps.append(TopicPartition("unrelated", 0, 999))
        return {requests[0].group_id: _Future(_GroupOffsets(tps))}

    def list_topics(self, *args, **kwargs):
        self.list_topics_calls.append(kwargs)
        return _ClusterMeta(self._topics)

    def list_offsets(self, request, request_timeout=None):
        out = {}
        for tp, spec in request.items():
            kind = type(spec).__name__
            if kind == "EarliestSpec":
                value = self.earliest[tp.partition]
            elif kind == "LatestSpec":
                value = self.latest[tp.partition]
            elif kind == "TimestampSpec":
                value = self.at_timestamp.get(tp.partition, -1)
            else:  # pragma: no cover -- a new spec type should fail loudly
                raise AssertionError(f"unexpected spec {kind}")
            out[tp] = _Future(_Offset(value))
        return out


def diff_for(admin, mode, value=None, **kwargs):
    return compute_diff(
        admin, group=GROUP, topic=TOPIC, mode=mode, value=value, bootstrap="fake:9092", **kwargs
    )


def by_partition(diff):
    return {p.partition: p for p in diff.partitions}


# ---------------------------------------------------------------------------


class SafetyTest(unittest.TestCase):
    """The dry run must not mutate anything. Asserted, not documented."""

    def test_list_topics_is_never_asked_about_one_topic(self):
        """Regression: handoff §14.

        `admin.list_topics(topic=X)` issues a metadata request naming X, and a
        broker with auto.create.topics.enable=true -- Kafka's default, and what
        these brokers run -- CREATES it. The call still reports "not found", so
        the tool prints the correct answer and leaves a new topic behind. This
        shipped once, in a tool whose docstring promised it was read-only.
        """
        admin = FakeAdmin(committed={0: 5}, earliest={0: 0}, latest={0: 10})
        diff_for(admin, "to_latest")

        self.assertTrue(admin.list_topics_calls, "list_topics was never called")
        for call in admin.list_topics_calls:
            self.assertNotIn(
                "topic",
                call,
                "list_topics(topic=...) auto-creates the topic on a default broker",
            )


class PartitionCoverageTest(unittest.TestCase):
    def test_partitions_come_from_metadata_not_from_committed_offsets(self):
        """A partition the group has never touched is the one most likely to be
        mis-seeked, so it has to appear in the diff rather than be skipped."""
        admin = FakeAdmin(
            committed={0: 5},  # p1 absent entirely
            earliest={0: 0, 1: 0},
            latest={0: 10, 1: 10},
        )
        rows = by_partition(diff_for(admin, "to_latest"))
        self.assertEqual(sorted(rows), [0, 1])
        self.assertIsNone(rows[1].current)

    def test_other_topics_are_filtered_out_of_committed(self):
        admin = FakeAdmin(committed={0: 5}, earliest={0: 0}, latest={0: 10})
        rows = by_partition(diff_for(admin, "to_latest"))
        self.assertEqual(list(rows), [0])
        self.assertEqual(rows[0].current, 5)

    def test_never_committed_partition_is_flagged_not_guessed(self):
        admin = FakeAdmin(committed={}, earliest={0: 0}, latest={0: 10})
        diff = diff_for(admin, "to_latest")
        row = by_partition(diff)[0]
        self.assertIsNone(row.current)
        self.assertIsNone(row.delta)
        self.assertEqual(row.direction, "unknown")
        self.assertIn("no committed offset", row.note)
        self.assertTrue(any("no committed offset" in w for w in diff.warnings))

    def test_output_is_sorted_by_partition(self):
        """The broker returns partitions in arbitrary order and this lands in a
        committed artifact that people diff between runs."""
        admin = FakeAdmin(
            committed={2: 1, 0: 1, 1: 1},
            earliest={0: 0, 1: 0, 2: 0},
            latest={0: 9, 1: 9, 2: 9},
        )
        diff = diff_for(admin, "to_latest")
        self.assertEqual([p.partition for p in diff.partitions], [0, 1, 2])

    def test_requesting_a_nonexistent_partition_is_refused(self):
        """Silently diffing a partition that does not exist would report a
        change nobody can apply."""
        admin = FakeAdmin(committed={0: 5}, earliest={0: 0}, latest={0: 10})
        with self.assertRaises(SystemExit):
            diff_for(admin, "to_latest", partitions=[0, 7])

    def test_missing_topic_is_refused(self):
        admin = FakeAdmin(committed={}, earliest={0: 0}, latest={0: 1}, topics={})
        with self.assertRaises(SystemExit):
            diff_for(admin, "to_latest")


class ModeTest(unittest.TestCase):
    def setUp(self):
        self.admin = FakeAdmin(
            committed={0: 50}, earliest={0: 10}, latest={0: 100}, at_timestamp={0: 70}
        )

    def test_to_earliest(self):
        self.assertEqual(by_partition(diff_for(self.admin, "to_earliest"))[0].proposed, 10)

    def test_to_latest(self):
        self.assertEqual(by_partition(diff_for(self.admin, "to_latest"))[0].proposed, 100)

    def test_to_offset(self):
        self.assertEqual(by_partition(diff_for(self.admin, "to_offset", 42))[0].proposed, 42)

    def test_shift_by_is_relative_to_current(self):
        row = by_partition(diff_for(self.admin, "shift_by", -20))[0]
        self.assertEqual(row.proposed, 30)
        self.assertEqual(row.delta, -20)
        self.assertEqual(row.direction, "rewind")

    def test_shift_by_without_a_committed_offset_proposes_nothing(self):
        """There is no defensible answer here, so it must not invent one.
        Falling back to earliest would replay the entire topic."""
        admin = FakeAdmin(committed={}, earliest={0: 10}, latest={0: 100})
        row = by_partition(diff_for(admin, "shift_by", -20))[0]
        self.assertIsNone(row.proposed)
        self.assertIn("nothing to shift from", row.note)

    def test_to_timestamp_uses_the_broker_answer(self):
        row = by_partition(diff_for(self.admin, "to_timestamp", 1700000000000))[0]
        self.assertEqual(row.proposed, 70)

    def test_to_timestamp_past_the_end_resolves_to_latest_and_says_so(self):
        """Every message predates the timestamp. Seeking to latest is the
        conventional reading, but it SKIPS the tail -- silently folding that in
        is how a backfill quietly loses data."""
        admin = FakeAdmin(
            committed={0: 50}, earliest={0: 10}, latest={0: 100}, at_timestamp={0: -1}
        )
        row = by_partition(diff_for(admin, "to_timestamp", 1700000000000))[0]
        self.assertEqual(row.proposed, 100)
        self.assertIn("skips tail", row.note)

    def test_unknown_mode_raises(self):
        with self.assertRaises(ValueError):
            diff_for(self.admin, "to_the_moon")


class OutOfRangeTest(unittest.TestCase):
    def test_proposed_past_latest_is_flagged_and_lag_goes_negative(self):
        """The broker ACCEPTS an out-of-range commit and the consumer then
        silently applies auto.offset.reset, which can mean skipping the whole
        topic. Negative lag_after is not clamped: it is the most direct
        statement that the committed offset would sit past the end of the log."""
        admin = FakeAdmin(committed={0: 50}, earliest={0: 10}, latest={0: 100})
        diff = diff_for(admin, "to_offset", 150)
        row = by_partition(diff)[0]
        self.assertTrue(row.out_of_range)
        self.assertEqual(row.lag_after, -50)
        self.assertIn("auto.offset.reset", row.note)
        self.assertTrue(any("out of range" in w for w in diff.warnings))

    def test_proposed_before_earliest_is_flagged(self):
        admin = FakeAdmin(committed={0: 50}, earliest={0: 10}, latest={0: 100})
        self.assertTrue(by_partition(diff_for(admin, "to_offset", 0))[0].out_of_range)

    def test_in_range_boundaries_are_not_flagged(self):
        admin = FakeAdmin(committed={0: 50}, earliest={0: 10}, latest={0: 100})
        for value in (10, 100):
            with self.subTest(value=value):
                self.assertFalse(by_partition(diff_for(admin, "to_offset", value))[0].out_of_range)


class TotalsTest(unittest.TestCase):
    """These two numbers are the summary a non-engineer approves on."""

    def test_replayed_and_skipped_are_counted_separately(self):
        """One partition rewinds and one advances in the same diff. Summing
        deltas would net them to zero and report a no-op operation that in fact
        both reprocesses and discards data. Replay is usually recoverable;
        skipped messages are never processed by this group at all."""
        admin = FakeAdmin(
            committed={0: 50, 1: 50},
            earliest={0: 0, 1: 0},
            latest={0: 100, 1: 100},
        )
        # p0 -> 30 (rewind 20), p1 stays put via an explicit per-partition run
        rewind = diff_for(admin, "to_offset", 30, partitions=[0])
        advance = diff_for(admin, "to_offset", 80, partitions=[1])
        self.assertEqual(rewind.totals["messages_replayed"], 20)
        self.assertEqual(rewind.totals["messages_skipped"], 0)
        self.assertEqual(advance.totals["messages_replayed"], 0)
        self.assertEqual(advance.totals["messages_skipped"], 30)

    def test_mixed_directions_in_one_diff_do_not_cancel(self):
        admin = FakeAdmin(
            committed={0: 20, 1: 80},
            earliest={0: 0, 1: 0},
            latest={0: 100, 1: 100},
        )
        totals = diff_for(admin, "to_offset", 50).totals
        self.assertEqual(totals["messages_replayed"], 30)  # p1: 80 -> 50
        self.assertEqual(totals["messages_skipped"], 30)  # p0: 20 -> 50

    def test_no_change_is_detected(self):
        admin = FakeAdmin(committed={0: 50}, earliest={0: 0}, latest={0: 100})
        self.assertTrue(diff_for(admin, "to_offset", 50).no_change)


class TimestampParsingTest(unittest.TestCase):
    def test_epoch_millis_passes_through(self):
        self.assertEqual(_parse_timestamp("1767225600000"), 1767225600000)

    def test_iso_with_offset(self):
        self.assertEqual(_parse_timestamp("2026-01-01T00:00:00+00:00"), 1767225600000)

    def test_z_suffix(self):
        self.assertEqual(_parse_timestamp("2026-01-01T00:00:00Z"), 1767225600000)

    def test_naive_is_refused_rather_than_assumed_utc(self):
        """A naive local time seeks to the wrong place on a runner in another
        zone, and does it silently."""
        with self.assertRaises(SystemExit):
            _parse_timestamp("2026-01-01T00:00:00")

    def test_garbage_is_refused(self):
        with self.assertRaises(SystemExit):
            _parse_timestamp("yesterday")


if __name__ == "__main__":
    unittest.main(verbosity=2)
