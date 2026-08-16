#!/usr/bin/env python3
"""B1 -- dry-run diff for a consumer-group offset seek.

Answers one question before anything is mutated: *if we applied this seek, what
exactly would change, per partition, and how many messages get replayed or
skipped?*

This is deliberately the same computation the future
``SeekConsumerGroupOperator(dry_run=True)`` performs. The operator is expected to
import :func:`compute_diff` rather than reimplement it, so the number a reviewer
approves in CI and the number the operator prints in a task log come from one
code path. A promotion gate that computes the delta differently from the
operator is a gate that can disagree with the thing it is gating.

Dependencies: ``confluent-kafka`` and the standard library. No Airflow import on
purpose -- this runs on a CI runner that should not need a 300 MB install to
tell you an offset is moving backwards.

Verified against confluent-kafka 2.6.0 (the declared floor) on the INT broker.

API note, the hard-won kind
---------------------------
The two classes needed for a single ``list_offsets`` call live in *opposite*
modules, and neither is re-exported by the other::

    from confluent_kafka import TopicPartition           # top level ONLY
    from confluent_kafka.admin import OffsetSpec         # admin ONLY

``ConsumerGroupTopicPartitions`` is top-level only as well, despite reading like
an admin type and appearing in admin call signatures. Verified on both 2.6.0 and
2.15.0 -- this is not version skew, it is just the API's shape.

A read-only tool that wasn't
----------------------------
``admin.list_topics(topic=X)`` -- the obvious way to ask "does this topic exist?"
-- CREATES X on any broker running the default ``auto.create.topics.enable=true``.
The call still returns "not found", so nothing in the output gives it away. See
:func:`compute_diff`. Found here on 2026-08-15, after the fact, by noticing junk
topics on INT that only this tool's error-path tests could have made.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import asdict, dataclass, field

from confluent_kafka import OFFSET_INVALID, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

SCHEMA_VERSION = 1

# Seek modes. These mirror the operator's proposed `seek_to` parameter, and the
# names match kafka-consumer-groups.sh's --to-* flags so anyone who has run the
# CLI reads this without a translation step.
MODES = ("to_earliest", "to_latest", "to_offset", "shift_by", "to_timestamp")


@dataclass
class PartitionDiff:
    partition: int
    earliest: int
    latest: int
    current: int | None          # None == group never committed here
    proposed: int | None         # None == could not be computed for this partition
    delta: int | None
    lag_before: int | None
    lag_after: int | None
    direction: str               # rewind | advance | none | unknown
    out_of_range: bool = False
    note: str | None = None


@dataclass
class OffsetDiff:
    schema_version: int
    generated_at: str
    group: str
    topic: str
    bootstrap: str
    mode: dict
    partitions: list[PartitionDiff]
    totals: dict
    warnings: list[str] = field(default_factory=list)

    @property
    def no_change(self) -> bool:
        return all(p.delta in (0, None) for p in self.partitions)


EXIT_OK, EXIT_ERROR, EXIT_USAGE = 0, 1, 2


def die(message: str, code: int = EXIT_USAGE) -> None:
    """Exit with an explicit code. `raise SystemExit(msg)` always exits 1, which
    would make a typo'd flag indistinguishable from a broker failure."""
    print(message, file=sys.stderr)
    sys.exit(code)


def write_json(path: str, payload: dict) -> None:
    """Write the artifact, creating parent directories.

    The promotion workflow writes into docs/changes/<CHG-ID>/, which only exists
    because capture_offsets.py happens to run first. Depending on that ordering
    is a trap: run this tool standalone, or reorder the workflow steps, and it
    died with a bare FileNotFoundError traceback -- after the broker reads had
    already succeeded. The diff was computed correctly and then thrown away.

    Failure here is fatal on purpose. A dry run whose artifact did not persist
    has nothing for the approver to read, and the apply job consumes this file
    rather than recomputing, so silently continuing would gate on a stale one.
    """
    parent = os.path.dirname(os.path.abspath(path))
    try:
        os.makedirs(parent, exist_ok=True)
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2)
            fh.write("\n")
    except OSError as exc:
        die(f"could not write artifact {path!r}: {exc}", EXIT_ERROR)


def _parse_timestamp(raw: str) -> int:
    """Accept epoch millis or an ISO-8601 string, return epoch millis."""
    if raw.isdigit():
        return int(raw)
    # datetime.fromisoformat() did not accept a 'Z' suffix until Python 3.11.
    # Without this normalisation the tool rejects 'Z' on 3.9/3.10 -- and the
    # naive-input error below actively tells the operator to add a 'Z', so the
    # advertised fix produced a second, more confusing rejection. Kafka
    # timestamps are quoted in UTC everywhere, so 'Z' is the form people
    # actually type. Found by tools/test_offset_diff.py the first time this
    # function was ever tested.
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError as exc:
        die(f"--to-timestamp: {raw!r} is neither epoch millis nor ISO-8601 ({exc})")
    if parsed.tzinfo is None:
        # Naive input is ambiguous and the failure is silent and off-by-hours.
        # Refuse rather than guess a zone.
        die(
            f"--to-timestamp: {raw!r} has no timezone. Use e.g. "
            f"'{raw}Z' or '{raw}+00:00' -- a naive local time would silently "
            f"seek to the wrong place on a runner in another zone."
        )
    return int(parsed.timestamp() * 1000)


def _committed(admin: AdminClient, group: str, topic: str, timeout: float) -> dict[int, int]:
    """Committed offset per partition for `group`, restricted to `topic`.

    Partitions the group has never committed to come back as OFFSET_INVALID
    (-1001) rather than being absent; they are dropped here and surface later as
    `current: None`. That happens for real whenever a topic gains partitions
    after a group last committed.
    """
    futures = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group)], request_timeout=timeout
    )
    result = next(iter(futures.values())).result()
    return {
        tp.partition: tp.offset
        for tp in result.topic_partitions
        if tp.topic == topic and tp.offset != OFFSET_INVALID
    }


def _watermarks(
    admin: AdminClient, topic: str, partitions: list[int], spec, timeout: float
) -> dict[int, int]:
    request = {TopicPartition(topic, p): spec for p in partitions}
    return {
        tp.partition: future.result().offset
        for tp, future in admin.list_offsets(request, request_timeout=timeout).items()
    }


def compute_diff(
    admin: AdminClient,
    group: str,
    topic: str,
    mode: str,
    value: int | None = None,
    value_raw: str | None = None,
    partitions: list[int] | None = None,
    bootstrap: str = "",
    timeout: float = 10.0,
) -> OffsetDiff:
    """Compute the per-partition effect of a seek. Reads only; mutates nothing.

    "Mutates nothing" is a property that has to be maintained, not assumed --
    see the ``list_topics`` note below for the one place it was already false.
    """
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}, expected one of {MODES}")

    warnings: list[str] = []

    committed = _committed(admin, group, topic, timeout)

    # NEVER pass topic= to list_topics.
    #
    # `admin.list_topics(topic=X)` issues a metadata request that NAMES topic X,
    # and brokers with auto.create.topics.enable=true -- Kafka's DEFAULT, and
    # what these brokers run -- CREATE the topic in response. The call still
    # reports "not found", so the bug is invisible from the tool's own output:
    # you get the right answer and a brand new topic.
    #
    # Caught on INT 2026-08-15 by finding `no.such` and `no.such.topic` on the
    # broker, left behind by this tool's own error-path tests. A dry run that
    # creates topics on a production broker is not a dry run.
    #
    # Fetching cluster-wide metadata instead is heavier on a broker with many
    # topics, and that is the right trade for a tool that promises to mutate
    # nothing.
    meta = admin.list_topics(timeout=timeout)
    if topic not in meta.topics or meta.topics[topic].error is not None:
        die(f"topic {topic!r} not found on {bootstrap}", EXIT_ERROR)
    available = sorted(meta.topics[topic].partitions)

    if partitions is None:
        # Partition list comes from the topic's metadata, NOT from the group's
        # committed offsets: a partition the group has never touched is exactly
        # the one most likely to be mis-seeked, so it has to appear in the diff.
        partitions = available
    else:
        # Validate rather than let list_offsets fail obscurely later. Silently
        # diffing a partition that does not exist would report a change nobody
        # can apply.
        missing = sorted(set(partitions) - set(available))
        if missing:
            die(
                f"topic {topic!r} has partitions {available}; requested {missing} do not exist",
                EXIT_ERROR,
            )

    earliest = _watermarks(admin, topic, partitions, OffsetSpec.earliest(), timeout)
    latest = _watermarks(admin, topic, partitions, OffsetSpec.latest(), timeout)

    at_ts: dict[int, int] = {}
    if mode == "to_timestamp":
        at_ts = _watermarks(
            admin, topic, partitions, OffsetSpec.for_timestamp(value), timeout
        )

    rows: list[PartitionDiff] = []
    # Sorted explicitly: the broker returns partitions in arbitrary order, and
    # this output lands in a committed artifact that people diff between runs.
    for p in sorted(partitions):
        lo, hi = earliest[p], latest[p]
        cur = committed.get(p)
        note = None
        proposed: int | None

        if mode == "to_earliest":
            proposed = lo
        elif mode == "to_latest":
            proposed = hi
        elif mode == "to_offset":
            proposed = value
        elif mode == "shift_by":
            if cur is None:
                proposed = None
                note = "no committed offset; shift_by has nothing to shift from"
            else:
                proposed = cur + value
        else:  # to_timestamp
            found = at_ts.get(p, -1)
            if found < 0:
                # Every message in this partition predates the timestamp.
                # Seeking to `latest` is the conventional reading, but it SKIPS
                # the tail, so state it rather than fold it in silently.
                proposed = hi
                note = "no message at/after timestamp; resolves to latest (skips tail)"
            else:
                proposed = found

        if cur is None and note is None:
            note = "group has no committed offset for this partition"

        out_of_range = proposed is not None and not (lo <= proposed <= hi)
        if out_of_range:
            note = (
                f"proposed {proposed} outside [{lo}, {hi}]; the broker accepts this "
                f"and the consumer then silently applies auto.offset.reset"
            )

        delta = None if (proposed is None or cur is None) else proposed - cur
        lag_before = None if cur is None else hi - cur
        # lag_after goes NEGATIVE when proposed > latest. Not clamped on purpose:
        # a negative lag is the most direct statement that the committed offset
        # would sit past the end of the log. The `!` column and the note above
        # explain it so it does not read as an arithmetic bug.
        lag_after = None if proposed is None else hi - proposed

        if delta is None:
            direction = "unknown"
        elif delta < 0:
            direction = "rewind"
        elif delta > 0:
            direction = "advance"
        else:
            direction = "none"

        rows.append(
            PartitionDiff(
                partition=p,
                earliest=lo,
                latest=hi,
                current=cur,
                proposed=proposed,
                delta=delta,
                lag_before=lag_before,
                lag_after=lag_after,
                direction=direction,
                out_of_range=out_of_range,
                note=note,
            )
        )

    for row in rows:
        if row.out_of_range:
            warnings.append(f"p{row.partition}: proposed offset out of range")
        if row.current is None:
            warnings.append(f"p{row.partition}: no committed offset for this group")

    # The two numbers a reviewer actually needs. "replayed" is reprocessing --
    # usually recoverable if the pipeline is idempotent. "skipped" is data that
    # will never be processed, which is the operation worth blocking.
    replayed = sum(-r.delta for r in rows if r.delta and r.delta < 0)
    skipped = sum(r.delta for r in rows if r.delta and r.delta > 0)

    return OffsetDiff(
        schema_version=SCHEMA_VERSION,
        generated_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        group=group,
        topic=topic,
        bootstrap=bootstrap,
        # value_raw preserves what the operator typed. For --to-timestamp the
        # resolved value is epoch millis, and an artifact that records only
        # 1786752000000 is unreviewable six months later.
        mode={"kind": mode, "value": value, "value_raw": value_raw},
        partitions=rows,
        totals={
            "messages_replayed": replayed,
            "messages_skipped": skipped,
            "lag_before": sum(r.lag_before for r in rows if r.lag_before is not None),
            "lag_after": sum(r.lag_after for r in rows if r.lag_after is not None),
        },
        warnings=warnings,
    )


def _fmt(value: int | None) -> str:
    return "-" if value is None else str(value)


def render_table(diff: OffsetDiff) -> str:
    header = ("PART", "EARLIEST", "LATEST", "CURRENT", "PROPOSED", "DELTA", "LAG->LAG", "")
    rows = [
        (
            str(r.partition),
            str(r.earliest),
            str(r.latest),
            _fmt(r.current),
            _fmt(r.proposed),
            "-" if r.delta is None else f"{r.delta:+d}",
            f"{_fmt(r.lag_before)} -> {_fmt(r.lag_after)}",
            "!" if r.out_of_range else "",
        )
        for r in diff.partitions
    ]
    widths = [max(len(c) for c in col) for col in zip(header, *rows)]
    line = lambda cells: "  ".join(c.ljust(w) for c, w in zip(cells, widths)).rstrip()

    mode_desc = diff.mode["kind"]
    if diff.mode["value"] is not None:
        mode_desc += f" {diff.mode['value']}"
    if diff.mode.get("value_raw") and str(diff.mode["value_raw"]) != str(diff.mode["value"]):
        mode_desc += f" ({diff.mode['value_raw']})"

    out = [
        f"group={diff.group}  topic={diff.topic}  broker={diff.bootstrap}",
        f"seek: {mode_desc}   (DRY RUN -- nothing has been changed)",
        "",
        line(header),
        line(["-" * w for w in widths]),
        *(line(r) for r in rows),
        "",
    ]

    t = diff.totals
    if diff.no_change:
        out.append("no change: proposed offsets equal current offsets")
    else:
        out.append(
            f"{t['messages_replayed']} message(s) REPLAYED, "
            f"{t['messages_skipped']} message(s) SKIPPED"
        )
        if t["messages_skipped"]:
            out.append(
                "  skipped messages are never processed by this group -- "
                "this is the irreversible half"
            )

    for row in diff.partitions:
        if row.note:
            out.append(f"  note p{row.partition}: {row.note}")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Dry-run diff for a consumer-group offset seek. Read-only.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Exit 0 = diff computed. Exit 2 = bad usage. Exit 1 = broker/topic error.\n"
        "A dangerous-but-valid seek still exits 0 -- deciding is policy_check.py's job.",
    )
    ap.add_argument("--group", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument(
        "--partitions",
        help="comma-separated subset, e.g. 0,2. Default: every partition of the topic.",
    )
    ap.add_argument("--timeout", type=float, default=10.0, help="broker request timeout (s)")

    seek = ap.add_mutually_exclusive_group(required=True)
    seek.add_argument("--to-earliest", action="store_true")
    seek.add_argument("--to-latest", action="store_true")
    seek.add_argument("--to-offset", type=int, metavar="N")
    seek.add_argument("--shift-by", type=int, metavar="N", help="relative; may be negative")
    seek.add_argument("--to-timestamp", metavar="TS", help="epoch millis or ISO-8601 with offset")

    ap.add_argument("--json", action="store_true", help="emit JSON on stdout instead of a table")
    ap.add_argument("--output", metavar="FILE", help="also write JSON here (for the change trail)")
    args = ap.parse_args(argv)

    value_raw = None
    if args.to_earliest:
        mode, value = "to_earliest", None
    elif args.to_latest:
        mode, value = "to_latest", None
    elif args.to_offset is not None:
        mode, value = "to_offset", args.to_offset
    elif args.shift_by is not None:
        mode, value = "shift_by", args.shift_by
    else:
        mode, value = "to_timestamp", _parse_timestamp(args.to_timestamp)
        value_raw = args.to_timestamp

    partitions = None
    if args.partitions:
        try:
            partitions = [int(p) for p in args.partitions.split(",") if p.strip()]
        except ValueError:
            ap.error(f"--partitions: expected comma-separated integers, got {args.partitions!r}")

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    diff = compute_diff(
        admin,
        group=args.group,
        topic=args.topic,
        mode=mode,
        value=value,
        value_raw=value_raw,
        partitions=partitions,
        bootstrap=args.bootstrap,
        timeout=args.timeout,
    )

    payload = asdict(diff)
    payload["no_change"] = diff.no_change

    if args.output:
        write_json(args.output, payload)

    print(json.dumps(payload, indent=2) if args.json else render_table(diff))
    return EXIT_OK


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
