#!/usr/bin/env python3
"""Apply an approved offset seek. **This is the only file that mutates anything.**

Takes the exact JSON artifact produced by ``offset_diff.py`` -- the same file a
human approved -- and writes those offsets. It does not recompute the target,
because recomputing would mean applying something nobody approved.

Optimistic concurrency, which is the point of this file
--------------------------------------------------------
Between the dry run and the approval, a consumer may have committed, or someone
may have run a manual reset. The approved artifact would then describe a world
that no longer exists, and applying it would silently clobber whatever happened
in between.

So before writing, this re-reads the group's committed offsets and requires them
to equal the ``current`` values recorded in the artifact. Any drift aborts with a
per-partition report and **nothing is written**. ``--force`` exists, is loud, and
should be an incident-time decision.

``apply_seek()`` is importable on purpose: the operator is expected to call it
rather than reimplement ``alter_consumer_group_offsets``, so the mutation CI
performs and the mutation an Airflow task performs are one code path -- the same
argument that keeps ``compute_diff`` in one place.

Exit codes: 0 applied, 1 drift detected / broker error, 2 bad usage.
"""

from __future__ import annotations

import argparse
import json
import sys

from confluent_kafka import OFFSET_INVALID, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

EXIT_OK, EXIT_ERROR, EXIT_USAGE = 0, 1, 2


def die(message: str, code: int = EXIT_USAGE) -> None:
    print(message, file=sys.stderr)
    sys.exit(code)


def read_committed(admin: AdminClient, group: str, topic: str, timeout: float) -> dict[int, int]:
    futures = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group)], request_timeout=timeout
    )
    result = next(iter(futures.values())).result()
    return {
        tp.partition: tp.offset
        for tp in result.topic_partitions
        if tp.topic == topic and tp.offset != OFFSET_INVALID
    }


def apply_seek(
    admin: AdminClient,
    diff: dict,
    force: bool = False,
    timeout: float = 10.0,
) -> list[TopicPartition]:
    """Write the offsets described by `diff`. Returns the partitions written.

    Raises RuntimeError on drift unless `force`. Callers that catch it should
    treat it as "the approval is stale", not as a transient failure.
    """
    group, topic = diff["group"], diff["topic"]

    targets = [p for p in diff["partitions"] if p["proposed"] is not None and p["delta"] != 0]
    if not targets:
        return []

    live = read_committed(admin, group, topic, timeout)
    drift = [
        (p["partition"], p["current"], live.get(p["partition"]))
        for p in targets
        if live.get(p["partition"]) != p["current"]
    ]
    if drift:
        detail = "\n".join(
            f"    p{part}: artifact recorded current={was}, broker now has {now}"
            for part, was, now in drift
        )
        message = (
            f"group {group!r} moved since the dry run was approved:\n{detail}\n"
            f"  Applying would overwrite changes nobody reviewed."
        )
        if not force:
            raise RuntimeError(message + "\n  Re-run offset_diff.py and get a fresh approval.")
        print(f"WARNING: --force, applying over drift anyway:\n{message}", file=sys.stderr)

    partitions = [TopicPartition(topic, p["partition"], p["proposed"]) for p in targets]

    # alter_consumer_group_offsets takes a LIST of ConsumerGroupTopicPartitions
    # and returns a dict of futures keyed by group id -- not by partition. A
    # single future covers the whole group, so a partial failure surfaces as one
    # exception rather than per-partition results.
    futures = admin.alter_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group, partitions)], request_timeout=timeout
    )
    next(iter(futures.values())).result()
    return partitions


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Apply an approved offset_diff.py artifact. The only mutating tool.",
        epilog="Exit 0 applied, 1 drift or broker error, 2 bad usage.",
    )
    ap.add_argument("--diff", required=True, help="the APPROVED offset_diff.py JSON, or -")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument(
        "--force",
        action="store_true",
        help="apply even if the group drifted since the dry run. Incident use only.",
    )
    args = ap.parse_args(argv)

    raw = sys.stdin.read() if args.diff == "-" else open(args.diff).read()
    try:
        diff = json.loads(raw)
    except json.JSONDecodeError as exc:
        die(f"--diff: not valid JSON ({exc})")
    for key in ("schema_version", "group", "topic", "partitions"):
        if key not in diff:
            die(f"--diff: missing {key!r}; is this an offset_diff.py artifact?")

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    try:
        written = apply_seek(admin, diff, force=args.force, timeout=args.timeout)
    except RuntimeError as exc:
        die(f"REFUSING TO APPLY: {exc}", EXIT_ERROR)
    except Exception as exc:
        die(f"apply failed: {type(exc).__name__}: {exc}", EXIT_ERROR)

    if not written:
        print("nothing to apply: every partition is already at its proposed offset")
        return EXIT_OK

    print(f"applied {len(written)} partition(s) to group {diff['group']!r}:")
    for tp in written:
        print(f"  {tp.topic}:{tp.partition} -> {tp.offset}")

    # Read back rather than trusting the write. The broker acknowledging an
    # alter is not the same as the offsets reading back as intended.
    live = read_committed(admin, diff["group"], diff["topic"], args.timeout)
    bad = [tp for tp in written if live.get(tp.partition) != tp.offset]
    if bad:
        die(
            "post-apply verification FAILED: "
            + ", ".join(f"p{tp.partition} reads {live.get(tp.partition)}, wanted {tp.offset}" for tp in bad),
            EXIT_ERROR,
        )
    print("verified: all offsets read back as intended")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
