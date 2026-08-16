#!/usr/bin/env python3
"""B2 -- deterministic policy gate for a consumer-group offset seek.

Reads the JSON produced by ``offset_diff.py`` and a policy file, and returns
ALLOW or DENY. **There is no model in this path.** Every verdict is a named
function over declared data, so the same diff and the same policy always produce
the same answer, and a denial can always be traced to one line of YAML.

That separation is the point of the design: the ``/promote`` command narrates
and explains, but the decision is made here, by code a reviewer can read in five
minutes and a test can pin.

Design rules worth knowing before editing
-----------------------------------------
1. **An unknown key in the policy is a hard error, not a no-op.** A typo'd rule
   name that silently evaluated to "pass" would be the single worst failure this
   file could have -- the gate would report ALLOW while enforcing nothing.
2. **Fail closed.** Any unexpected exception during evaluation becomes DENY.
3. **Rules that are not configured are reported**, not hidden. Omission is a
   legitimate choice, but it should be visible in the artifact.

Exit codes: 0 ALLOW, 1 DENY, 2 bad usage / malformed policy or diff.
(2 is distinct on purpose: a broken policy file must not look like a denial.)
Dependencies: PyYAML and the standard library.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Callable

import yaml

POLICY_SCHEMA_VERSION = 1
Verdict = str  # "pass" | "deny" | "skip"

EXIT_ALLOW, EXIT_DENY, EXIT_BADINPUT = 0, 1, 2


def die(message: str) -> None:
    """Exit 2 for malformed input.

    Deliberately NOT `raise SystemExit(msg)`, which exits 1 and would be
    indistinguishable from a legitimate DENY. The promotion workflow has to tell
    "policy blocked this change" apart from "the policy file is broken" -- the
    first is a normal outcome, the second means the gate is not running at all.
    """
    print(message, file=sys.stderr)
    sys.exit(EXIT_BADINPUT)


@dataclass
class RuleResult:
    rule: str
    verdict: Verdict
    detail: str


# name -> (config_value, diff, ctx) -> RuleResult-ish tuple (ok, detail)
RULES: dict[str, Callable[..., tuple[bool, str]]] = {}
DESCRIPTIONS: dict[str, str] = {}


def rule(name: str, description: str):
    def register(fn):
        RULES[name] = fn
        DESCRIPTIONS[name] = description
        return fn

    return register


# --------------------------------------------------------------------------
# Rules. One function each, no shared state, no I/O.
# --------------------------------------------------------------------------


@rule("allowed_topics", "topic must appear in this allowlist")
def _allowed_topics(cfg: list[str], diff: dict, ctx: dict) -> tuple[bool, str]:
    topic = diff["topic"]
    ok = topic in cfg
    return ok, f"topic {topic!r} {'is' if ok else 'is NOT'} in the allowlist {cfg}"


@rule("allowed_groups", "consumer group must appear in this allowlist")
def _allowed_groups(cfg: list[str], diff: dict, ctx: dict) -> tuple[bool, str]:
    group = diff["group"]
    ok = group in cfg
    return ok, f"group {group!r} {'is' if ok else 'is NOT'} in the allowlist {cfg}"


@rule("forbidden_modes", "these seek modes may not be used in this environment")
def _forbidden_modes(cfg: list[str], diff: dict, ctx: dict) -> tuple[bool, str]:
    kind = diff["mode"]["kind"]
    ok = kind not in cfg
    return ok, f"mode {kind!r} {'is permitted' if ok else 'is FORBIDDEN'} here (forbidden: {cfg})"


@rule("max_messages_skipped", "cap on messages the group would never process")
def _max_skipped(cfg: int, diff: dict, ctx: dict) -> tuple[bool, str]:
    n = diff["totals"]["messages_skipped"]
    ok = n <= cfg
    return ok, f"{n} message(s) would be skipped, limit {cfg}"


@rule("max_messages_replayed", "cap on messages the group would reprocess")
def _max_replayed(cfg: int, diff: dict, ctx: dict) -> tuple[bool, str]:
    n = diff["totals"]["messages_replayed"]
    ok = n <= cfg
    return ok, f"{n} message(s) would be replayed, limit {cfg}"


@rule("max_partitions_changed", "cap on how many partitions one seek may move")
def _max_partitions(cfg: int, diff: dict, ctx: dict) -> tuple[bool, str]:
    n = sum(1 for p in diff["partitions"] if p["delta"] not in (0, None))
    ok = n <= cfg
    return ok, f"{n} partition(s) would change, limit {cfg}"


@rule("allow_out_of_range", "whether a proposed offset outside [earliest, latest] is tolerated")
def _out_of_range(cfg: bool, diff: dict, ctx: dict) -> tuple[bool, str]:
    bad = [p["partition"] for p in diff["partitions"] if p["out_of_range"]]
    if not bad:
        return True, "all proposed offsets are within [earliest, latest]"
    if cfg:
        return True, f"partitions {bad} are out of range, but the policy tolerates it"
    return False, (
        f"partitions {bad} would be seeked outside the log; the broker accepts this "
        f"and the consumer then silently applies auto.offset.reset"
    )


@rule(
    "allow_uncommitted_partitions",
    "whether partitions the group has never committed to may be seeked",
)
def _uncommitted(cfg: bool, diff: dict, ctx: dict) -> tuple[bool, str]:
    bad = [p["partition"] for p in diff["partitions"] if p["current"] is None]
    if not bad:
        return True, "every partition has a committed offset"
    if cfg:
        return True, f"partitions {bad} have no committed offset, tolerated by policy"
    return False, (
        f"partitions {bad} have no committed offset for this group -- usually means the "
        f"topic gained partitions and this seek was written against the old layout"
    )


@rule("require_diff_schema_version", "the diff artifact must match this schema version")
def _schema(cfg: int, diff: dict, ctx: dict) -> tuple[bool, str]:
    got = diff.get("schema_version")
    ok = got == cfg
    return ok, f"diff schema_version={got}, policy requires {cfg}"


@rule("max_diff_age_seconds", "how stale the dry-run may be at approval time")
def _age(cfg: int, diff: dict, ctx: dict) -> tuple[bool, str]:
    generated = dt.datetime.fromisoformat(diff["generated_at"])
    age = int((ctx["now"] - generated).total_seconds())
    ok = age <= cfg
    return ok, (
        f"dry run is {age}s old, limit {cfg}s"
        + ("" if ok else " -- the broker has likely moved; re-run offset_diff.py")
    )


# --------------------------------------------------------------------------


def load_policy(path: str) -> dict:
    with open(path) as fh:
        policy = yaml.safe_load(fh)
    if not isinstance(policy, dict):
        die(f"{path}: expected a YAML mapping at the top level")

    version = policy.get("version")
    if version != POLICY_SCHEMA_VERSION:
        die(
            f"{path}: version {version!r}, this evaluator understands "
            f"{POLICY_SCHEMA_VERSION}. Refusing rather than guessing."
        )

    rules = policy.get("rules")
    if not isinstance(rules, dict) or not rules:
        die(f"{path}: 'rules:' must be a non-empty mapping")

    # See design rule 1. A typo here must stop the pipeline, because the
    # alternative is a gate that reports ALLOW while enforcing nothing.
    unknown = sorted(set(rules) - set(RULES))
    if unknown:
        die(
            f"{path}: unknown rule(s) {unknown}.\n"
            f"Known rules: {sorted(RULES)}\n"
            f"Refusing to evaluate -- an unrecognised rule would silently enforce nothing."
        )
    return policy


def evaluate(policy: dict, diff: dict, now: dt.datetime) -> list[RuleResult]:
    ctx = {"now": now}
    results: list[RuleResult] = []
    configured = policy["rules"]

    for name in sorted(RULES):
        if name not in configured:
            results.append(RuleResult(name, "skip", "not configured in this policy"))
            continue
        try:
            ok, detail = RULES[name](configured[name], diff, ctx)
        except Exception as exc:  # design rule 2: fail closed
            results.append(
                RuleResult(name, "deny", f"rule raised {type(exc).__name__}: {exc} (failing closed)")
            )
            continue
        results.append(RuleResult(name, "pass" if ok else "deny", detail))
    return results


def render(policy: dict, diff: dict, results: list[RuleResult], allowed: bool) -> str:
    width = max(len(r.rule) for r in results)
    lines = [
        f"policy: {policy.get('environment', '?')} (v{policy['version']})"
        f"   owners: {', '.join(policy.get('owners', ['unowned']))}",
        f"change: seek {diff['mode']['kind']} on {diff['topic']} / {diff['group']}",
        "",
    ]
    marks = {"pass": "PASS", "deny": "DENY", "skip": "  --"}
    for r in results:
        lines.append(f"  {marks[r.verdict]}  {r.rule.ljust(width)}  {r.detail}")

    skipped = [r.rule for r in results if r.verdict == "skip"]
    if skipped:
        lines += ["", f"  {len(skipped)} known rule(s) not configured: {', '.join(skipped)}"]

    denials = [r for r in results if r.verdict == "deny"]
    lines += ["", f"VERDICT: {'ALLOW' if allowed else 'DENY'}"]
    if denials:
        lines.append(f"  blocked by {len(denials)} rule(s): {', '.join(r.rule for r in denials)}")
        lines.append("  no offsets have been changed.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Deterministic ALLOW/DENY gate over an offset_diff.py artifact. No LLM.",
        epilog="Exit 0 ALLOW, 1 DENY, 2 bad usage or malformed input.",
    )
    ap.add_argument("--policy", required=True, help="path to a policy YAML, e.g. policy/prod.yaml")
    ap.add_argument("--diff", required=True, help="offset_diff.py JSON, or - for stdin")
    ap.add_argument("--json", action="store_true", help="emit the verdict as JSON")
    ap.add_argument("--output", metavar="FILE", help="also write the verdict JSON here")
    ap.add_argument(
        "--now",
        help="override 'now' as ISO-8601, for testing max_diff_age_seconds deterministically",
    )
    args = ap.parse_args(argv)

    policy = load_policy(args.policy)

    raw = sys.stdin.read() if args.diff == "-" else open(args.diff).read()
    try:
        diff = json.loads(raw)
    except json.JSONDecodeError as exc:
        die(f"--diff: not valid JSON ({exc})")
    for key in ("schema_version", "topic", "group", "mode", "partitions", "totals"):
        if key not in diff:
            die(f"--diff: missing {key!r}; is this an offset_diff.py artifact?")

    now = (
        dt.datetime.fromisoformat(args.now)
        if args.now
        else dt.datetime.now(dt.timezone.utc)
    )

    results = evaluate(policy, diff, now)
    allowed = not any(r.verdict == "deny" for r in results)

    payload = {
        "allowed": allowed,
        "policy": args.policy,
        "environment": policy.get("environment"),
        "evaluated_at": now.isoformat(timespec="seconds"),
        "change": {
            "topic": diff["topic"],
            "group": diff["group"],
            "mode": diff["mode"],
            "totals": diff["totals"],
        },
        "results": [r.__dict__ for r in results],
    }
    if args.output:
        # Parent may not exist -- see the note in offset_diff.write_json. Losing
        # the verdict artifact matters even on a DENY: the verdict is the record
        # of why a change was refused, and exit codes alone don't carry reasons.
        try:
            os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
            with open(args.output, "w") as fh:
                json.dump(payload, fh, indent=2)
                fh.write("\n")
        except OSError as exc:
            die(f"could not write verdict {args.output!r}: {exc}")

    print(json.dumps(payload, indent=2) if args.json else render(policy, diff, results, allowed))
    return EXIT_ALLOW if allowed else EXIT_DENY


if __name__ == "__main__":
    sys.exit(main())
