#!/usr/bin/env python3
"""One pipeline, several audiences. Same run, different questions.

    python tools/views.py pm     --diff docs/changes/CHG-001/offset-diff.json
    python tools/views.py qa
    python tools/views.py devops

Everything here is a VIEW over artifacts something else already produced --
offset_diff.py's JSON, the files in contribution/, .github/workflows/,
policy/. Nothing is retyped and nothing is asserted that is not read off disk,
because a stakeholder document that is maintained by hand is wrong within a
month and is then worse than nothing: people keep trusting it.

Why bother, when the engineer's view (the linter, the test suite) already
exists? Because the other three people in the change do not read a pytest
summary, and the failure that costs the most is not a bug -- it is a change
that was technically correct and that nobody understood the consequences of.

  PM      What changes for users, how bad if wrong, can we undo it.
          Reads the diff artifact. No Kafka vocabulary in the lead sentence;
          if the first two lines do not survive being read aloud in a standup,
          this view has failed.

  QA      Which touchpoints have tests, what each layer of test can and
          cannot prove, and what is NOT covered. The gap list is the point.
          A coverage report that only lists what exists is marketing.

  DevOps  What runs where, what gates it, and what the rollback is. Derived
          from the workflow and policy files themselves, so adding an
          environment or a workflow shows up here without anyone remembering.

Markdown on stdout, so it composes with $GITHUB_STEP_SUMMARY.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(path: str):
    """Read a JSON or YAML file. safe_load, always -- these files come from the
    repo, but a view that can execute its input is a view that turns a config
    file into a code path."""
    with open(path) as fh:
        return json.load(fh) if path.endswith(".json") else yaml.safe_load(fh)


# The ten touchpoints a Kafka provider contribution has to hit upstream. This
# list is a fact about apache/airflow's conventions, not about this repo, so it
# is declared. Everything else below -- whether each one is DONE -- is read off
# the filesystem.
#
# `pattern` is a glob relative to contribution/, or None when the touchpoint has
# no file in a PR at all. `required` is the field that keeps the gap list worth
# reading: a touchpoint that is deliberately not needed here (this change adds
# no hook -- it reuses upstream's KafkaBaseHook) is NOT a gap, and listing it as
# one is how a gap list gets four entries nobody acts on and then gets ignored.
# Absent-and-required is a gap. Absent-and-not-required is a decision, and the
# note has to say why.
TOUCHPOINTS = [
    (1, "hook surface", "providers/apache/kafka/src/**/hooks/*.py", False,
     "no new hook: this reuses upstream's KafkaBaseHook, which is the whole "
     "reason the operator needs no connection handling of its own"),
    (2, "operator module", "providers/apache/kafka/src/**/operators/reset_offsets.py", True,
     "the change itself"),
    (3, "provider.yaml registration", "providers/apache/kafka/provider.yaml", True,
     "invisible coupling -- nothing in the operator references it"),
    (4, "docs index entry", "providers/apache/kafka/docs/**/*.rst", True,
     "operators.rst / index.rst entry so the operator is discoverable"),
    (5, "mirrored unit test", "providers/apache/kafka/tests/unit/**/test_reset_offsets.py", True,
     "pytest and coverage both assume the mirror; one directory off = never run"),
    (6, "integration test",
     "providers/apache/kafka/tests/integration/**/test_reset_offsets_integration.py", True,
     "the only layer that talks to a broker"),
    (7, "system test / example DAG",
     "providers/apache/kafka/tests/system/**/example_dag_reset_offsets.py", True,
     "the only layer that renders a Jinja template"),
    (8, "changelog / newsfragment", "providers/apache/kafka/**/newsfragments/*", True,
     "release notes are generated from these; a missing one ships silently"),
    (9, "version_compat shim", "providers/apache/kafka/src/**/version_compat.py", True,
     "how the provider supports more than one Airflow minor at a time"),
    (10, "pre-commit suite", None, False,
     "runs in CI (conventions.yml), not a file in the PR"),
]

# What each layer of test can prove, and — the useful half — what it cannot.
# Keyed to the workflow that runs it so the claim is checkable.
TEST_LAYERS = [
    ("unit", "int-tests.yml",
     "operator wiring, argument validation, the seek arithmetic against a fake AdminClient",
     "nothing about a real broker, and nothing about templating: the operator is "
     "constructed directly, so a missing template_fields entry still passes"),
    ("integration", "int-tests.yml",
     "committed offsets actually move; a group with live members is refused; dry run "
     "leaves the broker untouched",
     "nothing about a DagRun. to_timestamp is passed as a literal epoch here, so "
     "template rendering is still unproven"),
    ("system / DagRun", "int-tests.yml",
     "to_timestamp='{{ data_interval_start }}' renders per run, on a deployed Airflow "
     "with a different Python and provider version than INT",
     "runs dry_run=True only. The apply path is never exercised by a DAG"),
    ("promotion gate", "int-tests.yml, offset-change-prod.yml",
     "the policy evaluator allows a bounded rewind and denies to_earliest, on real "
     "broker data, for the named rule",
     "cannot tell you the policy VALUES are right -- only that they are enforced"),
    ("convention lint", "conventions.yml",
     "9 of 10 rules fire on a real mistake in the real contribution, offline",
     "cannot tell you a test is GOOD, only that one exists where the tooling looks"),
]


# --------------------------------------------------------------------------- PM


def _plain_mode(mode: dict) -> str:
    kind = mode.get("kind")
    return {
        "to_earliest": "back to the very beginning of the retained history",
        "to_latest": "forward to the newest message, skipping everything in between",
        "to_offset": f"to a specific position ({mode.get('value')})",
        "shift_by": f"{abs(int(mode.get('value') or 0))} messages "
                    f"{'back' if (mode.get('value') or 0) < 0 else 'forward'}",
        "to_timestamp": f"back to where it was at {mode.get('value_raw') or mode.get('value')}",
    }.get(kind, str(kind))


def pm_view(diff: dict, *, change_id: str | None, reason: str | None,
            verdict: dict | None, policy: dict | None, policy_path: str | None) -> str:
    totals = diff["totals"]
    replayed = totals["messages_replayed"]
    skipped = totals["messages_skipped"]
    partitions = len([p for p in diff["partitions"] if p["delta"]])

    lines = []
    lines.append(f"## What this change does{f' — `{change_id}`' if change_id else ''}")
    lines.append("")
    # The lead. Deliberately no offsets, no partitions, no 'consumer group'.
    lines.append(
        f"We are moving the **{diff['group']}** service's reading position in the "
        f"**{diff['topic']}** stream {_plain_mode(diff['mode'])}."
    )
    if reason:
        lines.append("")
        lines.append(f"**Why:** {reason}")
    lines.append("")

    lines.append("### If this is wrong")
    lines.append("")
    if replayed:
        lines.append(
            f"* **{replayed:,} events would be processed a second time.** Recoverable "
            "if downstream handling is idempotent — the usual symptom is duplicate "
            "records or duplicate notifications, not lost data."
        )
    if skipped:
        lines.append(
            f"* **{skipped:,} events would never be processed at all.** This is the "
            "irreversible half. Putting the position back does not reprocess them; "
            "they are simply skipped, and nothing downstream will ever show them."
        )
    if not replayed and not skipped:
        lines.append("* Nothing is reprocessed and nothing is skipped — this is a no-op.")
    lines.append(f"* {partitions} of {len(diff['partitions'])} data streams are affected.")
    lines.append("")

    lines.append("### Undo")
    lines.append("")
    lines.append(
        "The exact position before the change is captured to `restore.sh` *before* "
        "anything is written, so the position itself is one command to put back. "
        "That restores the POSITION, not the consequences: anything already "
        "reprocessed stays reprocessed, and anything skipped stays skipped."
    )
    lines.append("")

    lines.append("### Who decides")
    lines.append("")
    # Prefer the verdict's own record of which policy was applied over anything
    # passed on the command line: the verdict is the artifact that gets filed,
    # and a summary that names a different policy than the one actually enforced
    # is the specific lie this whole path exists to prevent.
    env = (verdict or {}).get("environment") or (policy or {}).get("environment") or "unknown"
    named = (verdict or {}).get("policy") or policy_path or "policy/"
    owners = ", ".join((policy or {}).get("owners") or []) or "the change's named owner"
    lines.append(
        f"An automated gate (`{named}`) checks this against the limits agreed for "
        f"**{env}**, and a named reviewer ({owners}) has to approve before anything "
        "is written. A change the gate rejects never reaches a human — nobody is "
        "asked to approve past a red check."
    )

    if verdict:
        allowed = verdict.get("allowed")
        lines.append("")
        lines.append(
            f"**Gate: {'PASSED — waiting on your approval' if allowed else 'BLOCKED'}**"
        )
        lines.append("")
        for rule in verdict.get("results", []):
            if rule.get("verdict") == "deny":
                lines.append(f"* blocked by `{rule.get('rule')}` — {rule.get('detail')}")
        # A rule that did not run is not a rule that passed. Saying so here, in
        # the PM's copy, is the difference between "checked and fine" and "not
        # checked" -- which is exactly the distinction a green tick erases.
        skipped = [r.get("rule") for r in verdict.get("results", []) if r.get("verdict") == "skip"]
        if skipped:
            lines.append(
                f"* not checked (no limit configured for this environment): "
                f"{', '.join(f'`{s}`' for s in skipped)}"
            )
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- QA


def qa_view(root: str) -> str:
    contribution = os.path.join(root, "contribution")
    lines = ["## Test coverage by touchpoint", ""]
    lines.append("| # | Touchpoint | Present | File |")
    lines.append("|---|---|---|---|")

    gaps = []
    required = 0
    for number, name, pattern, is_required, note in TOUCHPOINTS:
        required += bool(is_required)
        found = sorted(glob.glob(os.path.join(contribution, pattern), recursive=True)) \
            if pattern else []
        if found:
            rel = os.path.relpath(found[0], root)
            lines.append(f"| {number} | {name} | yes | `{rel}` |")
        elif not is_required:
            # Deliberately absent. Distinguished from a gap in the table itself,
            # because "n/a" and "MISSING" are the two things a reader must never
            # have to guess between.
            lines.append(f"| {number} | {name} | n/a by design | {note} |")
        else:
            lines.append(f"| {number} | {name} | **MISSING** | _{note}_ |")
            gaps.append((number, name, note))

    lines += ["", "## What each layer proves — and what it does not", ""]
    for layer, where, proves, cannot in TEST_LAYERS:
        lines.append(f"**{layer}** _(runs in {where})_")
        lines.append("")
        lines.append(f"* proves: {proves}")
        lines.append(f"* does NOT prove: {cannot}")
        lines.append("")

    lines += ["## Gaps", ""]
    if gaps:
        lines.append(
            f"{len(gaps)} of {required} required touchpoints are missing from "
            "`contribution/`:"
        )
        lines.append("")
        for number, name, note in gaps:
            lines.append(f"* **touchpoint {number}, {name}** — {note}")
        lines.append("")
        lines.append(
            "These are gaps in the contribution, not in the tooling. They are listed "
            "here rather than quietly omitted because a coverage report that only "
            "shows what exists is the thing QA cannot use."
        )
    else:
        lines.append("None. Every required touchpoint has an artifact.")

    lines += ["", "### Known limits of this report", "",
              "* Presence, not quality. This says a test file exists where the tooling "
              "will find it. It cannot say the assertions inside are worth anything.",
              "* The layer claims above are maintained by hand in `tools/views.py` and "
              "checked against the workflow files by `tools/test_views.py`; a claim "
              "that names a workflow which no longer runs that suite will fail there, "
              "but a claim that is merely optimistic will not."]
    return "\n".join(lines) + "\n"


# ----------------------------------------------------------------------- DevOps


def _triggers(doc: dict) -> str:
    # PyYAML parses the bare key `on:` as the boolean True. This bites every
    # tool that reads a workflow file and is invisible until you print it.
    on = doc.get("on", doc.get(True)) or {}
    if isinstance(on, str):
        return on
    if isinstance(on, list):
        return ", ".join(on)
    return ", ".join(on.keys())


def _runs_on(job: dict) -> str:
    runs = job.get("runs-on")
    return ",".join(runs) if isinstance(runs, list) else str(runs)


def devops_view(root: str) -> str:
    lines = ["## Where things run", "", "| Workflow | Triggered by | Runner | Jobs | Approval |",
             "|---|---|---|---|---|"]
    for path in sorted(glob.glob(os.path.join(root, ".github", "workflows", "*.yml"))):
        doc = _load(path)
        jobs = doc.get("jobs") or {}
        environments = sorted({j["environment"] for j in jobs.values() if j.get("environment")})
        lines.append(
            f"| `{os.path.basename(path)}` | {_triggers(doc)} | "
            f"{'; '.join(dict.fromkeys(_runs_on(j) for j in jobs.values()))} | {len(jobs)} | "
            f"{', '.join(environments) if environments else 'none'} |"
        )

    lines += ["", "## Promotion gates", "",
              "| Environment | Policy | Skipped cap | Replayed cap | Partitions | Diff max age | Forbidden |",
              "|---|---|---|---|---|---|---|"]
    order = {"int.yaml": 0, "stag.yaml": 1, "prod.yaml": 2}
    paths = sorted(glob.glob(os.path.join(root, "policy", "*.yaml")),
                   key=lambda p: order.get(os.path.basename(p), 99))
    for path in paths:
        doc = _load(path)
        rules = doc.get("rules") or {}
        lines.append(
            f"| {doc.get('environment')} | `policy/{os.path.basename(path)}` | "
            f"{rules.get('max_messages_skipped', '—')} | "
            f"{rules.get('max_messages_replayed', '—')} | "
            f"{rules.get('max_partitions_changed', '—')} | "
            f"{rules.get('max_diff_age_seconds', '—')} | "
            f"{', '.join(rules.get('forbidden_modes') or []) or 'none'} |"
        )
    lines.append("")
    lines.append(
        "`—` means the rule is **not configured** for that environment, so it is not "
        "checked at all. Read it as absent, not as zero: `policy_check.py` reports "
        "those as `skip`, and a skip is not a pass."
    )
    lines.append("")
    lines.append(
        "Strictness must not decrease left to right, and staging must match production "
        "exactly on every non-scope rule. Both are enforced by "
        "`tools/test_policy_check.py`, not by review."
    )

    lines += ["", "## Rollback", "",
              "| What broke | How to put it back |", "|---|---|",
              "| Committed offsets | `docs/changes/<id>/restore.sh`, captured before "
              "anything is written. Restores the position, not the consequences. |",
              "| A bad deploy | Rebuild the image with the previous wheel "
              "(`deploy/production/`). Separate operation from the offsets. |",
              "| A demo environment | `tools/reset_demo.sh restore int\\|stag`. PROD "
              "requires `CONFIRM=prod-reset`. |"]

    lines += ["", "## Controls that are not in a workflow file", "",
              "* `.cursor/hooks.json` → `guard_shell.py` blocks the mutating tools from "
              "an agent shell, `failClosed: true`. Proven by `tools/test_shell_guard.py`.",
              "* The `production` environment's required-reviewer rule is REPOSITORY "
              "configuration, not file configuration. A workflow cannot grant itself an "
              "approver, so `offset-change-prod.yml` asserts rather than assumes — and if the "
              "repo's visibility or plan changes, that rule can vanish silently.",
              "* PROD deliberately does not expose the `broker:29092` test listener that "
              "INT and STAG carry. Making a workflow need it would be the wrong fix."]
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------------- main


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("audience", choices=["pm", "qa", "devops"])
    parser.add_argument("--diff", help="offset_diff.py JSON (required for pm)")
    parser.add_argument("--verdict", help="policy_check.py JSON, optional")
    parser.add_argument("--policy", help="the policy YAML this was gated against")
    parser.add_argument("--change-id")
    parser.add_argument("--reason")
    parser.add_argument("--root", default=REPO)
    parser.add_argument("-o", "--output")
    args = parser.parse_args(argv)

    if args.audience == "pm":
        if not args.diff:
            parser.error("pm needs --diff: this view is a reading of a real dry run, "
                         "not a template")
        diff = _load(args.diff)
        verdict = _load(args.verdict) if args.verdict else None
        policy = _load(args.policy) if args.policy else None
        text = pm_view(diff, change_id=args.change_id, reason=args.reason,
                       verdict=verdict, policy=policy, policy_path=args.policy)
    elif args.audience == "qa":
        text = qa_view(args.root)
    else:
        text = devops_view(args.root)

    if args.output:
        with open(args.output, "w") as fh:
            fh.write(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
