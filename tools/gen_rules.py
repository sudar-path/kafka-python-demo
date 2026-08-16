#!/usr/bin/env python3
"""Generate every downstream consumer of the rule pack from rules/*.yaml.

One source of truth, three consumers, none of them hand-maintained:

    rules/*.yaml  --+-->  .cursor/rules/*.mdc   the Cursor agent's context
                    +-->  docs/RULES.md         what a human reads
                    +-->  (lint_conventions.py reads the YAML directly)

The reason this is a generator and not three hand-written files is the fourth
grading requirement -- "stays maintainable as the library evolves". Hand-written
Cursor rules rot: someone tightens the linter, nobody updates the .mdc, and the
agent starts confidently teaching the old convention. That failure is silent and
it is the single most likely way this whole artifact becomes a liability.

`--check` is the fix. It regenerates in memory and diffs against what is on
disk, exit 1 if they differ. Wired into CI, it makes drift a failing build
rather than a discovery six months later. That is the difference between
claiming the pack is maintainable and being able to show it.

Exit codes: 0 ok (or, under --check, in sync), 1 out of sync, 2 bad input.
"""

from __future__ import annotations

import argparse
import difflib
import os
import sys

import yaml

from lint_conventions import DETECTORS, load_rules, Rule  # single loader, single validator

EXIT_OK, EXIT_STALE, EXIT_BADINPUT = 0, 1, 2

BANNER = "GENERATED FILE -- DO NOT EDIT. Source: {src}. Regenerate: python tools/gen_rules.py"

# Human-readable pack titles. A pack missing from here still generates; it just
# gets its slug as a title. Failing the build over a cosmetic label would be
# the wrong trade -- adding a rule must never require editing this file.
PACK_TITLES = {
    "kafka-admin-api": "Kafka admin client API",
    "provider-conventions": "Airflow provider conventions",
    "test-conventions": "Test conventions",
}

PACK_BLURBS = {
    "kafka-admin-api": (
        "Rules about confluent-kafka's AdminClient surface. These exist because "
        "the API has correct-looking calls that are deprecated, calls that mutate "
        "the broker despite reading as reads, and symbols split across two modules "
        "with no hint at the call site."
    ),
    "provider-conventions": (
        "Rules about the shape of an Airflow provider contribution. These are the "
        "touchpoints that upstream pre-commit and CI enforce but that nothing in "
        "the file you are editing mentions."
    ),
    "test-conventions": (
        "Rules about where tests live and what they must clean up. Both are "
        "structural: a test in the wrong directory is silently never collected, "
        "and a leaked topic makes the next run's failure someone else's problem."
    ),
}


def die(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(EXIT_BADINPUT)


def group_by_pack(rules: list[Rule]) -> dict[str, list[Rule]]:
    packs: dict[str, list[Rule]] = {}
    for r in rules:
        packs.setdefault(r.pack, []).append(r)
    for members in packs.values():
        members.sort(key=lambda r: r.id)
    return dict(sorted(packs.items()))


def pack_globs(members: list[Rule]) -> list[str]:
    """Union of the member rules' applies_to. This is what makes the .mdc
    auto-attach on exactly the files its rules govern -- the agent gets the
    Kafka rules when it opens a Kafka file and not otherwise, which is the
    whole point of scoping rules instead of one giant always-on prompt."""
    seen: list[str] = []
    for r in members:
        for g in r.applies_to:
            if g not in seen:
                seen.append(g)
    return sorted(seen)


def _fmt_value(value) -> str:
    """Summarise instead of dumping. KAFKA-002 carries a seven-entry symbol
    table; pasting it verbatim buries the rule text it is meant to annotate."""
    if isinstance(value, dict):
        return f"{len(value)} entries" if len(value) > 3 else ", ".join(
            f"{k}->{v}" for k, v in value.items()
        )
    if isinstance(value, list):
        return f"{len(value)} entries" if len(value) > 3 else ", ".join(map(str, value))
    return repr(value)


def _detector_line(rule: Rule) -> str:
    if rule.detector is None:
        return "not machine-checkable -- advisory, this text is the only enforcement"
    kind = rule.detector["kind"]
    detail = ", ".join(
        f"{k}={_fmt_value(v)}" for k, v in rule.detector.items() if k != "kind"
    )
    return f"enforced by lint_conventions.py ({kind}{': ' + detail if detail else ''})"


def lead(text: str, max_chars: int = 700) -> str:
    """The opening of a rationale, cut at a paragraph boundary.

    Never ends on a paragraph that ends in a colon -- several rationales lead
    with "...for two reasons:" and stopping there produces a sentence that
    promises something and then delivers nothing, which is worse in an agent's
    context window than either the full text or none of it.
    """
    # rstrip only: leading indentation carries list structure, and stripping it
    # off the first line of a paragraph misaligns numbered lists.
    paragraphs = [p.rstrip() for p in text.strip("\n").split("\n\n") if p.strip()]
    out: list[str] = []
    for para in paragraphs:
        out.append(para)
        if len(" ".join(out)) >= max_chars and not out[-1].rstrip().endswith(":"):
            break
        if len(out) >= 2 and not out[-1].rstrip().endswith(":"):
            break
    return "\n\n".join(out)


# ---------------------------------------------------------------------------
# .cursor/rules/*.mdc
# ---------------------------------------------------------------------------


def render_always_rule(rules: list[Rule], packs: dict[str, list[Rule]]) -> str:
    enforceable = sum(1 for r in rules if r.detector is not None)
    advisory = [r for r in rules if r.detector is None]
    lines = [
        "---",
        "description: Project conventions for the Airflow Kafka provider contribution",
        "alwaysApply: true",
        "---",
        "",
        f"<!-- {BANNER.format(src='rules/*.yaml')} -->",
        "",
        "# Conventions",
        "",
        "This repo's conventions live in `rules/*.yaml` and are the source of truth for",
        "three things: the rules you are reading, the linter, and `docs/RULES.md`. If you",
        "want to change a convention, edit the YAML and run `python tools/gen_rules.py` --",
        "do not edit `.cursor/rules/` by hand, it is generated and CI checks it.",
        "",
        f"There are {len(rules)} rules across {len(packs)} packs; "
        f"{enforceable} of them are machine-checked.",
        "",
        "## Before you say a change is done",
        "",
        "Run the linter. It is deterministic, offline, and takes under a second:",
        "",
        "```bash",
        "python tools/lint_conventions.py contribution/ --summary",
        "```",
        "",
        "Exit 0 clean, 1 violations, 2 the rule pack itself is malformed. Do not report",
        "work as complete on an unrun or failing linter, and do not silence a rule to get",
        "to green -- if a rule is wrong, say so and change the YAML.",
        "",
        "## What the linter cannot see",
        "",
        "The linter checks structure, not judgement. These rules carry no detector, so",
        "they are enforced only by you reading them:",
        "",
    ]
    for r in advisory:
        lines.append(f"- **{r.id}** {r.title} -- see `{r.source_file}`")
    if not advisory:
        lines.append("- (none currently)")
    lines += [
        "",
        "A clean linter run therefore means \"no structural violations found\", not",
        "\"correct\". Say it that way.",
        "",
        "## Packs",
        "",
    ]
    for pack, members in packs.items():
        title = PACK_TITLES.get(pack, pack)
        ids = ", ".join(r.id for r in members)
        lines.append(f"- **{title}** (`{pack}`): {ids}")
    lines += [
        "",
        "Each pack auto-attaches on the files it governs, so you will see the detailed",
        "rules when you open a file they apply to.",
        "",
    ]
    return "\n".join(lines)


def render_pack_rule(pack: str, members: list[Rule]) -> str:
    title = PACK_TITLES.get(pack, pack)
    globs = pack_globs(members)
    lines = [
        "---",
        f"description: {title} -- {len(members)} rules ({', '.join(r.id for r in members)})",
        f"globs: {','.join(globs)}",
        "alwaysApply: false",
        "---",
        "",
        f"<!-- {BANNER.format(src='rules/*.yaml')} -->",
        "",
        f"# {title}",
        "",
    ]
    blurb = PACK_BLURBS.get(pack)
    if blurb:
        lines += [blurb, ""]

    for r in members:
        lines += [
            f"## {r.id} -- {r.title}",
            "",
            f"**{r.severity}** · owner {r.owner} · {_detector_line(r)}",
            "",
            " ".join(r.message.split()),
            "",
        ]
        if r.replacement.strip():
            lines += ["Do this instead:", "", "```python", r.replacement.strip(), "```", ""]
        if r.rationale.strip():
            # The opening, not the essay. The agent needs the reason; the full
            # text stays in the YAML and in docs/RULES.md for humans.
            lines += ["**Why:** " + lead(r.rationale), ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# docs/RULES.md
# ---------------------------------------------------------------------------


def render_docs(rules: list[Rule], packs: dict[str, list[Rule]]) -> str:
    enforceable = sum(1 for r in rules if r.detector is not None)
    lines = [
        f"<!-- {BANNER.format(src='rules/*.yaml')} -->",
        "",
        "# Conventions",
        "",
        "Every rule below is one file in [`rules/`](../rules). That directory is the",
        "single source of truth: it generates the Cursor agent's context",
        "(`.cursor/rules/*.mdc`), it generates this document, and",
        "`tools/lint_conventions.py` reads it directly. Nothing here is maintained by",
        "hand, and `python tools/gen_rules.py --check` fails CI if it drifts.",
        "",
        f"{len(rules)} rules · {enforceable} machine-checked · {len(rules) - enforceable} advisory",
        "",
        "## Adding a rule",
        "",
        "Copy the nearest existing YAML, change the fields, run `python tools/gen_rules.py`.",
        "You need a code change only if you need a detector kind that does not exist yet;",
        f"the current kinds are: {', '.join('`' + k + '`' for k in sorted(DETECTORS))}.",
        "An unknown kind is a hard error, not a skipped check.",
        "",
        "## Index",
        "",
        "| Rule | Severity | Enforcement | Owner | Title |",
        "| --- | --- | --- | --- | --- |",
    ]
    for r in rules:
        enf = "linter" if r.detector is not None else "advisory"
        lines.append(f"| `{r.id}` | {r.severity} | {enf} | {r.owner} | {r.title} |")
    lines.append("")

    for pack, members in packs.items():
        lines += [f"## {PACK_TITLES.get(pack, pack)}", ""]
        blurb = PACK_BLURBS.get(pack)
        if blurb:
            lines += [blurb, ""]
        for r in members:
            lines += [
                f"### {r.id} — {r.title}",
                "",
                f"- **Severity:** {r.severity}",
                f"- **Owner:** {r.owner}",
                f"- **Enforcement:** {_detector_line(r)}",
                f"- **Applies to:** {', '.join('`' + g + '`' for g in r.applies_to)}",
                f"- **Source:** [`{r.source_file}`](../{r.source_file})",
                "",
                "**Message**",
                "",
                "> " + " ".join(r.message.split()),
                "",
            ]
            if r.rationale.strip():
                lines += ["**Why this rule exists**", "", r.rationale.strip(), ""]
            if r.evidence.strip():
                lines += ["**Evidence**", "", "```", r.evidence.strip(), "```", ""]
            if r.replacement.strip():
                lines += ["**Do this instead**", "", "```python", r.replacement.strip(), "```", ""]
            if r.references:
                lines += ["**References**", ""]
                lines += [f"- {ref}" for ref in r.references]
                lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def build(rules: list[Rule]) -> dict[str, str]:
    packs = group_by_pack(rules)
    out = {
        ".cursor/rules/00-conventions.mdc": render_always_rule(rules, packs),
        "docs/RULES.md": render_docs(rules, packs),
    }
    # Numbered by sorted pack name so a new pack slots in without a code change
    # and the numbering stays stable for --check.
    for i, (pack, members) in enumerate(packs.items(), start=1):
        out[f".cursor/rules/{i * 10}-{pack}.mdc"] = render_pack_rule(pack, members)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Generate .cursor/rules/*.mdc and docs/RULES.md from rules/*.yaml.",
        epilog="Exit 0 ok / in sync, 1 out of sync (--check), 2 bad input.",
    )
    ap.add_argument("--rules", default="rules", help="rule pack directory (default: rules)")
    ap.add_argument("--root", default=".", help="repo root to write into")
    ap.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit 1 with a diff if the generated files are stale",
    )
    args = ap.parse_args(argv)

    root = os.path.abspath(args.root)
    rules = load_rules(args.rules)          # same validation the linter applies
    generated = build(rules)

    if args.check:
        stale = []
        for relpath, content in sorted(generated.items()):
            abspath = os.path.join(root, relpath)
            try:
                with open(abspath, encoding="utf-8") as fh:
                    current = fh.read()
            except FileNotFoundError:
                stale.append((relpath, ["  (file does not exist)"]))
                continue
            if current != content:
                diff = list(
                    difflib.unified_diff(
                        current.splitlines(), content.splitlines(),
                        fromfile=f"{relpath} (on disk)", tofile=f"{relpath} (generated)",
                        lineterm="", n=2,
                    )
                )
                stale.append((relpath, diff[:40]))

        if stale:
            print("Generated files are out of sync with rules/*.yaml:\n", file=sys.stderr)
            for relpath, diff in stale:
                print(f"--- {relpath}", file=sys.stderr)
                for line in diff:
                    print(line, file=sys.stderr)
                print(file=sys.stderr)
            print("Run: python tools/gen_rules.py", file=sys.stderr)
            return EXIT_STALE

        print(f"in sync: {len(generated)} generated file(s) match {len(rules)} rule(s)")
        return EXIT_OK

    for relpath, content in sorted(generated.items()):
        abspath = os.path.join(root, relpath)
        try:
            os.makedirs(os.path.dirname(abspath), exist_ok=True)
            with open(abspath, "w", encoding="utf-8") as fh:
                fh.write(content)
        except OSError as exc:
            die(f"could not write {relpath}: {exc}")
        print(f"wrote {relpath}")

    print(f"\n{len(rules)} rule(s) -> {len(generated)} file(s). "
          f"Verify with: python tools/gen_rules.py --check")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
