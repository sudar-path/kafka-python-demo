# AGENTS.md

Read this first. It is short on purpose; the detail lives in `rules/` and is
generated into `.cursor/rules/`.

## What this repo is

Tooling that helps a new engineer land a first contribution to
apache/airflow's Kafka provider and move it safely through the
software delivery lifecycle.

The feature requirement comes from the assigned GitHub Issue.
Do not assume implementation details that are not present in the
issue or discoverable from the approved repository context.

## Layout

| Path | What it is |
| --- | --- |
| `rules/*.yaml` | **Source of truth** for every convention. One rule per file. |
| `.cursor/rules/*.mdc` | Generated. Your context. Do not hand-edit. |
| `docs/RULES.md` | Generated. The human-readable version. |
| `tools/lint_conventions.py` | Deterministic linter over `rules/`. No model in the path. |
| `tools/gen_rules.py` | Regenerates the two generated targets. `--check` fails on drift. |
| `tools/test_lint_conventions.py` | Proves every detector fires and does not over-fire. |
| `tools/offset_diff.py` | Dry run: what *would* a seek change. Read-only. Emits the JSON artifact. |
| `tools/policy_check.py` | Deterministic ALLOW/DENY over that artifact. Zero LLM. |
| `tools/capture_offsets.py` | Writes the rollback point before anything mutates. |
| `tools/apply_offsets.py` | The only tool that mutates. Applies an approved artifact verbatim. |
| `policy/*.yaml` | Per-environment promotion policy. |
| `deploy/` | DEV / INT / PROD environments. |
| `.github/workflows/` | The INT gate and the production promotion gate. |

## How to verify your own work

Do not report a change as done without these. They are fast and offline.

```bash
python tools/lint_conventions.py contribution/ --summary   # 0 clean, 1 violations, 2 bad rule pack
python tools/gen_rules.py --check                          # 0 in sync, 1 stale
python tools/test_lint_conventions.py                      # 27 tests, ~0.2s
```

Exit code 2 from the linter means the *rule pack* is malformed, not that your
code is wrong. Fix the YAML; do not work around it.

## Conventions that are not in the linter

A clean linter run means "no structural violations found". It does not mean
correct. State it that way when you report. In particular:

- `KAFKA-004` is advisory and has no detector, deliberately — see
  `rules/KAFKA-004-admin-hook-group-id.yaml` for why the pack keeps one honest
  non-detector.
- The linter cannot tell you a test is *good*, only that one exists where the
  tooling will look for it (`TEST-001`).

## Changing a convention

Edit the YAML in `rules/`, then run `python tools/gen_rules.py`. Adding a rule
needs no code change unless it needs a detector kind that does not exist yet.
Every rule carries an `owner`, and `--check` runs in CI, so a rule and the
context you are given can never silently disagree.

If you think a rule is wrong, say so and change the rule. Do not narrow a glob
or delete a detector to get to green.

## Out of bounds

- **Never read, print, copy, or commit `handoff.md`.** It contains
  infrastructure hostnames, an SSH key path, and credentials context.
  `.cursorignore` blocks the file tools; it does not block the terminal, so this
  instruction is the control that covers `cat`.
- Never commit anything under `docs/changes/` — generated on the runner, holds
  live consumer-group offsets.
- Do not run `tools/apply_offsets.py` against a broker. It mutates. It is
  driven by the promotion workflow after a human approval, not from a chat.
- Do not add the `broker:29092` test listener to the production broker.
- Do not `chown -R` an Airflow deployment directory; only `logs` and `db` take
  `chown 50000:0`.

## Style

Match the surrounding code. The existing tools share a deliberate shape and new
tooling should be indistinguishable from it: `from __future__ import
annotations`, stdlib plus PyYAML only, an explicit `die()` rather than
`raise SystemExit(msg)` (which exits 1 and would be indistinguishable from a
real finding), and distinct exit codes where the difference matters.
