---
name: scaffold-operator
description: Scaffold a new Airflow provider operator across all ten touchpoints — module, provider.yaml registration, docs, mirrored unit test, integration test, example DAG, newsfragment, version compat. Use when adding an operator, hook, sensor, or trigger to the Kafka provider, or when a contribution is failing upstream pre-commit for reasons unrelated to its logic.
---

# Scaffolding a provider contribution

## The problem this solves

Adding one operator to an Airflow provider touches **ten** files. Nothing in the
operator module you are editing mentions nine of them. A new engineer writes
perfectly good code, tests pass locally, and CI rejects the PR for reasons that
have nothing to do with the code they wrote — most often `provider.yaml`, which
is coupled to the module tree only through a pre-commit hook.

Cost of learning this by trial: roughly a day per person, spread over three or
four review round-trips. That is the ramp cost this skill removes.

## The ten touchpoints

Work through these in order. Do not skip ahead to the tests — 3 and 4 are the
ones that fail CI, and they are cheapest to do while the module is fresh.

| # | File | Why it exists |
| --- | --- | --- |
| 1 | `hooks/client.py` | Extend `KafkaAdminClientHook` if you need a new admin call. |
| 2 | `src/.../operators/<name>.py` | The operator itself. Template below. |
| 3 | `provider.yaml` | **Registration.** Least discoverable, most likely to be missed. `PROV-004`. |
| 4 | `docs/operators/index.rst` | Docs build fails on an undocumented operator. |
| 5 | `tests/unit/apache/kafka/operators/test_<name>.py` | Mirrored path, exactly. `TEST-001`. |
| 6 | `tests/integration/...` | Against a live broker. Must clean up its topics — `TEST-002`. |
| 7 | `tests/system/.../example_<name>.py` | The example DAG. Doubles as the docs example. |
| 8 | `newsfragments/<pr>.significant.rst` | Changelog. |
| 9 | `version_compat.py` | Only if you touched an API that moved between Airflow versions. |
| 10 | pre-commit | `pre-commit run --all-files`. Runs the provider.yaml/module-tree consistency check. |

## Procedure

1. **Read the nearest existing operator first** — `operators/consume.py`. Match
   its shape. But do not copy blindly: `TEST-002` exists because the nearest
   existing integration test leaks topics, and the scaffold must not imitate a
   defect just because it is adjacent.

2. **Write the operator** from `assets/operator_template.py`. Non-negotiable:
   - ASF licence header (`PROV-001`)
   - `from __future__ import annotations` (`PROV-002`)
   - `template_fields` listing **every** parameter a DAG author might template
     (`PROV-003`) — see the warning below
   - `:param:` docstrings for every argument; the docs build reads them

3. **Register it in `provider.yaml`** (`PROV-004`). Add the dotted module path
   under `operators: → python-modules:`. This is step 3 and not step 8 because
   it is the one that gets forgotten.

4. **Mirror the test path exactly** (`TEST-001`). A unit test one directory off
   is silently never collected — the suite stays green and the code is untested.

5. **Reuse the existing hook.** Call `KafkaAdminClientHook` (or extend it in
   `hooks/client.py`) rather than opening a second client inside the operator.
   Two clients means two config paths, and they quietly diverge.

6. **Verify before you report:**

   ```bash
   python tools/lint_conventions.py contribution/ --summary
   pre-commit run --all-files
   ```

   Say "no structural violations found", not "correct". The linter checks
   structure; it cannot tell you the logic is right.

## The `template_fields` trap

This is the one that silently destroys the feature rather than breaking the
build. The entire argument for `ExampleOperator` is:

```python
target="{{ ds }}"    # rendered by the Airflow scheduler
```

If `target` is not in `template_fields`, the operator receives the literal
string `"{{ ds }}"`. **Nothing errors.** The task just does the wrong thing.
A mocked unit test cannot see it, and it only surfaces under a real scheduler.
Hence `PROV-003` is a static rule rather than a review comment.

## Assets

- `assets/operator_template.py` — operator skeleton, all four static rules satisfied
- `assets/test_template.py` — unit test skeleton at the mirrored path
- `assets/integration_test_template.py` — integration test with teardown in a fixture

## What this skill does not do

It does not write your `execute()` logic, choose your parameter names, or judge
whether the operator should exist. It gets the ten touchpoints right so review
can be about the thing that actually needs a human.
