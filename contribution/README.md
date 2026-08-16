# `contribution/` — the PR, as a directory

This holds the change a new engineer is making to `apache/airflow`: an
operator that moves a Kafka consumer group's committed offsets. Every file sits
at the **exact path it would occupy upstream**, prefixed by `contribution/`:

```
contribution/providers/apache/kafka/
├── provider.yaml                                          # touchpoint 3
├── src/airflow/providers/apache/kafka/operators/
│   └── reset_offsets.py                                   # touchpoint 2
└── tests/
    ├── unit/apache/kafka/operators/test_reset_offsets.py  # touchpoint 5
    └── integration/test_reset_offsets_integration.py      # touchpoint 6
```

## Why the paths are mirrored rather than flattened

The convention rules in [rules/](../rules/) are written against upstream's
paths, because that is where the conventions actually live. Mirroring means one
rule governs both trees. The alternative — a flat `contribution/operator.py`
plus a second copy of every path-based rule to match it — produces two rule sets
that drift, and the one that drifts is the one nobody runs.

Two detectors resolve an overlay prefix to make this work: `path_mirror` and
`registry_sync` anchor on the last occurrence of their configured root, so
`contribution/providers/…/src/…` and `providers/…/src/…` are treated as the same
file. See `_split_overlay` in [tools/lint_conventions.py](../tools/lint_conventions.py).

The practical consequence: **TEST-001 and PROV-004 fire here.** Delete the unit
test and the linter says so. Remove the module from this `provider.yaml` and the
linter says so — without needing an airflow checkout on disk.

## What is real and what is a stub

| File | State |
| --- | --- |
| `reset_offsets.py` | Real. `execute()` runs end to end against a broker. |
| `provider.yaml` | An excerpt, not upstream's full file — the `operators:` section only, which is the part the PR edits. |
| `test_reset_offsets.py` | Real unit tests. No broker; the seek arithmetic is covered by `tools/` tests. |
| `test_reset_offsets_integration.py` | **Stubs.** `NotImplementedError`. Needs a broker; see below. |

The integration stubs are deliberately left failing rather than deleted. A
missing integration test is invisible; one that raises `NotImplementedError` is
a line item in a test report.

## Running it

Unit tests and the linter need neither a broker nor Airflow:

```bash
python tools/lint_conventions.py contribution/ --summary
```

The integration tests need a Kafka broker and the provider installed. There is
no broker on the authoring laptop (no Docker); they run on the INT VM.
