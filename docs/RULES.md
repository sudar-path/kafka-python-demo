<!-- GENERATED FILE -- DO NOT EDIT. Source: rules/*.yaml. Regenerate: python tools/gen_rules.py -->

# Conventions

Every rule below is one file in [`rules/`](../rules). That directory is the
single source of truth: it generates the Cursor agent's context
(`.cursor/rules/*.mdc`), it generates this document, and
`tools/lint_conventions.py` reads it directly. Nothing here is maintained by
hand, and `python tools/gen_rules.py --check` fails CI if it drifts.

10 rules · 9 machine-checked · 1 advisory

## Adding a rule

Copy the nearest existing YAML, change the fields, run `python tools/gen_rules.py`.
You need a code change only if you need a detector kind that does not exist yet;
the current kinds are: `forbidden_call`, `forbidden_kwarg`, `import_location`, `paired_call`, `path_mirror`, `registry_sync`, `require_class_attr`, `require_header`, `require_import`.
An unknown kind is a hard error, not a skipped check.

## Index

| Rule | Severity | Enforcement | Owner | Title |
| --- | --- | --- | --- | --- |
| `KAFKA-001` | error | linter | @sudar-path | Use list_consumer_groups(), never the deprecated list_groups() |
| `KAFKA-002` | error | linter | @sudar-path | confluent-kafka admin types are split across two modules; import from the right one |
| `KAFKA-003` | error | linter | @sudar-path | Never pass topic= to list_topics() -- it CREATES the topic |
| `KAFKA-004` | advisory | advisory | @sudar-path | A kafka connection carrying group.id makes every admin call emit CONFWARN |
| `PROV-001` | error | linter | @sudar-path | Every Python file needs the ASF licence header |
| `PROV-002` | error | linter | @sudar-path | Every module starts with `from __future__ import annotations` |
| `PROV-003` | error | linter | @sudar-path | Operators must declare template_fields |
| `PROV-004` | error | linter | @sudar-path | A new operator module must be registered in provider.yaml |
| `TEST-001` | error | linter | @sudar-path | Test files mirror the src path exactly |
| `TEST-002` | error | linter | @sudar-path | Integration tests that create topics must delete them |

## Kafka admin client API

Rules about confluent-kafka's AdminClient surface. These exist because the API has correct-looking calls that are deprecated, calls that mutate the broker despite reading as reads, and symbols split across two modules with no hint at the call site.

### KAFKA-001 — Use list_consumer_groups(), never the deprecated list_groups()

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (forbidden_call: attr='list_groups')
- **Applies to:** `providers/apache/kafka/**/*.py`, `contribution/**/*.py`, `tools/**/*.py`
- **Source:** [`rules/KAFKA-001-deprecated-list-groups.yaml`](../rules/KAFKA-001-deprecated-list-groups.yaml)

**Message**

> list_groups() is deprecated. Use admin.list_consumer_groups() to enumerate and admin.describe_consumer_groups() to inspect.

**Why this rule exists**

This is the rule the whole pack exists to justify, because it is invisible to
every check a normal team already runs:

  1. It is the name a model reaches for -- shortest, most obvious, and it is
     what every pre-2.0 example on the internet uses.
  2. It WORKS. Against a real broker it returns real data. Nothing fails.
  3. Its docstring says nothing about deprecation, so reading the source or
     the API reference tells you nothing is wrong.
  4. The provider's own unit tests for this hook are mock-based, so a mocked
     test stays green forever.
  5. The deprecation is observable ONLY by making a live call and catching a
     DeprecationWarning at runtime.

Docs, code review and the existing test suite all miss it simultaneously.
That is the class of defect worth automating. The ones a careful reviewer
already catches are not.

**Evidence**

```
Runtime probe, INT VM, confluent-kafka 2.6.0 (the declared floor), live
broker, 2026-08-15:

    admin.list_groups(timeout=5)
    -> DeprecationWarning: list_groups() is deprecated, use
       list_consumer_groups() and describe_consumer_groups() instead.

    list_groups.__doc__ mentions "deprecat*":  False
```

**Do this instead**

```python
# before
groups = admin.list_groups(timeout=10)

# after
listing = admin.list_consumer_groups(request_timeout=10).result()
described = admin.describe_consumer_groups(
    [g.group_id for g in listing.valid], request_timeout=10
)
```

**References**

- handoff.md §4 Finding 1 / §4a (the demo closer)
- confluent-kafka >= 2.0.2 ships both replacements; the 2.6.0 floor is fine

### KAFKA-002 — confluent-kafka admin types are split across two modules; import from the right one

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (import_location: symbols=7 entries)
- **Applies to:** `providers/apache/kafka/**/*.py`, `contribution/**/*.py`, `tools/**/*.py`
- **Source:** [`rules/KAFKA-002-split-module-imports.yaml`](../rules/KAFKA-002-split-module-imports.yaml)

**Message**

> This symbol is not exported by the module you imported it from. The two modules are not interchangeable and neither re-exports the other.

**Why this rule exists**

A single list_offsets() call needs TopicPartition (top level) and OffsetSpec
(admin) -- from OPPOSITE modules. Because AdminClient lives in
confluent_kafka.admin, the co-located import is the natural guess for every
admin-shaped type, and for half of them it is wrong.

ConsumerGroupTopicPartitions is the sharpest case: it reads like an admin
type, it appears in admin call signatures, and it is top-level only.

Cheap, deterministic, high hit rate. An ImportError is a fast failure rather
than a subtle one, but it costs a new engineer a build cycle every time, and
it is exactly the kind of thing that is nowhere in the docs.

**Evidence**

```
Confirmed identical on confluent-kafka 2.6.0 (INT) and 2.15.0 (STAG
container), 2026-08-15 -- so this is the API's shape, NOT version skew:

    from confluent_kafka.admin import ConsumerGroupTopicPartitions  # ImportError
    from confluent_kafka        import ConsumerGroupTopicPartitions  # correct
```

**Do this instead**

```python
from confluent_kafka import (
    OFFSET_INVALID, ConsumerGroupTopicPartitions, TopicPartition,
)
from confluent_kafka.admin import AdminClient, OffsetSpec
```

**References**

- handoff.md §4 Finding 3
- tools/offset_diff.py -- module docstring, 'API note, the hard-won kind'

### KAFKA-003 — Never pass topic= to list_topics() -- it CREATES the topic

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (forbidden_kwarg: attr='list_topics', kwarg='topic')
- **Applies to:** `providers/apache/kafka/**/*.py`, `contribution/**/*.py`, `tools/**/*.py`
- **Source:** [`rules/KAFKA-003-list-topics-autocreates.yaml`](../rules/KAFKA-003-list-topics-autocreates.yaml)

**Message**

> admin.list_topics(topic=X) issues a metadata request that NAMES X, and a broker with auto.create.topics.enable=true (Kafka's default) creates it. Fetch cluster-wide metadata and check membership instead.

**Why this rule exists**

The failure is invisible from the tool's own output. list_topics(topic=X)
races the creation it triggers, so the call still reports "not found" -- you
get the correct answer AND a brand new topic. Nothing in stdout gives it away.

This matters most in exactly the place it is most tempting: an existence
check on the read-only side of a promotion gate. A dry run that creates
topics on a production broker is not a dry run, and the policy gate cannot
catch it either, because the diff artifact of a failed lookup is empty.

The replacement is heavier on a broker with thousands of topics. That is the
correct trade for a tool whose entire contract is "mutates nothing".

**Evidence**

```
Found on INT 2026-08-15 by noticing `no.such` and `no.such.topic` on the
broker -- topics nothing seeds, left behind by offset_diff.py's own
error-path testing. Reproduced deliberately before fixing:

    $ offset_diff.py --topic canary.should.not.exist --to-latest
    topic 'canary.should.not.exist' not found on localhost:9092    # exit 1, correct
    $ kafka-topics.sh --list
    __consumer_offsets
    canary.should.not.exist          # <-- created by the "read-only" tool
    payments.events

Verified absent after the fix.
```

**Do this instead**

```python
# before -- creates `topic` as a side effect
meta = admin.list_topics(topic=topic, timeout=timeout)
if topic not in meta.topics:
    die(...)

# after -- never names the topic, so nothing can be auto-created
meta = admin.list_topics(timeout=timeout)
if topic not in meta.topics or meta.topics[topic].error is not None:
    die(...)
```

**References**

- handoff.md §14 -- 'a read-only tool that wasn't'
- tools/offset_diff.py:198-216 -- the fix, with the reasoning kept inline

### KAFKA-004 — A kafka connection carrying group.id makes every admin call emit CONFWARN

- **Severity:** advisory
- **Owner:** @sudar-path
- **Enforcement:** not machine-checkable -- advisory, this text is the only enforcement
- **Applies to:** `providers/apache/kafka/**/*.py`, `contribution/**/*.py`
- **Source:** [`rules/KAFKA-004-admin-hook-group-id.yaml`](../rules/KAFKA-004-admin-hook-group-id.yaml)

**Message**

> KafkaAdminClientHook builds on a producer handle and passes the whole connection `extra` through, so a shared kafka_default connection that carries group.id (which the consumer path needs) warns on every admin call.

**Why this rule exists**

This is a wart, not a defect -- and saying so is part of the rule. Nothing
breaks; the warning is noise. It matters for two reasons:

  1. Operator design. A new admin operator should not require its own
     connection just to silence a warning, but it also should not surprise
     an operator with rdkafka noise in every task log. Decide deliberately.
  2. It is honest PR material. "Here is a wart we found while building"
     reads better in an upstream contribution than pretending the surface
     was clean.

A detector is possible in principle (parse Airflow Connection extras and
flag group.id where the connection is used by an admin hook) but it would
need to resolve connection usage across files to avoid false positives on
the consumer path, which legitimately needs group.id. Not worth it yet.
Revisit if it bites someone a second time.

**Evidence**

```
Observed against the live STAG broker, 2026-08-15, running
KafkaAdminClientHook through the kafka_default connection:

    CONFWARN|rdkafka#producer-1|: Configuration property group.id is a
    consumer property and will be ignored by this producer instance
```

**References**

- handoff.md §4 Finding 4
- handoff.md §9 -- 'one of the three is a wart rather than a defect', say so

## Airflow provider conventions

Rules about the shape of an Airflow provider contribution. These are the touchpoints that upstream pre-commit and CI enforce but that nothing in the file you are editing mentions.

### PROV-001 — Every Python file needs the ASF licence header

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (require_header: contains='Licensed to the Apache Software Foundation (ASF) under one')
- **Applies to:** `providers/apache/kafka/**/*.py`, `contribution/**/*.py`
- **Source:** [`rules/PROV-001-asf-licence-header.yaml`](../rules/PROV-001-asf-licence-header.yaml)

**Message**

> Missing the ASF licence header. Upstream's pre-commit rejects the file and the PR never reaches a human reviewer.

**Why this rule exists**

Touchpoint 2 of 10. Mechanical, invisible, and it costs a full CI round trip
the first time -- which is precisely the ramp tax this pack exists to remove.
A new engineer copying the nearest existing file usually gets this right by
accident; a model generating a file from scratch usually does not.

Cheap to detect, zero false positives, high frequency. This is the boring
half of the pack and the boring half is most of the value.

**Evidence**

```
apache/airflow @ tag 3.3.1: every .py file under providers/apache/kafka/
carries the header, and .pre-commit-config.yaml enforces it repo-wide.
```

**Do this instead**

```python
# Copy the 16-line header verbatim from any existing file in the subtree,
# e.g. providers/apache/kafka/src/airflow/providers/apache/kafka/hooks/client.py
```

**References**

- handoff.md §3 -- the 10 touchpoints, #2 and #10

### PROV-002 — Every module starts with `from __future__ import annotations`

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (require_import: module='__future__', name='annotations')
- **Applies to:** `providers/apache/kafka/**/*.py`, `contribution/**/*.py`, `tools/**/*.py`
- **Source:** [`rules/PROV-002-future-annotations.yaml`](../rules/PROV-002-future-annotations.yaml)

**Message**

> Missing `from __future__ import annotations`. Upstream requires it in every module, and without it the `X | None` annotations used throughout the provider fail at import time on the supported Python floor.

**Why this rule exists**

Touchpoint 2. This is a convention with teeth: the provider annotates with
PEP 604 unions (`int | None`) in function signatures, which are evaluated at
definition time unless annotations are postponed. Omit the import and the
module raises TypeError on import rather than failing a lint.

Worth knowing that the failure mode is an import error in an unrelated test,
not a message naming the missing import.

**Evidence**

```
Present in every module under providers/apache/kafka/ at tag 3.3.1, and in
all four tools in this repo.
```

**Do this instead**

```python
from __future__ import annotations
```

**References**

- handoff.md §3 -- touchpoint 2

### PROV-003 — Operators must declare template_fields

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (require_class_attr: base_suffix='Operator', attr='template_fields')
- **Applies to:** `providers/apache/kafka/**/operators/*.py`, `contribution/**/operators/*.py`
- **Source:** [`rules/PROV-003-operator-template-fields.yaml`](../rules/PROV-003-operator-template-fields.yaml)

**Message**

> This Operator subclass does not declare `template_fields`. Without it, Jinja templating is silently disabled for every parameter on the operator.

**Why this rule exists**

Touchpoint 2, and the one that decides whether the contribution is worth
making at all.

The whole argument for ExampleOperator is this DAG:

    rewind = ExampleOperator(
        task_id="rewind",
        group_id="etl_orders",
        topics=["orders"],
        to_timestamp="{{ data_interval_start }}",   # <-- templated
    )

That `{{ data_interval_start }}` is what makes Kafka backfillable from
Airflow. If `to_timestamp` is not in template_fields, the operator receives
the literal string "{{ data_interval_start }}" and the feature does not
exist. Nothing errors -- the seek just goes somewhere absurd.

Failing silently and only under a real scheduler is why this needs to be a
static rule: a mocked unit test cannot see it, and STAG is the only
environment where it would surface.

**Evidence**

```
ConsumeFromTopicOperator at tag 3.3.1 declares:

    template_fields = ("topics", "apply_function_args",
                       "apply_function_kwargs", "kafka_config_id")

So the convention is already established in the subtree the contribution
slots into -- the new operator has to match it, not invent it.
```

**Do this instead**

```python
class ExampleOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "group_id", "topics", "to_timestamp", "kafka_config_id",
    )
```

**References**

- handoff.md §3 -- the punchline DAG, and touchpoint 2

### PROV-004 — A new operator module must be registered in provider.yaml

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (registry_sync: registry='providers/apache/kafka/provider.yaml', src_root='providers/apache/kafka/src', section='operators')
- **Applies to:** `providers/apache/kafka/src/**/operators/*.py`, `contribution/**/src/**/operators/*.py`
- **Source:** [`rules/PROV-004-provider-yaml-registration.yaml`](../rules/PROV-004-provider-yaml-registration.yaml)

**Message**

> This operator module is not listed in provider.yaml's operator registry. Upstream's pre-commit fails when the module tree and the registry disagree.

**Why this rule exists**

Touchpoint 3, and the single least discoverable one. Nothing in the operator
file references provider.yaml, and nothing in provider.yaml references the
file -- the coupling exists only in a pre-commit hook. A new engineer writes
a perfectly good operator, tests pass locally, and CI rejects it for a
reason that has nothing to do with the code they wrote.

It is also the touchpoint an agent is most likely to miss, because the
operator file it is editing contains no hint that a second file exists.

Registration is not cosmetic: the registry is what makes the operator
discoverable in the Airflow UI and in `airflow providers get`.

**Evidence**

```
providers/apache/kafka/provider.yaml at tag 3.3.1 lists exactly two operator
modules -- operators.consume and operators.produce -- which is also the
evidence for the gap this contribution fills: there is no admin operator at
all, and topic admin is hook-only.
```

**Do this instead**

```python
# provider.yaml
operators:
  - integration-name: Apache Kafka
    python-modules:
      - airflow.providers.apache.kafka.operators.consume
      - airflow.providers.apache.kafka.operators.produce
      - airflow.providers.apache.kafka.operators.example_operator   # <-- add
```

**References**

- handoff.md §3 -- touchpoint 3, and the gap being filled

## Test conventions

Rules about where tests live and what they must clean up. Both are structural: a test in the wrong directory is silently never collected, and a leaked topic makes the next run's failure someone else's problem.

### TEST-001 — Test files mirror the src path exactly

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (path_mirror: src_root='providers/apache/kafka/src/airflow/providers/apache/kafka', test_root='providers/apache/kafka/tests/unit/apache/kafka', prefix='test_')
- **Applies to:** `providers/apache/kafka/src/**/operators/*.py`, `providers/apache/kafka/src/**/hooks/*.py`, `contribution/**/src/**/operators/*.py`, `contribution/**/src/**/hooks/*.py`
- **Source:** [`rules/TEST-001-mirrored-test-path.yaml`](../rules/TEST-001-mirrored-test-path.yaml)

**Message**

> No mirrored unit test found. Upstream's layout requires tests/unit/apache/kafka/<subpackage>/test_<module>.py for every src module.

**Why this rule exists**

Touchpoint 5. The mirror is not a style preference -- pytest collection and
upstream's coverage tooling both assume it, and a test placed one directory
off is silently never run. "Silently never run" is the worst outcome
available here: the suite is green and the code is untested.

This is also the rule that makes "strengthen tests" a structural check
rather than a judgement call. It cannot tell you a test is GOOD. It can tell
you a test EXISTS in the place the tooling will look, which is the
precondition for everything else.

**Evidence**

```
Verified against the layout at tag 3.3.1:

    src/airflow/providers/apache/kafka/hooks/client.py
    -> tests/unit/apache/kafka/hooks/test_client.py

79 unit tests collect from that tree on INT. A file outside it contributes
zero.
```

**Do this instead**

```python
# for contribution/.../operators/example_operator.py, create:
providers/apache/kafka/tests/unit/apache/kafka/operators/test_example_operator.py
```

**References**

- handoff.md §3 -- touchpoint 5
- handoff.md §5a -- 79 unit tests green at the floor

### TEST-002 — Integration tests that create topics must delete them

- **Severity:** error
- **Owner:** @sudar-path
- **Enforcement:** enforced by lint_conventions.py (paired_call: requires='create_topic', pair='delete_topic')
- **Applies to:** `providers/apache/kafka/tests/integration/**/*.py`, `contribution/**/tests/integration/**/*.py`
- **Source:** [`rules/TEST-002-integration-topic-teardown.yaml`](../rules/TEST-002-integration-topic-teardown.yaml)

**Message**

> This module creates topics and never deletes them. On a shared broker the leftovers accumulate and test runs begin interfering with each other.

**Why this rule exists**

This is a defect in the suite the new engineer is about to copy conventions
from, which makes it the most useful kind of rule: it stops the scaffold
imitating the nearest existing file when the nearest existing file is wrong.

It is harmless today only because the leaked topics happen to be idempotent
against their own leftovers -- reruns stayed green. That is luck, not
design, and it stops being true the moment two runs overlap. int-tests.yml
carries both a `concurrency:` group and an `if: always()` cleanup step to
contain the blast radius, but containment is not a fix.

Directly serves the QA audience: this is the concrete "weak test" that
/harden-tests targets, and the argument for generating fixtures with
teardown rather than copying.

**Evidence**

```
Observed on INT after a full integration run, 2026-08-15. Broker retained:

    operator.consumer.test.integration.test_{1,2,3}
    operator.producer.test.integration.test_{1,2}
    trigger.await_message.test.integration.test_1

Only test_admin_client.py cleans up after itself -- it calls
hook.delete_topic. The operator and trigger tests create and abandon.
```

**Do this instead**

```python
@pytest.fixture
def topic(hook):
    name = f"operator.reset.test.integration.{uuid4().hex[:8]}"
    hook.create_topic(topics=[(name, 3, 1)])
    yield name
    hook.delete_topic(topics=[name])     # runs even if the test fails
```

**References**

- handoff.md §5a Finding 5
- .github/workflows/int-tests.yml -- the cleanup step this rule makes unnecessary
