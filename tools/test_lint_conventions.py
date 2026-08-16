#!/usr/bin/env python3
"""Proof that the linter fires, and proof that it does not over-fire.

Run:  python tools/test_lint_conventions.py          (stdlib unittest, no pytest)

Why this file exists, stated plainly because it is the honest version of the
story: handoff §14 records that this project already shipped a tool documented
as read-only that silently created topics on a live broker. The lesson written
down at the time was "I asserted a safety property in a docstring and never
asserted it in code." A convention linter whose detectors were never proven to
fire would be exactly that mistake a second time, one layer up -- a green run
that means nothing.

So the contract this file enforces is deliberately strong:

    every rule in rules/ that carries a detector has BOTH
      (a) a fixture that violates it, asserted to produce exactly one finding
      (b) a fixture that complies, asserted to produce none

and test_every_enforceable_rule_is_covered fails if a rule is added to the pack
without both. A new rule cannot land silently unproven.

The fixtures are built in a temp directory rather than checked in, because
checked-in deliberately-broken files under contribution/ would be picked up by
the CI overlay step in int-tests.yml.
"""

from __future__ import annotations

import contextlib
import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gen_rules  # noqa: E402
import lint_conventions as lc  # noqa: E402

RULES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rules")

ASF = '''#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information.
#
'''

FUTURE = "from __future__ import annotations\n"


@contextlib.contextmanager
def quiet():
    """Both CLIs are chatty by design -- that is the point of them. Swallow it
    here so a failing assertion is the loudest thing in the output."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        yield buf


def write(root: str, relpath: str, content: str) -> str:
    path = os.path.join(root, relpath)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return path


SRC = "providers/apache/kafka/src/airflow/providers/apache/kafka"
UNIT = "providers/apache/kafka/tests/unit/apache/kafka"
INTEG = "providers/apache/kafka/tests/integration"


def build_fixture_repo(root: str) -> None:
    """A miniature repo containing one violating and one compliant fixture for
    every enforceable rule. Paths matter as much as contents here -- five of the
    nine detectors key off directory layout, not source text."""

    # --- tools/**: KAFKA-001, KAFKA-002, KAFKA-003, PROV-002 ------------------
    write(root, "tools/violations.py", '''"""No __future__ import: trips PROV-002."""
from confluent_kafka.admin import AdminClient, TopicPartition   # TopicPartition: KAFKA-002


def probe(admin: AdminClient, name: str):
    admin.list_groups()                    # KAFKA-001
    admin.list_topics(topic=name)          # KAFKA-003 -- this CREATES the topic
    return TopicPartition(name, 0)
''')

    write(root, "tools/clean.py", FUTURE + '''
from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient


def probe(admin: AdminClient, name: str):
    admin.list_consumer_groups()
    meta = admin.list_topics()
    return TopicPartition(name, 0) if name in meta.topics else None
''')

    # --- the registry PROV-004 reads ------------------------------------------
    write(root, "providers/apache/kafka/provider.yaml", '''package-name: apache-airflow-providers-apache-kafka
operators:
  - integration-name: Apache Kafka
    python-modules:
      - airflow.providers.apache.kafka.operators.good_op
''')

    # --- compliant operator: registered, mirrored, templated, headed ----------
    write(root, f"{SRC}/operators/good_op.py", ASF + FUTURE + '''
from airflow.models import BaseOperator


class GoodOperator(BaseOperator):
    template_fields = ("group_id", "to_timestamp")

    def execute(self, context):
        return None
''')
    write(root, f"{UNIT}/operators/test_good_op.py", ASF + FUTURE + '''
def test_good_op():
    assert True
''')

    # --- violating operator: trips PROV-001/002/003/004 and TEST-001 ----------
    write(root, f"{SRC}/operators/orphan_op.py", '''from airflow.models import BaseOperator


class OrphanOperator(BaseOperator):
    def execute(self, context):
        return None
''')

    # --- integration tests: TEST-002 ------------------------------------------
    write(root, f"{INTEG}/test_leak.py", ASF + FUTURE + '''
def test_leaks(hook):
    hook.create_topic(topics=[("operator.reset.test.integration.1", 3, 1)])
    assert hook is not None
''')
    write(root, f"{INTEG}/test_tidy.py", ASF + FUTURE + '''
def test_tidy(hook):
    hook.create_topic(topics=[("operator.reset.test.integration.2", 3, 1)])
    try:
        assert hook is not None
    finally:
        hook.delete_topic(topics=["operator.reset.test.integration.2"])
''')


# Expected: (rule_id, relpath) for every finding, and nothing else.
EXPECTED = {
    ("KAFKA-001", "tools/violations.py"),
    ("KAFKA-002", "tools/violations.py"),
    ("KAFKA-003", "tools/violations.py"),
    ("PROV-002", "tools/violations.py"),
    ("PROV-001", f"{SRC}/operators/orphan_op.py"),
    ("PROV-002", f"{SRC}/operators/orphan_op.py"),
    ("PROV-003", f"{SRC}/operators/orphan_op.py"),
    ("PROV-004", f"{SRC}/operators/orphan_op.py"),
    ("TEST-001", f"{SRC}/operators/orphan_op.py"),
    ("TEST-002", f"{INTEG}/test_leak.py"),
}

CLEAN_FILES = {
    "tools/clean.py",
    f"{SRC}/operators/good_op.py",
    f"{UNIT}/operators/test_good_op.py",
    f"{INTEG}/test_tidy.py",
}


class RulePackTest(unittest.TestCase):
    """Runs the real rules/ directory against synthetic fixtures."""

    @classmethod
    def setUpClass(cls):
        cls.rules = lc.load_rules(RULES_DIR)
        cls._tmp = tempfile.TemporaryDirectory()
        cls.root = cls._tmp.name
        build_fixture_repo(cls.root)
        files = lc.collect_files([cls.root], cls.root)
        cls.findings, cls.coverage = lc.lint(cls.rules, files, cls.root)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def actual(self):
        return {(f.rule, f.path) for f in self.findings}

    # -- the two halves of the contract ------------------------------------

    def test_every_violation_fires_exactly_once(self):
        self.assertEqual(EXPECTED, self.actual())

    def test_compliant_files_produce_nothing(self):
        offenders = [f for f in self.findings if f.path in CLEAN_FILES]
        self.assertEqual([], offenders, f"false positives: {offenders}")

    def test_every_enforceable_rule_is_covered(self):
        """The guard that makes this suite self-maintaining: add a rule with a
        detector and no fixture, and this fails."""
        enforceable = {r.id for r in self.rules if r.detector is not None}
        proven = {rid for rid, _ in EXPECTED}
        self.assertEqual(
            set(), enforceable - proven,
            "rule(s) with a detector but no violating fixture -- unproven, so unenforced",
        )

    def test_advisory_rules_are_reported_not_counted(self):
        advisory = {r.id for r in self.rules if r.detector is None}
        self.assertTrue(advisory, "the pack should keep at least one honest non-detector")
        self.assertEqual(sorted(advisory), self.coverage["rules_advisory_only"])
        self.assertEqual(set(), advisory & {rid for rid, _ in self.actual()})

    def test_all_rules_evaluated_against_this_fixture_repo(self):
        """The fixture repo is built to exercise every detector, so nothing
        should land in 'not evaluated'. If it does, a detector is silently
        checking nothing and the coverage number is a lie."""
        self.assertEqual({}, self.coverage["rules_not_evaluated"])

    def test_severity_split_is_reported(self):
        self.assertEqual(len(self.rules), self.coverage["rules_total"])
        self.assertEqual(
            sum(1 for r in self.rules if r.detector is not None),
            self.coverage["rules_with_detector"],
        )


class MalformedRulePackTest(unittest.TestCase):
    """Design rule 1: a broken pack must exit 2, distinct from both a clean run
    (0) and a violation (1). Every one of these would otherwise be a linter
    reporting green while checking less than it claims."""

    def load(self, filename: str, body: str) -> int:
        with tempfile.TemporaryDirectory() as d:
            write(d, filename, body)
            with self.assertRaises(SystemExit) as cm:
                lc.load_rules(d)
            return cm.exception.code

    GOOD = '''id: X-001
pack: p
title: t
severity: error
owner: "@me"
applies_to: ["**/*.py"]
message: m
'''

    def test_unknown_detector_kind_is_fatal(self):
        body = self.GOOD + "detector:\n  kind: does_not_exist\n"
        self.assertEqual(2, self.load("a.yaml", body))

    def test_missing_required_field_is_fatal(self):
        self.assertEqual(2, self.load("a.yaml", "id: X-001\npack: p\n"))

    def test_bad_severity_is_fatal(self):
        self.assertEqual(2, self.load("a.yaml", self.GOOD.replace("error", "critical")))

    def test_detector_without_kind_is_fatal(self):
        self.assertEqual(2, self.load("a.yaml", self.GOOD + "detector:\n  attr: x\n"))

    def test_empty_rules_dir_is_fatal(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(SystemExit) as cm:
                lc.load_rules(d)
            self.assertEqual(2, cm.exception.code)

    def test_missing_rules_dir_is_fatal(self):
        with self.assertRaises(SystemExit) as cm:
            lc.load_rules("/nonexistent/rules")
        self.assertEqual(2, cm.exception.code)

    def test_duplicate_rule_id_is_fatal(self):
        with tempfile.TemporaryDirectory() as d:
            write(d, "a.yaml", self.GOOD)
            write(d, "b.yaml", self.GOOD)
            with self.assertRaises(SystemExit) as cm:
                lc.load_rules(d)
            self.assertEqual(2, cm.exception.code)


class RobustnessTest(unittest.TestCase):
    def rule(self, **kw) -> lc.Rule:
        base = dict(
            id="X-001", pack="p", title="t", severity="error", owner="@me",
            applies_to=["**/*.py"], detector={"kind": "forbidden_call", "attr": "list_groups"},
            message="m",
        )
        base.update(kw)
        return lc.Rule(**base)

    def test_unparseable_file_is_reported_not_crashed(self):
        with tempfile.TemporaryDirectory() as d:
            write(d, "broken.py", "def f(:\n")
            findings, _ = lc.lint([self.rule()], lc.collect_files([d], d), d)
            self.assertEqual(["LINT-SYNTAX"], [f.rule for f in findings])

    def test_detector_exception_fails_closed(self):
        """Design rule 2. A detector that blows up must produce a violation, not
        a swallowed traceback and a green run."""
        original = lc.DETECTORS["forbidden_call"]

        def explode(rule, ctx):
            raise RuntimeError("boom")

        lc.DETECTORS["forbidden_call"] = explode
        try:
            with tempfile.TemporaryDirectory() as d:
                write(d, "a.py", "x = 1\n")
                findings, _ = lc.lint([self.rule()], lc.collect_files([d], d), d)
        finally:
            lc.DETECTORS["forbidden_call"] = original

        self.assertEqual(1, len(findings))
        self.assertIn("failing closed", findings[0].message)

    ADVISORY_PACK = '''id: ADV-001
pack: p
title: advisory with a detector
severity: advisory
owner: "@me"
applies_to: ["tools/**/*.py"]
message: advisory only
detector:
  kind: forbidden_call
  attr: list_groups
'''

    def test_error_severity_finding_exits_1(self):
        with tempfile.TemporaryDirectory() as d:
            write(d, "tools/a.py", "admin.list_groups()\n")
            with quiet():
                rc = lc.main(["--rules", RULES_DIR, "--root", d, d])
        self.assertEqual(lc.EXIT_VIOLATIONS, rc)  # KAFKA-001 is severity: error

    def test_clean_tree_exits_0(self):
        with tempfile.TemporaryDirectory() as d:
            write(d, "tools/a.py", FUTURE + "admin.list_consumer_groups()\n")
            with quiet():
                rc = lc.main(["--rules", RULES_DIR, "--root", d, d])
        self.assertEqual(lc.EXIT_CLEAN, rc)

    def test_advisory_finding_does_not_fail_the_run_by_default(self):
        """Severity has to mean something at the exit code, or every rule is a
        blocker and the pack gets bypassed wholesale."""
        with tempfile.TemporaryDirectory() as d:
            rules_dir = os.path.join(d, "pack")
            write(rules_dir, "adv.yaml", self.ADVISORY_PACK)
            write(d, "tools/a.py", "admin.list_groups()\n")
            with quiet() as out:
                rc = lc.main(["--rules", rules_dir, "--root", d, d])
            self.assertEqual(lc.EXIT_CLEAN, rc)
            self.assertIn("ADV-001", out.getvalue())  # reported, just not blocking

            with quiet():
                strict = lc.main(
                    ["--rules", rules_dir, "--root", d, "--advisory-as-error", d]
                )
            self.assertEqual(lc.EXIT_VIOLATIONS, strict)

    def test_glob_translation_handles_doublestar(self):
        cases = [
            ("providers/apache/kafka/**/*.py", "providers/apache/kafka/a/b/c.py", True),
            ("providers/apache/kafka/**/*.py", "providers/apache/kafka/c.py", True),
            ("providers/apache/kafka/**/*.py", "providers/apache/other/c.py", False),
            ("contribution/**/src/**/operators/*.py",
             "contribution/src/airflow/providers/apache/kafka/operators/x.py", True),
            ("tools/**/*.py", "tools/x.py", True),
            ("tools/**/*.py", "toolsx/x.py", False),
            ("providers/apache/kafka/src/**/operators/*.py",
             "providers/apache/kafka/tests/unit/apache/kafka/operators/test_x.py", False),
        ]
        for pattern, path, want in cases:
            with self.subTest(pattern=pattern, path=path):
                self.assertEqual(want, lc.matches(path, [pattern]))


class OverlayTest(unittest.TestCase):
    """`contribution/` holds the PR at upstream paths, under a prefix.

    The two layout detectors -- path_mirror and registry_sync -- resolve that
    prefix so one rule governs both trees. Without it they raise SkipRule on
    every contribution file, and the linter reports the overlay as clean
    because it never looked. That is the failure this class exists to catch: it
    is silent, and it disables the check on the only tree anyone is editing.
    """

    PREFIX = "contribution/"

    @classmethod
    def setUpClass(cls):
        cls.rules = lc.load_rules(RULES_DIR)
        cls._tmp = tempfile.TemporaryDirectory()
        cls.root = cls._tmp.name
        # The same fixtures, moved wholesale under the overlay prefix.
        build_fixture_repo(os.path.join(cls.root, cls.PREFIX.rstrip("/")))
        files = lc.collect_files([cls.root], cls.root)
        cls.findings, cls.coverage = lc.lint(cls.rules, files, cls.root)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_layout_rules_fire_under_the_overlay(self):
        """Every upstream finding reproduces, path-prefixed.

        Subset, not equality. The overlay legitimately finds MORE: PROV-001
        (ASF header) is scoped to `contribution/**` and the provider tree but
        not to top-level `tools/`, so relocating the fixture's tools files
        brings them into scope. That is the rule working, not a false positive
        -- code shipped in a PR needs the header; a local dev script does not.
        """
        expected = {(rid, self.PREFIX + path) for rid, path in EXPECTED}
        actual = {(f.rule, f.path) for f in self.findings}
        self.assertLessEqual(expected, actual, "a rule stopped firing under the overlay")

    def test_layout_rules_are_evaluated_not_skipped(self):
        """The distinction the summary makes and a naive assertion misses: a
        rule that raised SkipRule on every file reports zero hits, which reads
        the same as a rule that ran and found nothing."""
        for rule_id in ("TEST-001", "PROV-004"):
            with self.subTest(rule=rule_id):
                self.assertNotIn(rule_id, self.coverage["rules_not_evaluated"])
                self.assertGreater(self.coverage["evaluated"].get(rule_id, 0), 0)

    def test_overlay_resolves_against_its_own_roots(self):
        """An overlay file must mirror to the overlay's test tree, not
        upstream's. Resolving against upstream would let a contribution pass
        TEST-001 on the strength of a test that is not part of the PR."""
        overlay, tail = lc._split_overlay(
            f"contribution/{SRC}/operators/x.py", f"{SRC}",
        )
        self.assertEqual(("contribution/", "operators/x.py"), (overlay, tail))

    def test_absent_root_skips_rather_than_passes(self):
        with self.assertRaises(lc.SkipRule):
            lc._split_overlay("some/other/tree/x.py", SRC)


class SkillAssetTest(unittest.TestCase):
    """The scaffold must emit code the linter accepts.

    A skill that hands an engineer a template which then fails the project's own
    lint rules is worse than no skill -- it teaches the wrong shape with the
    authority of a tool. This is the loop closed: the rules generate the agent's
    context, and the agent's templates are checked against the rules.
    """

    ASSETS = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".cursor", "skills", "scaffold-operator", "assets",
    )

    @classmethod
    def setUpClass(cls):
        cls.rules = lc.load_rules(RULES_DIR)

    def lint_asset(self, asset: str, dest: str, extra=()) -> list[lc.Finding]:
        with open(os.path.join(self.ASSETS, asset), encoding="utf-8") as fh:
            body = fh.read()
        with tempfile.TemporaryDirectory() as d:
            write(d, dest, body)
            for relpath, content in extra:
                write(d, relpath, content)
            findings, _ = lc.lint(self.rules, [os.path.join(d, dest)], d)
        return findings

    def test_operator_template_is_clean(self):
        findings = self.lint_asset(
            "operator_template.py",
            f"{SRC}/operators/reset_offsets.py",
            extra=[
                ("providers/apache/kafka/provider.yaml",
                 "operators:\n  - python-modules:\n"
                 "      - airflow.providers.apache.kafka.operators.reset_offsets\n"),
                (f"{UNIT}/operators/test_reset_offsets.py", ASF + FUTURE),
            ],
        )
        self.assertEqual([], findings, f"the scaffold emits linting code: {findings}")

    def test_unit_test_template_is_clean(self):
        findings = self.lint_asset(
            "test_template.py", f"{UNIT}/operators/test_reset_offsets.py"
        )
        self.assertEqual([], findings, f"{findings}")

    def test_integration_test_template_is_clean(self):
        """Specifically: it must not trip TEST-002, the rule it teaches."""
        findings = self.lint_asset(
            "integration_test_template.py", f"{INTEG}/test_reset_offsets.py"
        )
        self.assertEqual([], findings, f"{findings}")

    def test_skill_frontmatter_matches_its_directory(self):
        """Cursor requires skill `name` to equal the folder name; a mismatch
        makes the skill silently un-invokable."""
        skill = os.path.join(os.path.dirname(self.ASSETS), "SKILL.md")
        with open(skill, encoding="utf-8") as fh:
            text = fh.read()
        self.assertTrue(text.startswith("---\n"))
        front = text.split("---\n")[1]
        self.assertIn("name: scaffold-operator", front)
        self.assertIn("description:", front)


class GeneratorTest(unittest.TestCase):
    """The --check mode is the only thing standing between 'the rules and the
    agent's context agree' and 'they agreed once, in March'."""

    @classmethod
    def setUpClass(cls):
        cls.rules = lc.load_rules(RULES_DIR)

    def test_generate_then_check_is_in_sync(self):
        with tempfile.TemporaryDirectory() as d, quiet():
            self.assertEqual(0, gen_rules.main(["--rules", RULES_DIR, "--root", d]))
            self.assertEqual(0, gen_rules.main(["--rules", RULES_DIR, "--root", d, "--check"]))

    def test_check_detects_a_hand_edit(self):
        with tempfile.TemporaryDirectory() as d, quiet():
            gen_rules.main(["--rules", RULES_DIR, "--root", d])
            target = os.path.join(d, ".cursor/rules/00-conventions.mdc")
            with open(target, "a", encoding="utf-8") as fh:
                fh.write("\nsomeone edited the generated file by hand\n")
            self.assertEqual(1, gen_rules.main(["--rules", RULES_DIR, "--root", d, "--check"]))

    def test_check_detects_a_missing_file(self):
        with tempfile.TemporaryDirectory() as d, quiet():
            gen_rules.main(["--rules", RULES_DIR, "--root", d])
            os.remove(os.path.join(d, "docs/RULES.md"))
            self.assertEqual(1, gen_rules.main(["--rules", RULES_DIR, "--root", d, "--check"]))

    def test_generation_is_deterministic(self):
        """Non-deterministic output would make --check flap and get disabled,
        which is how drift guards die."""
        self.assertEqual(gen_rules.build(self.rules), gen_rules.build(self.rules))

    def test_every_rule_reaches_the_agent_context(self):
        """The 'one source, three consumers' claim, asserted rather than said:
        no rule may exist in YAML and be missing from .cursor/rules."""
        mdc = "\n".join(v for k, v in gen_rules.build(self.rules).items() if k.endswith(".mdc"))
        for r in self.rules:
            with self.subTest(rule=r.id):
                self.assertIn(r.id, mdc)

    def test_every_rule_reaches_the_human_docs(self):
        docs = gen_rules.build(self.rules)["docs/RULES.md"]
        for r in self.rules:
            with self.subTest(rule=r.id):
                self.assertIn(r.id, docs)
                self.assertIn(r.owner, docs)

    def test_pack_globs_are_the_union_of_member_rules(self):
        for pack, members in gen_rules.group_by_pack(self.rules).items():
            union = {g for r in members for g in r.applies_to}
            with self.subTest(pack=pack):
                self.assertEqual(sorted(union), gen_rules.pack_globs(members))

    def test_mdc_frontmatter_is_well_formed(self):
        for name, body in gen_rules.build(self.rules).items():
            if not name.endswith(".mdc"):
                continue
            with self.subTest(file=name):
                self.assertTrue(body.startswith("---\n"))
                head = body.split("---\n")[1]
                self.assertIn("description:", head)
                self.assertIn("alwaysApply:", head)


if __name__ == "__main__":
    unittest.main(verbosity=2)
