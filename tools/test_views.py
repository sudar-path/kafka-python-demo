#!/usr/bin/env python3
"""Proof that the stakeholder views are DERIVED, not written.

Run:  python tools/test_views.py          (stdlib unittest, no pytest)

A stakeholder document is a liability the moment it stops matching the system.
Nobody notices, because nobody re-reads it -- they read it once, form a belief,
and act on that belief a month later. So the tests that matter here are not
"does it render" but:

  * change the underlying file, and the view changes with it
  * the numbers in the PM view are the numbers in the artifact
  * a missing touchpoint appears in the gap list rather than being omitted
  * a new workflow or a new policy shows up without anyone editing views.py

The one part that is genuinely hand-maintained -- TEST_LAYERS, the claims about
what each test layer can and cannot prove -- is pinned to the workflow files by
test_every_layer_names_a_workflow_that_exists. That catches a claim naming a
workflow that no longer exists. It cannot catch a claim that is merely
optimistic, and the QA view says so in its own output.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml  # noqa: E402

from views import (  # noqa: E402
    TEST_LAYERS,
    TOUCHPOINTS,
    devops_view,
    main,
    pm_view,
    qa_view,
)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Shaped exactly like offset_diff.py's output -- see the schema in its
# write_json(). test_the_fixture_matches_a_real_artifact keeps it honest.
DIFF = {
    "schema_version": 1,
    "generated_at": "2026-08-16T12:00:00+00:00",
    "group": "payments-reconciler",
    "topic": "payments.events",
    "bootstrap": "localhost:9092",
    "mode": {"kind": "shift_by", "value": -10, "value_raw": None},
    "partitions": [
        {"partition": 0, "earliest": 0, "latest": 88, "current": 83, "proposed": 73,
         "delta": -10, "lag_before": 5, "lag_after": 15, "direction": "rewind",
         "out_of_range": False, "note": None},
        {"partition": 1, "earliest": 0, "latest": 111, "current": 99, "proposed": 89,
         "delta": -10, "lag_before": 12, "lag_after": 22, "direction": "rewind",
         "out_of_range": False, "note": None},
    ],
    "totals": {"messages_replayed": 20, "messages_skipped": 0,
               "lag_before": 17, "lag_after": 37},
    "warnings": [],
    "no_change": False,
}


def skipping_diff(skipped: int) -> dict:
    diff = json.loads(json.dumps(DIFF))
    diff["mode"] = {"kind": "to_latest", "value": None, "value_raw": None}
    diff["totals"] = {"messages_replayed": 0, "messages_skipped": skipped,
                      "lag_before": 17, "lag_after": 0}
    for part in diff["partitions"]:
        part["direction"] = "skip"
        part["delta"] = 10
    return diff


class PMViewTest(unittest.TestCase):
    def render(self, diff=DIFF, **kwargs):
        kwargs.setdefault("change_id", None)
        kwargs.setdefault("reason", None)
        kwargs.setdefault("verdict", None)
        kwargs.setdefault("policy", None)
        kwargs.setdefault("policy_path", None)
        return pm_view(diff, **kwargs)

    def test_the_numbers_come_from_the_artifact(self):
        """Not a template with plausible figures in it."""
        diff = json.loads(json.dumps(DIFF))
        diff["totals"]["messages_replayed"] = 41234
        text = self.render(diff)
        self.assertIn("41,234", text)
        self.assertNotIn("20 events", text)

    def test_the_lead_sentence_has_no_kafka_vocabulary(self):
        """This is the whole design constraint of the PM view. The first
        paragraph has to survive being read aloud to someone who does not know
        what a consumer group is; if it does not, the reader substitutes their
        own guess about severity, and that guess is the decision."""
        lead = self.render().split("### If this is wrong")[0].lower()
        for jargon in ("offset", "consumer group", "partition", "broker",
                       "commit", "seek", "to_earliest", "epoch"):
            self.assertNotIn(jargon, lead, f"{jargon!r} in the PM lead paragraph")

    def test_skipped_messages_are_called_irreversible(self):
        """The single most important sentence in this repo. Replay is a
        nuisance; skip is data that is never processed, and the two are easy to
        conflate because both are 'moving the offset'."""
        text = pm_view(skipping_diff(140), change_id=None, reason=None,
                       verdict=None, policy=None, policy_path=None)
        self.assertIn("140", text)
        self.assertIn("never be processed", text)
        self.assertIn("irreversible", text)

    def test_a_replay_only_change_is_not_called_irreversible(self):
        """The counterpart: crying wolf on a safe change teaches the reader to
        skim the scary paragraph, which is how the real one gets missed."""
        text = self.render()
        self.assertIn("second time", text)
        self.assertNotIn("irreversible", text)

    def test_a_no_op_says_so(self):
        diff = json.loads(json.dumps(DIFF))
        diff["totals"] = {"messages_replayed": 0, "messages_skipped": 0,
                          "lag_before": 0, "lag_after": 0}
        self.assertIn("no-op", self.render(diff))

    def test_the_verdict_is_reported_with_its_reason(self):
        verdict = {
            "allowed": False,
            "environment": "production",
            "policy": "policy/prod.yaml",
            "results": [
                {"rule": "forbidden_modes", "verdict": "deny",
                 "detail": "mode to_earliest is forbidden"},
                {"rule": "max_partitions_changed", "verdict": "pass", "detail": "2 <= 3"},
            ],
        }
        text = self.render(verdict=verdict)
        self.assertIn("BLOCKED", text)
        self.assertIn("forbidden_modes", text)
        self.assertIn("mode to_earliest is forbidden", text)
        # A passing rule is not noise the PM needs; only denials and skips are.
        self.assertNotIn("2 <= 3", text)

    def test_a_skipped_rule_is_not_reported_as_a_pass(self):
        """`skip` means the policy configured no limit, so nothing was checked.
        Rendering that as silence lets a reader conclude the check was made."""
        verdict = {
            "allowed": True,
            "environment": "integration",
            "policy": "policy/int.yaml",
            "results": [
                {"rule": "max_messages_skipped", "verdict": "skip", "detail": "not configured"},
            ],
        }
        text = self.render(verdict=verdict)
        self.assertIn("not checked", text)
        self.assertIn("max_messages_skipped", text)

    def test_the_policy_named_is_the_one_that_was_enforced(self):
        """If --policy and the verdict disagree, the verdict wins: it is the
        record of what actually ran."""
        verdict = {"allowed": True, "environment": "production",
                   "policy": "policy/prod.yaml", "results": []}
        text = self.render(verdict=verdict, policy_path="policy/int.yaml")
        self.assertIn("policy/prod.yaml", text)
        self.assertIn("production", text)

    def test_the_fixture_matches_a_real_artifact(self):
        """Guards the fixture itself. If offset_diff.py's schema moves, every
        assertion above keeps passing against a shape that no longer exists."""
        import offset_diff  # noqa: F401  (import cost is the point: it must load)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "d.json")
            with open(path, "w") as fh:
                json.dump(DIFF, fh)
            buf = io.StringIO()
            with redirect_stdout(buf):
                main(["pm", "--diff", path])
        self.assertIn("payments-reconciler", buf.getvalue())


class QAViewTest(unittest.TestCase):
    def test_presence_is_read_off_the_filesystem(self):
        """Delete a touchpoint's file and it must move to the gap list. This is
        the difference between a coverage report and a claim."""
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "repo")
            shutil.copytree(os.path.join(REPO, "contribution"),
                            os.path.join(root, "contribution"))
            before = qa_view(root)
            self.assertIn("| 5 | mirrored unit test | yes |", before)

            os.remove(os.path.join(
                root, "contribution/providers/apache/kafka/tests/unit/apache/kafka"
                      "/operators/test_reset_offsets.py"))
            after = qa_view(root)
        self.assertIn("| 5 | mirrored unit test | **MISSING** |", after)
        self.assertIn("touchpoint 5", after.split("## Gaps")[1])

    def test_the_real_gaps_are_named(self):
        """docs, newsfragment and version_compat are genuinely absent from
        contribution/. The view must say so on the real tree, unprompted --
        surfacing your own incompleteness is the only reason QA would trust
        anything else in the report."""
        gaps = qa_view(REPO).split("## Gaps")[1]
        for expected in ("docs index entry", "changelog / newsfragment", "version_compat"):
            self.assertIn(expected, gaps)

    def test_what_is_present_is_present(self):
        text = qa_view(REPO)
        self.assertIn("| 2 | operator module | yes |", text)
        self.assertIn("| 6 | integration test | yes |", text)
        self.assertIn("| 7 | system test / example DAG | yes |", text)

    def test_every_layer_states_a_limit(self):
        """A layer whose 'cannot prove' is empty is a layer nobody thought hard
        about, and it will be read as covering more than it does."""
        for layer, _where, proves, cannot in TEST_LAYERS:
            with self.subTest(layer=layer):
                self.assertTrue(proves.strip())
                self.assertTrue(cannot.strip(), f"{layer} claims no limits")
        self.assertIn("does NOT prove", qa_view(REPO))

    def test_every_layer_names_a_workflow_that_exists(self):
        """Pins the hand-written half to reality. A layer that says it runs in
        a workflow which no longer exists is a coverage claim with nothing
        behind it."""
        for _layer, where, _proves, _cannot in TEST_LAYERS:
            for name in [w.strip() for w in where.split(",")]:
                with self.subTest(workflow=name):
                    self.assertTrue(
                        os.path.exists(os.path.join(REPO, ".github", "workflows", name)),
                        f"{name} does not exist",
                    )

    def test_touchpoint_numbering_is_the_full_set(self):
        self.assertEqual([t[0] for t in TOUCHPOINTS], list(range(1, 11)))

    def test_a_deliberate_non_requirement_is_not_a_gap(self):
        """Touchpoint 1 has no file because this change adds no hook -- it reuses
        upstream's KafkaBaseHook. Counting that as missing coverage gives the gap
        list an entry nobody can act on, and a gap list with unactionable entries
        in it is one people learn to skim."""
        text = qa_view(REPO)
        self.assertIn("| 1 | hook surface | n/a by design |", text)
        gaps = text.split("## Gaps")[1]
        self.assertNotIn("hook surface", gaps)

    def test_every_deliberate_omission_explains_itself(self):
        """`required=False` is a claim that a reviewer has to be able to check.
        An empty note turns it into 'we did not do this' with no reason."""
        for number, name, _pattern, required, note in TOUCHPOINTS:
            if not required:
                with self.subTest(touchpoint=number):
                    self.assertGreater(len(note), 30, f"{name}: thin justification")


class DevOpsViewTest(unittest.TestCase):
    def test_every_workflow_appears(self):
        text = devops_view(REPO)
        for name in os.listdir(os.path.join(REPO, ".github", "workflows")):
            if name.endswith(".yml"):
                with self.subTest(workflow=name):
                    self.assertIn(f"`{name}`", text)

    def test_a_new_workflow_appears_without_editing_views_py(self):
        with tempfile.TemporaryDirectory() as tmp:
            wf = os.path.join(tmp, ".github", "workflows")
            os.makedirs(wf)
            os.makedirs(os.path.join(tmp, "policy"))
            with open(os.path.join(wf, "brand-new.yml"), "w") as fh:
                fh.write("name: n\non: [push]\njobs:\n  j:\n    runs-on: [self-hosted, wat]\n"
                         "    environment: production\n    steps:\n      - run: 'true'\n")
            text = devops_view(tmp)
        self.assertIn("`brand-new.yml`", text)
        self.assertIn("self-hosted,wat", text)
        self.assertIn("production", text)

    def test_triggers_survive_yamls_on_is_true_trap(self):
        """PyYAML parses the bare key `on:` as the boolean True. Every tool that
        reads a workflow file hits this, and the symptom is a silently empty
        Trigger column rather than an error."""
        text = devops_view(REPO)
        self.assertIn("workflow_dispatch", text)
        self.assertNotIn("| True |", text)
        self.assertNotIn("|  |", text)

    def test_policy_caps_are_the_real_ones(self):
        """The gate table is what DevOps reads before deciding whether a
        promotion needs a human. Retyped numbers here would be worse than no
        table: it would be trusted."""
        text = devops_view(REPO)
        for name in ("int.yaml", "stag.yaml", "prod.yaml"):
            path = os.path.join(REPO, "policy", name)
            if not os.path.exists(path):
                continue
            with open(path) as fh:
                rules = (yaml.safe_load(fh) or {}).get("rules") or {}
            row = [ln for ln in text.splitlines() if f"`policy/{name}`" in ln]
            self.assertEqual(len(row), 1, f"expected one row for {name}")
            with self.subTest(policy=name):
                self.assertIn(f"| {rules['max_messages_replayed']} |", row[0])
                self.assertIn(str(rules["max_partitions_changed"]), row[0])

    def test_environments_are_ordered_along_the_promotion_path(self):
        """int, then stag, then prod. Alphabetical order puts prod first, which
        reads as the default rather than the last gate."""
        text = devops_view(REPO)
        positions = [text.index(f"`policy/{n}`")
                     for n in ("int.yaml", "stag.yaml", "prod.yaml")
                     if f"`policy/{n}`" in text]
        self.assertEqual(positions, sorted(positions))

    def test_rollback_is_stated(self):
        text = devops_view(REPO)
        self.assertIn("restore.sh", text)
        self.assertIn("CONFIRM=prod-reset", text)


class CLITest(unittest.TestCase):
    def test_pm_without_a_diff_is_refused(self):
        """There is no way to render this view from nothing, and a template
        with placeholder numbers in it would eventually be pasted somewhere."""
        err = io.StringIO()
        with redirect_stderr(err), self.assertRaises(SystemExit) as caught:
            main(["pm"])
        self.assertNotEqual(caught.exception.code, 0)
        self.assertIn("--diff", err.getvalue())

    def test_each_audience_renders_markdown(self):
        for audience in ("qa", "devops"):
            with self.subTest(audience=audience):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    self.assertEqual(main([audience]), 0)
                self.assertTrue(buf.getvalue().startswith("## "))


if __name__ == "__main__":
    unittest.main(verbosity=2)
