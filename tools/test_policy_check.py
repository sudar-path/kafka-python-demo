#!/usr/bin/env python3
"""Proof that the promotion gate actually denies things.

Run:  python tools/test_policy_check.py             (stdlib unittest, no pytest)

policy_check.py is the last automated thing standing between a dry-run artifact
and a production broker, and it had no tests. The failure mode that matters is
not "it denies a change it should have allowed" -- somebody notices that within
a minute. It is the silent inverse: a gate that reports ALLOW while enforcing
nothing, which looks identical to a gate that is working.

So the two design rules in policy_check.py are tested first and hardest:

    1. an unrecognised rule name is fatal, not ignored
    2. a rule that raises denies, rather than being skipped

Everything after that is per-rule arithmetic.
"""

from __future__ import annotations

import datetime as dt
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import policy_check  # noqa: E402
from tools.policy_check import POLICY_SCHEMA_VERSION, evaluate, load_policy  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOW = dt.datetime(2026, 8, 16, 12, 0, 0, tzinfo=dt.timezone.utc)

# The promotion path, in order. A change moves left to right and each hop must
# be at least as strict as the one before it -- see
# ShippedPolicyTest.test_strictness_never_relaxes_along_the_promotion_path.
# Files listed here that do not exist are skipped, so adding a fourth
# environment means adding it to this tuple and nothing else.
PROMOTION_PATH = ("int.yaml", "stag.yaml", "prod.yaml")

# Numeric ceilings: lower is stricter.
SAFETY_CAPS = (
    "max_messages_skipped",
    "max_messages_replayed",
    "max_partitions_changed",
    "max_diff_age_seconds",
)
# Booleans where True is the permissive value.
SAFETY_FLAGS = ("allow_out_of_range", "allow_uncommitted_partitions")
# Scope, not strictness. These describe blast radius and are the only rules an
# environment is allowed to differ on without that counting as a relaxation.
SCOPE_RULES = ("allowed_topics", "allowed_groups")


def make_diff(**overrides):
    """A diff artifact shaped like offset_diff.OffsetDiff serialised to JSON."""
    diff = {
        "schema_version": 1,
        "generated_at": (NOW - dt.timedelta(seconds=60)).isoformat(),
        "group": "payments-reconciler",
        "topic": "payments.events",
        "bootstrap": "localhost:9092",
        "mode": {"kind": "to_timestamp", "value": 1767225600000, "value_raw": "2026-01-01T00:00:00Z"},
        "partitions": [
            {
                "partition": 0,
                "earliest": 0,
                "latest": 100,
                "current": 50,
                "proposed": 40,
                "delta": -10,
                "lag_before": 50,
                "lag_after": 60,
                "direction": "rewind",
                "out_of_range": False,
                "note": None,
            }
        ],
        "totals": {
            "messages_replayed": 10,
            "messages_skipped": 0,
            "lag_before": 50,
            "lag_after": 60,
        },
        "warnings": [],
    }
    diff.update(overrides)
    return diff


def make_policy(**rules):
    return {"version": POLICY_SCHEMA_VERSION, "environment": "test", "rules": rules}


def verdicts(policy, diff, now=NOW):
    return {r.rule: r.verdict for r in evaluate(policy, diff, now)}


def one(policy, diff, name, now=NOW):
    return next(r for r in evaluate(policy, diff, now) if r.rule == name)


class DesignRuleTest(unittest.TestCase):
    """The two properties that make the gate meaningful at all."""

    def test_an_unknown_rule_name_is_fatal(self):
        """Design rule 1. A typo in a policy file must stop the pipeline.

        The alternative is the worst outcome available: `max_messages_skiped: 0`
        is silently ignored, the gate reports ALLOW, and everyone believes a cap
        is in force that has never once been evaluated.
        """
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
            fh.write(
                f"version: {POLICY_SCHEMA_VERSION}\nrules:\n  max_messages_skiped: 0\n"
            )
            path = fh.name
        try:
            with self.assertRaises(SystemExit):
                load_policy(path)
        finally:
            os.unlink(path)

    def test_a_rule_that_raises_denies(self):
        """Design rule 2: fail closed.

        A malformed diff must not let a change through on a technicality. If the
        evaluator cannot decide, the answer is no.
        """
        broken = make_diff()
        del broken["totals"]  # KeyError inside the rule
        result = one(make_policy(max_messages_skipped=0), broken, "max_messages_skipped")
        self.assertEqual(result.verdict, "deny")
        self.assertIn("failing closed", result.detail)

    def test_an_unconfigured_rule_skips_and_says_so(self):
        """Skip is honest -- but it must be visible, not silently absent, or a
        policy that configures nothing reads as a policy that passed."""
        results = evaluate(make_policy(allowed_topics=["payments.events"]), make_diff(), NOW)
        skipped = [r for r in results if r.verdict == "skip"]
        self.assertTrue(skipped)
        self.assertTrue(all("not configured" in r.detail for r in skipped))
        # every known rule is accounted for in the output, one way or another
        self.assertEqual({r.rule for r in results}, set(policy_check.RULES))

    def test_version_mismatch_is_refused(self):
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
            fh.write("version: 999\nrules:\n  max_messages_skipped: 0\n")
            path = fh.name
        try:
            with self.assertRaises(SystemExit):
                load_policy(path)
        finally:
            os.unlink(path)


class AllowlistTest(unittest.TestCase):
    def test_topic_allowlist(self):
        self.assertEqual(
            one(make_policy(allowed_topics=["payments.events"]), make_diff(), "allowed_topics").verdict,
            "pass",
        )
        self.assertEqual(
            one(make_policy(allowed_topics=["other"]), make_diff(), "allowed_topics").verdict,
            "deny",
        )

    def test_group_allowlist(self):
        self.assertEqual(
            one(make_policy(allowed_groups=["nope"]), make_diff(), "allowed_groups").verdict,
            "deny",
        )

    def test_forbidden_modes(self):
        policy = make_policy(forbidden_modes=["to_earliest"])
        self.assertEqual(one(policy, make_diff(), "forbidden_modes").verdict, "pass")
        earliest = make_diff(mode={"kind": "to_earliest", "value": None, "value_raw": None})
        self.assertEqual(one(policy, earliest, "forbidden_modes").verdict, "deny")


class CapTest(unittest.TestCase):
    def test_skipped_cap_is_inclusive(self):
        diff = make_diff(totals={"messages_replayed": 0, "messages_skipped": 5,
                                 "lag_before": 0, "lag_after": 0})
        self.assertEqual(one(make_policy(max_messages_skipped=5), diff, "max_messages_skipped").verdict, "pass")
        self.assertEqual(one(make_policy(max_messages_skipped=4), diff, "max_messages_skipped").verdict, "deny")

    def test_replayed_and_skipped_are_capped_independently(self):
        """A large replay is usually recoverable; a single skipped message may
        not be. A policy has to be able to allow one and forbid the other."""
        diff = make_diff(totals={"messages_replayed": 1000, "messages_skipped": 0,
                                 "lag_before": 0, "lag_after": 0})
        policy = make_policy(max_messages_replayed=5000, max_messages_skipped=0)
        self.assertEqual(verdicts(policy, diff)["max_messages_replayed"], "pass")
        self.assertEqual(verdicts(policy, diff)["max_messages_skipped"], "pass")

    def test_partitions_changed_ignores_no_op_partitions(self):
        parts = [
            dict(partition=0, earliest=0, latest=9, current=5, proposed=3, delta=-2,
                 lag_before=4, lag_after=6, direction="rewind", out_of_range=False, note=None),
            dict(partition=1, earliest=0, latest=9, current=5, proposed=5, delta=0,
                 lag_before=4, lag_after=4, direction="none", out_of_range=False, note=None),
            dict(partition=2, earliest=0, latest=9, current=None, proposed=None, delta=None,
                 lag_before=None, lag_after=None, direction="unknown", out_of_range=False, note="x"),
        ]
        result = one(make_policy(max_partitions_changed=1), make_diff(partitions=parts),
                     "max_partitions_changed")
        self.assertEqual(result.verdict, "pass")
        self.assertIn("1 partition(s)", result.detail)


class SafetyRuleTest(unittest.TestCase):
    def test_out_of_range_denied_by_default_and_tolerable_explicitly(self):
        parts = [dict(partition=0, earliest=0, latest=9, current=5, proposed=50, delta=45,
                      lag_before=4, lag_after=-41, direction="advance", out_of_range=True, note="x")]
        diff = make_diff(partitions=parts)
        self.assertEqual(one(make_policy(allow_out_of_range=False), diff, "allow_out_of_range").verdict, "deny")
        self.assertEqual(one(make_policy(allow_out_of_range=True), diff, "allow_out_of_range").verdict, "pass")

    def test_uncommitted_partition_denied_unless_tolerated(self):
        parts = [dict(partition=0, earliest=0, latest=9, current=None, proposed=0, delta=None,
                      lag_before=None, lag_after=9, direction="unknown", out_of_range=False, note="x")]
        diff = make_diff(partitions=parts)
        self.assertEqual(
            one(make_policy(allow_uncommitted_partitions=False), diff, "allow_uncommitted_partitions").verdict,
            "deny",
        )

    def test_schema_version_must_match(self):
        self.assertEqual(
            one(make_policy(require_diff_schema_version=1), make_diff(), "require_diff_schema_version").verdict,
            "pass",
        )
        self.assertEqual(
            one(make_policy(require_diff_schema_version=2), make_diff(), "require_diff_schema_version").verdict,
            "deny",
        )


class StalenessTest(unittest.TestCase):
    """The window between "a human approved this diff" and "it is applied" is
    the window in which the broker moves."""

    def test_fresh_diff_passes(self):
        self.assertEqual(one(make_policy(max_diff_age_seconds=300), make_diff(), "max_diff_age_seconds").verdict, "pass")

    def test_stale_diff_is_denied_with_the_remedy_in_the_message(self):
        old = make_diff(generated_at=(NOW - dt.timedelta(hours=3)).isoformat())
        result = one(make_policy(max_diff_age_seconds=300), old, "max_diff_age_seconds")
        self.assertEqual(result.verdict, "deny")
        self.assertIn("re-run offset_diff.py", result.detail)

    def test_age_is_measured_against_the_supplied_clock(self):
        """`now` is a parameter, not datetime.now(), so the apply job can
        re-check staleness against its own clock after the approval wait."""
        diff = make_diff()
        policy = make_policy(max_diff_age_seconds=300)
        self.assertEqual(one(policy, diff, "max_diff_age_seconds", now=NOW).verdict, "pass")
        later = NOW + dt.timedelta(hours=1)
        self.assertEqual(one(policy, diff, "max_diff_age_seconds", now=later).verdict, "deny")


class ShippedPolicyTest(unittest.TestCase):
    """The policies in policy/ are configuration, and configuration rots."""

    def policies(self):
        found = {}
        for name in sorted(os.listdir(os.path.join(REPO, "policy"))):
            if name.endswith((".yaml", ".yml")):
                found[name] = load_policy(os.path.join(REPO, "policy", name))
        return found

    def test_every_shipped_policy_loads(self):
        loaded = self.policies()
        self.assertTrue(loaded, "no policy files found")
        for name, policy in loaded.items():
            with self.subTest(policy=name):
                # load_policy already rejects unknown rules; this asserts the
                # file is reachable and parses, which is what CI needs to know.
                self.assertIn("rules", policy)

    def test_every_shipped_policy_evaluates_without_raising(self):
        for name, policy in self.policies().items():
            with self.subTest(policy=name):
                results = evaluate(policy, make_diff(), NOW)
                self.assertEqual({r.rule for r in results}, set(policy_check.RULES))

    def test_strictness_never_relaxes_along_the_promotion_path(self):
        """Each environment must be at least as strict as the one before it.

        Started life as a two-file int-vs-prod check and became a chain the
        moment stag.yaml existed, because the two-file version would have
        happily allowed a staging policy looser than both of them -- the exact
        file the promotion path leans on hardest.

        This is the kind of drift that happens one careful edit at a time. Every
        individual relaxation has a reason on the day it is made; nobody ever
        diffs the whole chain.
        """
        loaded = self.policies()
        chain = [name for name in PROMOTION_PATH if name in loaded]
        if len(chain) < 2:
            self.skipTest("need at least two policies on the promotion path")

        for looser_name, tighter_name in zip(chain, chain[1:]):
            looser = loaded[looser_name]["rules"]
            tighter = loaded[tighter_name]["rules"]

            for name in SAFETY_CAPS:
                if name in looser and name in tighter:
                    with self.subTest(step=f"{looser_name}->{tighter_name}", rule=name):
                        self.assertLessEqual(
                            tighter[name], looser[name],
                            f"{tighter_name}'s {name} ({tighter[name]}) is looser than "
                            f"{looser_name}'s ({looser[name]})",
                        )

            # `allow_*: true` is the permissive value, so bool ordering is the
            # comparison: an environment may turn one off, never back on.
            for name in SAFETY_FLAGS:
                if name in looser and name in tighter:
                    with self.subTest(step=f"{looser_name}->{tighter_name}", rule=name):
                        self.assertLessEqual(
                            int(bool(tighter[name])), int(bool(looser[name])),
                            f"{tighter_name} re-enables {name} that {looser_name} forbids",
                        )

            # A rule enforced upstream and simply absent downstream is a
            # downgrade that no value comparison above can see.
            dropped = sorted(set(looser) - set(tighter))
            self.assertFalse(
                dropped,
                f"rules enforced in {looser_name} but missing from {tighter_name}: {dropped}",
            )

    def test_stag_is_prod_shaped(self):
        """Staging must be identical to production on every safety value.

        Not merely "no looser" -- identical. The claim the staging leg makes is
        "passes stag predicts passes prod", and that claim survives exactly as
        long as the two gates are the same gate. A stag.yaml that is stricter
        than prod is also a bug: it denies rehearsals that production would have
        allowed, and the fix people reach for is to stop running the staging
        leg.

        Scope is the one permitted difference, and it is permitted because scope
        IS the difference between the two environments: same rules, smaller
        blast radius. See the header of policy/stag.yaml.
        """
        loaded = self.policies()
        if not {"stag.yaml", "prod.yaml"} <= set(loaded):
            self.skipTest("need both stag.yaml and prod.yaml")

        def safety_only(rules):
            return {k: v for k, v in rules.items() if k not in SCOPE_RULES}

        stag = safety_only(loaded["stag.yaml"]["rules"])
        prod = safety_only(loaded["prod.yaml"]["rules"])
        differing = sorted(k for k in set(stag) | set(prod) if stag.get(k) != prod.get(k))
        self.assertFalse(
            differing,
            "stag.yaml and prod.yaml must agree on every non-scope rule; they "
            f"differ on {differing}. Either copy prod's value into stag, or -- if "
            "production genuinely needs to change -- change prod.yaml in a "
            "reviewed PR first and copy it down.",
        )

    def test_staging_is_labelled_staging(self):
        """A policy file's `environment` is stamped into the artifact and
        printed in the approval summary. Copying prod.yaml to stag.yaml and
        forgetting this one line produces a staging artifact that announces
        itself as production -- which is how the wrong thing gets approved."""
        for name, expected in (
            ("int.yaml", "integration"),
            ("stag.yaml", "staging"),
            ("prod.yaml", "production"),
        ):
            policy = self.policies().get(name)
            if policy is None:
                continue
            with self.subTest(policy=name):
                self.assertEqual(policy.get("environment"), expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
