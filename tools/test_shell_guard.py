#!/usr/bin/env python3
"""Proof that the shell guard blocks what it claims to block.

Run:  python tools/test_shell_guard.py               (stdlib unittest, no pytest)

A hook that was never tested is worth less than the AGENTS.md paragraph it
replaced, because everyone now believes there is a control. Same failure this
repo already shipped once (handoff §14: a tool documented read-only that created
topics) and the same remedy as test_lint_conventions.py -- assert the thing
fires, and assert it does not over-fire.

Over-firing matters as much as under-firing here. A guard that blocks
`offset_diff.py` because the string "offsets" appears in it gets switched off
within a day, and then nothing is guarded.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOOK = os.path.join(REPO, ".cursor", "hooks", "guard_shell.py")
sys.path.insert(0, os.path.join(REPO, ".cursor", "hooks"))

import guard_shell  # noqa: E402


BLOCKED = {
    "apply-offsets": [
        "python tools/apply_offsets.py --diff d.json",
        "./tools/apply_offsets.py --diff d.json --force",
        "cd /tmp && uv run apply_offsets.py --diff d.json",
        ".venv/bin/python tools/apply_offsets.py --diff docs/changes/c1/diff.json",
    ],
    "git-push": [
        "git push",
        "git push origin main",
        "git push --force origin HEAD:main",
        "git -C /tmp/clone push origin main",
    ],
    "prod-reset": [
        "tools/reset_demo.sh restore prod",
        "CONFIRM=prod-reset tools/reset_demo.sh clean prod",
        "./tools/reset_demo.sh restore all",
    ],
    "kafka-offset-write": [
        "docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh "
        "--bootstrap-server localhost:9092 --reset-offsets --group g --topic t --to-offset 0 --execute",
        "ssh ubuntu@host 'kafka-consumer-groups --reset-offsets --group g --to-earliest --execute'",
    ],
    "track-operational-notes": [
        "git add handoff.md",
        "git add -f handoff.md",
        "git add baselines/int.baseline",
        "git add secrets/key.pem",
    ],
}

# Every one of these is something a developer legitimately runs all day. If any
# start failing, the guard has become the problem it was meant to prevent.
ALLOWED = [
    "python tools/offset_diff.py --group g --topic t --to-latest",
    "python tools/policy_check.py --policy policy/prod.yaml --diff d.json",
    "python tools/lint_conventions.py . --summary",
    "python tools/test_offset_diff.py",
    "tools/reset_demo.sh status prod",
    "tools/reset_demo.sh status all",
    "tools/reset_demo.sh restore int",
    "tools/reset_demo.sh snapshot all",
    "git status",
    "git add contribution/",
    "git commit -m 'add reset offsets operator'",
    "git log --oneline -5",
    "git diff --staged",
    "docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh "
    "--bootstrap-server localhost:9092 --describe --group payments-reconciler",
    "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list",
    "python tools/apply_offsets.py --help",
    "pytest providers/apache/kafka/tests/unit -q",
    "cat tools/apply_offsets.py",
    "grep -n 'apply_seek' tools/apply_offsets.py",
]


class ClassifyTest(unittest.TestCase):
    def test_every_blocked_command_matches_its_rule(self):
        for expected_rule, commands in BLOCKED.items():
            for command in commands:
                with self.subTest(rule=expected_rule, command=command):
                    hit = guard_shell.classify(command)
                    self.assertIsNotNone(hit, "not blocked at all")
                    self.assertEqual(hit[0], expected_rule)

    def test_ordinary_commands_are_not_blocked(self):
        for command in ALLOWED:
            with self.subTest(command=command):
                hit = guard_shell.classify(command)
                self.assertIsNone(hit, f"over-fired as {hit[0] if hit else ''}")

    def test_every_rule_has_at_least_one_proof(self):
        """A rule added without a fixture would be untested and look tested."""
        declared = {name for name, _, _ in guard_shell.RULES}
        self.assertEqual(declared, set(BLOCKED), "rule set and fixtures disagree")

    def test_messages_say_what_to_do_instead(self):
        """A block that does not name the alternative just gets worked around."""
        for _, commands in BLOCKED.items():
            hit = guard_shell.classify(commands[0])
            self.assertGreater(len(hit[1]), 60, "message is too terse to act on")

    def test_empty_command_is_allowed(self):
        self.assertIsNone(guard_shell.classify(""))


class ProcessTest(unittest.TestCase):
    """The hook is invoked as a subprocess, so the wire contract is part of it:
    exit 2 blocks, anything else fails open."""

    def run_hook(self, payload):
        proc = subprocess.run(
            [sys.executable, HOOK],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
        )
        return proc.returncode, json.loads(proc.stdout)

    def test_blocked_command_exits_2_and_denies(self):
        code, out = self.run_hook({"command": "python tools/apply_offsets.py --diff d.json"})
        self.assertEqual(code, 2)
        self.assertEqual(out["permission"], "deny")
        self.assertIn("apply-offsets", out["userMessage"])

    def test_allowed_command_exits_0(self):
        code, out = self.run_hook({"command": "git status"})
        self.assertEqual(code, 0)
        self.assertEqual(out["permission"], "allow")

    def test_unparseable_input_denies(self):
        """failClosed covers a crash; this covers the subtler case where the
        hook runs fine but cannot see the command. It must not vouch for what
        it did not read."""
        proc = subprocess.run(
            [sys.executable, HOOK], input="not json", capture_output=True, text=True
        )
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(json.loads(proc.stdout)["permission"], "deny")

    def test_missing_command_key_is_allowed(self):
        code, out = self.run_hook({"cwd": "/tmp"})
        self.assertEqual(code, 0)
        self.assertEqual(out["permission"], "allow")


class WiringTest(unittest.TestCase):
    """The best-written hook does nothing if hooks.json does not point at it."""

    def setUp(self):
        with open(os.path.join(REPO, ".cursor", "hooks.json")) as fh:
            self.config = json.load(fh)

    def test_hook_is_registered_for_beforeShellExecution(self):
        entries = self.config["hooks"]["beforeShellExecution"]
        self.assertTrue(any("guard_shell.py" in e["command"] for e in entries))

    def test_hook_fails_closed(self):
        """Cursor fails OPEN on any exit code other than 2, so a syntax error in
        the guard would silently disable it. This flag is what makes a broken
        guard loud instead of absent."""
        entry = next(e for e in self.config["hooks"]["beforeShellExecution"]
                     if "guard_shell.py" in e["command"])
        self.assertIs(entry.get("failClosed"), True)

    def test_the_referenced_script_exists(self):
        entry = next(e for e in self.config["hooks"]["beforeShellExecution"]
                     if "guard_shell.py" in e["command"])
        path = entry["command"].split()[-1]
        self.assertTrue(os.path.exists(os.path.join(REPO, path)), f"{path} missing")


class DistributionBoundaryTest(unittest.TestCase):
    """The other half of the boundary: what must not enter git history.

    The shell guard stops an agent from *running* the dangerous thing.
    .gitignore stops the dangerous thing from *leaving the machine*. Both were
    asserted in prose long before either was asserted in code, and .gitignore
    in particular did not exist at all until 2026-08-16 -- the tree was covered
    only by .cursorignore, which git does not read, and by a CI step that runs
    after the push it is supposed to prevent.
    """

    # Everything conventions.yml's "No operational notes in the tree" step
    # greps for. That step is a BACKSTOP: by the time it fails, the commit is
    # already on a remote. Anything it can catch must also be in .gitignore, so
    # that the backstop is the second line and not the only one.
    MUST_BE_IGNORED = [
        "handoff.md",
        "*.pem",
        "*.key",
        "docs/changes/",
        "baselines/",
        "tools/run_on_int.sh",
        "tools/reset_demo.sh",
        # Added 2026-08-16, after the pre-push manifest check found both of these
        # staged for a public repo. They are here rather than in a comment because
        # the two that got missed were missed by reading, and reading is what this
        # class exists to stop relying on.
        #
        # *.pdf          -- the assignment brief; the interviewer's material.
        # settings.local.json -- agent permission grants, which quote approved
        #                   commands verbatim and therefore carry the SSH key path
        #                   and the STAG hostname. Nothing in the name says so.
        "*.pdf",
        "**/settings.local.json",
    ]

    def setUp(self):
        path = os.path.join(REPO, ".gitignore")
        self.assertTrue(os.path.exists(path), ".gitignore is missing entirely")
        with open(path) as fh:
            self.patterns = {
                ln.strip() for ln in fh
                if ln.strip() and not ln.lstrip().startswith("#")
            }

    def test_everything_the_ci_backstop_catches_is_ignored_first(self):
        for pattern in self.MUST_BE_IGNORED:
            with self.subTest(pattern=pattern):
                self.assertIn(
                    pattern, self.patterns,
                    f"{pattern} is caught by the conventions.yml backstop but is not "
                    "in .gitignore, so the only thing standing between it and a "
                    "remote is someone reading `git status` carefully",
                )

    # git ls-files takes pathspecs; .gitignore takes gitignore patterns. The two
    # spell the same intent differently, so the cross-check below needs a map
    # rather than string equality. Keeping it explicit means a new entry that
    # genuinely has no .gitignore equivalent has to be thought about here.
    PATHSPEC_TO_IGNORE = {
        "docs/changes/*": "docs/changes/",
        "baselines/*": "baselines/",
        "*settings.local.json": "**/settings.local.json",
    }

    def test_the_backstop_and_the_ignore_file_cannot_drift(self):
        """The two layers must cover the same set, derived rather than retyped.

        Hand-copied lists are how this repo nearly shipped an SSH key path: the
        backstop and .gitignore were edited at different times by different
        reasoning, and nothing compared them. Reading conventions.yml here means
        adding a pattern to one layer and forgetting the other is a test failure
        rather than a thing someone notices later, or doesn't.
        """
        with open(os.path.join(REPO, ".github", "workflows", "conventions.yml")) as fh:
            workflow = fh.read()

        # The backstop's second check: git ls-files -- '<pathspec>' ...
        block = workflow.split("git ls-files --", 1)
        self.assertEqual(len(block), 2, "the backstop step is gone or was renamed")
        pathspecs = re.findall(r"'([^']+)'", block[1].split(")", 1)[0])
        self.assertTrue(pathspecs, "parsed no pathspecs out of the backstop")

        for spec in pathspecs:
            expected = self.PATHSPEC_TO_IGNORE.get(spec, spec)
            with self.subTest(pathspec=spec):
                self.assertIn(
                    expected, self.patterns,
                    f"conventions.yml greps for {spec!r} but .gitignore has no "
                    f"{expected!r}, so the only thing stopping it reaching a remote "
                    "is a CI step that runs after the push",
                )

    def test_the_generated_agent_context_is_not_ignored(self):
        """The inverse mistake, and an easy one: sweeping generated files into
        .gitignore by reflex would drop .cursor/rules/ and docs/RULES.md, which
        are exactly the files that have to be committed and reviewed."""
        for pattern in self.patterns:
            self.assertNotIn(".cursor/rules", pattern)
            self.assertNotIn("RULES.md", pattern)


if __name__ == "__main__":
    unittest.main(verbosity=2)
