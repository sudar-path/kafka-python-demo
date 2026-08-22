#!/usr/bin/env python3
"""Proof that the workflow rehearsal generator produces a script worth trusting.

Run:  python tools/test_workflow_dryrun.py          (stdlib unittest, no pytest)

The failure mode that matters is not "it crashes" -- it is a generated script
that runs, exits 0, and did not actually execute the workflow. That reads as
"rehearsed and green" and is worse than not rehearsing at all, so most of these
tests are about the generator refusing to produce a misleading script.

The tests execute the generated bash. A generator that emits syntactically
invalid shell is the single most likely bug here, and only bash can tell you.
"""

from __future__ import annotations

import glob
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from workflow_dryrun import generate  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOWS = os.path.join(REPO, ".github", "workflows")


def write_workflow(text: str) -> str:
    fh = tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False)
    fh.write(textwrap.dedent(text))
    fh.close()
    return fh.name


def run_script(script: str, cwd: str):
    path = os.path.join(cwd, "rehearse.sh")
    with open(path, "w") as fh:
        fh.write(script)
    return subprocess.run(["bash", path], capture_output=True, text=True, cwd=cwd)


class GeneratedShellTest(unittest.TestCase):
    def test_the_generated_script_is_valid_bash(self):
        script = generate(os.path.join(WORKFLOWS, "int-tests.yml"), None, allow=False)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "s.sh")
            with open(path, "w") as fh:
                fh.write(script)
            proc = subprocess.run(["bash", "-n", path], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_steps_run_in_order_and_all_of_them_run(self):
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - name: one
                    run: echo STEP-one
                  - name: two
                    run: echo STEP-two
                  - name: three
                    run: echo STEP-three
        """)
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_script(generate(wf, None, allow=False), tmp)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        order = [ln for ln in proc.stdout.splitlines() if ln.startswith("STEP-")]
        self.assertEqual(order, ["STEP-one", "STEP-two", "STEP-three"])

    def test_a_failing_step_does_not_stop_the_rest(self):
        """The whole reason to rehearse locally: the runner stops at the first
        red step, so a push buys you exactly one bug. This must not."""
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - name: broken
                    run: exit 3
                  - name: later
                    run: echo REACHED-later
        """)
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_script(generate(wf, None, allow=False), tmp)
        self.assertIn("REACHED-later", proc.stdout)
        self.assertIn("FAILED (exit 3)", proc.stdout)
        self.assertNotEqual(proc.returncode, 0, "a failed step must fail the rehearsal")
        self.assertIn("broken", proc.stdout.split("FAILED STEPS")[-1])

    def test_job_env_is_exported(self):
        wf = write_workflow("""
            name: t
            on: push
            env: {TOPLEVEL: from-top}
            jobs:
              j:
                runs-on: ubuntu-latest
                env: {SCOPED: from-job}
                steps:
                  - name: read env
                    run: echo "GOT $TOPLEVEL $SCOPED"
        """)
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_script(generate(wf, None, allow=False), tmp)
        self.assertIn("GOT from-top from-job", proc.stdout)

    def test_github_path_carries_between_steps(self):
        """Every workflow here starts by putting uv on $GITHUB_PATH. If that did
        not carry, the rehearsal would fail on a PATH problem the real runner
        does not have -- and the next move is to distrust the harness."""
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - name: extend path
                    run: mkdir -p bin && printf '#!/bin/sh\\necho HELLO-FROM-BIN\\n' > bin/mytool && chmod +x bin/mytool && echo "$PWD/bin" >> "$GITHUB_PATH"
                  - name: use it
                    run: mytool
        """)
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_script(generate(wf, None, allow=False), tmp)
        self.assertIn("HELLO-FROM-BIN", proc.stdout, proc.stdout + proc.stderr)

    def test_env_set_at_runtime_is_visible_to_a_later_step(self):
        """int-tests.yml computes REPO_DIR in a step and then uses it as
        `working-directory: ${{ env.REPO_DIR }}`, because ${{ runner.temp }} does
        not exist at job level. Resolving env.* against the static env: block
        would expand that to the empty string and rehearse `cd ""` -- which
        succeeds, silently running every later step in the wrong directory."""
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - name: compute it
                    run: |
                      mkdir -p late/dir && touch late/dir/marker
                      echo "LATE_DIR=$PWD/late/dir" >> "$GITHUB_ENV"
                  - name: use it
                    working-directory: ${{ env.LATE_DIR }}
                    run: ls marker && echo "CWD-OK"
        """)
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_script(generate(wf, None, allow=False), tmp)
        self.assertIn("CWD-OK", proc.stdout, proc.stdout + proc.stderr)
        self.assertEqual(proc.returncode, 0)

    def test_working_directory_is_honoured(self):
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - name: make it
                    run: mkdir -p sub && touch sub/marker
                  - name: in it
                    working-directory: sub
                    run: ls marker
        """)
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_script(generate(wf, None, allow=False), tmp)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)


class RefusalTest(unittest.TestCase):
    """Refusing to generate beats generating something that lies."""

    def test_unresolvable_expression_is_refused(self):
        wf = write_workflow("""
            name: t
            on: workflow_dispatch
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - name: uses an input
                    run: echo "${{ inputs.group }}"
        """)
        with self.assertRaises(SystemExit) as caught:
            generate(wf, None, allow=False)
        self.assertIn("inputs.group", str(caught.exception))

    def test_unresolvable_expression_can_be_forced(self):
        wf = write_workflow("""
            name: t
            on: workflow_dispatch
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - name: uses an input
                    run: echo "${{ inputs.group }}"
        """)
        self.assertIn("${{ inputs.group }}", generate(wf, None, allow=True))

    def test_env_expressions_resolve(self):
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                env: {PIN: "2.6.0"}
                steps:
                  - name: pinned
                    run: echo "want ${{ env.PIN }}"
        """)
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_script(generate(wf, None, allow=False), tmp)
        self.assertIn("want 2.6.0", proc.stdout)

    def test_multiple_jobs_require_a_choice(self):
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              a: {runs-on: ubuntu-latest, steps: [{name: x, run: "true"}]}
              b: {runs-on: ubuntu-latest, steps: [{name: y, run: "true"}]}
        """)
        with self.assertRaises(SystemExit) as caught:
            generate(wf, None, allow=False)
        self.assertIn("--job", str(caught.exception))
        # Naming a job resolves it, and picks THAT job's steps -- not the first.
        self.assertIn("== y ==", generate(wf, "b", allow=False))
        self.assertNotIn("== x ==", generate(wf, "b", allow=False))

    def test_a_job_with_no_run_steps_is_refused(self):
        """An all-`uses:` job generates a script that prints SKIP lines and
        exits 0. That is a green rehearsal of nothing."""
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - uses: actions/checkout@v4
        """)
        with self.assertRaises(SystemExit) as caught:
            generate(wf, None, allow=False)
        self.assertIn("no `run:` steps", str(caught.exception))

    def test_uses_steps_are_skipped_visibly(self):
        wf = write_workflow("""
            name: t
            on: push
            jobs:
              j:
                runs-on: ubuntu-latest
                steps:
                  - uses: actions/checkout@v4
                  - name: real
                    run: echo hi
        """)
        script = generate(wf, None, allow=False)
        self.assertIn("SKIP (uses: actions/checkout@v4)", script)


class ShippedWorkflowsTest(unittest.TestCase):
    """Every workflow in this repo must remain rehearsable. If one stops being
    generatable, the rehearsal step quietly stops covering it."""

    JOBS = {
        "int-tests.yml": [None],
        "conventions.yml": [None],
        # offset-change-prod.yml is driven by workflow_dispatch inputs, so its run
        # bodies legitimately carry ${{ inputs.* }}. It is rehearsable only with
        # --allow-expressions, and rehearsing it means running the APPLY job
        # against a broker -- which is not something a test does.
    }

    def test_each_workflow_generates_valid_bash(self):
        for name, jobs in self.JOBS.items():
            for job in jobs:
                with self.subTest(workflow=name, job=job):
                    script = generate(os.path.join(WORKFLOWS, name), job, allow=False)
                    with tempfile.TemporaryDirectory() as tmp:
                        path = os.path.join(tmp, "s.sh")
                        with open(path, "w") as fh:
                            fh.write(script)
                        proc = subprocess.run(
                            ["bash", "-n", path], capture_output=True, text=True
                        )
                    self.assertEqual(proc.returncode, 0, f"{name}: {proc.stderr}")

    def test_offset_change_prod_needs_the_escape_hatch(self):
        """Documents the exclusion above rather than leaving it to a comment: if
        offset-change-prod.yml ever stops using inputs inline, this test fails and the
        workflow should be added to JOBS."""
        path = os.path.join(WORKFLOWS, "offset-change-prod.yml")
        if not os.path.exists(path):
            self.skipTest("offset-change-prod.yml not present")
        with self.assertRaises(SystemExit):
            generate(path, "dry-run", allow=False)


class ShellFailureModeTest(unittest.TestCase):
    """Two idioms that report a SUCCESSFUL outcome as a red job.

    `bash -n` cannot see either of these -- both are syntactically perfect, and
    both depend on runtime exit codes. They are here because both shipped, and
    both were found by the first real run of int-tests.yml on the remote rather
    than by anything local:

      1. `[ -f "$f" ] && echo ...` as the last statement of a loop body. Under
         `bash -e` the loop takes the exit status of its final iteration, so the
         step fails whenever the last entry does not match -- which for
         `contribution/*` is always, since README.md sorts before providers/.
         The overlay had already succeeded. The work was done and the job was
         still red, which is the worst version of this bug: the failure points
         nowhere near the cause.

      2. `... | grep ... | ...` under `set -o pipefail`. Finding nothing is the
         HEALTHY outcome for a leak check, but grep calls it exit 1, so the
         cleanup step passed only when there was something to clean up.

    Both are scanned as text across every workflow, including offset-change-prod.yml,
    which the rehearsal harness cannot generate.
    """

    # Continuations are joined before scanning: both bugs span a `\` line break
    # in the real workflows, and a line-at-a-time scan would miss them.
    CONTINUATION = re.compile(r"\\\n\s*")

    def _run_blocks(self):
        """(workflow, step name, script) for every `run:` in every workflow."""
        for path in sorted(glob.glob(os.path.join(WORKFLOWS, "*.yml"))):
            with open(path) as fh:
                doc = yaml.safe_load(fh)
            # `on:` parses as the boolean True; irrelevant here, but jobs must
            # be reached without assuming the key set.
            for job in (doc.get("jobs") or {}).values():
                for step in job.get("steps") or []:
                    if isinstance(step.get("run"), str):
                        yield (
                            os.path.basename(path),
                            step.get("name", "<unnamed>"),
                            self.CONTINUATION.sub(" ", step["run"]),
                        )

    def test_no_bare_test_and_echo_as_the_last_statement_in_a_loop(self):
        guard = re.compile(r"^\s*(\[\[?|test)\s.*?\]\]?\s*&&\s*\S")
        for workflow, step, script in self._run_blocks():
            lines = script.splitlines()
            for i, line in enumerate(lines[:-1]):
                if not guard.match(line) or "||" in line:
                    continue  # `... || true` already neutralises the exit code
                if re.match(r"^\s*done\s*$", lines[i + 1]):
                    self.fail(
                        f"{workflow} / {step}: `[ ... ] && ...` is the last "
                        f"statement in a loop body, so under `bash -e` the whole "
                        f"step fails when the final iteration does not match:\n"
                        f"    {line.strip()}\n"
                        f"Use `if ... then ... fi`."
                    )

    def test_grep_in_a_pipefail_pipeline_cannot_fail_the_step(self):
        for workflow, step, script in self._run_blocks():
            if "pipefail" not in script:
                continue
            for line in script.splitlines():
                if not re.search(r"\|\s*grep\b", line):
                    continue
                stripped = line.strip()
                # An `if`/`while` condition consumes the exit code deliberately.
                if re.match(r"^(if|while|until|elif)\s", stripped):
                    continue
                # So does ANY `||` after the grep -- `|| true`, `|| :`, and the
                # `| grep -qx true || { ... }` guard idiom in int-tests.yml,
                # which is correct code and which an earlier, narrower version of
                # this check flagged. A rule that fires on correct code is worth
                # less than no rule: it gets suppressed, and then the real
                # instances go with it.
                #
                # Known blind spot: a `||` inside the pattern itself (an extended
                # regex alternation) would read as handling. Accepted -- erring
                # toward silence here, because the cost of a false alarm in a
                # convention check is that people stop reading it.
                after_grep = stripped[stripped.rfind("grep"):]
                if "||" in after_grep:
                    continue
                self.fail(
                    f"{workflow} / {step}: grep runs inside a `pipefail` "
                    f"pipeline with its exit code load-bearing, so matching "
                    f"nothing fails the step:\n    {stripped}\n"
                    f"If no match is a valid outcome, add `|| true`."
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
