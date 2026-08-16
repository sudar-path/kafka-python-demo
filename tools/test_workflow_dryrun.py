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

import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

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
        script = generate(os.path.join(WORKFLOWS, "stag-tests.yml"), None, allow=False)
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
        "stag-tests.yml": [None],
        "int-tests.yml": [None],
        "conventions.yml": [None],
        # promote-prod.yml is driven by workflow_dispatch inputs, so its run
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

    def test_promote_prod_needs_the_escape_hatch(self):
        """Documents the exclusion above rather than leaving it to a comment: if
        promote-prod.yml ever stops using inputs inline, this test fails and the
        workflow should be added to JOBS."""
        path = os.path.join(WORKFLOWS, "promote-prod.yml")
        if not os.path.exists(path):
            self.skipTest("promote-prod.yml not present")
        with self.assertRaises(SystemExit):
            generate(path, "dry-run", allow=False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
