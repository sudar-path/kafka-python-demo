#!/usr/bin/env python3
"""beforeShellExecution hook: refuse the commands that are not the agent's call.

AGENTS.md already says "do not run apply_offsets.py against a broker" and "do
not push". Prose in a context file is a strong suggestion to a model and nothing
at all to a model that has compacted the file out of its context, or to the next
model, or to a teammate who never read it. This is the same rule expressed as
something that actually stops the command.

Contract (Cursor hooks):
  * stdin  -- JSON describing the pending shell call; `command` is what matters
  * stdout -- JSON verdict {"permission": "allow"|"deny"|"ask", ...}
  * exit 2 -- block. Any other exit code fails OPEN, which is why hooks.json
              also sets "failClosed": true for this hook: a crash here must not
              silently become permission to run.

Deliberately stdlib-only and importable without side effects, so
tools/test_shell_guard.py can prove each rule fires. An unproven control is the
mistake this repo already made once (handoff §14) and the reason
test_lint_conventions.py exists.

What this is NOT: a security boundary. It reads one command string and can be
walked around by anyone who wants to -- a base64'd payload, a wrapper script, a
different terminal. It is a guardrail against the plausible accident, not a
sandbox. The real controls are the `environment: production` approval gate and
the fact that PROD credentials are not on this laptop.
"""

from __future__ import annotations

import json
import os
import re
import sys

# Commands that only read. Checked first, and they short-circuit to allow.
#
# Without this the guard blocks `cat tools/apply_offsets.py` and
# `grep apply_seek tools/apply_offsets.py`, because a substring match cannot
# tell "run this" from "look at this". Blocking someone from READING the
# dangerous tool is pure friction, and friction is how a guardrail gets turned
# off. Both cases were caught by tools/test_shell_guard.py.
#
# Yes, `cat x | bash` walks straight through this. See the module docstring:
# guardrail against the plausible accident, not a sandbox.
READ_ONLY_COMMANDS = {
    "cat", "bat", "head", "tail", "less", "more", "wc", "grep", "rg", "ag",
    "find", "ls", "tree", "file", "stat", "diff", "cmp", "md5sum", "shasum",
    "code", "vim", "vi", "nano", "open", "column", "jq", "yq", "awk",
}

# Leading `FOO=bar` assignments are part of how these tools are invoked
# (CONFIRM=prod-reset ..., PY=python ...), so they are skipped when looking for
# the actual command word.
_ENV_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")


def _leading_command(command: str) -> str:
    for token in command.strip().split():
        if _ENV_ASSIGNMENT.match(token):
            continue
        return os.path.basename(token)
    return ""


# (name, pattern, message). Order matters only for which message you get first.
RULES = [
    (
        "apply-offsets",
        # The apply path, however it is invoked -- python tools/apply_offsets.py,
        # ./tools/apply_offsets.py, uv run apply_offsets.py.
        re.compile(r"apply_offsets\.py(?!.*(--help|-h\b))"),
        "tools/apply_offsets.py is the only tool here that mutates a broker. It "
        "runs from promote-prod.yml, on an artifact a human approved, after the "
        "environment:production gate -- not from a chat. To preview a change use "
        "tools/offset_diff.py, which is read-only.",
    ),
    (
        "git-push",
        # `git -C /path push` and `git -c k=v push` take a VALUE after the
        # flag, so a bare `(?:-\S+\s+)*` stops at the path and never reaches
        # `push`. That gap made the single most obvious bypass -- point git at
        # another working copy -- sail through.
        re.compile(r"\bgit\s+(?:(?:-[cC]\s+\S+|-\S+)\s+)*push\b"),
        "Pushing is the repository owner's decision, not the agent's. The tree "
        "carries handoff.md, baselines/ and infrastructure hostnames that are "
        "gitignored but have never been checked by hand against the manifest in "
        "handoff §16a. Ask, then push yourself.",
    ),
    (
        "prod-reset",
        re.compile(r"reset_demo\.sh\s+(restore|clean)\s+(prod|all)\b"),
        "restore/clean against PROD destroys the state a rehearsal is measured "
        "against, and PROD has no broker auth so nothing else will stop it. Run "
        "it yourself with CONFIRM=prod-reset if that is really what you want.",
    ),
    (
        "kafka-offset-write",
        # Kafka's own CLI can do everything apply_offsets.py can. Blocking only
        # the Python tool would be theatre.
        re.compile(r"kafka-consumer-groups(\.sh)?\b.*--reset-offsets\b.*--execute\b"),
        "This commits offsets on a real broker via Kafka's admin CLI. Same "
        "boundary as apply_offsets.py: preview with tools/offset_diff.py, apply "
        "through the promotion workflow.",
    ),
    (
        "track-operational-notes",
        re.compile(r"\bgit\s+add\b.*\b(handoff\.md|baselines/|.*\.pem|.*\.key)"),
        "handoff.md, baselines/ and key material must never be tracked. They are "
        "in .gitignore and conventions.yml has a backstop, but `git add -f` beats "
        "both and the miss is not recoverable by deleting the file afterwards.",
    ),
]


def classify(command: str):
    """Return (rule_name, message) for the first rule that matches, else None."""
    if not command:
        return None
    if _leading_command(command) in READ_ONLY_COMMANDS:
        return None
    for name, pattern, message in RULES:
        if pattern.search(command):
            return name, message
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        # failClosed in hooks.json turns a non-zero exit into a block. Refusing
        # on unparseable input is the conservative half of that bargain: if the
        # hook cannot see the command, it cannot vouch for it.
        print(
            json.dumps(
                {
                    "permission": "deny",
                    "userMessage": "shell guard could not parse the hook payload",
                    "agentMessage": "The shell guard received input it could not parse and denied by default.",
                }
            )
        )
        return 2

    command = payload.get("command") or ""
    hit = classify(command)

    if hit is None:
        print(json.dumps({"permission": "allow"}))
        return 0

    name, message = hit
    print(
        json.dumps(
            {
                "permission": "deny",
                "userMessage": f"blocked by shell guard [{name}]",
                "agentMessage": f"Blocked by .cursor/hooks guard rule '{name}'.\n\n{message}",
            }
        )
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
