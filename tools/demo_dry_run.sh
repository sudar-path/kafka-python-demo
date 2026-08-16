#!/usr/bin/env bash
#
# The local dry run: what a new engineer's mistakes look like, one at a time.
#
# This runs the CONVENTION layer end to end with no broker, no Airflow, and no
# network -- stdlib ast + PyYAML, about a fifth of a second. It is the loop the
# rule pack exists to close: make a plausible first-contribution mistake, get
# told which touchpoint you missed and why, fix it.
#
# It does NOT run the operator. ResetConsumerGroupOffsetsOperator.execute()
# needs a live Kafka broker and an installed provider; that runs on the INT VM,
# not here. Keeping the two separate is deliberate -- this script's claim is
# "the rules fire on real mistakes", and it can make that claim offline.
#
# Every break is applied to a COPY under a temp dir. contribution/ is never
# modified, so a demo that dies halfway leaves nothing to clean up.
#
# Usage:  tools/demo_dry_run.sh [-q]     -q prints one line per break

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Resolve PY as a path OR a command name. `[ -x python ]` is false for a bare
# name, so an earlier version silently ignored PY= and always used python3 --
# which on this laptop is 3.9.6 without PyYAML, and the script reported
# "contribution/ is ALREADY failing" on a tree that was fine.
PY="${PY:-$REPO/.venv/bin/python}"
[ -x "$PY" ] || PY="$(command -v "$PY" || command -v python3)"
if ! "$PY" -c 'import yaml' 2>/dev/null; then
  echo "$PY cannot import yaml. Use the venv, or pip install PyYAML." >&2
  exit 2
fi
QUIET=""
[ "${1:-}" = "-q" ] && QUIET=1

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp -R "$REPO/contribution" "$WORK/contribution"

OP="$WORK/contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py"
UNIT="$WORK/contribution/providers/apache/kafka/tests/unit/apache/kafka/operators/test_reset_offsets.py"
# Located rather than hardcoded. The literal path here went stale the moment the
# integration test was moved to mirror upstream's layout, and the TEST-002 case
# then reported FAIL for a missing file rather than for a missing teardown --
# a broken demo that looks like a broken rule.
INTEG="$(find "$WORK/contribution" -path '*tests/integration/*' -name 'test_reset_offsets_integration.py' | head -1)"
[ -n "$INTEG" ] || { echo "cannot locate the integration test under contribution/" >&2; exit 2; }
YAML="$WORK/contribution/providers/apache/kafka/provider.yaml"

PASS=0
FAIL=0

lint() {
  "$PY" "$REPO/tools/lint_conventions.py" "$WORK/contribution" \
    --rules "$REPO/rules" --root "$WORK"
}

# break <expected-rule-id> <description> -- stdin is a shell snippet that
# introduces the mistake. Snapshot, break, lint, assert, restore.
break_case() {
  local want="$1" desc="$2" snippet out rc
  snippet="$(cat)"

  cp -R "$WORK/contribution" "$WORK/.snapshot"
  ( cd "$WORK" && eval "$snippet" )

  out="$(lint 2>&1)"; rc=$?
  rm -rf "$WORK/contribution" && mv "$WORK/.snapshot" "$WORK/contribution"

  if grep -q "$want" <<<"$out" && [ "$rc" -eq 1 ]; then
    PASS=$((PASS + 1))
    printf '  \033[32mPASS\033[0m  %-10s %s\n' "$want" "$desc"
    # The finding itself, plus its `fix:`/`see:` continuation lines. This is
    # the part worth reading: the message names the touchpoint, and `see:`
    # points at the rule file, so the engineer can argue with the rule rather
    # than just obey it.
    if [ -z "$QUIET" ]; then
      grep -A2 "ERROR $want" <<<"$out" | sed 's/^/        /'
      echo
    fi
  else
    FAIL=$((FAIL + 1))
    printf '  \033[31mFAIL\033[0m  %-10s %s (exit %s, rule not reported)\n' "$want" "$desc" "$rc"
    printf '%s\n' "$out" | sed 's/^/        /'
  fi
}

echo
echo "clean tree first -- if this is not silent, nothing below means anything"
lint --summary >/dev/null 2>&1 || true
if lint >/dev/null 2>&1; then
  echo "  OK: contribution/ passes all 9 enforceable rules"
else
  echo "  contribution/ is ALREADY failing. Fix that before reading the breaks:"
  lint | sed 's/^/    /'
  exit 2
fi

echo
echo "now the mistakes, one touchpoint at a time"
echo

break_case TEST-001 "touchpoint 5: no mirrored unit test" <<'EOF'
rm contribution/providers/apache/kafka/tests/unit/apache/kafka/operators/test_reset_offsets.py
EOF

break_case PROV-004 "touchpoint 3: operator not registered in provider.yaml" <<'EOF'
grep -v 'operators.reset_offsets' contribution/providers/apache/kafka/provider.yaml > /tmp/pv.$$ \
  && mv /tmp/pv.$$ contribution/providers/apache/kafka/provider.yaml
EOF

break_case PROV-003 "a seek mode left out of template_fields" <<'EOF'
perl -0pi -e 's/^        "to_timestamp",\n//m' \
  contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py
perl -0pi -e 's/    template_fields: Sequence\[str\] = \([^)]*\)/    pass/s' \
  contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py
EOF

break_case PROV-002 "no 'from __future__ import annotations'" <<'EOF'
perl -0pi -e 's/^from __future__ import annotations\n//m' \
  contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py
EOF

break_case PROV-001 "ASF licence header stripped" <<'EOF'
perl -0pi -e 's/^#\n# Licensed to the Apache.*?# under the License\.\n//s' \
  contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py
EOF

break_case KAFKA-003 "list_topics(topic=...) -- silently CREATES the topic" <<'EOF'
perl -0pi -e 's/(        self\._require_empty_group\(admin\))/        admin.list_topics(topic=self.topics[0])\n$1/' \
  contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py
EOF

break_case KAFKA-001 "list_groups() -- removed in confluent-kafka 2.x" <<'EOF'
perl -0pi -e 's/(        self\._require_empty_group\(admin\))/        admin.list_groups()\n$1/' \
  contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py
EOF

break_case KAFKA-002 "import from the wrong confluent_kafka submodule" <<'EOF'
perl -0pi -e 's/^from airflow\.exceptions import AirflowException$/from confluent_kafka.admin import TopicPartition\nfrom airflow.exceptions import AirflowException/m' \
  contribution/providers/apache/kafka/src/airflow/providers/apache/kafka/operators/reset_offsets.py
EOF

break_case TEST-002 "integration test creates a topic and never deletes it" <<'EOF'
perl -0pi -e 's/^    hook\.delete_topic\(topics=\[name\]\)$/    pass  # leaked/m' \
  "$(find contribution -path '*tests/integration/*' -name 'test_reset_offsets_integration.py' | head -1)"
EOF

echo
echo "----------------------------------------------------------------------"
printf '%d of %d enforceable rules demonstrated on a real mistake' "$PASS" "$((PASS + FAIL))"
echo
echo
echo "KAFKA-004 is advisory with no detector -- it ships as agent context and"
echo "docs only. It is counted nowhere above, on purpose."
echo

[ "$FAIL" -eq 0 ] || exit 1
