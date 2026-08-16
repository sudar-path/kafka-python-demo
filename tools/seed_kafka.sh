#!/usr/bin/env bash
#
# B6 — seed a Kafka broker with a topic, messages, and a consumer group that has
# committed offsets and measurable, UNEVEN per-partition lag.
#
# Why this exists: every downstream tool in the promotion gate (offset_diff.py,
# capture_offsets.py, policy_check.py) reads consumer-group offsets. Against an
# empty broker they all render nothing, which demos as "the tool is broken".
#
# Runs on the VM (uses `docker exec` into the Kafka container). Env-agnostic:
# used on INT for the dev loop and on PROD for the demo.
#
#   NOTE: handoff.md §12 lists this as deploy/production/seed-prod-kafka.sh.
#   Renamed because it is not production-specific -- it sits with the tools that
#   consume its output, and the target env is a flag.
#
# IDEMPOTENT BY TEARDOWN. Re-running deletes the group and topic and rebuilds
# them, so the end state is identical every time. That is deliberate: §5c
# established the Kafka containers declare NO volumes, so a broker recreate
# wipes every topic and committed offset. The demo cannot assume yesterday's
# state, so seeding has to be cheap and repeatable rather than incremental.
#
set -euo pipefail

TOPIC="${TOPIC:-payments.events}"
GROUP="${GROUP:-payments-reconciler}"
PARTITIONS="${PARTITIONS:-3}"
MESSAGES="${MESSAGES:-300}"
CONTAINER="${CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

# Lag to leave on partitions 0,1,2,... Uneven ON PURPOSE: uniform lag makes a
# per-partition diff look like pointless detail. Uneven lag is the whole reason
# the operator seeks per partition instead of taking one global offset.
LAGS="${LAGS:-5 12 3}"

usage() {
  cat <<EOF
usage: $(basename "$0") [--topic NAME] [--group NAME] [--partitions N]
                        [--messages N] [--lags "5 12 3"]
                        [--container NAME] [--bootstrap HOST:PORT]

All options also readable from the equivalent UPPERCASE env var.
Destructive: deletes and recreates \$TOPIC and \$GROUP on the target broker.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --topic)      TOPIC="$2"; shift 2 ;;
    --group)      GROUP="$2"; shift 2 ;;
    --partitions) PARTITIONS="$2"; shift 2 ;;
    --messages)   MESSAGES="$2"; shift 2 ;;
    --lags)       LAGS="$2"; shift 2 ;;
    --container)  CONTAINER="$2"; shift 2 ;;
    --bootstrap)  BOOTSTRAP="$2"; shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# Every Kafka CLI call goes through here so the container name and bootstrap
# address are stated exactly once.
K() {
  local script="$1"; shift
  docker exec "${CONTAINER}" "/opt/kafka/bin/${script}" \
    --bootstrap-server "${BOOTSTRAP}" "$@"
}

step "preflight"
docker inspect -f '{{.State.Running}}' "${CONTAINER}" >/dev/null 2>&1 || {
  echo "::error::container '${CONTAINER}' not found on this host." >&2
  echo "Run this ON the VM, not from a laptop. See handoff.md §5 for hosts." >&2
  exit 1
}
K kafka-topics.sh --list >/dev/null 2>&1 || {
  echo "::error::broker not answering on ${BOOTSTRAP} inside '${CONTAINER}'." >&2
  exit 1
}
echo "  broker up, ${CONTAINER} @ ${BOOTSTRAP}"

step "teardown (idempotency)"
# Group first: deleting a topic out from under a group leaves orphaned committed
# offsets that still show up in list_consumer_group_offsets and confuse the diff.
if K kafka-consumer-groups.sh --list 2>/dev/null | grep -qx "${GROUP}"; then
  K kafka-consumer-groups.sh --delete --group "${GROUP}" >/dev/null
  echo "  deleted group ${GROUP}"
fi
if K kafka-topics.sh --list 2>/dev/null | grep -qx "${TOPIC}"; then
  K kafka-topics.sh --delete --topic "${TOPIC}" >/dev/null
  # Deletion is asynchronous. Creating before it completes fails with
  # TopicExistsException, intermittently, which reads like a flaky script.
  for _ in $(seq 1 30); do
    K kafka-topics.sh --list 2>/dev/null | grep -qx "${TOPIC}" || break
    sleep 1
  done
  K kafka-topics.sh --list 2>/dev/null | grep -qx "${TOPIC}" && {
    echo "::error::topic ${TOPIC} still present 30s after delete" >&2; exit 1; }
  echo "  deleted topic ${TOPIC}"
fi

step "create topic ${TOPIC} (${PARTITIONS} partitions)"
K kafka-topics.sh --create --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" --replication-factor 1 >/dev/null
echo "  created"

step "produce ${MESSAGES} messages"
# Keyed so Kafka's partitioner spreads them deterministically -- same keys in,
# same per-partition counts out, every run. Unkeyed messages round-robin by
# batch and the partition counts drift between runs.
python3 - "$MESSAGES" <<'PY' > /tmp/seed-payload.txt
import sys
n = int(sys.argv[1])
for i in range(1, n + 1):
    acct = f"ACCT-{i % 97:04d}"          # 97 is prime -> even spread, no clumping
    print(f'{acct}:{{"event_id":{i},"account":"{acct}","amount_cents":{1000 + i * 7},"type":"settlement"}}')
PY
docker cp /tmp/seed-payload.txt "${CONTAINER}:/tmp/seed-payload.txt"
docker exec "${CONTAINER}" bash -c \
  "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${BOOTSTRAP} \
     --topic ${TOPIC} --reader-property parse.key=true \
     --reader-property key.separator=: < /tmp/seed-payload.txt"
rm -f /tmp/seed-payload.txt
echo "  produced ${MESSAGES}"

step "commit offsets for group ${GROUP}, leaving lag [${LAGS}]"
# No consumer is started. `--reset-offsets` against a group that does not exist
# CREATES it in EMPTY state with these committed offsets (verified on INT
# 2026-08-15). Empty is exactly the state alter_consumer_group_offsets requires
# -- that API refuses a group with active members -- so seeding this way also
# keeps the broker in the state the operator will later mutate.
read -r -a LAG_ARR <<< "${LAGS}"
for p in $(seq 0 $((PARTITIONS - 1))); do
  hwm=$(K kafka-get-offsets.sh --topic "${TOPIC}" --partitions "$p" \
        | awk -F: '{print $3}')
  lag="${LAG_ARR[$p]:-0}"
  target=$(( hwm - lag ))
  [ "$target" -lt 0 ] && target=0
  K kafka-consumer-groups.sh --group "${GROUP}" --topic "${TOPIC}:${p}" \
    --reset-offsets --to-offset "$target" --execute >/dev/null
  echo "  p${p}: high watermark ${hwm}, committed ${target}, lag $(( hwm - target ))"
done

step "verify"
K kafka-consumer-groups.sh --describe --group "${GROUP}"

# The group must be EMPTY, not STABLE. A live member would make every downstream
# offset mutation fail at apply time rather than here, which is a much worse
# place to discover it.
# Match on the group name in column 1. `--state` prints a prose line, a blank,
# and a header before the data row, so any "skip N lines" parse picks up the
# literal string "STATE" from the header and reports a false failure.
state=$(K kafka-consumer-groups.sh --describe --group "${GROUP}" --state \
        | awk -v g="${GROUP}" '$1 == g {print $(NF-1)}' | head -1)
if [ "${state}" != "Empty" ]; then
  echo "::error::group ${GROUP} is '${state}', expected 'Empty'." >&2
  echo "Something is actively consuming; offset mutation will be rejected." >&2
  exit 1
fi

cat <<EOF

Seeded. topic=${TOPIC} group=${GROUP} partitions=${PARTITIONS} state=Empty

Next:
  tools/offset_diff.py --group ${GROUP} --topic ${TOPIC}
EOF
