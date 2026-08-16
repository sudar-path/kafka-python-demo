#!/usr/bin/env bash
#
# INT bootstrap — provider dev environment at the declared dependency floor.
#
# INT answers one question: "is the code correct against the OLDEST dependency set
# the provider claims to support?" That is confluent-kafka 2.6.0, not whatever a
# fresh `pip install` resolves to (2.15.0, which is what STAG runs).
#
# Verified end to end on the INT VM 2026-08-15:
#   79 unit tests green, 9 integration tests green against the real broker,
#   using upstream's own test files unmodified.
#
# Idempotent. Run from anywhere. Takes ~1 minute.
#
set -euo pipefail

AIRFLOW_TAG="${AIRFLOW_TAG:-3.3.1}"
CONFLUENT_PIN="${CONFLUENT_PIN:-2.6.0}"     # the floor from providers/apache/kafka/pyproject.toml
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"    # must be < 3.14, above which the floor jumps to 2.13.2
REPO_DIR="${REPO_DIR:-$HOME/airflow}"

# tests_common/pytest_plugin.py shells out to `uv` at plugin-import time, before any
# test runs. Without it on PATH you get FileNotFoundError and zero collection.
# The self-hosted runner does not inherit the login shell PATH -- CI must write this
# to $GITHUB_PATH, not rely on .bashrc.
export PATH="$HOME/.local/bin:$PATH"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "uv"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
uv --version

step "airflow checkout @ tag ${AIRFLOW_TAG}"
# Pin the TAG, not main. main is core 3.4.0-dev, and tests_common injects the repo's
# airflow-core/src onto sys.path -- so repo source silently shadows the installed wheel.
# Mixing 3.4.0-dev source with 3.3.1 wheels yields 144 collection errors ending in
# "'EmptyOperator' object has no attribute 'is_stub'", which tells you nothing.
# Pinning also aligns INT with the apache/airflow:3.3.1 image on STAG and PROD.
if [ ! -d "${REPO_DIR}/.git" ]; then
  git clone --depth 1 --filter=blob:none https://github.com/apache/airflow.git "${REPO_DIR}"
fi
cd "${REPO_DIR}"
git fetch --depth 1 origin tag "${AIRFLOW_TAG}"
git checkout -q "${AIRFLOW_TAG}"
git log -1 --format='  at %h (%ad) %d' --date=short

step "venv @ python ${PYTHON_VERSION}"
uv venv --python "${PYTHON_VERSION}"

step "core + floor"
uv pip install -q \
  "apache-airflow==${AIRFLOW_TAG}" \
  "confluent-kafka==${CONFLUENT_PIN}" \
  pytest pytest-asyncio

step "in-repo packages"
# --no-deps is load-bearing: without it the resolver drags confluent-kafka past the
# floor and INT stops testing what it exists to test.
# common/messaging is required but not obviously declared -- without it the kafka
# provider's trigger tests raise ModuleNotFoundError and pytest aborts the whole run.
uv pip install -q --no-deps -e providers/apache/kafka
uv pip install -q --no-deps -e providers/common/compat
uv pip install -q --no-deps -e providers/common/messaging

# devel-common must come from the repo, NOT PyPI apache-airflow-devel-common==0.1.1 --
# the pytest plugin and the checkout have to be the same generation.
uv pip install -q -e devel-common

step "optional provider extras"
# Without these, 3 unit tests fail on missing optional imports (openlineage x2, and
# ManagedKafkaHook which wants google >= 14.1.0). Re-pin core and the floor here so
# the resolver cannot drift while pulling them in.
uv pip install -q \
  "apache-airflow==${AIRFLOW_TAG}" \
  "confluent-kafka==${CONFLUENT_PIN}" \
  "apache-airflow-providers-openlineage" \
  "apache-airflow-providers-google>=14.1.0"

step "pins held"
uv pip list 2>/dev/null | grep -Ei "^(apache-airflow|confluent-kafka|apache-airflow-providers-apache-kafka) "
.venv/bin/python -c "
import airflow, confluent_kafka
assert confluent_kafka.__version__ == '${CONFLUENT_PIN}', \
    f'floor drifted to {confluent_kafka.__version__}'
print(f'  airflow {airflow.__version__}  confluent-kafka {confluent_kafka.__version__}  OK')
"

cat <<EOF

Ready. Run the suites with:

  cd ${REPO_DIR}
  export PATH=\$HOME/.local/bin:\$PATH
  export AIRFLOW_HOME=\$HOME/airflow-home

  .venv/bin/python -m pytest providers/apache/kafka/tests/unit -q
      -> expect 79 passed

  INTEGRATION_KAFKA=true .venv/bin/python -m pytest \\
      providers/apache/kafka/tests/integration -q --integration kafka
      -> expect 9 passed  (needs BOTH the env var and the --integration flag)

The integration suite requires the broker to answer on broker:29092, which upstream
hardcodes. See deploy/integration/kafka-compose.override.yaml.
EOF
