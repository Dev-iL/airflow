#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Orchestrator for the async-session POC benchmark.
#
# Workflow:
#   1. Precheck: asyncpg importable inside breeze.
#   2. Export DB pool calibration (INV-G17: combined pool >= 500).
#   3. Enable the synthetic bench routes via gate env var.
#   4. Caller is responsible for `breeze start-airflow --backend postgres ...`
#      with this script's env_for_server.sh sourced first.
#   5. Seed DB (1000 bench_var_* rows).
#   6. Run the 24-cell sweep against the six /__bench routes.
#
# The script does NOT start breeze itself — `breeze start-airflow` is
# interactive and long-lived. Instead, this script generates env_for_server.sh
# which the operator sources before running breeze, then runs sweep + seed.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

: "${BENCH_TARGET_BASE_URL:=http://localhost:8080}"
: "${BENCH_DURATION:=30}"
: "${BENCH_WARMUP:=5}"
: "${BENCH_SLEEP_MS:=500}"
: "${BENCH_CONCURRENCIES:=50 100 200 400}"

read -r -a CONCURRENCIES <<<"$BENCH_CONCURRENCIES"

# Calibration: combined pool >= 500 (INV-G17, R-6). Defaults sized for the
# c=400 sweep with ~50% headroom (PG-11).
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE="${AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE:-40}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW="${AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW:-560}"

export AIRFLOW__BENCHMARK__ENABLE_ASYNC_SESSION_ROUTES=true

sync_pool_total=$((AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE + AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW))
if [[ "$sync_pool_total" -lt 500 ]]; then
    echo "FATAL: combined pool size $sync_pool_total < 500 (INV-G17). Increase POOL_SIZE/MAX_OVERFLOW." >&2
    exit 2
fi

cat >"$HERE/env_for_server.sh" <<EOF
# Source this before launching breeze:
#   source dev/async_session_poc/env_for_server.sh
#   breeze start-airflow --backend postgres
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=$AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=$AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW
export AIRFLOW__BENCHMARK__ENABLE_ASYNC_SESSION_ROUTES=true
EOF
echo "Wrote $HERE/env_for_server.sh"

echo "Precheck: asyncpg importable inside breeze..."
if ! breeze run python -c 'import asyncpg; print(asyncpg.__version__)'; then
    echo "FATAL: asyncpg not importable inside breeze (R-7/INV-G18). Install before running the bench." >&2
    exit 3
fi

echo "Precheck: bench routes are reachable..."
if ! curl -fsS -o /dev/null "${BENCH_TARGET_BASE_URL}/execution/__bench/select1/sync"; then
    echo "FATAL: ${BENCH_TARGET_BASE_URL}/execution/__bench/select1/sync not reachable." >&2
    echo "       Did you source env_for_server.sh and start breeze with --backend postgres?" >&2
    exit 4
fi

echo "Seeding 1000 bench_var_* Variables..."
breeze run python "$HERE/seed.py"

RESULTS_CSV="$HERE/results.csv"
rm -f "$RESULTS_CSV"

run_cell() {
    local route_label="$1" mode="$2" path="$3" concurrency="$4"
    local url="${BENCH_TARGET_BASE_URL}${path}"
    echo "[bench] route=${route_label} mode=${mode} c=${concurrency}  url=${url}"
    uv run --project airflow-core python "$HERE/run_bench.py" \
        --url "$url" \
        --concurrency "$concurrency" \
        --duration "$BENCH_DURATION" \
        --warmup "$BENCH_WARMUP" \
        --route "$route_label" \
        --mode "$mode" \
        --output "$RESULTS_CSV"
}

for c in "${CONCURRENCIES[@]}"; do
    run_cell "select1"           "sync"  "/execution/__bench/select1/sync"                          "$c"
    run_cell "select1"           "async" "/execution/__bench/select1/async"                         "$c"
    run_cell "sleep_${BENCH_SLEEP_MS}ms" "sync"  "/execution/__bench/sleep/sync?ms=${BENCH_SLEEP_MS}"  "$c"
    run_cell "sleep_${BENCH_SLEEP_MS}ms" "async" "/execution/__bench/sleep/async?ms=${BENCH_SLEEP_MS}" "$c"
    run_cell "var_count"         "sync"  "/execution/__bench/var_count/sync"                        "$c"
    run_cell "var_count"         "async" "/execution/__bench/var_count/async"                       "$c"
done

echo "Done. Results: $RESULTS_CSV"
