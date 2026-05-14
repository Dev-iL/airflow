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
# Multi-worker follow-up sweep (POC Next Step #1 + #2).
#
# Runs the {sleep, var_count} A/B cells with a MULTI-PROCESS client (--procs)
# against an API server whose AIRFLOW__API__WORKERS is set via
# files/airflow-breeze-config/environment_variables.env. The server-worker count
# is recorded per row (passed through --server-workers) so one CSV spans configs.
#
# The server is NOT started here — start breeze start-airflow separately with the
# desired AIRFLOW__API__WORKERS, then run this with a matching SERVER_WORKERS.
#
# Env knobs (all optional):
#   SERVER_WORKERS   AIRFLOW__API__WORKERS the server was started with (label only). Default 1.
#   BENCH_PROCS      client OS processes. Default 4.
#   BENCH_BASE_URL   default http://localhost:28080  (breeze forwards the api server here)
#   BENCH_DURATION / BENCH_WARMUP / BENCH_SLEEP_MS / BENCH_CONCURRENCIES
#   BENCH_OUTPUT     CSV path. Default dev/async_session_poc/results_multiworker.csv

set -euo pipefail
HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

: "${SERVER_WORKERS:=1}"
: "${BENCH_PROCS:=4}"
: "${BENCH_BASE_URL:=http://localhost:28080}"
: "${BENCH_DURATION:=30}"
: "${BENCH_WARMUP:=5}"
: "${BENCH_SLEEP_MS:=500}"
: "${BENCH_CONCURRENCIES:=50 100 200 400}"
: "${BENCH_OUTPUT:=$HERE/results_multiworker.csv}"

read -r -a CONCURRENCIES <<<"$BENCH_CONCURRENCIES"

run_cell() {
    local route_label="$1" mode="$2" path="$3" concurrency="$4"
    echo "[bench] workers=${SERVER_WORKERS} procs=${BENCH_PROCS} route=${route_label} mode=${mode} c=${concurrency}"
    uv run --project airflow-core --frozen python "$HERE/run_bench.py" \
        --url "${BENCH_BASE_URL}${path}" \
        --concurrency "$concurrency" \
        --procs "$BENCH_PROCS" \
        --duration "$BENCH_DURATION" \
        --warmup "$BENCH_WARMUP" \
        --route "$route_label" \
        --mode "$mode" \
        --server-workers "$SERVER_WORKERS" \
        --output "$BENCH_OUTPUT"
}

for c in "${CONCURRENCIES[@]}"; do
    run_cell "sleep_${BENCH_SLEEP_MS}ms" "sync"  "/execution/__bench/sleep/sync?ms=${BENCH_SLEEP_MS}"  "$c"
    run_cell "sleep_${BENCH_SLEEP_MS}ms" "async" "/execution/__bench/sleep/async?ms=${BENCH_SLEEP_MS}" "$c"
    run_cell "var_count" "sync"  "/execution/__bench/var_count/sync"  "$c"
    run_cell "var_count" "async" "/execution/__bench/var_count/async" "$c"
done

echo "Done (workers=${SERVER_WORKERS}). Results appended to: $BENCH_OUTPUT"
