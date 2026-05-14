<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Multi-worker rerun (POC Next Step #1 + #2)](#multi-worker-rerun-poc-next-step-1--2)
  - [Headline](#headline)
  - [Environment](#environment)
  - [Clean-regime cells (error rate < 2%) — the trustworthy signal](#clean-regime-cells-error-rate--2--the-trustworthy-signal)
  - [Why the high-concurrency cells are not interpretable](#why-the-high-concurrency-cells-are-not-interpretable)
  - [What this run *did* establish](#what-this-run-did-establish)
  - [What a definitive crossover test still requires](#what-a-definitive-crossover-test-still-requires)
  - [Harness / infrastructure corrections discovered during this run](#harness--infrastructure-corrections-discovered-during-this-run)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
 TODO: This license is not consistent with the license used in the project.
       Delete the inconsistent license and above line and rerun pre-commit to insert a good license.

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Multi-worker rerun (POC Next Step #1 + #2)

This is the follow-up the original POC deferred: re-run the A/B sweep with a multi-worker uvicorn/gunicorn API server (`AIRFLOW__API__WORKERS` ∈ {1, 4}) and a multi-process client (`run_bench.py --procs 4`, removing the single-process client-GIL confound R-2). Raw data: [`results_multiworker.csv`](results_multiworker.csv) (32 cells = 2 routes × 4 concurrencies × 2 modes × 2 worker counts).

## Headline

The POC's central prediction — *"async's ceiling rises with workers, eventually crossing sync at high concurrency"* — is **not confirmed** by this run, and the experiment as configured **cannot** confirm it on this hardware. The clean, uncontaminated cells (error rate < 2%) continue to favour async by a modest margin (+2% to +24% RPS), reproducing and slightly extending the POC's low-concurrency result. Every high-concurrency cell is dominated by saturation, and at 4 workers specifically by **Postgres connection exhaustion** — so the high-concurrency comparison measures overload behaviour, not a throughput ceiling.

## Environment

Single 8-core / 31 GB host. API server (in the breeze container) and the 4-process client share those 8 cores — they compete for CPU, so cells where the box saturates are not clean measurements. Postgres 14, `max_connections = 100` (default). Async pool calibrated as the POC specified: `SQL_ALCHEMY_POOL_SIZE=40`, `SQL_ALCHEMY_MAX_OVERFLOW=560` (per worker).

## Clean-regime cells (error rate < 2%) — the trustworthy signal

| route | workers | concurrency | sync RPS | async RPS | async vs sync |
|---|---:|---:|---:|---:|---:|
| `var_count` | 1 | 50 | 265.2 | 286.0 | **+8%** |
| `var_count` | 1 | 100 | 253.7 | 287.1 | **+13%** |
| `var_count` | 4 | 50 | 695.9 | 708.0 | **+2%** (tie) |
| `sleep_500ms` | 1 | 50 | 79.6 | 98.4 | **+24%** |
| `sleep_500ms` | 4 | 50 | 98.4 | 157.9 ⚠ | async run had 50% errors — not clean |

In every cell that stays under ~2% error, async is at least as fast as sync, by +2% to +24%. This is the same "unblock the event loop" effect the POC measured at c=50, now also visible at c=100 for `var_count`. Going from 1→4 workers scaled clean-regime throughput ~2.5–2.6× for `var_count` (265→696 sync, 286→708 async) — workers help, as expected.

## Why the high-concurrency cells are not interpretable

Every cell at concurrency ≥ 100 ran at 15–95% error rate on **both** sync and async. Two compounding causes:

1. **CPU contention.** 4 server workers + 4 client processes + Postgres + the other start-airflow components (scheduler, triggerer, dag-processor) oversubscribe 8 cores. At c ≥ 100 the box is saturated; the numbers reflect queueing and timeouts, not a clean ceiling.
2. **Postgres connection exhaustion at 4 workers (decisive).** The POC's pool calibration (`pool_size=40 + max_overflow=560`) was sized for a *single* worker. With 4 workers each process gets its own async engine and pool, so the API server alone holds 4 × 40 = **160 connections idle** — already above `max_connections=100` — and demands up to 4 × 600 = **2400** under load. During and after the 4-worker sweep, Postgres returned `FATAL: sorry, too many clients already` (a fresh `psql` could not even connect). `sleep_500ms` async at c=50/workers=4 showing 50% errors is the tell: `pg_sleep(0.5)` holds a connection for half a second, so the long-held connections exhaust the limit fastest on the async path, which acquires them most aggressively on the event loop.

Under that overload, async degraded *more* than sync (e.g. `var_count` w=4 c=100: sync 566 RPS / 6% err vs async 143 RPS / 43% err). That is a real observation about behaviour-under-overload, but it is **not** evidence about the async-vs-sync throughput ceiling, because the regime is connection-starved rather than CPU- or loop-bound in a controlled way.

## What this run *did* establish

- **The R-2 (client-GIL) confound is removed and the workers=1 inversion persists.** With a 4-process client, async still inverts above c=50 at workers=1. So that inversion is **server-side** (single event loop serialising CPU-bound request/response framing), not a client-side measurement artifact.
- **Workers scale throughput** in the clean regime (~2.5×, 1→4).
- **A concrete prerequisite for any valid multi-worker async benchmark**, previously unstated: per-worker `pool_size × workers` (plus baseline component connections) must fit within Postgres `max_connections`. The POC's single-worker calibration actively breaks the multi-worker experiment. A valid run needs either small per-worker pools (e.g. `pool_size≈10`, `max_overflow≈15`, so 4 × 25 = 100) **or** a raised `max_connections`, ideally both.

## What a definitive crossover test still requires

1. **Separate hosts** for server and client (remove the 8-shared-core CPU contention).
2. **Pool sizing reconciled with `max_connections`** as above, so high-concurrency cells fail on the loop/threadpool ceiling under test — not on connection starvation.
3. **A raised Postgres `max_connections`** (e.g. 500+) matched to `workers × per-worker pool`.

Until those hold, the crossover question stays **open**: this box can show the clean-regime async win (real, modest) but cannot drive a *clean* high-concurrency comparison.

## Harness / infrastructure corrections discovered during this run

Recorded so the next person does not rediscover them:

- **Forwarded API-server port is `:28080`** on the host (breeze forwarding), not `:8080` as the original README states.
- **Config reaches the API-server process only via `files/airflow-breeze-config/environment_variables.env`** (sourced inside the container). Host `export AIRFLOW__…` before `breeze start-airflow` is **not** forwarded — breeze writes a curated env allowlist into the compose env file, and arbitrary `AIRFLOW__*` host vars are not in it. The original README's `source env_for_server.sh; breeze start-airflow` does not actually propagate those vars.
- **Headless `breeze start-airflow`** needs a sized pseudo-TTY: plain background fails with `Stdin is not a tty`; a bare pty makes mprocs panic on a 0×0 terminal; a **detached tmux session with an explicit size** (`tmux new-session -d -x 220 -y 50`) works.
- **`run_bench.py --procs M`** spreads load over M processes and merges raw latencies across the host-shared `CLOCK_MONOTONIC`; `--server-workers` records the server config per CSV row. Driver: [`run_multiworker.sh`](run_multiworker.sh).
