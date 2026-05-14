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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [TL;DR](#tldr)
- [What we kept from PR #36504](#what-we-kept-from-pr-36504)
- [What we deliberately set aside](#what-we-deliberately-set-aside)
- [What this POC actually does](#what-this-poc-actually-does)
- [Key outcomes](#key-outcomes)
- [Why a single uvicorn worker matters](#why-a-single-uvicorn-worker-matters)
- [Next steps](#next-steps)
- [Reproducing](#reproducing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## TL;DR

The **motivation** behind PR #36504 — *the Triggerer talks to the metadata DB directly, so we should make those DB calls async to free the event loop* — is **architecturally obsolete on Airflow 3.x**. Triggerer (and Workers) no longer hit the metadata DB directly; they go through the Execution API over HTTP.

What **remains valid and is now the actual win** is the lower-level idea underneath that PR: **converting Airflow's FastAPI routes from sync `SessionDep` to async `AsyncSessionDep` removes a real bottleneck — but the bottleneck is the Starlette/AnyIO threadpool inside the API server, not the Triggerer's loop.**

This POC builds a minimal, reproducible A/B benchmark of that conversion against a single Airflow 3.3.0 + PostgreSQL 14 deployment and produces honest measured numbers — both where async helps and where it surprisingly doesn't.

## What we kept from PR #36504

1. **The core conversion pattern.** Sync `SessionDep` → async `AsyncSessionDep`, `def` → `async def`, `session.scalar(...)` → `await session.scalar(...)`, `session.scalars(...).all()` → `(await session.scalars(...)).all()`. PR #36504 already demonstrated this mechanically for several call sites; the pattern is unchanged.
2. **The async-SQLAlchemy plumbing it presupposed.** Airflow 3.x already ships `create_async_engine`, `async_sessionmaker`, `AsyncSessionDep`, `create_session_async`, `paginated_select_async`, the `sql_alchemy_conn_async` config key, and automatic driver derivation via `AIO_LIBS_MAPPING` (`airflow-core/src/airflow/settings.py:240, 387, 415-421`). *None of this had to be re-built* — adoption is what's missing, not infrastructure. The current adoption rate inside `core_api` is ~0/49 routes using `AsyncSessionDep`.
3. **The route-conversion shape.** The POC converts exactly one real route — `GET /execution/variables/keys` in `airflow-core/src/airflow/api_fastapi/execution_api/routes/variables.py` — as a low-blast-radius proof that the migration is mechanical: response model unchanged, query-param signatures unchanged, status codes unchanged, all 30 existing variable-execution-API tests pass byte-identical.

## What we deliberately set aside

1. **The Triggerer-direct-DB framing.** In 3.x, the Triggerer evaluates deferred tasks but communicates via the Execution API, not direct DB access. PR #36504's headline benefit ("Triggerer event loop unblocked") no longer applies — there is no Triggerer code path in the hot zone the original change targeted. The README and `RESULTS.md` include an explicit disclaimer paragraph: *"Triggerer and Workers do not hit the metadata DB directly in 3.x."*
2. **`get_variable` / `get_connection` conversions.** Both call into the SecretsBackend chain (`Variable.get(...)`, `Connection.get_connection_from_secrets(...)`). Converting them requires building **async secrets backends**, which is PR-#36504-scale work and well beyond a single-endpoint POC. They are explicitly out of scope here.
3. **`ti_heartbeat` and other production hot paths.** Heartbeat is what matters most for throughput, so it deserves its own careful PR rather than being folded into the A/B harness. The route itself is actually a clean conversion candidate — pure SQL (a fast-path `UPDATE` by primary key, then a `SELECT … FOR UPDATE` fallback with 404/409/410 diagnostics); it does *not* do state-machine mutation, RTIF, or tracing work (those live in `ti_run` / `ti_update_state` / `ti_put_rtif`, not in heartbeat). What makes it follow-up material is operational blast radius — it is the highest-QPS write path in the system — not code complexity. The POC picks `get_variable_keys` as the cleanest low-blast-radius candidate and reserves heartbeat for a dedicated PR.
4. **SQLite and MySQL.** Single-writer locks on SQLite (via aiosqlite) and the aiomysql-vs-asyncmy driver picking on MySQL would muddy the measurement. Postgres-only (asyncpg) for this pass.
5. **Newsfragment, PR, provider changes, prod rollout.** This is a POC under `dev/`. No newsfragment, no `provider.yaml` edit, no `chart/` edit. A follow-up migration PR is downstream of these numbers.

## What this POC actually does

Three pieces of instrumentation, all under `airflow-core/src/airflow/api_fastapi/execution_api/` and `dev/async_session_poc/`:

- **A real-route conversion** — `get_variable_keys` → `async def` + `AsyncSessionDep` + awaited `session.scalar/scalars`.
- **A synthetic A/B router** at `/__bench/...`, mounted only when `AIRFLOW__BENCHMARK__ENABLE_ASYNC_SESSION_ROUTES=true` (exact-string gate; no `.lower()`, no truthy-set membership). Six routes — three pairs of identical SQL where the only difference is sync `SessionDep` vs async `AsyncSessionDep`:
  - `/select1/{sync,async}` — `SELECT 1` (per-request session overhead)
  - `/sleep/{sync,async}?ms=N` — `SELECT pg_sleep(N/1000.0)` (the threadpool-ceiling pedagogy)
  - `/var_count/{sync,async}` — `SELECT COUNT(*) FROM variable` (realistic aggregate; seeded with 1000 `bench_var_*` rows)
- **A pure-Python `httpx.AsyncClient`-based harness** with a CSV/markdown reporting pipeline (`dev/async_session_poc/`). 24-cell sweep: 6 routes × {50, 100, 200, 400} concurrency, 30s measured + 5s warmup per cell.

## Key outcomes

Full numbers and per-route tables live in [`dev/async_session_poc/RESULTS.md`](RESULTS.md). The headline:

### Where async wins cleanly — c=50

At concurrency 50 (below the 40-token AnyIO threadpool ceiling), async is consistently faster than sync on every route, with **zero error rate** on both sides:

| route | sync RPS | async RPS | sync p99 | async p99 |
|---|---:|---:|---:|---:|
| `/select1` | 121.3 | 131.7 | 1749 ms | 1747 ms |
| `/sleep` 500ms | 78.5 | **96.4** | 893 ms | **558 ms** |
| `/var_count` | 156.6 | 175.2 | 1403 ms | 1378 ms |

The `/sleep` row is the cleanest signal: identical 500 ms server-side work, ~23% higher async throughput, ~38% better tail latency. **This is the "PR #36504 effect"** — at modest concurrency the async path avoids the per-request threadpool-dispatch overhead that the sync path pays.

### Where it inverts surprisingly — c≥100

Above the threadpool ceiling, async becomes *slower* than sync on this single-uvicorn-worker setup, and both modes accumulate large queueing latency and httpx client-side timeouts:

| route | concurrency | sync RPS | async RPS |
|---|---:|---:|---:|
| `/sleep` 500ms | 100 | 46.5 | 39.5 |
| `/sleep` 500ms | **200** | **43.9** | **37.3** ⚠ |
| `/sleep` 500ms | 400 | 44.5 | 29.2 |

This is the **AC-5.4 inversion** the POC's acceptance criterion specifically calls out as diagnostic-required. We ran the inversion to ground and reached the following evidence-backed conclusion (full diagnostics in `RESULTS.md` § Anomalies, raw log at `dev/async_session_poc/diagnostics/pg_stat_activity.log`):

- **R-6 (DB-pool saturation) — ruled out.** 1 Hz `pg_stat_activity` sampling during the full 24-cell sweep recorded a **peak active connection count of 51** — out of a calibrated pool of **600** (`sync_pool_size=40 + sync_max_overflow=560`; async engine reads the same cfg keys). The active count plateaus at ~40-41 (the sync threadpool's 40 workers each holding one connection) plus a small ~10-connection async overlay. The DB pool was never the gate.
- **R-3 (sync threadpool exhaustion) — firing for sync as designed.** The active-conn mode at 40-41 is exactly the Starlette threadpool's 40-worker ceiling holding 40 simultaneous sync queries. This is the *intended* pedagogical effect for the sync side.
- **The async path is bottlenecked above the DB pool.** With c=200 async awaiting `pg_sleep(0.5)`, ~200 simultaneous active connections were expected; we saw ~10 incremental async connections above the sync baseline. The bottleneck is upstream of the DB.
- **Operative causes — R-2 (client GIL) and R-3-async (single uvicorn worker event loop), in combination.** The harness is a single Python process driving 200+ concurrent `httpx.AsyncClient` requests; the API server is a single uvicorn worker whose event loop handles all async-route work + response framing on one Python interpreter. Sync routes get *effective* parallelism via the threadpool's 40 workers; async routes get only the serialization of one event loop.

In short: **the async-route conversion does what it advertises — the route itself no longer blocks the event loop on DB I/O — but a single-worker uvicorn ceiling and a single-process client harness combine to hide that gain at high concurrency.**

## Why a single uvicorn worker matters

This is the subtle part and the most important thing for the original PR thread's audience to internalize:

- A sync route inside an async framework is dispatched to the AnyIO threadpool (default size **40**). 40 sync queries can run concurrently.
- An async route stays on the event loop. With one uvicorn worker, **N** async coroutines can each `await` simultaneously — but every CPU-bound step (request parsing, response serialization, JSON encoding, middleware) is serialized on one Python interpreter.

The threadpool gives sync routes a fixed-but-real form of parallelism even without process workers. Async routes need *either* (a) more uvicorn workers, *or* (b) lower-CPU-overhead handling per request, to fully exploit the unblocked event loop. We didn't run with multiple workers in this POC (deliberately, to isolate the route-level mechanism); that's the single-biggest follow-up.

## Next steps

These are out of scope for this manifest and tracked here for the PR thread:

1. **Re-run with multiple workers. — DONE (inconclusive on this hardware); see [`RESULTS_MULTIWORKER.md`](RESULTS_MULTIWORKER.md).** Ran `AIRFLOW__API__WORKERS` ∈ {1, 4} with a 4-process client. The prediction ("async's ceiling rises with workers, crossing sync at high concurrency") was **not confirmed**: clean-regime cells (err < 2%) still favour async modestly (+2% to +24%), but every high-concurrency cell saturated, and at 4 workers Postgres hit `too many clients` because the POC's single-worker pool calibration (`pool_size=40 + max_overflow=560`) multiplies per worker and blows past `max_connections=100`. A definitive crossover test needs separate server/client hosts, per-worker pools sized so `pool_size × workers ≤ max_connections`, and a raised `max_connections`.
2. **Multi-process client. — DONE.** `run_bench.py --procs M` spreads load over M processes and merges raw latencies across the host-shared `CLOCK_MONOTONIC` (removes R-2). Finding: with the client GIL removed, the workers=1 inversion **persisted**, locating it server-side (single event loop), not in the client.
3. **Profile the async path with `py-spy`** during a c=200 `/sleep` run. Confirm where the time actually goes: connection acquisition, query dispatch, or response serialization.
4. **Migrate `ti_heartbeat`.** Heartbeat is the production hot path (one call per task per heartbeat interval, per worker). The conversion is mechanically small — the route is pure SQL (fast-path `UPDATE` + `SELECT … FOR UPDATE` fallback), with no state-machine / RTIF / tracing logic — but it is the highest-QPS write path, so it needs a careful, parity-focused PR (commit/lock semantics under async, the `rowcount` fast-path branches, and the asyncpg/pgbouncer prepared-statement caveat).
5. **Async secrets backends.** Prerequisite to converting `get_variable` / `get_connection`. PR-#36504-scale work; tracked separately.
6. **Pick a small batch of low-risk Execution-API read endpoints to migrate** in a real PR — list endpoints with simple `select` patterns, no SecretsBackend dependency, comprehensive existing test coverage. Candidates: variable-keys (already migrated here), connection-list (without value lookup), task-instance read endpoints.

## Reproducing

Everything needed to re-run is in [`README.md`](README.md). One liner:

```bash
source dev/async_session_poc/env_for_server.sh   # emitted by run_all.sh
breeze start-airflow --backend postgres            # in one terminal
bash dev/async_session_poc/run_all.sh              # in another
```

Override sweep parameters via `BENCH_*` env vars (`BENCH_TARGET_BASE_URL`, `BENCH_DURATION`, `BENCH_WARMUP`, `BENCH_SLEEP_MS`, `BENCH_CONCURRENCIES`). Pool calibration knobs (`AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE`, `AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW`) must reach the api-server process; `run_all.sh` writes `env_for_server.sh` for that purpose.

Raw 24-cell results: [`results.csv`](results.csv). Analysis and anomaly diagnostics: [`RESULTS.md`](RESULTS.md). Captured `pg_stat_activity` log: [`diagnostics/pg_stat_activity.log`](diagnostics/pg_stat_activity.log).
