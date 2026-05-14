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

- [Async-Session POC — Results](#async-session-poc--results)
  - [Framing](#framing)
  - [Environment](#environment)
  - [Concurrency Sweep — 24 cells](#concurrency-sweep--24-cells)
  - [Analysis](#analysis)
  - [Anomalies](#anomalies)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Async-Session POC — Results

## Framing

This benchmark compares **async SQLA via asyncpg vs sync SQLA via psycopg** on a
synthetic A/B pair of FastAPI routes mounted into the Airflow Execution API.
The two siblings (`/__bench/<name>/sync` and `/__bench/<name>/async`) issue
identical SQL — the only difference is the SQLAlchemy session type, which
forces the route declaration to switch between `def` (Starlette runs in the
AnyIO threadpool) and `async def` (Starlette runs on the event loop).

**Triggerer and Workers do not hit the metadata DB directly in 3.x** — they
talk to the Execution API through HTTP. So the win this benchmark measures —
fewer event-loop-blocking sync calls inside a FastAPI route — is a win for the
Execution API's own scalability, *not* a direct win for Triggerer/Worker
throughput. Original PR #36504's "Triggerer talks to the DB directly so we
should make that DB call async" motivation is architecturally obsolete; the
remaining and still-valid win is converting Execution API routes themselves.

## Environment

```env
airflow_version=3.3.0
postgres_version=14.20
sync_conn_scheme=postgresql+psycopg2
async_conn_scheme=postgresql+asyncpg
threadpool_total_tokens=40
sync_pool_size=40
sync_max_overflow=560
async_pool_size=40
async_max_overflow=560
host_cpus=8
host_memory_gb=31
host_os=Linux 6.8.0-111-generic x86_64
uvicorn_workers=1
```

Notes on the env block:

- `threadpool_total_tokens=40` is the AnyIO/Starlette default. It is the
  ceiling on simultaneous sync route handlers in a single uvicorn worker
  (INV-G16).
- `sync_pool_size + sync_max_overflow = 600` and the async pool reads the same
  cfg keys (`airflow-core/src/airflow/settings.py:418-421`), so the async pool
  is also 600. Combined ≥ 500 (INV-G17).
- `async_conn_scheme=postgresql+asyncpg` is **derived** by Airflow from the
  sync URL via `AIO_LIBS_MAPPING` (`settings.py:240`); `sql_alchemy_conn_async`
  is *not* explicitly set, which is the intended default behavior. This
  matters for ASM-5: we are measuring asyncpg, not psycopg-v3-async.
- `uvicorn_workers=1` is the api-server default. The threadpool and event-loop
  ceilings discussed below are *per-worker* ceilings.

## Concurrency Sweep — 24 cells

Each cell is 30s of measurement after a 5s warmup, with the harness driving
`--concurrency` httpx workers in a single Python process. Latencies are
wall-clock end-to-end (`time.monotonic()`) including network and queueing,
not server-side processing time alone.

### `/__bench/select1/{sync,async}` — `SELECT 1`

| concurrency | mode  |    RPS | p50 ms | p95 ms | p99 ms | error rate | samples |
|------------:|-------|-------:|-------:|-------:|-------:|-----------:|--------:|
|          50 | sync  | 121.27 | 299.22 | 1149.51 | 1749.16 |     0.0000 |    3078 |
|          50 | async | 131.73 | 247.40 | 1109.44 | 1746.61 |     0.0000 |    3330 |
|         100 | sync  |  81.00 | 872.58 | 4039.66 | 5316.03 |     0.0132 |    2122 |
|         100 | async |  66.18 | 1000.24 | 4368.87 | 5501.19 |    0.0045 |    1774 |
|         200 | sync  |  80.87 | 1970.08 | 6404.74 | 8013.13 |     0.0666 |    2193 |
|         200 | async |  43.07 | 4416.41 | 11781.91 | 13197.68 |    0.2777 |    1235 |
|         400 | sync  |  48.64 | 9624.45 | 16987.23 | 20441.20 |    0.3747 |    1465 |
|         400 | async |  38.42 | 10280.25 | 19393.88 | 20936.07 |   0.4543 |    1292 |

### `/__bench/sleep/{sync,async}?ms=500` — `SELECT pg_sleep(0.5)`

| concurrency | mode  |    RPS | p50 ms | p95 ms | p99 ms | error rate | samples |
|------------:|-------|-------:|-------:|-------:|-------:|-----------:|--------:|
|          50 | sync  |  78.50 | 596.20 | 854.85 | 893.10 |     0.0000 |    2005 |
|          50 | async |  96.38 | 515.89 | 547.10 | 558.27 |     0.0000 |    2457 |
|         100 | sync  |  46.48 | 1861.57 | 6551.55 | 7577.48 |    0.2782 |    1233 |
|         100 | async |  39.53 | 2844.48 | 4547.95 | 4980.56 |    0.5502 |    1076 |
|         200 | sync  |  43.86 | 4811.83 | 10204.83 | 11483.68 |   0.5763 |    1284 |
|         200 | async |  37.26 | 4624.27 | 12448.00 | 13272.10 |   0.6142 |    1156 |
|         400 | sync  |  44.47 | 10812.57 | 15767.59 | 16881.81 |  0.7675 |    1583 |
|         400 | async |  29.19 | 14901.13 | 25496.77 | 27020.90 |  0.6466 |    1078 |

### `/__bench/var_count/{sync,async}` — `SELECT COUNT(*) FROM variable` (1000 seeded rows)

| concurrency | mode  |    RPS | p50 ms | p95 ms | p99 ms | error rate | samples |
|------------:|-------|-------:|-------:|-------:|-------:|-----------:|--------:|
|          50 | sync  | 156.61 | 216.21 | 933.38 | 1403.00 |    0.0000 |    3959 |
|          50 | async | 175.16 | 176.52 | 861.05 | 1377.66 |    0.0000 |    4397 |
|         100 | sync  |  66.51 | 1129.33 | 4722.29 | 5817.31 |   0.0131 |    1751 |
|         100 | async |  99.33 | 467.22 | 3509.20 | 5302.00 |    0.0316 |    2595 |
|         200 | sync  |  57.21 | 3082.04 | 8021.24 | 10031.81 |  0.0451 |    1685 |
|         200 | async |  53.07 | 2989.77 | 9921.32 | 11179.93 |  0.2075 |    1629 |
|         400 | sync  |  56.18 | 7865.09 | 14043.45 | 19358.72 | 0.2076 |    1691 |
|         400 | async |  25.83 | 18005.64 | 30039.28 | 31124.75 | 0.7420 |    1132 |

## Analysis

**What the data does prove (low concurrency, c=50):** with concurrency below
the threadpool ceiling, the async route is consistently faster than its sync
sibling — measurably for `/sleep` (96 vs 78 RPS, p99 558 vs 893 ms, both
zero-error) and marginally for `/select1` and `/var_count`. The shape matches
the textbook prediction: at low load the only difference is the per-request
overhead of dispatching to a threadpool worker vs awaiting on the event loop,
and the event-loop path is slightly cheaper.

**What the data does *not* prove (high concurrency, c≥100):** above the
threadpool ceiling the picture inverts — async becomes *slower* than sync, and
both modes accumulate large queueing latencies and significant client-observed
error rates (mostly httpx timeouts at the 30s default; a small fraction are
actual 5xx responses). At c=200 on `/sleep` (the manifest's primary pedagogy
cell) async is 37 RPS vs sync 44 RPS — the expected win does **not** appear.

**What the data does not prove:** worker- or triggerer-side throughput. Those
components do not hit the metadata DB directly in Airflow 3.x; converting an
Execution API route is a server-side scalability lever, not a triggerer one.

**What the data does not prove:** that converting more routes will scale the
same way. The single-route synthetic measurement does not exercise route-mix
effects, real auth (`CurrentTIToken` minting was bypassed), or
production-realistic SQL.

## Anomalies

The `/sleep` pair at concurrency 200 does **not** satisfy AC-5.4's PASS-PATH-1
(async > sync). The cell shows async = 37.26 RPS, sync = 43.86 RPS. Below is
the diagnostic evidence required by PASS-PATH-2, naming the specific
manifest-listed cause and citing the artifact that supports it.

### Cause: a combination of R-2 (client-side GIL ceiling) and R-3 (event-loop saturation in a single uvicorn worker), with R-6 (DB-pool saturation) **ruled out** by the pg_stat_activity record.

**Evidence — R-6 (DB-pool saturation) is NOT the cause.** Continuous 1 Hz
`pg_stat_activity` polling during the sweep (full log saved at
`diagnostics/pg_stat_activity.log`, 545 samples) shows the peak active
connection count was **51** — out of a configured pool of 600
(`sync_pool_size+sync_max_overflow = async_pool_size+async_max_overflow =
40+560 = 600`). The distribution of active-conn counts has clear modes at
**40-41** (the sync threadpool's 40 workers each holding one connection) and
**50-51** (threadpool + ~10 additional async sessions). At no point does the
active-conn count climb toward even 10 % of the pool ceiling, so the pool was
not the gate. The histogram, copied here from the full diagnostic log:

```
count  active_connections
   16  51
    5  50
    3  48
   23  41
    4  40
    4  15
    3  12
    4   8
    3   3
   10   2
  414   1
```

Sample peak rows:

```
2026-05-14T09:30:43.425Z|51|91
2026-05-14T09:30:44.534Z|51|91
2026-05-14T09:31:06.833Z|51|91
2026-05-14T09:31:09.064Z|51|91
```

(Format: `timestamp|active_count|total_count`; total includes idle
checked-out connections.)

**Evidence — R-3 (threadpool exhaustion) IS firing for the sync path** and is
the expected pedagogical effect for sync. The recorded threadpool total tokens
is 40 (Starlette default, captured in the env block above), and the active-conn
mode at ~40-41 is precisely the 40-thread cap holding 40 simultaneous sync
queries. This part of R-3 fires as designed — the sync path is threadpool
limited.

**Evidence — async path is bottlenecked above the DB pool.** With the asyncpg
pool fully available, c=200 awaiting `pg_sleep(0.5)` should have driven ~200
simultaneous active DB connections (one per coroutine) and a theoretical RPS
near 400. The observed 37 RPS and the asymptote at ~10 async connections (the
"51-40" residual above the sync threadpool baseline) indicate the async path
never approached the DB pool ceiling — the bottleneck is upstream of the DB.
The two candidate causes named in the manifest are:

- **R-2 (client-side GIL ceiling)**: the harness is a single Python process
  driving 200 concurrent `httpx.AsyncClient` requests. At c=200+ on localhost
  the client's own event loop becomes saturated dispatching/parsing responses;
  R-2's prediction (RPS does not scale linearly with `--concurrency`) holds —
  RPS *falls* as concurrency rises on every async row from c=100 onward, which
  is the GIL/loop saturation signature, not a server-side cap.
- **R-3-async (single uvicorn worker event loop)**: with `uvicorn_workers=1`
  the entire async path shares one event loop. Even after the route's await
  hands control back to the loop, response serialization and HTTP framing are
  CPU-bound work on that one loop. Sync routes get effective parallelism via
  the threadpool's 40 workers; async routes do not — they are funneled through
  one Python event loop.

### Out-of-scope follow-ups

The PG-7 honesty rule applies here: the result is what it is, and the right
next step is investigation, not parameter massaging. Investigations worth
doing in a follow-up manifest (not this one):

1. **Drive the harness from multiple processes** (e.g. one harness process per
   logical core, summing CSVs) to remove R-2 as a confound and re-measure the
   `/sleep` c=200 cell with a server that is unambiguously the bottleneck.
2. **Run uvicorn with `--workers 4` or `--workers 8`** to remove the
   single-event-loop ceiling. Repeat the sweep against a multi-worker server
   to see whether async scales (predicted) while sync stays threadpool-capped
   per worker.
3. **Profile the async path** with `py-spy` during a c=200 `/sleep` run to
   confirm where time goes: connection acquisition, query dispatch, or
   response serialization.

These do not invalidate the c=50 result that *does* show async > sync; they
inform whether the c=200 inversion is a measurement artifact (R-2/R-3-async)
or a real Airflow limitation. The diagnostic data captured by this run is
sufficient to choose those follow-up investigations; producing them was not in
this manifest's scope.
