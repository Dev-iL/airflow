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
"""
Pure-Python httpx concurrency-sweep harness for the async-session POC.

Spawns ``--concurrency`` worker coroutines against ``--url`` for ``--duration``
seconds, discards the first ``--warmup`` seconds, and reports RPS + p50/p95/p99
+ error rate. Output is a single CSV row (header + data) by default, or
appended to a CSV file when ``--output`` is a path other than ``-``.

``--procs M`` spreads the concurrency across M OS processes to remove the
single-process client GIL ceiling (the POC's R-2 confound). Each process drives
its share of the concurrency on its own asyncio loop; raw samples are merged in
the parent so RPS sums and percentiles are computed over the combined sample
set (not averaged per-process). Cross-process merge is sound because
``time.monotonic()`` reads the host-wide ``CLOCK_MONOTONIC`` on Linux, so the
parent's warmup cutoff and deadline are comparable in every child.

Intentionally tiny — the harness is the measurement instrument, not a benchmark
framework. ``hey``/``wrk``/``oha``/``locust`` are not pre-installed inside
breeze; this script needs only ``httpx``.

Examples:

    python run_bench.py --url http://localhost:8080/execution/__bench/sleep/async?ms=500 \\
        --concurrency 200 --procs 4 --duration 30 --warmup 5 \\
        --server-workers 4 --route sleep_500ms --mode async --output results_multiworker.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import httpx

# server_workers / client_procs are recorded so a multi-worker sweep's CSV is
# self-describing across server-worker configs and client-process counts.
_LABEL_COLUMNS = ("route", "mode", "server_workers", "client_procs", "concurrency")
_METRIC_COLUMNS = ("rps", "p50_ms", "p95_ms", "p99_ms", "error_rate", "sample_count")

# Full-sweep rows (--route + --mode passed) include label columns first so a
# multi-cell results.csv self-describes; bare self-test rows (no labels) emit
# only the metric columns and therefore start with ``rps,``.
CSV_COLUMNS = _LABEL_COLUMNS + _METRIC_COLUMNS

BenchRow = dict[str, float | int | str]

# A raw sample as a plain tuple so it pickles cheaply across the process pool:
# (completed_at_monotonic, latency_s, status).
RawSample = tuple[float, float, int]


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile on a pre-sorted list. Empty list → 0.0."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


async def _worker(client: httpx.AsyncClient, url: str, deadline: float, samples: list[RawSample]) -> None:
    while True:
        start = time.monotonic()
        if start >= deadline:
            return
        try:
            response = await client.get(url)
            status = response.status_code
        except httpx.HTTPError:
            status = 0
        end = time.monotonic()
        samples.append((end, end - start, status))


async def _collect(url: str, concurrency: int, deadline: float, timeout_s: float) -> list[RawSample]:
    """Drive ``concurrency`` coroutines against ``url`` until ``deadline`` (absolute monotonic)."""
    samples: list[RawSample] = []
    timeout = httpx.Timeout(timeout_s)
    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        workers = [asyncio.create_task(_worker(client, url, deadline, samples)) for _ in range(concurrency)]
        await asyncio.gather(*workers, return_exceptions=True)
    return samples


def _child_collect(url: str, concurrency: int, deadline: float, timeout_s: float) -> list[RawSample]:
    """Process-pool entrypoint: run one asyncio loop's share of the load. Must be top-level (picklable)."""
    return asyncio.run(_collect(url, concurrency, deadline, timeout_s))


def _split_concurrency(total: int, procs: int) -> list[int]:
    """Divide total concurrency across procs as evenly as possible."""
    base, extra = divmod(total, procs)
    return [base + (1 if i < extra else 0) for i in range(procs)]


def _aggregate(samples: list[RawSample], warmup_cutoff: float, measured_window_s: float, args) -> BenchRow:
    measured = [s for s in samples if s[0] >= warmup_cutoff]
    latencies_ms = sorted(s[1] * 1000.0 for s in measured)
    errors = sum(1 for s in measured if s[2] != 200)
    n = len(measured)
    return {
        "route": args.route,
        "mode": args.mode,
        "server_workers": args.server_workers,
        "client_procs": args.procs,
        "concurrency": args.concurrency,
        "rps": round(n / measured_window_s, 2),
        "p50_ms": round(_percentile(latencies_ms, 50), 2),
        "p95_ms": round(_percentile(latencies_ms, 95), 2),
        "p99_ms": round(_percentile(latencies_ms, 99), 2),
        "error_rate": round(errors / n, 4) if n else 0.0,
        "sample_count": n,
    }


def _run(args: argparse.Namespace) -> BenchRow:
    started = time.monotonic()
    deadline = started + args.duration
    warmup_cutoff = started + args.warmup
    measured_window_s = max(args.duration - args.warmup, 1e-9)

    if args.procs <= 1:
        samples = _child_collect(args.url, args.concurrency, deadline, args.timeout)
    else:
        shares = _split_concurrency(args.concurrency, args.procs)
        samples = []
        with ProcessPoolExecutor(max_workers=args.procs) as pool:
            futures = [pool.submit(_child_collect, args.url, c, deadline, args.timeout) for c in shares]
            for fut in futures:
                samples.extend(fut.result())

    return _aggregate(samples, warmup_cutoff, measured_window_s, args)


def _columns_for(row: BenchRow) -> tuple[str, ...]:
    """Pick column order based on whether label fields were supplied."""
    if row.get("route") or row.get("mode"):
        return CSV_COLUMNS
    return _METRIC_COLUMNS


def _write(row: BenchRow, output: str) -> None:
    columns = _columns_for(row)
    if output == "-":
        writer = csv.DictWriter(sys.stdout, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(row)
        return

    path = Path(output)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Async-session POC benchmark harness.")
    parser.add_argument("--url", required=True, help="Target URL.")
    parser.add_argument(
        "--concurrency", type=int, required=True, help="Total concurrent workers across all procs."
    )
    parser.add_argument("--duration", type=float, required=True, help="Total run time in seconds.")
    parser.add_argument(
        "--procs", type=int, default=1, help="Client OS processes to spread concurrency over (default: 1)."
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=5.0,
        help="Seconds of warmup discarded from aggregates (default: 5).",
    )
    parser.add_argument(
        "--timeout", type=float, default=30.0, help="Per-request timeout in seconds (default: 30)."
    )
    parser.add_argument(
        "--output",
        default="-",
        help="CSV output path; '-' means stdout (default). Appends if file exists.",
    )
    parser.add_argument("--route", default="", help="Label for the 'route' column (e.g., 'sleep_500ms').")
    parser.add_argument(
        "--mode", default="", choices=("", "sync", "async"), help="Label for the 'mode' column."
    )
    parser.add_argument(
        "--server-workers",
        dest="server_workers",
        default="",
        help="Label for the 'server_workers' column (the API server's AIRFLOW__API__WORKERS).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    row = _run(args)
    _write(row, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
