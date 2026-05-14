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
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.routing import Mount

from airflow.api_fastapi.app import cached_app, purge_cached_app

GATE_ENV = "AIRFLOW__BENCHMARK__ENABLE_ASYNC_SESSION_ROUTES"

ROUTES = [
    "/execution/__bench/select1/sync",
    "/execution/__bench/select1/async",
    "/execution/__bench/sleep/sync",
    "/execution/__bench/sleep/async",
    "/execution/__bench/var_count/sync",
    "/execution/__bench/var_count/async",
]

pytestmark = pytest.mark.db_test


def _build_client(monkeypatch, env_value: str | None) -> TestClient:
    """Build a fresh app with the gate env var set to ``env_value`` (None = unset)."""
    if env_value is None:
        monkeypatch.delenv(GATE_ENV, raising=False)
    else:
        monkeypatch.setenv(GATE_ENV, env_value)

    purge_cached_app()
    app = cached_app(apps="execution")
    return TestClient(app)


def _exec_app(app: FastAPI) -> FastAPI:
    for route in app.routes:
        if isinstance(route, Mount) and route.path == "/execution" and isinstance(route.app, FastAPI):
            return route.app
    raise RuntimeError("Execution API sub-app not found")


@pytest.fixture(autouse=True)
def _purge_after():
    yield
    purge_cached_app()


def test_routes_gated_off_unset(monkeypatch):
    """With the env var unset, all six bench routes must 404."""
    client = _build_client(monkeypatch, None)
    for path in ROUTES:
        r = client.get(path)
        assert r.status_code == 404, f"{path} should be unmounted (env unset) but got {r.status_code}"


@pytest.mark.parametrize("env_value", ["True", "1", "yes", "", "false", "TRUE"])
def test_routes_gated_off_truthy_variants(monkeypatch, env_value):
    """Any value other than the exact case-sensitive string 'true' leaves the routes unmounted."""
    client = _build_client(monkeypatch, env_value)
    for path in ROUTES:
        r = client.get(path)
        assert r.status_code == 404, (
            f"{path} should be unmounted for env={env_value!r} but got {r.status_code}"
        )


@pytest.mark.backend("postgres")
def test_routes_gated_on(monkeypatch):
    """With the env var exactly 'true', all six routes are mounted and return 200 with the expected body.

    Requires the Postgres backend because the synthetic ``/sleep`` SQL uses
    ``pg_sleep`` (the manifest is Postgres-only by design — see ASM-7).
    """
    client = _build_client(monkeypatch, "true")

    r = client.get("/execution/__bench/select1/sync")
    assert r.status_code == 200, r.text
    assert r.json() == {"value": 1}

    r = client.get("/execution/__bench/select1/async")
    assert r.status_code == 200, r.text
    assert r.json() == {"value": 1}

    r = client.get("/execution/__bench/sleep/sync", params={"ms": 0})
    assert r.status_code == 200, r.text
    assert r.json() == {"slept_s": 0.0}

    r = client.get("/execution/__bench/sleep/async", params={"ms": 0})
    assert r.status_code == 200, r.text
    assert r.json() == {"slept_s": 0.0}

    r = client.get("/execution/__bench/var_count/sync")
    assert r.status_code == 200, r.text
    assert "count" in r.json()

    r = client.get("/execution/__bench/var_count/async")
    assert r.status_code == 200, r.text
    assert "count" in r.json()


def test_sleep_route_rejects_out_of_range_ms(monkeypatch):
    """``ms`` must be within [0, 10000]; FastAPI returns 422 for out-of-range.

    Pure parameter validation — does not actually execute SQL, so backend-agnostic.
    """
    client = _build_client(monkeypatch, "true")

    r = client.get("/execution/__bench/sleep/sync", params={"ms": -1})
    assert r.status_code == 422

    r = client.get("/execution/__bench/sleep/async", params={"ms": 10_001})
    assert r.status_code == 422
