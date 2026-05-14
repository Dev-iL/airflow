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
Synthetic A/B bench routes for the async-session POC.

Mounted only when the env var ``AIRFLOW__BENCHMARK__ENABLE_ASYNC_SESSION_ROUTES``
equals the literal string ``"true"`` (case-sensitive). The single underscore in
``_benchmark`` and the dunder path prefix ``__bench`` signal that this is debug
instrumentation, not production-facing surface.

Each pair of routes (``/select1``, ``/sleep``, ``/var_count``) issues identical
SQL — the only difference between siblings is sync ``SessionDep`` vs async
``AsyncSessionDep``. That isolation is the whole point of the benchmark.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from sqlalchemy import func, select, text

from airflow.api_fastapi.common.db.common import AsyncSessionDep, SessionDep
from airflow.models.variable import Variable

router = APIRouter()


@router.get("/select1/sync")
def select1_sync(session: SessionDep) -> dict[str, int]:
    value: int | None = session.scalar(select(text("1")))
    return {"value": value if value is not None else 0}


@router.get("/select1/async")
async def select1_async(session: AsyncSessionDep) -> dict[str, int]:
    value: int | None = await session.scalar(select(text("1")))
    return {"value": value if value is not None else 0}


@router.get("/sleep/sync")
def sleep_sync(
    session: SessionDep,
    ms: Annotated[int, Query(ge=0, le=10_000)] = 500,
) -> dict[str, float]:
    seconds = ms / 1000.0
    session.execute(text("SELECT pg_sleep(:s)"), {"s": seconds})
    return {"slept_s": seconds}


@router.get("/sleep/async")
async def sleep_async(
    session: AsyncSessionDep,
    ms: Annotated[int, Query(ge=0, le=10_000)] = 500,
) -> dict[str, float]:
    seconds = ms / 1000.0
    await session.execute(text("SELECT pg_sleep(:s)"), {"s": seconds})
    return {"slept_s": seconds}


@router.get("/var_count/sync")
def var_count_sync(session: SessionDep) -> dict[str, int]:
    count: int | None = session.scalar(select(func.count()).select_from(Variable))
    return {"count": count if count is not None else 0}


@router.get("/var_count/async")
async def var_count_async(session: AsyncSessionDep) -> dict[str, int]:
    count: int | None = await session.scalar(select(func.count()).select_from(Variable))
    return {"count": count if count is not None else 0}
