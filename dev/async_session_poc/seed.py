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
Idempotent seeder for the async-session POC benchmark.

Inserts ``bench_var_0000``..``bench_var_0999`` Variables (1000 rows) so the
realistic-query path (``GET /execution/variables/keys``) has non-trivial work.
Re-running is a no-op: rows already at the target key are left alone.

Run inside breeze:

    breeze run python dev/async_session_poc/seed.py
"""

from __future__ import annotations

import sys

N_VARIABLES = 1000
KEY_FMT = "bench_var_{:04d}"
VAL_FMT = "v_{:04d}"


def main() -> int:
    from sqlalchemy import func, select
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from airflow.models.variable import Variable
    from airflow.utils.session import create_session
    from airflow.utils.sqlalchemy import get_dialect_name

    rows = [
        {"key": KEY_FMT.format(i), "val": VAL_FMT.format(i), "description": None, "is_encrypted": False}
        for i in range(N_VARIABLES)
    ]

    with create_session() as session:
        dialect = get_dialect_name(session)
        if dialect == "postgresql":
            stmt = pg_insert(Variable.__table__).values(rows).on_conflict_do_nothing(index_elements=["key"])
            session.execute(stmt)
        else:
            existing = set(
                session.scalars(select(Variable.key).where(Variable.key.in_([r["key"] for r in rows]))).all()
            )
            missing = [r for r in rows if r["key"] not in existing]
            if missing:
                session.execute(Variable.__table__.insert(), missing)
        session.commit()

        total = session.scalar(
            select(func.count()).select_from(
                select(Variable.key).where(Variable.key.like("bench_var_%")).subquery()
            )
        )
        print(total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
