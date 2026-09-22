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
"""Run a local Hamilton SQL graph with datasource-based lineage."""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
from pathlib import Path

import pendulum

from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator
from airflow.sdk import DAG, task

from tests_common.test_utils.watcher import watcher

MODULE = "system.apache.hamilton.sql_transforms"
SALES_QUERY = """
WITH paid AS (SELECT * FROM orders WHERE status = 'paid')
SELECT p.order_date, c.country, p.amount
FROM paid p JOIN customers c ON p.customer_id = c.id
"""


with DAG(
    dag_id="example_hamilton_sql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "hamilton", "sql"],
) as dag:

    @task
    def prepare_databases() -> dict[str, str]:
        directory = Path(tempfile.mkdtemp(prefix="airflow-hamilton-sql-"))
        sales_path = directory / "sales.db"
        with sqlite3.connect(sales_path) as connection:
            connection.executescript(
                """
                CREATE TABLE orders (
                    customer_id INTEGER, order_date TEXT, amount REAL, status TEXT
                );
                INSERT INTO orders VALUES (1, '2026-09-21', 10.0, 'paid');
                INSERT INTO orders VALUES (1, '2026-09-21', 5.0, 'paid');
                CREATE TABLE customers (id INTEGER, country TEXT);
                INSERT INTO customers VALUES (1, 'NL');
                """
            )
        return {
            "sales_query": SALES_QUERY,
            "sales_db": f"sqlite:///{sales_path}",
            "warehouse_db": f"sqlite:///{directory / 'warehouse.db'}",
            "output_table": "daily_revenue",
        }

    databases = prepare_databases()

    # [START sql_operator]
    saved = HamiltonOperator(
        task_id="build_revenue_report",
        modules=[MODULE],
        final_vars=["saved_revenue"],
        inputs=databases,
    )
    # [END sql_operator]

    @task
    def check_report(result: dict, inputs: dict[str, str]) -> None:
        warehouse_path = inputs["warehouse_db"].removeprefix("sqlite:///")
        with sqlite3.connect(warehouse_path) as connection:
            assert connection.execute("SELECT country, amount FROM daily_revenue").fetchall() == [
                ("NL", 15.0)
            ]
        assert result["saved_revenue"]["sql_metadata"]["rows"] == 1

    @task(trigger_rule="all_done")
    def cleanup(inputs: dict[str, str]) -> None:
        sales_path = Path(inputs["sales_db"].removeprefix("sqlite:///"))
        shutil.rmtree(sales_path.parent)

    checked = check_report(saved.output, databases)
    checked >> cleanup(databases)
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
