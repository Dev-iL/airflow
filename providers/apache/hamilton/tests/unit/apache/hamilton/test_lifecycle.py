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
"""Lifecycle proofs for Hamilton tasks."""

from __future__ import annotations

import json
import sqlite3

import pytest
import structlog

from airflow._shared.timezones import timezone
from airflow.providers.apache.hamilton.decorators.hamilton import hamilton_task
from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator
from airflow.sdk import DAG, DagRunState, task
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

MODULE = "system.apache.hamilton.transforms"
SQL_MODULE = "system.apache.hamilton.sql_transforms"
SALES_QUERY = """
WITH paid AS (SELECT * FROM orders WHERE status = 'paid')
SELECT p.order_date, c.country, p.amount
FROM paid p JOIN customers c ON p.customer_id = c.id
"""


@pytest.mark.parametrize(
    ("decorated", "same_resource"),
    [
        pytest.param(False, False, id="operator"),
        pytest.param(True, True, id="decorated-same-resource"),
    ],
)
def test_file_lineage_is_emitted_by_the_airflow_listener(
    run_task, listener_manager, tmp_path, decorated, same_resource
):
    from airflow.providers.openlineage.plugins.listener import OpenLineageListener

    source = tmp_path / "source.json"
    destination = tmp_path / "destination.json"
    event_log = tmp_path / "events.json"
    source.write_text('{"value": 7}')
    output_path = source if same_resource else destination
    with conf_vars(
        {
            ("openlineage", "transport"): json.dumps({"type": "file", "log_file_path": str(event_log)}),
            ("openlineage", "execute_in_thread"): "True",
        }
    ):
        listener_manager(OpenLineageListener())
        with DAG("hamilton_file_lineage"):
            options = dict(
                modules=[MODULE],
                final_vars=["saved"],
                inputs={"input_path": str(source), "output_path": str(output_path)},
            )
            if decorated:

                @hamilton_task(**options)
                def prepare() -> None:
                    return None

                operator = prepare().operator
            else:
                operator = HamiltonOperator(task_id="transform", **options)
        run_task(operator)
        from airflow.sdk.execution_time.task_runner import finalize

        finalize(run_task.ti, run_task.state, run_task.context, structlog.get_logger(logger_name="task"))

    assert run_task.state == TaskInstanceState.SUCCESS
    events = [
        json.loads(path.read_text()) for path in tmp_path.iterdir() if path != source and path != destination
    ]
    complete = next(event for event in events if event["eventType"] == "COMPLETE")
    assert complete["job"]["name"].endswith(f".{operator.task_id}")
    assert {(dataset["namespace"], dataset["name"]) for dataset in complete["inputs"]} == {
        ("file://localhost", str(source))
    }
    assert {(dataset["namespace"], dataset["name"]) for dataset in complete["outputs"]} == {
        ("file://localhost", str(output_path))
    }


@pytest.mark.parametrize(
    ("modules", "final_vars", "inputs", "diagnostic"),
    [
        pytest.param([MODULE], ["doubled"], {"value": 2}, None, id="no-io"),
        pytest.param(
            ["unit.apache.hamilton.malformed_transforms"],
            ["malformed"],
            {},
            "Hamilton lineage skipped invalid loader/saver metadata",
            id="malformed-metadata",
        ),
    ],
)
def test_listener_emits_no_datasets_without_valid_file_metadata(
    run_task, listener_manager, tmp_path, caplog, modules, final_vars, inputs, diagnostic
):
    from airflow.providers.openlineage.plugins.listener import OpenLineageListener
    from airflow.sdk.execution_time.task_runner import finalize

    event_log = tmp_path / "events.json"
    with conf_vars(
        {
            ("openlineage", "transport"): json.dumps({"type": "file", "log_file_path": str(event_log)}),
            ("openlineage", "execute_in_thread"): "True",
        }
    ):
        listener_manager(OpenLineageListener())
        operator = HamiltonOperator(
            task_id="transform", modules=modules, final_vars=final_vars, inputs=inputs
        )
        run_task(operator)
        finalize(run_task.ti, run_task.state, run_task.context, structlog.get_logger(logger_name="task"))

    assert run_task.state == TaskInstanceState.SUCCESS
    complete_events = [
        event
        for event in (json.loads(path.read_text()) for path in event_log.parent.glob(f"{event_log.name}*"))
        if event["eventType"] == "COMPLETE" and event["job"]["name"].endswith(f".{operator.task_id}")
    ]
    assert len(complete_events) == 1
    complete = complete_events[0]
    assert complete["inputs"] == []
    assert complete["outputs"] == []
    if diagnostic:
        assert diagnostic in caplog


@pytest.mark.parametrize(
    ("decorated", "conversion_error"),
    [
        pytest.param(False, False, id="operator"),
        pytest.param(True, False, id="decorator"),
        pytest.param(False, True, id="conversion-error"),
    ],
)
def test_sql_lineage_is_emitted_by_the_airflow_listener(
    run_task, listener_manager, tmp_path, monkeypatch, caplog, decorated, conversion_error
):
    from hamilton.plugins import h_openlineage

    supports_sql_lineage = hasattr(h_openlineage, "sql_datasets")
    if conversion_error:
        if not supports_sql_lineage:
            pytest.skip("Hamilton SQL conversion is not released")

        def fail_conversion(*args, **kwargs):
            raise RuntimeError("postgresql://user:secret@db.example/sales")

        monkeypatch.setattr(h_openlineage, "sql_datasets", fail_conversion)

    from airflow.providers.openlineage.plugins.listener import OpenLineageListener
    from airflow.sdk.execution_time.task_runner import finalize

    sales_path = tmp_path / "sales.db"
    warehouse_path = sales_path if decorated else tmp_path / "warehouse.db"
    output_table = "orders" if decorated else "daily_revenue"
    event_log = tmp_path / "events.json"
    with sqlite3.connect(sales_path) as connection:
        connection.executescript(
            """
            CREATE TABLE orders (
                customer_id INTEGER, order_date TEXT, amount REAL, status TEXT
            );
            INSERT INTO orders VALUES (1, 'd1', 10.0, 'paid'), (1, 'd1', 5.0, 'paid');
            CREATE TABLE customers (id INTEGER, country TEXT);
            INSERT INTO customers VALUES (1, 'NL');
            """
        )
    inputs = {
        "sales_query": SALES_QUERY,
        "sales_db": f"sqlite:///{sales_path}",
        "warehouse_db": f"sqlite:///{warehouse_path}",
        "output_table": output_table,
    }
    with conf_vars(
        {
            ("openlineage", "transport"): json.dumps({"type": "file", "log_file_path": str(event_log)}),
            ("openlineage", "execute_in_thread"): "True",
        }
    ):
        listener_manager(OpenLineageListener())
        if decorated:

            @hamilton_task(modules=[SQL_MODULE], final_vars=["saved_revenue"], inputs=inputs)
            def prepare() -> None:
                return None

            operator = prepare().operator
        else:
            operator = HamiltonOperator(
                task_id="sql_transform",
                modules=[SQL_MODULE],
                final_vars=["saved_revenue"],
                inputs=inputs,
            )
        run_task(operator)
        finalize(run_task.ti, run_task.state, run_task.context, structlog.get_logger(logger_name="task"))

    assert run_task.state == TaskInstanceState.SUCCESS
    with sqlite3.connect(warehouse_path) as connection:
        assert connection.execute(f"SELECT country, amount FROM {output_table}").fetchall() == [("NL", 15.0)]
    complete_events = [
        event
        for event in (json.loads(path.read_text()) for path in event_log.parent.glob(f"{event_log.name}*"))
        if event["eventType"] == "COMPLETE" and event["job"]["name"].endswith(f".{operator.task_id}")
    ]
    assert len(complete_events) == 1
    complete = complete_events[0]
    input_datasets = {(dataset["namespace"], dataset["name"]) for dataset in complete["inputs"]}
    output_datasets = {(dataset["namespace"], dataset["name"]) for dataset in complete["outputs"]}
    if supports_sql_lineage and not conversion_error:
        assert input_datasets == {
            (f"sqlite://{sales_path.resolve()}", "customers"),
            (f"sqlite://{sales_path.resolve()}", "orders"),
        }
        assert output_datasets == {(f"sqlite://{warehouse_path.resolve()}", output_table)}
        if decorated:
            assert input_datasets & output_datasets == {(f"sqlite://{sales_path.resolve()}", "orders")}
    else:
        assert input_datasets == set()
        assert output_datasets == set()
    if conversion_error:
        assert "Hamilton lineage skipped invalid loader/saver metadata" in caplog


def test_sql_execution_failure_fails_the_task(run_task, tmp_path):
    operator = HamiltonOperator(
        task_id="sql_failure",
        modules=[SQL_MODULE],
        final_vars=["saved_revenue"],
        inputs={
            "sales_query": "SELECT * FROM missing_table",
            "sales_db": f"sqlite:///{tmp_path / 'sales.db'}",
            "warehouse_db": f"sqlite:///{tmp_path / 'warehouse.db'}",
            "output_table": "daily_revenue",
        },
    )

    run_task(operator)

    assert run_task.state == TaskInstanceState.FAILED
    assert run_task.error is not None


@pytest.mark.db_test
@pytest.mark.usefixtures("testing_dag_bundle")
def test_dag_test_maps_decorated_task_with_xcom_config():
    with DAG(
        "hamilton_mapped_lifecycle",
        schedule=None,
        start_date=timezone.datetime(2025, 1, 1),
    ) as dag:

        @task
        def get_config() -> dict[str, int]:
            return {"factor": 3}

        config = get_config()

        @task.hamilton(modules=[MODULE], final_vars=["doubled"], config=config)
        def prepare(value: int) -> dict[str, int]:
            return {"value": value}

        decorated_mapped = prepare.expand(value=[2, 4])
        operator_mapped = HamiltonOperator.partial(
            task_id="operator_mapped",
            modules=[MODULE],
            final_vars=["doubled"],
            config={"factor": config["factor"]},
        ).expand(inputs=[{"value": 3}, {"value": 5}])

    from airflow.models.dag import DagModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import LazyDeserializedDAG
    from airflow.settings import Session

    session = Session()
    session.add(DagModel(dag_id=dag.dag_id, bundle_name="testing"))
    session.commit()
    SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name="testing")
    session.close()
    dag_run = dag.test()

    assert dag_run.state == DagRunState.SUCCESS
    results = [
        ti.xcom_pull(task_ids=decorated_mapped.operator.task_id, map_indexes=ti.map_index)
        for ti in sorted(dag_run.get_task_instances(), key=lambda ti: ti.map_index)
        if ti.task_id == decorated_mapped.operator.task_id
    ]
    assert results == [{"doubled": 6}, {"doubled": 12}]
    operator_results = [
        ti.xcom_pull(task_ids=operator_mapped.task_id, map_indexes=ti.map_index)
        for ti in sorted(dag_run.get_task_instances(), key=lambda ti: ti.map_index)
        if ti.task_id == operator_mapped.task_id
    ]
    assert operator_results == [{"doubled": 9}, {"doubled": 15}]


@pytest.mark.db_test
@pytest.mark.usefixtures("testing_dag_bundle")
def test_dag_test_reconstructs_hamilton_adapter_and_materializer(tmp_path):
    from hamilton import base
    from hamilton.io.materialization import to

    input_path = tmp_path / "input.json"
    output_path = tmp_path / "doubled.json"
    unrelated_output_path = tmp_path / "unrelated.json"
    input_path.write_text('{"value": 2}')
    with DAG(
        "hamilton_materializer_lifecycle",
        schedule=None,
        start_date=timezone.datetime(2025, 1, 1),
    ) as dag:

        @task.hamilton(
            modules=[MODULE],
            final_vars=["saved_doubled"],
            inputs={"input_path": str(input_path), "output_path": str(unrelated_output_path)},
            adapters=[base.SimplePythonGraphAdapter()],
            materializers=[
                to.json(id="saved_doubled", dependencies=["transformed"], path=str(output_path)),
            ],
        )
        def prepare() -> None:
            return None

        prepare()

    from airflow.models.dag import DagModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import LazyDeserializedDAG
    from airflow.settings import Session

    session = Session()
    session.add(DagModel(dag_id=dag.dag_id, bundle_name="testing"))
    session.commit()
    SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name="testing")
    session.close()
    dag_run = dag.test()

    assert dag_run.state == DagRunState.SUCCESS
    assert json.loads(output_path.read_text()) == {"value": 4}
    assert not unrelated_output_path.exists()
