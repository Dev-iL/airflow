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

import json

import pytest

from airflow.providers.apache.hamilton.decorators.hamilton import hamilton_task
from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator
from airflow.sdk import DAG
from airflow.utils.state import TaskInstanceState

MODULE = "system.apache.hamilton.transforms"


@pytest.mark.parametrize("decorated", [False, True])
def test_real_graph_in_task_runner(run_task, decorated):
    with DAG("hamilton"):
        options = dict(modules=[MODULE], final_vars=["doubled"], inputs={"value": 3}, config={"factor": 4})
        if decorated:

            @hamilton_task(**options)
            def prepare(value: int) -> dict:
                return {"value": value}

            operator = prepare(5).operator
        else:
            operator = HamiltonOperator(task_id="transform", **options)
    run_task(operator)
    assert run_task.state == TaskInstanceState.SUCCESS
    run_task.xcom.assert_pushed("return_value", {"doubled": 20 if decorated else 12})


@pytest.mark.parametrize("decorated", [False, True])
def test_file_graph(run_task, tmp_path, decorated):
    source = tmp_path / "source.json"
    destination = tmp_path / "destination.json"
    source.write_text('{"value": 7}')
    with DAG("hamilton_file"):
        options = dict(
            modules=[MODULE],
            final_vars=["saved"],
            inputs={"input_path": str(source), "output_path": str(destination)},
        )
        if decorated:

            @hamilton_task(**options)
            def prepare():
                return None

            operator = prepare().operator
        else:
            operator = HamiltonOperator(task_id="transform", **options)
    run_task(operator)
    assert run_task.state == TaskInstanceState.SUCCESS
    assert json.loads(destination.read_text()) == {"value": 14}
    lineage = run_task.ti.task.get_openlineage_facets_on_complete(run_task.ti)
    assert [(d.namespace, d.name) for d in lineage.inputs] == [("file://localhost", str(source))]
    assert [(d.namespace, d.name) for d in lineage.outputs] == [("file://localhost", str(destination))]
