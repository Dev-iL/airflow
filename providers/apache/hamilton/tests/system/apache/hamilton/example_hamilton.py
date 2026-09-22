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
"""Run a local Hamilton graph, persist JSON, and map a TaskFlow transformation."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pendulum

from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator
from airflow.sdk import DAG, task

from tests_common.test_utils.watcher import watcher

MODULE = "system.apache.hamilton.transforms"


def _get_saved_path(result: dict) -> str:
    return result["saved"]["file_metadata"]["path"]


def _get_doubled(result: dict) -> int:
    return result["doubled"]


with DAG(
    dag_id="example_hamilton",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "hamilton"],
) as dag:

    @task
    def prepare_files() -> dict:
        directory = Path(tempfile.mkdtemp(prefix="airflow-hamilton-"))
        source = directory / "source.json"
        source.write_text(json.dumps({"value": 3}))
        return {"input_path": str(source), "output_path": str(directory / "result.json")}

    paths = prepare_files()

    # [START operator]
    saved = HamiltonOperator(
        task_id="transform_file",
        modules=[MODULE],
        final_vars=["saved"],
        inputs=paths,
        result_handler=_get_saved_path,
    )
    # [END operator]

    # [START decorator]
    @task.hamilton(modules=[MODULE], final_vars=["saved"], result_handler=_get_saved_path)
    def transform_again(path: str) -> dict:
        return {"input_path": path, "output_path": path}

    saved_again = transform_again(saved.output)
    # [END decorator]

    @task
    def prepare_config() -> dict:
        return {"factor": 3}

    # [START mapping]
    @task.hamilton(
        modules=[MODULE], final_vars=["doubled"], config=prepare_config(), result_handler=_get_doubled
    )
    def scale(value: int) -> dict:
        return {"value": value}

    mapped_results = scale.expand(value=[1, 2, 3])
    # [END mapping]

    @task
    def check_results(path: str, values: list[int]) -> None:
        assert json.loads(Path(path).read_text()) == {"value": 12}
        assert list(values) == [3, 6, 9]

    checked = check_results(saved_again, mapped_results)  # type: ignore[arg-type]

    @task(trigger_rule="all_done")
    def cleanup(inputs: dict) -> None:
        shutil.rmtree(Path(inputs["input_path"]).parent)

    checked >> cleanup(paths)
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
