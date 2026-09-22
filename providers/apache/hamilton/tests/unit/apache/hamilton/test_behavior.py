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

import functools
import importlib

import pytest
from hamilton import base
from hamilton.lifecycle.base import BasePostNodeExecute

from airflow.providers.apache.hamilton.decorators.hamilton import hamilton_task
from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator, _get_mapping
from airflow.sdk import DAG
from airflow.utils.state import TaskInstanceState

MODULE = "system.apache.hamilton.transforms"


def extract_doubled(result):
    return result["doubled"]


def build_answer(result):
    return {"answer": result["doubled"]}


def convert_dataframe_result(result):
    return result.to_dict()


def fail_result_handler(result):
    raise RuntimeError("result handler failed")


class CaptureLifecycleAdapter(BasePostNodeExecute):
    def __init__(self):
        self.executed_nodes = []

    def post_node_execute(self, *, node_, success, **kwargs):
        if success:
            self.executed_nodes.append(node_.name)


@pytest.mark.parametrize(
    ("config", "inputs", "overrides", "expected"),
    [
        ('{"factor": 3}', '{"value": 4}', "{}", 12),
        (None, {"value": 4}, None, 8),
        (None, None, {"doubled": 9}, 9),
        ({"factor": 5}, {"value": 4}, {}, 20),
    ],
)
def test_operator_normalizes_rendered_mappings(run_task, config, inputs, overrides, expected):
    operator = HamiltonOperator(
        task_id="transform",
        modules=[MODULE],
        final_vars=["doubled"],
        config=config,
        inputs=inputs,
        overrides=overrides,
        result_handler=extract_doubled,
    )

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    run_task.xcom.assert_pushed("return_value", expected)


@pytest.mark.parametrize(
    ("value", "name", "exception", "message"),
    [
        ("not json", "inputs", ValueError, "valid JSON object"),
        ("[]", "config", TypeError, "dictionary, a JSON object, or None"),
        ([], "overrides", TypeError, "dictionary, a JSON object, or None"),
        ({1: "value"}, "inputs", TypeError, "only string keys"),
    ],
)
def test_mapping_validation_errors_are_actionable(value, name, exception, message):
    with pytest.raises(exception, match=message):
        _get_mapping(value, name)


@pytest.mark.parametrize(
    ("kwargs", "exception", "message"),
    [
        ({"modules": [], "final_vars": ["doubled"]}, ValueError, "non-empty sequence"),
        ({"modules": MODULE, "final_vars": ["doubled"]}, ValueError, "non-empty sequence"),
        ({"modules": [1], "final_vars": ["doubled"]}, TypeError, "module names or module objects"),
        ({"modules": [MODULE], "final_vars": "doubled"}, TypeError, "sequence of node names"),
        ({"modules": [MODULE], "final_vars": [1]}, TypeError, "sequence of node names"),
        (
            {"modules": [MODULE], "final_vars": ["doubled"], "result_handler": 1},
            TypeError,
            "must be callable",
        ),
    ],
)
def test_constructor_validation_errors_are_actionable(kwargs, exception, message):
    with pytest.raises(exception, match=message):
        HamiltonOperator(task_id="transform", **kwargs)


def test_decorator_result_handler_and_multiple_outputs(run_task):
    with DAG("hamilton_multiple_outputs"):

        @hamilton_task(
            modules=[MODULE],
            final_vars=["doubled"],
            config={"factor": 3},
            result_handler=build_answer,
            multiple_outputs=True,
        )
        def prepare() -> dict[str, int]:
            return {"value": 4}

        operator = prepare().operator

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    run_task.xcom.assert_pushed("return_value", {"answer": 12})
    run_task.xcom.assert_pushed("answer", 12)


def test_decorator_default_does_not_split_scalar_result(run_task):
    with DAG("hamilton_scalar"):

        @hamilton_task(
            modules=[MODULE],
            final_vars=["doubled"],
            result_handler=extract_doubled,
        )
        def prepare() -> dict[str, int]:
            return {"value": 4}

        operator = prepare().operator

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    run_task.xcom.assert_pushed("return_value", 8)


def test_decorator_multiple_outputs_rejects_scalar_final_result(run_task):
    with DAG("hamilton_bad_multiple_outputs"):

        @hamilton_task(
            modules=[MODULE],
            final_vars=["doubled"],
            result_handler=extract_doubled,
            multiple_outputs=True,
        )
        def prepare():
            return {"value": 4}

        operator = prepare().operator

    run_task(operator)

    assert run_task.state == TaskInstanceState.FAILED
    assert isinstance(run_task.error, TypeError)


def test_decorator_passes_standard_context_arguments_to_preparation(run_task):
    with DAG("hamilton_context"):

        @hamilton_task(modules=[MODULE], final_vars=["doubled"], result_handler=extract_doubled)
        def prepare(ti):
            assert ti.task_id == "prepare"
            return {"value": 4}

        operator = prepare().operator

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    run_task.xcom.assert_pushed("return_value", 8)


def test_custom_lifecycle_adapter_and_result_handler_run_with_real_graph(run_task):
    adapter = CaptureLifecycleAdapter()
    operator = HamiltonOperator(
        task_id="transform",
        modules=[MODULE],
        final_vars=["doubled"],
        inputs={"value": 4},
        adapters=[adapter],
        result_handler=extract_doubled,
    )

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    assert "doubled" in adapter.executed_nodes
    run_task.xcom.assert_pushed("return_value", 8)


def test_hamilton_result_adapter_reaches_result_handler_unchanged(run_task, tmp_path):
    source = tmp_path / "source.json"
    source.write_text('{"value": 7}')
    operator = HamiltonOperator(
        task_id="transform",
        modules=[MODULE],
        final_vars=["payload"],
        inputs={"input_path": str(source)},
        adapters=[base.PandasDataFrameResult()],
        result_handler=convert_dataframe_result,
    )

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    run_task.xcom.assert_pushed("return_value", {"payload": {"value": 7}})


def test_module_override_uses_later_module_when_enabled(run_task, tmp_path, monkeypatch):
    (tmp_path / "first_hamilton_module.py").write_text("def result() -> int:\n    return 1\n")
    (tmp_path / "second_hamilton_module.py").write_text("def result() -> int:\n    return 2\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    first = importlib.import_module("first_hamilton_module")
    second = importlib.import_module("second_hamilton_module")
    operator = HamiltonOperator(
        task_id="transform",
        modules=[first, second],
        final_vars=["result"],
        allow_module_overrides=True,
        result_handler=lambda result: result["result"],
    )

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    run_task.xcom.assert_pushed("return_value", 2)


def test_native_templates_render_mapping_values_before_hamilton_runs(run_task):
    with DAG(
        "hamilton_native_templates",
        render_template_as_native_obj=True,
        params={"factor": 3, "value": 4, "override": 11},
    ):
        operator = HamiltonOperator(
            task_id="transform",
            modules=[MODULE],
            final_vars=["doubled"],
            config={"factor": "{{ params.factor }}", "nested": {"factor": "{{ params.factor }}"}},
            inputs={"value": "{{ params.value }}"},
            overrides={"doubled": "{{ params.override }}"},
            result_handler=extract_doubled,
        )

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    assert run_task.ti.task.config == {"factor": 3, "nested": {"factor": 3}}
    assert run_task.ti.task.inputs == {"value": 4}
    assert run_task.ti.task.overrides == {"doubled": 11}
    run_task.xcom.assert_pushed("return_value", 11)


def test_do_xcom_push_false_runs_graph_without_result_xcom(run_task):
    operator = HamiltonOperator(
        task_id="transform",
        modules=[MODULE],
        final_vars=["doubled"],
        inputs={"value": 4},
        do_xcom_push=False,
    )

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    assert run_task.xcom.get("return_value") is None


def test_decorator_rejects_async_preparation_callables():
    async def prepare():
        return {"value": 4}

    with pytest.raises(TypeError, match="synchronous input preparation callable"):
        hamilton_task(prepare, modules=[MODULE], final_vars=["doubled"])()


def test_decorator_rejects_wrapped_async_preparation_callable():
    async def async_prepare():
        return {"value": 4}

    @functools.wraps(async_prepare)
    def prepare():
        return {"value": 4}

    with pytest.raises(TypeError, match="synchronous input preparation callable"):
        hamilton_task(prepare, modules=[MODULE], final_vars=["doubled"])()


def test_decorator_rejects_async_callable_object():
    class AsyncPreparation:
        async def __call__(self):
            return {"value": 4}

    with pytest.raises(TypeError, match="synchronous input preparation callable"):
        hamilton_task(AsyncPreparation(), modules=[MODULE], final_vars=["doubled"])()


def test_decorator_rejects_partial_async_preparation_callable():
    async def prepare():
        return {"value": 4}

    with pytest.raises(TypeError, match="synchronous input preparation callable"):
        hamilton_task(functools.partial(prepare), modules=[MODULE], final_vars=["doubled"])()


def test_decorator_rejects_invalid_preparation_result(run_task):
    with DAG("hamilton_bad_preparation"):

        @hamilton_task(modules=[MODULE], final_vars=["doubled"])
        def prepare():
            return ["value"]

        operator = prepare().operator

    run_task(operator)

    assert run_task.state == TaskInstanceState.FAILED
    assert isinstance(run_task.error, TypeError)
    assert "dictionary or None" in str(run_task.error)


def test_decorator_rejects_prepared_inputs_with_non_string_keys(run_task):
    with DAG("hamilton_bad_prepared_keys"):

        @hamilton_task(modules=[MODULE], final_vars=["doubled"])
        def prepare():
            return {1: 4}

        operator = prepare().operator

    run_task(operator)

    assert run_task.state == TaskInstanceState.FAILED
    assert isinstance(run_task.error, TypeError)
    assert "Prepared inputs must contain only string keys" in str(run_task.error)


def test_preparation_failure_uses_standard_retry_state(run_task):
    with DAG("hamilton_retry"):

        @hamilton_task(modules=[MODULE], final_vars=["doubled"], retries=1)
        def prepare():
            raise ValueError("preparation failed")

        operator = prepare().operator

    run_task(operator)

    assert run_task.state == TaskInstanceState.UP_FOR_RETRY
    assert isinstance(run_task.error, ValueError)


def test_decorator_clears_stale_lineage_before_preparation_fails(run_task):
    with DAG("hamilton_lineage_reset"):

        @hamilton_task(modules=[MODULE], final_vars=["doubled"])
        def prepare():
            return ["invalid"]

        operator = prepare().operator

    operator._lineage_collector = object()

    run_task(operator)

    assert run_task.state == TaskInstanceState.FAILED
    assert run_task.ti.task._lineage_collector is None


@pytest.mark.parametrize(
    ("inputs", "result_handler"),
    [({"value": 4}, fail_result_handler), ({"value": None}, extract_doubled)],
)
def test_graph_or_result_handler_failures_fail_the_task(run_task, inputs, result_handler):
    operator = HamiltonOperator(
        task_id="transform",
        modules=[MODULE],
        final_vars=["doubled"],
        inputs=inputs,
        result_handler=result_handler,
    )

    run_task(operator)

    assert run_task.state == TaskInstanceState.FAILED
    assert run_task.error is not None
