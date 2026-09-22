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
"""Prepare Hamilton inputs with TaskFlow."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator, _get_mapping
from airflow.providers.common.compat.sdk import DecoratedOperator, task_decorator_factory
from airflow.providers.standard.operators.python import PythonOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import TaskDecorator


def _validate_python_callable(python_callable: Callable) -> None:
    unwrapped = inspect.unwrap(python_callable)
    if inspect.iscoroutinefunction(unwrapped) or inspect.iscoroutinefunction(
        inspect.unwrap(unwrapped.__call__)
    ):
        raise TypeError("@task.hamilton requires a synchronous input preparation callable")


class _HamiltonDecoratedOperator(DecoratedOperator, PythonOperator, HamiltonOperator):
    template_fields: Sequence[str] = (*PythonOperator.template_fields, *HamiltonOperator.template_fields)
    template_fields_renderers = {
        **PythonOperator.template_fields_renderers,
        **HamiltonOperator.template_fields_renderers,
    }
    shallow_copy_attrs: Sequence[str] = (
        *PythonOperator.shallow_copy_attrs,
        *HamiltonOperator.shallow_copy_attrs,
    )
    custom_operator_name = "@task.hamilton"

    def __init__(self, *, python_callable: Callable, op_args: Any, op_kwargs: Any, **kwargs: Any) -> None:
        _validate_python_callable(python_callable)
        kwargs.setdefault("show_return_value_in_logs", False)
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            kwargs_to_upstream={
                "python_callable": python_callable,
                "op_args": op_args,
                "op_kwargs": op_kwargs,
            },
            **kwargs,
        )

    def execute_callable(self) -> Any:
        self._lineage_collector = None
        prepared = super().execute_callable()
        if prepared is not None and not isinstance(prepared, dict):
            raise TypeError("@task.hamilton input preparation must return a dictionary or None")
        return self._execute_hamilton(_get_mapping(prepared, "Prepared inputs"))


def hamilton_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool = False,
    **kwargs: Any,
) -> TaskDecorator:
    """
    Prepare runtime inputs and execute Hamilton using the operator's options.

    The synchronous callable returns a string-keyed dictionary or None. Its keys
    override static ``inputs``. Normal PythonOperator arguments and context are
    available. ``multiple_outputs`` defaults to False regardless of the preparation
    function's annotation; if True, Airflow splits the final Hamilton result.
    ``show_return_value_in_logs`` defaults to False.

    :param python_callable: Synchronous input preparation function.
    :param multiple_outputs: Split the final mapping into individual XCom values.
    :param kwargs: HamiltonOperator and decorated PythonOperator options.
    """
    if python_callable is not None:
        _validate_python_callable(python_callable)
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_HamiltonDecoratedOperator,
        **kwargs,
    )
