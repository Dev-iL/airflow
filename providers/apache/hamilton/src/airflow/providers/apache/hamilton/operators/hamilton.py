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
"""Run Hamilton transformations inside a worker task."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Sequence
from types import ModuleType
from typing import TYPE_CHECKING, Any

from airflow.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.hamilton.lineage import HamiltonLineageCollector
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.sdk import Context


def _get_mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            raise ValueError(f"{name} must render to a valid JSON object or a dictionary") from None
    if not isinstance(value, dict):
        raise TypeError(f"{name} must render to a dictionary, a JSON object, or None")
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} must contain only string keys")
    return value


class HamiltonOperator(BaseOperator):
    """
    Execute a synchronous Hamilton graph and return its final result.

    :param modules: Importable module names or module objects, in precedence order.
        Modules are imported and the graph is built during task execution.
    :param final_vars: Names of requested outputs, including any saver node IDs.
    :param config: Hamilton graph configuration. Templated dictionary, JSON object,
        or None (the default, interpreted as an empty dictionary after rendering).
    :param inputs: Runtime inputs, with the same templating rules as config.
    :param overrides: Node overrides, with the same templating rules as config.
    :param adapters: Optional Hamilton lifecycle and result adapters.
    :param materializers: Optional Hamilton materializers registered with the Builder.
        Only savers requested through final_vars execute.
    :param allow_module_overrides: Allow later modules to replace earlier definitions.
        Defaults to False.
    :param result_handler: Optional synchronous callable transforming the graph result
        before Airflow handles XCom. Defaults to returning the result unchanged.
    """

    template_fields: Sequence[str] = ("config", "inputs", "overrides")
    template_fields_renderers = dict.fromkeys(template_fields, "json")
    shallow_copy_attrs: Sequence[str] = ("modules", "adapters", "materializers", "result_handler")

    def __init__(
        self,
        *,
        modules: Sequence[str | ModuleType],
        final_vars: Sequence[str],
        config: dict[str, Any] | str | None = None,
        inputs: dict[str, Any] | str | None = None,
        overrides: dict[str, Any] | str | None = None,
        adapters: Sequence[Any] | None = None,
        materializers: Sequence[Any] | None = None,
        allow_module_overrides: bool = False,
        result_handler: Callable[[Any], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if isinstance(modules, (str, bytes)) or not modules:
            raise ValueError("modules must be a non-empty sequence of module names or objects")
        if any(not isinstance(module, (str, ModuleType)) for module in modules):
            raise TypeError("modules must contain only module names or module objects")
        if isinstance(final_vars, (str, bytes)) or not all(isinstance(name, str) for name in final_vars):
            raise TypeError("final_vars must be a sequence of node names")
        if result_handler is not None and not callable(result_handler):
            raise TypeError("result_handler must be callable")
        self.modules = modules
        self.final_vars = final_vars
        self.config = config
        self.inputs = inputs
        self.overrides = overrides
        self.adapters = adapters
        self.materializers = materializers
        self.allow_module_overrides = allow_module_overrides
        self.result_handler = result_handler
        self._lineage_collector: HamiltonLineageCollector | None = None

    def execute(self, context: Context) -> Any:
        return self._execute_hamilton()

    def _execute_hamilton(self, prepared_inputs: dict[str, Any] | None = None) -> Any:
        # Hamilton and transformation imports belong in the worker, not Dag parsing.
        from hamilton import driver

        from airflow.providers.apache.hamilton.lineage import HamiltonLineageCollector

        self._lineage_collector = HamiltonLineageCollector()
        config = _get_mapping(self.config, "config")
        inputs = {**_get_mapping(self.inputs, "inputs"), **(prepared_inputs or {})}
        overrides = _get_mapping(self.overrides, "overrides")
        modules = [
            importlib.import_module(module) if isinstance(module, str) else module for module in self.modules
        ]
        builder = driver.Builder().with_modules(*modules).with_config(config)
        builder = builder.with_adapters(*(self.adapters or ()), self._lineage_collector)
        if self.allow_module_overrides:
            builder = builder.allow_module_overrides()
        if self.materializers:
            builder = builder.with_materializers(*self.materializers)
        result = builder.build().execute(final_vars=list(self.final_vars), inputs=inputs, overrides=overrides)
        return self.result_handler(result) if self.result_handler is not None else result

    def get_openlineage_facets_on_complete(self, task_instance: Any) -> OperatorLineage:
        """Contribute collected datasets to Airflow's task completion event."""
        from airflow.providers.openlineage.extractors import OperatorLineage

        if self._lineage_collector is None:
            return OperatorLineage()
        return self._lineage_collector.get_openlineage_facets()
