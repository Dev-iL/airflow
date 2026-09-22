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

from typing import Any, ClassVar

import pytest

from airflow.providers.apache.hamilton.decorators.hamilton import hamilton_task
from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time.xcom import resolve_xcom_backend
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars


class RecordingXCom(BaseXCom):
    pushed_values: ClassVar[dict[str, Any]] = {}

    @classmethod
    def set(cls, key, value, **kwargs):
        cls.pushed_values[key] = value


@pytest.mark.parametrize("decorated", [False, True])
@conf_vars({("core", "xcom_backend"): "unit.apache.hamilton.test_xcom.RecordingXCom"})
def test_final_value_uses_configured_xcom_backend(run_task, monkeypatch, decorated):
    monkeypatch.setattr(RecordingXCom, "pushed_values", {})
    # The task runner binds the configured backend once when its process starts.
    backend = resolve_xcom_backend()
    assert backend is RecordingXCom
    monkeypatch.setattr("airflow.sdk.execution_time.task_runner.XCom", backend)
    options = dict(modules=["system.apache.hamilton.transforms"], final_vars=["doubled"], inputs={"value": 3})
    if decorated:

        @hamilton_task(**options)
        def prepare():
            return None

        operator = prepare().operator
    else:
        operator = HamiltonOperator(task_id="transform", **options)

    run_task(operator)

    assert run_task.state == TaskInstanceState.SUCCESS
    assert RecordingXCom.pushed_values == {"return_value": {"doubled": 6}}
