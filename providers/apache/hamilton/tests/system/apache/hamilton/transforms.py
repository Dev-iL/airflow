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
"""Reusable transformations for the local Hamilton example."""

from __future__ import annotations

from hamilton.function_modifiers import load_from, save_to, source


def doubled(value: int, factor: int = 2) -> int:
    return value * factor


@load_from.json(path=source("input_path"))
def payload(raw_data: dict) -> dict:
    return raw_data


@save_to.json(path=source("output_path"), output_name_="saved")
def transformed(payload: dict, factor: int = 2) -> dict:
    return {"value": payload["value"] * factor}
