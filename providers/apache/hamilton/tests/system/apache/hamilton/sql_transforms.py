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
"""SQL transformations for the Hamilton lineage example."""

from __future__ import annotations

import pandas as pd  # noqa: TC002  # Hamilton evaluates node annotations at graph construction.
from hamilton.function_modifiers import load_from, save_to, source, value


@load_from.sql(query_or_table=source("sales_query"), db_connection=source("sales_db"))
def order_lines(frame: pd.DataFrame) -> pd.DataFrame:
    return frame


def daily_revenue(order_lines: pd.DataFrame) -> pd.DataFrame:
    return order_lines.groupby(["order_date", "country"], as_index=False)["amount"].sum()


@save_to.sql(
    table_name=source("output_table"),
    db_connection=source("warehouse_db"),
    if_exists=value("replace"),
    index=value(False),
    output_name_="saved_revenue",
)
def revenue_report(daily_revenue: pd.DataFrame) -> pd.DataFrame:
    return daily_revenue
