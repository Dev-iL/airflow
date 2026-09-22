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

from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.apache.hamilton.lineage import HamiltonLineageCollector, _get_file_identity
from airflow.providers.apache.hamilton.operators.hamilton import HamiltonOperator


def make_node(**tags):
    return SimpleNamespace(tags=tags)


def test_collector_keeps_file_identities_without_values(tmp_path):
    source = tmp_path / "source.json"
    destination = tmp_path / "destination.json"
    collector = HamiltonLineageCollector()

    collector.post_node_execute(
        run_id="run",
        node_=make_node(**{"hamilton.data_loader": True, "hamilton.data_loader.has_metadata": True}),
        kwargs={},
        success=True,
        error=None,
        result=({}, {"file_metadata": {"path": str(source)}}),
    )
    collector.post_node_execute(
        run_id="run",
        node_=make_node(**{"hamilton.data_saver": True}),
        kwargs={},
        success=True,
        error=None,
        result={"file_metadata": {"path": str(destination)}},
    )

    lineage = collector.get_openlineage_facets()

    assert [(dataset.namespace, dataset.name) for dataset in lineage.inputs] == [
        ("file://localhost", str(source))
    ]
    assert [(dataset.namespace, dataset.name) for dataset in lineage.outputs] == [
        ("file://localhost", str(destination))
    ]
    assert collector.inputs == {("file://localhost", str(source))}
    assert collector.outputs == {("file://localhost", str(destination))}


@mock.patch("airflow.providers.apache.hamilton.lineage.log", autospec=True)
@pytest.mark.parametrize(
    "path",
    [
        "file://untrusted.example/data.json?token=secret",
        "ftp://bucket/data.json",
        "s3://bucket",
    ],
)
def test_collector_ignores_failures_and_invalid_metadata_without_leaking_paths(log, tmp_path, path):
    collector = HamiltonLineageCollector()

    collector.post_node_execute(
        run_id="run",
        node_=make_node(**{"hamilton.data_saver": True}),
        kwargs={},
        success=False,
        error=RuntimeError("secret"),
        result={"file_metadata": {"path": str(tmp_path / "ignored.json")}},
    )
    collector.post_node_execute(
        run_id="run",
        node_=make_node(**{"hamilton.data_saver": True}),
        kwargs={},
        success=True,
        error=None,
        result={"file_metadata": {"path": path}},
    )

    lineage = collector.get_openlineage_facets()

    assert not lineage.inputs
    assert not lineage.outputs
    log.warning.assert_called_once_with("Hamilton lineage skipped invalid loader/saver metadata")


@mock.patch.dict("sys.modules", {"hamilton.plugins.h_openlineage": None})
@mock.patch("airflow.providers.apache.hamilton.lineage.log", autospec=True)
def test_collector_skips_sql_metadata_when_hamilton_conversion_is_unavailable(log):
    collector = HamiltonLineageCollector()

    collector.post_node_execute(
        run_id="run",
        node_=make_node(**{"hamilton.data_saver": True}),
        kwargs={},
        success=True,
        error=None,
        result={"sql_metadata": {"query": "select secret"}},
    )

    lineage = collector.get_openlineage_facets()

    assert not lineage.inputs
    assert not lineage.outputs
    log.warning.assert_called_once_with(
        "Hamilton SQL lineage requires a newer Hamilton OpenLineage integration"
    )


@mock.patch("airflow.providers.apache.hamilton.lineage.log", autospec=True)
def test_collector_delegates_sql_conversion_and_keeps_only_identities(log):
    from hamilton.plugins import h_openlineage
    from openlineage.client.event_v2 import Dataset

    if not hasattr(h_openlineage, "sql_datasets"):
        pytest.skip("Hamilton SQL conversion is not released")
    collector = HamiltonLineageCollector()
    metadata = {
        "sql_metadata": {
            "query": "select secret from orders",
            "source": {"dialect": "sqlite", "database": "/tmp/sales.db"},
        }
    }
    converted = SimpleNamespace(
        inputs=[Dataset("sqlite:///tmp/sales.db", "orders")], outputs=[], notes=["secret detail"]
    )

    with mock.patch.object(h_openlineage, "sql_datasets", autospec=True, return_value=converted) as convert:
        collector.post_node_execute(
            run_id="run",
            node_=make_node(**{"hamilton.data_loader": True, "hamilton.data_loader.has_metadata": True}),
            kwargs={},
            success=True,
            error=None,
            result=(object(), metadata),
        )

    convert.assert_called_once_with(metadata, operation="read")
    assert collector.inputs == {("sqlite:///tmp/sales.db", "orders")}
    assert collector.outputs == set()
    log.warning.assert_called_once_with("Hamilton SQL lineage is incomplete; some datasets were skipped")


def test_collector_uses_hamilton_postgres_dataset_identity():
    from hamilton.plugins import h_openlineage

    if not hasattr(h_openlineage, "sql_datasets"):
        pytest.skip("Hamilton SQL conversion is not released")
    collector = HamiltonLineageCollector()

    collector.post_node_execute(
        run_id="run",
        node_=make_node(**{"hamilton.data_loader": True, "hamilton.data_loader.has_metadata": True}),
        kwargs={},
        success=True,
        error=None,
        result=(
            object(),
            {
                "sql_metadata": {
                    "query": 'SELECT * FROM Orders JOIN reporting."Customers" ON 1=1',
                    "source": {
                        "dialect": "postgresql",
                        "host": "db.example",
                        "port": None,
                        "database": "sales",
                        "default_schema": "public",
                    },
                }
            },
        ),
    )

    assert collector.inputs == {
        ("postgres://db.example:5432", "sales.public.orders"),
        ("postgres://db.example:5432", "sales.reporting.Customers"),
    }


@mock.patch("airflow.providers.apache.hamilton.lineage.log", autospec=True)
def test_collector_does_not_leak_sql_conversion_errors(log):
    from hamilton.plugins import h_openlineage

    if not hasattr(h_openlineage, "sql_datasets"):
        pytest.skip("Hamilton SQL conversion is not released")
    collector = HamiltonLineageCollector()

    with mock.patch.object(
        h_openlineage,
        "sql_datasets",
        autospec=True,
        side_effect=RuntimeError("postgresql://user:secret@db.example/sales"),
    ):
        collector.post_node_execute(
            run_id="run",
            node_=make_node(**{"hamilton.data_saver": True}),
            kwargs={},
            success=True,
            error=None,
            result={"sql_metadata": {"query": "select secret"}},
        )

    assert collector.outputs == set()
    log.warning.assert_called_once_with("Hamilton lineage skipped invalid loader/saver metadata")


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("s3://user:secret@bucket/data.json?token=secret#fragment", ("s3://bucket", "/data.json")),
        ("file:///tmp/data%20file.json", ("file://localhost", "/tmp/data file.json")),
        ("/tmp/data?#%20.json", ("file://localhost", "/tmp/data?#%20.json")),
    ],
)
def test_file_identity_canonicalizes_supported_paths_without_credentials(path, expected):
    assert _get_file_identity(path) == expected


@mock.patch("openlineage.client.event_v2.Dataset", autospec=True, side_effect=RuntimeError)
def test_collector_returns_empty_lineage_when_dataset_conversion_fails(dataset):
    collector = HamiltonLineageCollector()
    collector.inputs.add(("s3://bucket", "/data.json"))

    lineage = collector.get_openlineage_facets()

    assert not lineage.inputs
    assert not lineage.outputs
    dataset.assert_called_once_with(namespace="s3://bucket", name="/data.json")


def test_operator_returns_empty_lineage_before_execution():
    operator = HamiltonOperator(
        task_id="transform", modules=["system.apache.hamilton.transforms"], final_vars=["doubled"]
    )

    lineage = operator.get_openlineage_facets_on_complete(None)

    assert not lineage.inputs
    assert not lineage.outputs
