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
"""Collect dataset identities without retaining Hamilton node results."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlsplit

from hamilton.lifecycle.base import BasePostNodeExecute

if TYPE_CHECKING:
    from hamilton.node import Node

    from airflow.providers.openlineage.extractors import OperatorLineage

log = logging.getLogger(__name__)


def _get_file_identity(path: str) -> tuple[str, str]:
    parsed = urlsplit(path)
    if not parsed.scheme or parsed.scheme == "file":
        if parsed.hostname not in (None, "", "localhost"):
            raise ValueError("Remote file authorities are unsupported")
        local_path = unquote(parsed.path) if parsed.scheme else path
        return "file://localhost", str(Path(local_path).resolve())
    if parsed.scheme not in {"s3", "gs", "gcs", "hdfs", "http", "https", "abfs", "abfss", "wasb", "wasbs"}:
        raise ValueError("Unsupported file scheme")
    if not parsed.hostname or not parsed.path.strip("/"):
        raise ValueError("File identity requires an authority and path")
    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    authority = f"{host}:{parsed.port}" if parsed.port else host
    return f"{parsed.scheme}://{authority}", unquote(parsed.path)


class HamiltonLineageCollector(BasePostNodeExecute):
    """Keep only sanitized identities of successfully executed loaders and savers."""

    def __init__(self) -> None:
        self.inputs: set[tuple[str, str]] = set()
        self.outputs: set[tuple[str, str]] = set()

    def post_node_execute(
        self,
        *,
        run_id: str,
        node_: Node,
        kwargs: dict[str, Any],
        success: bool,
        error: Exception | None,
        result: Any | None,
        task_id: str | None = None,
    ) -> None:
        if not success:
            return
        try:
            if node_.tags.get("hamilton.data_saver") is True:
                metadata = result
                datasets = self.outputs
            elif (
                node_.tags.get("hamilton.data_loader") is True
                and node_.tags.get("hamilton.data_loader.has_metadata") is True
            ):
                metadata = result[1] if isinstance(result, tuple) and len(result) == 2 else None
                datasets = self.inputs
            else:
                return
            if not isinstance(metadata, dict):
                raise TypeError("Loader/saver metadata must be a dictionary")
            if "sql_metadata" in metadata:
                try:
                    from hamilton.plugins.h_openlineage import sql_datasets
                except ImportError:
                    log.warning("Hamilton SQL lineage requires a newer Hamilton OpenLineage integration")
                    return
                operation = "write" if datasets is self.outputs else "read"
                sql_lineage = sql_datasets(metadata, operation=operation)
                converted = sql_lineage.outputs if operation == "write" else sql_lineage.inputs
                identities = {(dataset.namespace, dataset.name) for dataset in converted}
                if not all(isinstance(value, str) for identity in identities for value in identity):
                    raise TypeError("Hamilton SQL datasets must have string identities")
                datasets.update(identities)
                if sql_lineage.notes:
                    log.warning("Hamilton SQL lineage is incomplete; some datasets were skipped")
                return
            if "file_metadata" not in metadata:
                log.warning("Hamilton lineage skipped unsupported loader/saver metadata")
                return
            path = metadata["file_metadata"]["path"]
            if not isinstance(path, str) or not path:
                raise ValueError("File metadata requires a path")
            datasets.add(_get_file_identity(path))
        except Exception:
            # Node values and exception messages can contain credentials or signed URLs.
            log.warning("Hamilton lineage skipped invalid loader/saver metadata")

    def get_openlineage_facets(self) -> OperatorLineage:
        """Convert identities only when Airflow requests OpenLineage information."""
        from openlineage.client.event_v2 import Dataset

        from airflow.providers.openlineage.extractors import OperatorLineage

        try:
            return OperatorLineage(
                inputs=[Dataset(namespace=namespace, name=name) for namespace, name in sorted(self.inputs)],
                outputs=[Dataset(namespace=namespace, name=name) for namespace, name in sorted(self.outputs)],
            )
        except Exception:
            log.warning("Hamilton lineage could not convert collected dataset identities")
            return OperatorLineage()
