 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Dataset lineage with OpenLineage
================================

Install the provider's ``openlineage`` extra and configure the Airflow
`OpenLineage provider <https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/>`_
with a transport and namespace. Collection contributes input and output datasets
to Airflow's task completion event. The provider does not install Hamilton's
separately emitting OpenLineage adapter or create another graph-run event.
The extra installs Hamilton's OpenLineage dependency set, including
``openlineage-sql`` where that platform is supported; the provider uses only its
side-effect-free conversion function.

For local inspection, configure the OpenLineage file transport:

.. code-block:: bash

    export AIRFLOW__OPENLINEAGE__DISABLED=false
    export AIRFLOW__OPENLINEAGE__NAMESPACE=hamilton-example
    export AIRFLOW__OPENLINEAGE__TRANSPORT='{"type":"file","log_file_path":"/tmp/hamilton-lineage.jsonl"}'

Use a writable transport destination in each worker and follow the OpenLineage
provider's transport settings for your deployment.

Dataset identity
----------------

Successful tagged Hamilton loaders and savers, including graph materializers,
contribute recognized ``file_metadata.path`` values. Local paths and local
``file:`` URIs use namespace ``file://localhost`` and an absolute resolved path.
Remote file URLs use ``scheme://authority`` as the namespace and their path as
the dataset name. Supported schemes are ``s3``, ``gs``, ``gcs``, ``hdfs``, ``http``,
``https``, ``abfs``, ``abfss``, ``wasb``, and ``wasbs``. URL credentials, query
parameters, and fragments are excluded. Reads and writes use the same identity
rules. Local path identity assumes workers share the relevant filesystem.

The collector retains only sanitized identities, not node values, DataFrames,
arguments, inputs, or the Driver. Each graph execution starts with an empty
collector. A graph without file I/O contributes no datasets. Unsupported,
malformed, or incomplete metadata is skipped with a diagnostic. Collection or
conversion errors do not fail otherwise successful transformations.

SQL dataset identity
--------------------

Hamilton metadata schema ``1.1.0`` records the datasource used by its built-in
Pandas SQL loader and saver. The provider passes that metadata to Hamilton's
side-effect-free ``hamilton.plugins.h_openlineage.sql_datasets`` function and
adds the returned identities to Airflow's task completion event. It does not
install Hamilton's event-emitting adapter, inspect database connections, or run
the graph a second time.

SQLite datasets use namespace ``sqlite://{absolute database path}``; their names
are the physical table names. PostgreSQL datasets use namespace
``postgres://{host}:{port}`` and names ``{database}.{schema}.{table}``. Query
lineage includes each physical input and output table found by
``openlineage-sql`` while excluding aliases and common table expressions. A
writer's explicit schema takes precedence over the connection's verified default
schema. PostgreSQL naming has been verified from URL and metadata forms without a
live PostgreSQL server; the executable example below uses local SQLite.

.. literalinclude:: /../tests/system/apache/hamilton/example_hamilton_sql.py
    :language: python
    :start-after: [START sql_operator]
    :end-before: [END sql_operator]

The SQL example creates two local SQLite databases, reads a join, writes
``daily_revenue``, and checks the saved rows. With the OpenLineage file transport
configured above, Airflow emits ``orders`` and ``customers`` as inputs and
``daily_revenue`` as the output.

SQL support was validated against unreleased Hamilton revision
``a0e4f3f7b01db70008712e3c691e42dcd9adb076``. No released Hamilton dependency
floor containing this API exists yet. Released versions without the conversion
function continue to provide file lineage and skip SQL metadata with a fixed
diagnostic. Before publishing this provider, replace the development revision
with the first released Hamilton version containing the same contract and rerun
the installation matrix.

Unknown or in-memory datasources, unsupported dialects, missing schema context,
parser failures, malformed metadata, and a missing conversion function contribute
no fabricated datasets. Hamilton may return the identities it can fully resolve
and mark the remainder incomplete. The provider logs only fixed diagnostics, so
queries, credentials, and connection values are not copied into task logs.
