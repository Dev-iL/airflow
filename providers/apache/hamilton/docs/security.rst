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

.. include:: /../../../../devel-common/src/sphinx_exts/includes/security.rst

Hamilton transformations
------------------------

Transformation modules, adapters, materializers, and result handlers are trusted
Dag-author code executed with worker permissions. Deploy and review them using
the same controls as other task code. The provider does not inspect Connections
or access Airflow's metadata database directly.

File lineage excludes URL credentials and signed query parameters, but resource
paths themselves may be sensitive. Configure your lineage destination accordingly.
SQL lineage stores only dataset namespace/name identities in the task collector.
The provider does not retain queries or connection objects and emits fixed
diagnostics when conversion is incomplete, so connection credentials and query
values are not copied into task logs. Dataset names can still reveal database,
schema, and table names; protect the lineage destination accordingly.
Hamilton and user adapters can log values independently of the provider; avoid
embedding secrets in inputs, returned saver metadata, and result-handler output.
Airflow's normal rendered-template and XCom access controls also apply.
