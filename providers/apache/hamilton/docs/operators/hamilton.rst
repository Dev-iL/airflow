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


HamiltonOperator
================

:class:`~airflow.providers.apache.hamilton.operators.hamilton.HamiltonOperator`
imports the requested modules, builds a synchronous Hamilton Driver, executes
``final_vars``, and returns the result during worker task execution.

.. exampleinclude:: /../tests/system/apache/hamilton/example_hamilton.py
   :start-after: [START operator]
   :end-before: [END operator]

``modules`` is a non-empty sequence of dotted module names or Python modules.
``final_vars`` contains output node names. A Hamilton graph stays inside one
Airflow task; its individual functions are not Airflow tasks.

``config``, ``inputs``, and ``overrides`` default to ``None``. Each accepts a
string-keyed dictionary, a JSON-object string, or ``None``. Airflow renders these
fields, including nested values, before execution. Only ``None`` becomes an empty
dictionary. Invalid JSON raises ``ValueError``; non-objects and non-string keys raise
``TypeError``. Validation messages identify the field without including its values.

.. code-block:: python

    HamiltonOperator(
        task_id="calculate",
        modules=["my_project.transforms"],
        final_vars=["total"],
        config={"mode": "daily"},
        inputs='{"date": "{{ ds }}"}',
        overrides={"rate": 2},
    )

With ``render_template_as_native_obj=True`` on the Dag, Jinja expressions can
produce dictionaries directly. Upstream task outputs in any of these fields add
a scheduling dependency and resolve through normal Airflow XCom handling.
The operator supports ordinary ``partial(...).expand(inputs=[...])`` task mapping.

``adapters`` supplies Hamilton lifecycle or result adapters. ``materializers``
registers saver/loader factories with ``Builder.with_materializers``. Both default
to ``None``. ``allow_module_overrides=False`` preserves Hamilton's duplicate-node
validation; set it to true to let later modules replace earlier definitions.
``result_handler=None`` returns the graph result unchanged. See
:doc:`../xcom-and-materialization` for persistence, result shaping, and XCom.

Execution is synchronous. Hamilton dynamic execution, deferral, and a separate
Hamilton scheduling layer are not provided. Graph and result-handler errors fail
the Airflow task and follow its configured retry policy.
