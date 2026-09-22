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


Hamilton TaskFlow decorator
===========================

``@task.hamilton`` calls a synchronous Python function to prepare runtime inputs,
then runs the same Hamilton execution path as ``HamiltonOperator``. The function
returns a dictionary with string keys or ``None``. Its keys override static
``inputs``; returning anything else raises ``TypeError``.

.. exampleinclude:: /../tests/system/apache/hamilton/example_hamilton.py
   :start-after: [START decorator]
   :end-before: [END decorator]

The decorator accepts the operator's Hamilton options and ordinary TaskFlow
arguments, including context parameters. ``config``, ``inputs``, and ``overrides``
keep their public names and template behavior. Python preparation exceptions fail
the task, and retries run preparation and the graph again. Async functions, wrapped
async functions, and async callable objects are unsupported.

Chaining and mapping
--------------------

Function arguments can receive upstream outputs or be mapped. Upstream values in
Hamilton's template fields also create dependencies, including when another
function argument is mapped:

.. exampleinclude:: /../tests/system/apache/hamilton/example_hamilton.py
   :start-after: [START mapping]
   :end-before: [END mapping]

The final graph result, after ``result_handler``, becomes the task result.
``multiple_outputs=False`` is the default even when the preparation function is
annotated as returning a dictionary. Explicit ``multiple_outputs=True`` asks Airflow
to split a final mapping into individual XCom entries; a scalar or a mapping with
non-string keys fails. ``show_return_value_in_logs`` defaults to false.
