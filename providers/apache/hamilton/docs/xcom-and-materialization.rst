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


Results, XCom, and persistence
==============================

Hamilton's default Builder returns a dictionary of requested outputs. A custom
Hamilton result adapter can produce another type; ``result_handler`` receives
that value unchanged and returns the final Airflow task result. Use a synchronous
handler to reduce a large object to a count, summary, or saved resource path.

The provider uses the configured Airflow XCom backend. Moving XCom storage to
object storage does not make arbitrary Python objects serializable. The final
value must meet that backend's serialization requirements. Set
``do_xcom_push=False`` to execute the graph and any handler without pushing its
return value. This does not disable graph execution or file writes.

Request saver nodes explicitly
------------------------------

A registered materializer is not automatically executed. The provider calls
``Builder.with_materializers(...)`` followed by ``execute(final_vars=...)``.
Include the saver ID in ``final_vars``; unrelated savers remain unexecuted. The
same rule applies to saver nodes produced by ``@save_to``:

.. literalinclude:: /../tests/system/apache/hamilton/transforms.py
   :language: python
   :start-at: @save_to.json

Request ``saved`` to write this result. Requesting only ``transformed`` computes
its dictionary without requesting the saver. Saver metadata can itself become a
large or sensitive result; the example's handler returns only its output path.

Retries and failures
--------------------

Writes are Hamilton operations. A later node or result handler can fail after a
write succeeds. An Airflow retry repeats preparation and graph execution and may
repeat or overwrite those writes. There is no provider rollback or exactly-once
I/O guarantee. Choose idempotent destinations, partition by run when appropriate,
and implement transactional or atomic writes in your data adapter when required.

To remove the integration, replace its tasks and uninstall the provider after no
Dags reference it. There are no provider database migrations. Removing it does
not remove data already written by Hamilton.
