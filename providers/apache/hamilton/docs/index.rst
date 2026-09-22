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


Apache Hamilton provider
========================

Run reusable `Apache Hamilton <https://hamilton.apache.org/>`_ transformation modules
inside an Airflow task. Airflow owns scheduling, retries, mapping, and task results;
Hamilton executes the functions within each task. Choose ``HamiltonOperator`` when
inputs are already available, or ``@task.hamilton`` when a Python function prepares them.

Installation
------------

Install the provider in the Dag processor and worker environments:

.. code-block:: bash

    pip install apache-airflow-providers-apache-hamilton
    # Also install the optional Airflow lineage integration:
    pip install 'apache-airflow-providers-apache-hamilton[openlineage]'

This package is being prepared for its initial release. Until it is published, build
and install the local wheel using :doc:`installing-providers-from-sources`.

Requirements are Airflow 3.0 or later, Python 3.10.1 or later, Apache Hamilton 1.90.0
or later, standard provider 1.0.0 or later, and common compatibility provider 1.8.0
or later. The dependency uses the published ``apache-hamilton`` package. Hamilton
installs pandas and NumPy even for graphs that do not use them.
The JSON example requires no optional file-format engine. Install engines needed
by your own transformations, such as PyArrow for Parquet.

Deploy every named transformation module and its dependencies to workers using an
installed Python package or your Dag bundle. Use importable dotted module names in
production. Import resolution happens in the worker; the provider does not upload
modules. Adapter, materializer, and result-handler objects must be constructed by
the Dag file when the worker reconstructs the task; they are not portable serialized
configuration. Use importable callables and construct objects in the Dag definition.

Local example
-------------

The first example reads JSON, writes a result, reads and updates that result through
the decorator, and maps three transformations with an upstream configuration task.
The SQL example builds a revenue table from a local SQLite join. Both check their
results and remove their temporary directories. Local files require a shared
filesystem if tasks run on different workers.

From the Airflow source checkout, run:

.. code-block:: bash

    breeze run --backend sqlite pytest providers/apache/hamilton/tests/system/apache/hamilton/example_hamilton.py --system -v
    breeze run --backend sqlite pytest providers/apache/hamilton/tests/system/apache/hamilton/example_hamilton_sql.py --system -v

The ``system.apache.hamilton.transforms`` module is available from this provider's
``tests`` directory in that test environment. For a deployed Dag, package these
functions under your own importable module name and update ``MODULE`` in the example.
The watcher and ``get_test_run`` imports belong to Airflow's system-test harness.

.. toctree::
   :maxdepth: 1

   operators/hamilton
   decorators/hamilton
   xcom-and-materialization
   openlineage
   Security <security>
   changelog
   Installing from sources <installing-providers-from-sources>
   commits
   Python API <_api/airflow/providers/apache/hamilton/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/apache/hamilton/index>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-hamilton package
------------------------------------------------------

Execute reusable `Apache Hamilton <https://hamilton.apache.org/>`__ transformations inside Airflow tasks.

.. note::

   This provider has not been released. Version ``1.0.0`` is the planned first release.
   The generated publication, documentation, and download links below are placeholders;
   their release artifacts are not available. Until publication, install a wheel built
   from a source checkout containing this provider.


Release: 1.0.0

Provider package
----------------

This package is for the ``apache.hamilton`` provider.
All classes for this package are included in the ``airflow.providers.apache.hamilton`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-apache-hamilton``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.8.0``
``apache-airflow-providers-standard``       ``>=1.0.0``
``apache-hamilton``                         ``>=1.90.0``
==========================================  ==================

Optional cross provider package dependencies
--------------------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-apache-hamilton[openlineage]


==============================================================================================================  ===============
Dependent package                                                                                               Extra
==============================================================================================================  ===============
`apache-airflow-providers-openlineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage>`_  ``openlineage``
==============================================================================================================  ===============

Optional dependencies
---------------------

These extras install optional third-party libraries that enable additional features of the provider.
Install them when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-apache-hamilton[openlineage]


===============  =========================================================================================
Extra            Dependencies
===============  =========================================================================================
``openlineage``  ``apache-airflow-providers-openlineage>=2.3.0``, ``apache-hamilton[openlineage]>=1.90.0``
===============  =========================================================================================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-apache-hamilton 1.0.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hamilton-1.0.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hamilton-1.0.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hamilton-1.0.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-apache-hamilton 1.0.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hamilton-1.0.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hamilton-1.0.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_apache_hamilton-1.0.0-py3-none-any.whl.sha512>`__)
