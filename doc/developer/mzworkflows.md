# Introduction

This document describes the format and the available facilities for writing Python-based workflows, to be executed by the `mzworkflows` tool.

Using Python to define workflows and their participating services replaces the previous YAML-based format and is the preferred way to write tests going forward.

In this document, "workflow" and "test" are used interchangeably.

# Overview

Python-based workflows are contained in a file named `mzworkflows.py`. The file serves three different purposes:

1. Declare the services that are needed to run the workflows

2. Define individual workflows as Python functions

3. (Optionally) Define any services used in the workflow(s) that do not have a definition in the `mzcompose` library (e.g. a test that uses a custom container)

# Running

Python-based workflows are executed in the same manner as YAML-based ones:

```sh
cd test/directory
# Remove any previous containers and their volumes
./mzcompose down -v
./mzcompose run workflow-name
```

# Where should I put my test?

Depending on the test or the workflow that you intend to create, a suitable place should be chosen:

| Test | Suggested Location |
| ----------- | ----------- |
| SQL-level features | no new workflow needed; best written as a sqllogic test that can live in `test/sqllogictest` |
| Testdrive test against a standard Kafka + Schema Registry + Materialize   | no new workflow needed, place `.td` file in `test/testdrive` |
| Postgres direct replication | `test/pg-cdc` |
| Debezium | `test/debezium-avro` |
| Any test that uses non-standard services or non-standard command-line options | a new directory under `test/` |

# Examples

Here is a complete example:

```python
from materialize.mzcompose import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Workflow,
    Zookeeper,
)

services = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive()
]

def workflow_test(w: Workflow):
    w.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry", "materialized"])
    w.wait_for_mz()

    w.run_service(service="testdrive-svc", command="*.td")
```

Additional examples can be found here:
* [test/mzworkflows_examples/mzworkflows.py has sample workflows](https://github.com/MaterializeInc/materialize/blob/main/test/mzworkflows_examples/mzworkflows.py)
* [Some existing workflows have already been ported to Python](https://github.com/MaterializeInc/materialize/search?q=mzworkflows.py&type=code)

# Dealing with services

Given the declarative nature of `docker-compose`, participating services need to be declared at the top of the `mzworkflows.py` file, outside of a particular workflow. A service definition can not be created inside a workflow, but a declared service can be started, stopped and otherwise manipulated within a workflow.

The `mzcompose` library contains definitions for the most common services used when composing workflows. The definitive list is available [from the class documentation of `mzcompose`](https://dev.materialize.com/api/python/materialize/mzcompose.html#materialize.mzcompose.PythonService).

## Declaring the required services

To declare the services the workflow/test will use, define a top-level global `services` list in your `mzworkflows.py` file:

```python
from materialize.mzcompose import (Materialized, Testdrive)

services = [
    Materialized(),
    Testdrive(),
]
```

`Materialized` and `Testdrive` are pre-defined services available from the `mzcompose` library. Internally, `mzcompose` will convert the list of services into a YAML document that is fed to `docker-compose`.

## Services available from the library

[The class documentation of `mzcompose`](https://dev.materialize.com/api/python/materialize/mzcompose.html#materialize.mzcompose.PythonService) lists all available services.

## Defining new services

If a workflow or test requires a service that is not defined in the library, consider adding a definition if there is even the slightest chance that the same service may be reused in a different workflow in the future.

To add a new service library definition, use one of the existing classes that derive from `PythonService` in `./misc/python/materialize/mzcompose.py` as a starting point.

Truly one-off services can be declared inside the `mzworkflows.py` file itself:

```python
from materialize.mzcompose import PythonService

services = [PythonService(image = "vendor/container")]
```

# Dealing with workflows

A Python workflow is a Python function that contains the steps to execute as part of the workflow

```python
def workflow_simple_test(w: Workflow):
    w.start_services(services=["materialize"])
    w.wait_for_mz(service="materialize")
    w.kill_services(services=["materialize"])
```

A `mzworkflows.py` file can contain multiple workflows.

## Naming

The Python functions that start with `workflow_` are considered workflows. This prefix is stripped and any underscores in the name of the function are converted to dashes to come up with the name under which a workflow can be referenced. For example, the `def workflow_simple_test(...)` above can be executed as follows:

```sh
./mzcompose run simple-test
```

## Steps

A Python workflow can execute steps, which are the actions that were previously available in the YAML-based workflows.

The [definitive reference](https://dev.materialize.com/api/python/materialize/mzcompose.html#materialize.mzcompose.WorkflowStep) lists all the individual steps that are available.

The steps have been exposed as methods to the `Workflow` object, with the names of the methods derived from the names of the YAML steps, replacing dashes with underscores. Therefore, one can call a step as follows:

```python
def workflow_test(w: Workflow):
    w.start_services(services=["materialize"])
```

The one exception is the `run` step, which can be called as `w.run_service()`.

In the example above, we are calling the [`StartServicesStep`](https://dev.materialize.com/api/python/materialize/mzcompose.html#materialize.mzcompose.StartServicesStep).

## Starting services

Starting the services for a workflow requires additional care because:
- `w.start_services()` will not wait for a service to be fully up before moving on to start the next one which may depend on the previous one
- for complex services such as third-party databases, even if `w.wait_for_tcp()` completes, the service may not be fully ready to accept SQL commands and may return errors on them

Therefore, a couple of convenience steps are provided:

### `w.start_and_wait_for_tcp(services=[...])`

Starts the services from the list one after another, waiting for the respective port of the service to become open.

Note that unlike other steps that deal with services, this step does not take a list of service names, but rather `PythonService` objects.

### `w.wait_for_mz()`

Waits until Materialize is capable of answering `SELECT 1` queries. A similar step is available for checking other Postgres-compatible services.

### Example

The fool-proof sequence for starting the complete Materialize stack would be something along the following lines:

```python
services = [Zookeeper(), Kafka(), SchemaRegistry(), Materialized()]

def workflow_start_services(w: Workflow):
    w.start_and_wait_for_tcp(services=services)
    w.wait_for_mz()
```

## Executing SQL

A test cannot be considered complete if it does not connnect to the Materialize instance to run SQL commands.

### Accessing Materialize directly from Python

While it is possible to use a Python driver such as `psycopg3` or `pg8000` to run SQL, this should be mostly be considered for DDL and sometimes DML statements. Statements on whose result set you wish to assert on, such as `SELECT` queries, are better put in a Testdrive `.td` test and executed via the Testdrive service, for the reasons described below.

### Accessing Materialize via Testdrive

Putting the queries in a Testdrive `.td` file allows the entire convenience machinery of Testdrive to be used. For example:
- Testdrive automatically handles retrying the query until it returns the desired result
- Putting SQL queries in a `.td` file, rather than inlining them in Python with their own `assert` allows more individual queries to be included for a smaller effort. This enocourages better feature coverage (e.g. use more varied data types in the test, different types of sources or sinks, etc.)

To run a Testdrive test or tests:

```python
def workflow_simple_test(w: Workflow):
   ...
   w.run_service(service="testdrive-svc", command="*.td")
   ...
```

# YAML-based workflows

YAML-based workflows, defined in files named `mzcompose.yml` are being deprecated and should not be used for new tests.
