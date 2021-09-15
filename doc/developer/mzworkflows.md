# Introduction

This document describes the format and the available facilities for writing Python-based workflows, to be executed by the `mzworkflows` tool.

Using Python to define workflows and their participating services replaces the previous YAML-based format and is the preferred way to write tests going forward.

In this document, "workflow" and "test" may be used interchangeably.

# Overview

A Python-based test or workflow is contained in a file named `mzworkflows.py`. The file serves three different purposes:

1. Declare the services that are needed to run the workflows or tests

2. Define individual workflows as python functions

3. (Optionally) define any services used in the workflow or test that do not have a definition in the mzcompose library (e.g. a test that is bringing up a unique one-time container from the outside)

# Running

A Python workflow is executed in the same manner as the YAML-based one:

```
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
| any test that uses non-standard services or non-standard command-line options | a new directory under `test/` |

# Examples

[test/mzworkflows_examples/mzworkflows.py](https://github.com/MaterializeInc/materialize/blob/main/test/mzworkflows_examples/mzworkflows.py) contains example workflows.

Some existing workflows have already been ported to Python, and can be [seen here](https://github.com/MaterializeInc/materialize/search?q=mzworkflows.py&type=code)

# Dealing with services

Given the declarative nature of `docker-compose`, participating services need to be declared at the top of the `mzworkflows.py` file, outside of a particular workflow. A service definition can not be created inside a workflow, but a declared service can be started, stopped and otherwise manipulated within a workflow.

The `mzcompose` library contains definitions for the most common services used when composing workflows (Materialize itself, Zookeeper, Kafka, the Schema Registry, Testdrive, etc.)

## Declaring the required services

To declare the services the workflow/test will use, define a top-level global `services` list in your `mzworkflows.py` file:

```
from materialize.mzcompose import (Materialized, Testdrive)

services = [
    Materialized(),
    Testdrive(),
]
```

Internally, `mzcompose` will convert the list of services into a YAML document that can be fed to `docker-compose`

## Services available from the library

In the example above, `Materialized` and `Testdrive` are pre-defined services available from the mzcompose library itself. The definitive list is available [from the class documentation of `mzcompose`](https://dev.materialize.com/api/python/materialize/mzcompose.html#materialize.mzcompose.PythonService)

## Defining new services

If a workflow or test requires a service that is not defined in the library, consider adding a definition there if there is even the slightest chance that the same service may be reused in a different workflow in the future.

To add a new service library definition, use one of the existing classes that derive from `PythonService` in `./misc/python/materialize/mzcompose.py`

Truly one-off services can be declared inside the `mzworkflows.py` file itself:

```
from materialize.mzcompose import PythonService

services = [PythonService(image = "vendor/container")]
```

# Dealing with workflows

A Python workflow is a Python function that contains the steps to execute as part of the workflow

```
def workflow_simple_test(w: Workflow):
    w.start_services(services=["materialize"])
    w.wait_for_mz(service="materialize")
    w.kill_services(services=["materialize"])
```

A `mzworkflows.py` file can contain multiple workflows.

## Naming

The Python functions that start with `workflow_` are considered workflows. This prefix is stipped and any underscores in the name of the function are converted to dashes to come up with the name under which a workflow can be referenced. For example, the `def workflow_simple_test(...)` above can be executed as follows:

```
./mzcompose run simple-test
```

## Steps

A python workflow can execute Steps, which are the actions that were previously available in the YAML-based workflows.

The [definitive reference](https://dev.materialize.com/api/python/materialize/mzcompose.html#materialize.mzcompose.WorkflowStep) lists all the individual steps that are available.

The Steps have been exposed as methods to the `Workflow` object, with the names of the methods derived from the names of the YAML steps, replacing dashes with underscores. Therefore, one can call a step as follows:

```
def workflow_test(w: Workflow):
    w.start_services(services=["materialize"])
```

The one exception is the `run` step, which can be called as `w.run_service()`

## Starting services

Starting the services at the start of a workflow requires additional care because:
- `w.start_services()` will not wait for a service to be fully up before moving on to start the next one which may depend on the previous one
- for complex services such as third-party databases, even if `w.wait_for_tcp()` completes, the service may not be fully ready to accept SQL commands and may return errors on them

Therefore, a couple of convenience Steps are provided:

### `w.start_and_wait_for_tcp(services=[...])`

Starts the services from the list one after another, waiting for the respective port of the service to become open.

Note that unlike other Steps that deal with services, this Step does not take a list of service names, but rather of `PythonService` objects.

### `w.wait_for_mz()`

Waits until Materialize is capable of answering `SELECT 1` queries. A similar Step is available for checking other Postgres-compatible services.

Therefore, the fool-proof sequence for starting the complete Mz stack would be something along the following:

```
services = [Zookeeper(), Kafka(), SchemaRegistry(), Materialized()]

def workflow_start_confluents(w: Workflow):
    w.start_and_wait_for_tcp(services=confluents)
    w.wait_for_mz()
```

## Executing SQL

A test can not be considered complete if it does not connnect to the Materialize instance to run SQL commands.

### Accessing Materialize directly from Python

While it is possible to use a Python driver such as `psycopg3` or `pg8000` to run SQL, this should be mostly be considered for DDL and sometimes DML statements. Statements on whose result set you wish to assert on, such as `SELECT` queries, are better put in a Testdrive `.td` test and executed from there, for the reasons described below.

### Accessing Materialize via Testdrive

Putting the queries in a Testdrive `.td` file allows the entire convenience machinery of Testdrive to be brought to bear. For example:
- Testdrive automatically handles retrying the query until it returns the desired result
- putting SQL queries in a `.td` file, rather than inlining them in Python with their own `assert` alows more individual queries to be included for a smaller effort. This enocourages better feature coverage (e.g. use more varied data types in the test, different types of sources or sinks, etc.)

To run a Testdrive test or tests:

```
def workflow_simple_test(w: Workflow):
   ...
   w.run_service(service="testdrive-svc", command="test*.td")
   ...
```

# YAML-based workflows

YAML-based workflows, defined in files named `mzcompose.yml` are being deprecated and should not be used for new tests.
