# mzcompose workflows

This document describes the format and the available facilities for writing Python-based workflows, to be executed by the `mzcompose` tool.

Due to an accident of history, this file only documents the workflow features of `mzcompose`. For an overview of `mzcompose`, see [mzbuild.md](./mzbuild.md).
TODO(benesch): incorporate all these documents together.

In this document, "workflow" and "test" are used interchangeably.

# Overview

Python-based workflows are contained in a file named `mzcompose.py`. The file serves three different purposes:

1. Declare the services that are needed to run the workflows

2. Define individual workflows as Python functions

3. (Optionally) Define any services used in the workflow(s) that do not have a definition in the `mzcompose` library (e.g. a test that uses a custom container)

# Running

Docker Compose is required to run mzcompose-based workflows. See the [developer guide](./guide.md#docker) for how to install it.

Python-based workflows are executed like so:

```sh
cd test/directory
# Remove any previous containers and their volumes
./mzcompose down -v
./mzcompose run workflow-name
```

Almost all mzcompose files have a `default` workflow, so `./mzcompose run default` should do the right thing.

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

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive()
]

def workflow_test(c: Composition):
    c.up("zookeeper", "kafka", "schema-registry", "materialized")

    c.run_testdrive_files("*.td")
```

Additional examples can be found here:
* [test/mzcompose_examples/mzcompose.py has sample workflows](https://github.com/MaterializeInc/materialize/blob/main/test/mzcompose_examples/mzcompose.py)
* [Some existing workflows have already been ported to Python](https://github.com/MaterializeInc/materialize/search?q=mzcompose.py&type=code)

# Dealing with services

Given the declarative nature of `docker-compose`, participating services need to be declared at the top of the `mzcompose.py` file, outside of a particular workflow. A service definition can not be created inside a workflow, but a declared service can be started, stopped and otherwise manipulated within a workflow.

The `mzcompose` library contains definitions for the most common services used when composing workflows. The definitive list is available [from the class documentation of `mzcompose`](https://dev.materialize.com/api/python/misc/python/materialize/mzcompose/service.html#Service).

## Declaring the required services

To declare the services the workflow/test will use, define a top-level global `services` list in your `mzcompose.py` file:

```python
from materialize.mzcompose import (Materialized, Testdrive)

SERVICES = [
    Materialized(),
    Testdrive(),
]
```

`Materialized` and `Testdrive` are pre-defined services available from the `mzcompose` library. Internally, `mzcompose` will convert the list of services into a YAML document that is fed to `docker-compose`.

## Defining new services

If a workflow or test requires a service that is not defined in the library, consider adding a definition if there is even the slightest chance that the same service may be reused in a different workflow in the future.

To add a new service library definition, use one of the existing classes that derive from `PythonService` in `./misc/python/materialize/mzcompose.py` as a starting point.

Truly one-off services can be declared inside the `mzcompose.py` file itself:

```python
from materialize.mzcompose import PythonService

SERVICES = [PythonService(image = "vendor/container")]
```

# Dealing with workflows

A Python workflow is a Python function that contains the steps to execute as part of the workflow

```python
def workflow_simple_test(c: Composition):
    "Validate that materialized starts and can serve a simple SQL query"
    c.up("materialize")
    c.kill("materialize")
```

A `mzcompose.py` file can contain multiple workflows. All workflow files should contain a `default` workflow, if it makes sense.

## Naming

The Python functions that start with `workflow_` are considered workflows. This prefix is stripped and any underscores in the name of the function are converted to dashes to come up with the name under which a workflow can be referenced. For example, the `def workflow_simple_test(...)` above can be executed as follows:

```sh
./mzcompose run simple-test
```

## Interactions

A Python workflow can interact with the services in the composition.

The [definitive reference](https://dev.materialize.com/api/python/misc/python/materialize/mzcompose/composition.html#Composition) lists all the individual methods that are available.

For example, here is how to bring up the `materialized` service:

```python
def workflow_test(c: Composition):
    c.up("materialized")
```

In the example above, we are calling the [`up`](https://dev.materialize.com/api/python/misc/python/materialize/mzcompose/composition.html#Composition.up) method, which maps directly to a call to `docker-compose up`.

## Starting services

In the past, starting a service with `Composition.up` did not wait for the
service to become healthy. We had a variety of helper methods (e.g.,
`Composition.start_and_wait_for_tcp`) that would perform additional health
checks after starting the service.

Since February 2023, we leverage Docker Compose healthchecks. The definition
of each service specifies how to assess its readiness *and* declares what other
service's health it depends on. A simple `Composition.up` call is now all that
is required to bring up a service and wait for it to become ready:

```py
# Starts Zookeeper, waits for it to become healthy, then starts Kafka, waits
# for it to become healthy, then finally starts the schema registry and waits
# for it to become healthy before returning.
c.up("schema-registry")
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
def workflow_simple_test(c: Composition):
   ...
   c.run_testdrive_files("*.td")
   ...
```
