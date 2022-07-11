# Introduction

Zippy is meant as a general longevity/stress testing framework that will cause data to be ingested from topics or tables, be processed by materialized or recorded views and validated for correctness. All of this may happen in the face of process restarts and other events in the environment.

The framework pseudo-randomly decides what data will be ingested, what database objects will be created and what events will happen.

# Running

```bash
bin/mzcompose --find zippy down -v
bin/mzcompose --find zippy run default --scenario SCENARIO --actions N
```

# Architecture

## Test

A Test is a sequence of Actions that will be performed during the test. Zippy constructs the entire sequence on startup and then proceeds to run each individual Action contained therein.

## Scenario

A Scenario is a description of what Actions will participate in the test and what their frequency should be. The available scenarios are defined here:

[materialize.zippy.scenarios API documentation](https://dev.materialize.com/api/python/materialize/zippy/scenarios.html)

A `Scenario` class is essentially a list of Actions and their relative weights. When generating the sequence of Actions that will form a test, the framework will figure out which Actions are currently runable given the state of the system and will pick one of them, taking weights into account.

## Actions

An Action is an mzcompose or testdrive operation that is performed during the test. Actions include:

- stopping and starting processes
- creating topics, sources, tables and views
- inserting data into Kafka topics or tables
- validating sources, tables and views for correctness

The complete list of actions is available here:

[materialize.zippy.framework API documentation](https://dev.materialize.com/api/python/materialize/zippy/framework.html#materialize.zippy.framework.Action)

## Capabilities

The framework keeps track of what capabilities the environment currently has, which influences what actions the framework can pick to be run. For example, one can not run a `CreateSource` action before a `CreateTopic` action has been executed that will cause the environment to have a `TopicExists` capability.

# Data Validation

The framework validates the data returned by Materialize as follows:

## Tracking Topics and Tables

Each Kafka source or table contains a single integer column.

For each table and Kafka topic we keep a “min” and an “max” value, which represent the minimum and maximum value that were inserted/ingested so far and therefore are expected to be present in the topic or table.

## Tracking Insertions, Deletions and Updates

The framework makes sure that only unique value are inserted and any DML operations do not violate the uniqueness property.

Inserting N rows or Kafka messages will cause the “max” value to be increased by N.

Updates are made over the entire table, causing all the values in the table, as well as the “min” and the “max”, to shift by N.

Deletions are only made to N rows from the “head” or the “tail” of the source, causing the “max” or the “min” to be adjusted by N.

## Validating Sources

Database objects inherit the min/max from their source. A Kafka source will inherit from the Kafka topic. So, the framework can issue a query such as:

```sql
SELECT MIN(f1), MAX(f1) FROM source
```

And compare the result to the min/max values that it maintains across updates.

## Validating Views

View definitions are constructed in such a way that the min/max of the entire view can be calculated based on the mins/maxs of its inputs. For example:

```sql
CREATE MATERIALIZED VIEW view3 AS
SELECT MIN(source6.f1), MAX(source6.f1)
FROM source6 JOIN source8 USING (f1)
JOIN source9 USING (f1)
JOIN source7 USING (f1)
JOIN source10 USING (f1);
```

The expected results from such a view is the intersection of all the records that are present in all of the inputs. The min/max of the expected result is also the intersection of the min/max of all the inputs.

In addition to the `MIN` and the `MAX`, we also validate the `COUNT` and the `COUNT(DISTINCT` . As we never insert duplicate data, we expect that `COUNT = MAX - MIN` and `COUNT(DISTINCT = COUNT`

# Extending the Framework

## Adding a new scenario

Scenarios are defined in `misc/python/materialize/zippy/scenarios.py`

## Adding a new action

Actions have a `requires()` and a `provides()` method. The `requires()` method is a list of requirement sets. If all the requirements from a particular set are currently satisfied, the Action can be picked and scheduled. Consider the following definition:

```python
class CreateView(Action):
    @classmethod
    def requires(self) -> List[Set[Type[Capability]]]:
        return [{MzIsRunning, SourceExists}, {MzIsRunning, TableExists}]
```

`CreateView` can be scheduled if one of the two sets is satisfied, that is, if a table OR a source already exists in the system.

The `provides()` method returns a list of `Capabilites` that executing a given action provides to the system. For example, if the `CreateView` action will be creating a new view, it will return a `ViewExists` capability as a result of its `provides()` method. This will allow an action such as `ValidateView` , that has a `ViewExists` requirement, to be scheduled subsequently.
