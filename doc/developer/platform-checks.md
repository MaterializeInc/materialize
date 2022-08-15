# Introduction

The Platform Checks framework is "write once, run everywhere" - a **single piece** of test code that validates the operation of a particular
feature is written **once** and then have the framework takes care to execute it in **multiple** scenarios, such as while restarting or
upgrading pieces of the Materialize platform.

The Checks framework is `mzcompose`-based, with the test content expressed in `testdrive` fragments.

In the context of this framework:
- `Check` is an individual validation test
- `Scenario` is the context in which it will be run, such as upgrade.
- `Action` is an individual step that happens during a `Scenario`, such as stopping a particular container

# Running

```
bin/mzcompose --find platform-checks run default --scenario=SCENARIO [--check=CHECK] [--execution-mode= [--execution-mode={alltogether,oneatatime}]
```

The list of Checks available can be found [here](https://dev.materialize.com/api/python/materialize/checks/checks.html#materialize.checks.checks.Check)

The list of Scenarios is [here](https://dev.materialize.com/api/python/materialize/checks/scenarios.html#materialize.checks.scenarios.Scenario)

The list of available Actions for use in Scenarios is [here](https://dev.materialize.com/api/python/materialize/checks/actions.html)

In execution mode `altogether` (the defauklt), all Checks are run against a single Mz instance. This means more "stuff" happens
against that single instance, which has the potential for exposing various bugs that do not happen in isolation. At the same time,
a smaller number of adverse events can be injected in the test per unit of time.

In execution mode `oneatatime`, each Check is run in isolation against a completely fresh Mz instance. This allows more events to happen
per unit of time, but any issues that require greater load or variety of database objects will not show up.

# Debugging CI failures

If you experience a failure in the CI, the scenario that is being run is listed towards the top of the log:

```
Running `mzcompose run default --scenario=RestartEnvironmentdStoraged`
```

Immediately before the failure, the Check that is being run is reported:

```
Running validate() from <materialize.checks.threshold.Threshold object at 0x7f982d1e2950>
```

You can check if the failure is reproducible in isolation by running just the Check in question against just the Scenario in question:

```
./mzcompose run default --scenario=RestartEnvironmentdStoraged --check=Threshold
```

Sometimes, if a Mz container is unable to start, the check where the failure is reported may not be the one that have caused it, it
may be just the first one to attempt to access `computed` or `storaged` that are no longer running.

You can also check if the failure is related to restarts or upgrades in general by trying the "no-op" scenario that does not perform any
of those.

```
./mzcompose run default --scenario=NoRestartNoUpgrade --check=...
```

# Writing a Check

A check is a class deriving from `Check` that implements the following methods:

## `def initialize(self) -> Testdrive:`

Returns a single `Testdrive` fragment that is used to perform preparatory work in the check. This usually means creation of any
helper database objects that are separate from the feature under test, as well as creating the first instance of the feature being tested.

For example:

```
class Rollback(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE rollback_table (f1 INTEGER);
                > INSERT INTO rollback_table VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
                """
            )
        )
```

## `def manipulate(self) -> List[Testdrive]:`

`manipulate()` needs to return a list of two `Testdrive` fragments that further manipulate the object being tested. In this context,
manipulation means ingesting more data into the object, `ALTER`-ing the object in some way, or creating more objects, including derived ones.
See the Tips section below for more information on writing test content for the `manipulate()` section.

## `def validate(self) -> Testdrive:`

The `validate()` section is run one or more times during the test in order to validate the operation of the feature under test. It is always
run after all `initialize()` and `manipulate()` have run, so it should check that all actions and data ingestions that happened during
those sections have been properly processed by the database.

The `validate()` section may be run more than once so it needs to be coded defensively. Any database objects created in this section
must either be `TEMPORARY` or be explicitly dropped at the end of the section.

## Adding the Check to the tests

All checks are located in the `misc/python/materialize/checks/` directory, functionally grouped in files. A `Check` that performs
the creation of a particular type of resource is usually placed in the same file as the `Check` that validates the deletion of the
same resource type.

Checks need to be imported into the `test/platform_checks/mzcompose.py` file:

```
from materialize.checks.my_new_check_file import *  # noqa: F401 F403
```

The `noqa` directives are used to enable wildcard imports.

# Writing a Scenario

A Scenario is a list of sequential Actions that the framework will perform one after another:

```
class RestartEntireMz(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            RestartMzAction(),
            Manipulate(self.checks, phase=1),
            RestartMzAction(),
            Manipulate(self.checks, phase=2),
            RestartMzAction(),
            Validate(self.checks),
        ]

```

A Scenario always contains 5 mandatory steps -- starting a Mz instance, and the exection of the initialization,
manipulation (twice) and validation of all participating Checks. Any Actions that restart or upgrade containers
are then interspersed between those steps. Two `Manipulate` sections are run so that more complex, drawn-out
upgrade scenarios can be tested while ensuring that database activity happens during every stage of the upgrade.

The list of available Actions is (here)[https://dev.materialize.com/api/python/materialize/checks/actions.html#materialize.checks.actions.Action]

# Writing an Action

An Action is basically a short `mzcompose` fragment that operates on the Materialize instance:

```
class DropCreateDefaultReplica(Action):
    def execute(self, c: Composition) -> None:
        c.sql(
            """
           DROP CLUSTER REPLICA default.default_replica;
           CREATE CLUSTER REPLICA default.default_replica SIZE '1';
        """
        )
```

The Action's `execute()` method gets passed a `Composition` object, so that the Action can perform any operation
against the mzcompose composition. The methods of the `Composition` class are listed (here)[https://dev.materialize.com/api/python/materialize/mzcompose/index.html]

# Tips for creating a comprehensive `Check`

## Aim for "catalog comprehensiveness"

A good `Check` will attempt to exercise the full breadth of SQL syntax in order to make sure that the system catalog contains objects
with all sorts of definitions that we will then expect to survive and upgrade and properly operate afterwards.

For example, for database objects with a `WITH` SQL clause, every possible element that could be put in the `WITH` list should be exercised.

## Aim for "network comprehensiveness"

When writing Checks, consider the network interactions between the individual parts of Materialize and make sure they are as
comprehensively exercised as possible.

For objects that are serialized over the network, such as query plans, make sure that all relevant Protobuf messages (and nested or optional
parts thereof) will be generated and transmitted.

For objects that involve ingestion or sinking, make sure that plenty of data will flow. Insert or ingest additional rows of data throughout
the `manipulate()` steps.

## Aim for variety

A good `Check` for a particular database object creates more than one instance of that object and makes sure that the separate instances
are meaningfully different. For example, for materialized views, one would need a view that depends on tables and another that depends on
Kafka sources. Behavior of materialized views with and without an index is also different, so both types need to be represented.

## Aim for plenty of stuff to happen during the test

If you are testing, say, materialized views, make sure that such views are created not only during `initialize()` but also during
`validate()`. This will confirm that materialized views work not only if created on the freshly-started database, but also on
a database that is in the process of being upgraded or restarted.

If the feature or database object you are testing depends on other objects, make sure to create new objects that depend on both new
and old objects in the `manipulate()` sections. For example, materialized views depend on tables, so a comprehensive check will attempt
to create materialized views not only on tables that were created in the `initialize()` section, but also on tables that were created
later in the execution. This will confirm that the database is able to perform any type of DDL in the face of restarts or upgrades.
