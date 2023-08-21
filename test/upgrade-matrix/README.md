# Introduction

Existing upgrade tests, both the legacy ones in `test/upgrade` and the hard-coded upgrade
scenarios in `misc/python/materialize/checks/scenarios_upgrade.py` fail to capture all
the possible events that could happen over the lifetime of a Mz instance.

For example, the previously-mentioned test frameworks only use the several most-recent
Mz versions. This means that those tests will not attempt to upgrade an Mz instance
that contains database objects, catalog entries or other data that was created in
a very old Mz version.

At the same time, involving all possible old Mz versions in a test would cause a
combinatorial explosion, as we want to both create very old database objects *and*
for those objects be modified, referenced or otherwise exercised on other, more
recent versions.

# Design

To combat the combinatorial explosion, we will be randomly testing some upgrade
scenarios out of the space of all upgrade scenarios. To do that, we construct a
directed acyclic graph that represents all the states the Mz cluster could be in.

The graph contains nodes for states such as starting a particular Mz version or
stopping said version. It also contains nodes that represent the possible places
of execution of SQL statements. A valid upgrade path may go from version X to
version X+1 to X+2 without running any SQL, but another valid upgrade path would
run some DDLs on any of those versions.

We leverage the Platform Checks framework to provide the statements to run.
Therefore, we need to place the initialize() , manipulate #1
and manipulate #2 phases of Platform Checks framework in the graph as well, in
all places where they could legitimately run sucessfully.

We then walk the graph randomly in order to construct an upgrade scenario that starts
at some old Mz version and proceeds across valid actions all the way to the
latest HEAD. From the initial list of random walks, we prune those that are not valid:
 - a Mz version must first be shut down before another one can be started
 - the initialize phase must have run at some point prior to manipulate #1 and
   identically for the manipulate #2 phase.

From the first X (default = 20) walks, we construct a Platform Checks Scenario
and pass it to the Platform Checks framework for execution.

# Debugging

The framework uses a PRNG with a known random seed. The CI output will contain lines such as:

```
--- Random seed is 123456789
--- Checks to use: [<class 'materialize.checks.pg_cdc.PgCdc'>, <class 'materialize.checks.sink.SinkUpsert'>]
```
--- Testing upgrade scenario with id 1234: [BeginUpgradeSequence, BeginVersion(v0.42.4), ...
```

From this information, we can exactly the same upgrade scenario:

```
bin/mzcompose --find upgrade-matrix run default --seed=123456789 --scenario-id=1234 --check=SinkUpsert
```
