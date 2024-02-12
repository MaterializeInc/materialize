# Postgres consistency tests

## Overview

**These tests aim to ensure that no unknown inconsistencies with Postgres exist.**

## Getting started

To launch the tests using mzcompose, run
```
bin/mzcompose --find postgres-consistency down -v && bin/mzcompose --find postgres-consistency run default
```
To start the tests from a shell, use
```
bin/postgres-consistency-test --max-runtime-in-sec 60
```

## Query generation

Queries are generated using the output consistency test framework, which is also used to ensure consistency between
data-flow rendering and constant folding evaluations. See [this README](../output-consistency/README.md) for more
details on query generation.

To
* disable a certain function or operation that is not available in Postgres, set `is_pg_compatible` to `false` in the instance of `DbFunction` or `DbOperation`
* account for a syntactical difference, adjust the `PgSqlDialectAdjuster`
* adapt the result comparison, have a look at the `PostgresResultComparator`
* ignore known inconsistencies, extend the `PgInconsistencyIgnoreFilter`
