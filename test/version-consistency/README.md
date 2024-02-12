# Version consistency tests

## Overview

**These tests aim to ensure that no unknown inconsistencies with an earlier version of mz exist.**

## Getting started

To launch the tests using mzcompose, run
```
bin/mzcompose --find version-consistency down -v && bin/mzcompose --find version-consistency run default
```
To start the tests from a shell, use
```
bin/version-consistency-test --max-runtime-in-sec 60
```

## Query generation

Queries are generated using the output consistency test framework, which is also used to ensure consistency between
data-flow rendering and constant folding evaluations. See [this README](../output-consistency/README.md) for more
details on query generation.

To ignore known inconsistencies, extend the `VersionConsistencyIgnoreFilter`.
