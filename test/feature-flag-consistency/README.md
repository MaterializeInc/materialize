# Feature Flag consistency tests

## Overview

**These tests aim to ensure that a different feature flag configuration produces the same results.**

## Getting started

To launch the tests using mzcompose, run
```
bin/mzcompose --find feature-flag-consistency down -v && bin/mzcompose --find feature-flag-consistency run default
```
To start the tests from a shell, use
```
bin/feature-flag-consistency --max-runtime-in-sec 60
```

## Test explain plans

This can be done using the `--output-query-mode` parameter. Refer to the version-consistency test for more details.

## Query generation

Queries are generated using the output consistency test framework, which is also used to ensure consistency between
data-flow rendering and constant folding evaluations. See [this README](../output-consistency/README.md) for more
details on query generation.

To ignore known inconsistencies, extend the `FeatureFlagConsistencyIgnoreFilter`.
