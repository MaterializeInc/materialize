---
name: mz-test
description: >
  This skill should be used when the user wants to "run tests", "run testdrive",
  "run sqllogictest", "run mzcompose", "run cargo test", "run pgtest",
  "rewrite test results", "add a test", "reproduce a bug", "write a regression
  test", or mentions testing, testdrive, sqllogictest, mzcompose, pgtest,
  cargo test, nextest, flaky tests, or test failures in the Materialize
  repository. Use this skill even if the user just says "test this" or
  "how do I verify this works" without naming a specific framework.
---

# Testing Materialize

## Unit tests

Run unit tests with `cargo test`.
If available, use `cargo nextest` instead.
Use `bin/cargo-test` to run the full unit/integration test suite.
Rewrite datadriven test expectations with `REWRITE=1 cargo test ...`.
Do not manually update `*.snap` files.
Use `cargo test` followed by `cargo insta accept` to update snapshot files.

Some tests require access to a metadata store.
Set environment variables to `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257`.

Control log output during tests with `MZ_TEST_LOG_FILTER=<level>`, e.g. `MZ_TEST_LOG_FILTER=debug cargo test ...`.

The first run includes a cargo build step that can take several minutes.
Use a bash timeout of at least 600000ms (10 min) for test commands.

## sqllogictest

Run sqllogictest files with:

```
bin/sqllogictest --optimized -- PATH
```

`PATH` is relative to the repo root, usually a file in `test/sqllogictest/`.
Rewrite expected results with `bin/sqllogictest -- --rewrite-results PATH`.
For large test files, `--optimized` significantly improves execution speed at the cost of longer compile time.

## Testdrive

Run testdrive files with:

```
bin/mzcompose --find testdrive run default -- FILENAME.td
```

`FILENAME.td` is a file in `test/testdrive/`, relative to that directory (not the repo root).

## pgtest

Run pgtest files with:

```
cargo test -p environmentd pgtest
```

Run a specific pgtest file: `cargo test -p environmentd test_pgtest_FILE_NAME`.

## mzcompose

`NAME` is any directory containing an `mzcompose.py` file.

Run mzcompose compositions with:

```
bin/mzcompose --find NAME run WORKFLOW
```

`default` is a valid workflow in many compositions, provided a `workflow_default` exists that iterates over all workflows.
Not all compositions have a `workflow_default`.

Other useful commands:

* `bin/mzcompose --find NAME list` — list available compositions
* `bin/mzcompose --find NAME list-workflows` — list workflows for a composition
* `bin/mzcompose --find NAME logs` — view container logs
* `bin/mzcompose --find NAME down` — stop a composition

Many tests expect to start with fresh state.
Run `bin/mzcompose --find NAME down` between test runs, not just at the end of a session.

mzcompose builds optimized binaries by default, which are nearly as fast to run as release and much faster to link than debug builds.

* `--release` builds full release binaries, which can be slow.
  Instead of building release locally, let CI build the binary by creating a (draft) pull request and waiting for the build jobs to finish.
  Afterward, `mzcompose` will attempt to download the binaries instead of building them locally.
* `--dev` builds non-optimized debug binaries locally.

## Reproducing bugs

Reproducing bugs is a manual process.
Ask the user for reproduction instructions, or point them to the GitHub issue or the Buildkite annotation under "Test details & reproducer".

Many tests expect fresh state.
Always run `bin/mzcompose --find NAME down` before attempting to reproduce.

For flaky tests, the user can run in a loop until failure:

```
while true; do
    bin/mzcompose --find testdrive down && \
    bin/mzcompose --find testdrive run default FILENAME.td || break
done
```

If a flake cannot be reproduced locally, suggest the user trigger CI runs manually via https://trigger-ci.dev.materialize.com/.
Removing the "Target Branch" generates a random identifier, allowing parallel runs.
10-20 runs to reproduce a flake is fine.

## Additional logging for flaky tests

Add logging through `log_filter` in the test's `mzcompose.py`:

```python
Materialized(
    additional_system_parameter_defaults={
        # TODO: Remove when database-issues#NNNN is fixed
        "log_filter": "mz_storage::source::postgres=trace"
    },
)
```

If the flake might occur anywhere, extend `get_minimal_system_parameters` in `misc/python/materialize/mzcompose/__init__.py` with a `log_filter`.
Always add a TODO comment referencing the issue.

## Adding a test

Determine the right framework based on what you're testing:

* **SQL correctness, types, functions** (no external systems, no concurrency): sqllogictest (`.slt` in `test/sqllogictest/`).
  Use `mode cockroach`, test NULLs and edge cases.
  Do NOT modify files in `test/sqllogictest/sqlite` or `test/sqllogictest/cockroach` (upstream).
* **Sources/sinks, Kafka, catalog, pgwire** (external systems): testdrive (`.td` in `test/testdrive/`).
  Frameworks like `pg-cdc`, `mysql-cdc`, `sql-server-cdc`, `kafka-*` have targeted setups but also run testdrive files.
* **Raw pgwire messages** (COPY, extended protocol): pgtest (`.pt` in `test/pgtest/`).
  Wire new files in `src/environmentd/tests/pgwire.rs`.
* **Pure logic, decoding, pure functions**: Rust `#[mz_ore::test]` (or `#[mz_ore::test(tokio::test)]` for async).
* **Restart/upgrade issues**: Platform Checks.
  See `doc/developer/platform-checks.md`, especially the "Writing a Check" section.
  Do not use the old Legacy Upgrade tests.
* **Panics or unexpected query errors under concurrency**: extend Parallel Workload actions in `misc/python/materialize/parallel_workload/action.py`.
* **Many objects / limits**: add a `Generator` subclass in `test/limits/mzcompose.py`.
* **OOM / bounded memory**: add a `Scenario` in `test/bounded-memory/mzcompose.py`.
* **Performance micro-benchmarks**: Feature Benchmark scenarios in `misc/python/materialize/feature_benchmark/scenarios`.
  See `doc/developer/feature-benchmark.md`.

In most cases, appending to an existing `.slt` or `.td` file is sufficient.
For functional issues, aim for at least two different test frameworks that can independently detect the regression.

Read `doc/developer/guide-testing.md` for more detail on test frameworks.
