# Optimizer benchmarks

This package contains optimizer benchmarking scripts.

## Run Benchmarks

To run a benchmark you need to:

1. Intialize the database schema for your test scenario.
1. Run the workload of your test scenario.

These two steps are modeled by the `init` and `run` commands.
For example, for the `tpch` scenario, type

```bash
# For a locally running Materialize, use the defaults

# For Materialize Cloud (staging), use:
PGHOST=$mz_orgid.$aws_region.aws.staging.materialize.cloud
PGUSER=$mz_user
PGPASSWORD=$mz_password
PGPORT=6875
PGREQUIRESSL=true

# For a locally running Postgres, use:
PGHOST=localhost
PGUSER=$USER
PGPORT=5432

bin/optbench init tpch
bin/optbench run tpch --repository="$HOME/optbench"
```

The results of benchmark `run` are stored in the OS temp folder by default, but here we set the location to `$HOME/optbench` using the `--repository` option.
Result files have the form `optbench-${scenario}-${mz_version}.csv`, where

- `${scenario}` is the value of the `SCENARIO` argument passed to the `run` command, and
- `${mz_version}` is a suffix derived from the `SELECT mz_version()` result of the database instance under test.

For a full list of the available options, type the following:

```bash
bin/optbench init --help
bin/optbench run --help
```

## Comparing Two Benchmark Runs

To see the relative performance of a `diff` benchmark run against a `base` run, use the `compare` command.

For example, to compare the results of runs against database instances build from commits `07c937bee` and `3ab9c59b6` as described above, we will use the following command.

```bash
bin/optbench compare \
  $HOME/optbench/optbench-tpch-v0.10.1-dev-07c937bee.csv \
  $HOME/optbench/optbench-tpch-v0.10.1-dev-3ab9c59b6.csv
```

As before, for a full list of the available options, type the following:

```bash
bin/optbench compare --help
```

## Benchmark Scenarios

In the example above we used `tpch` as a benchmark scenario.

To add a new scenario `${scenario}`:

1. Add a file called `${scenario}.sql` in `mzbench_opt/schema`. Use `tpch.sql` as a reference.
1. Add a file called `${scenario}.sql` in `mzbench_opt/workload`. Use `tpch.sql` as a reference.

Queries in the workload file must be annotated with a comment of the form `-- name: ${name}`.
Prefer short names such as `Q01`, `Q02`, etc. in order to keep the width of the generated reports as narrow as possible.
