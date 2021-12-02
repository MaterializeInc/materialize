# `mzbench-opt`

The `mzbench-opt` package contains optimizer benchmarking scripts.

## Installation (`pyenv`)

We need a Python version higher than `3.9.x`

```bash
PYTHON_VERSION=3.9.5
```

Run the following commands only once on each host:

```bash
pyenv virtualenv ${PYTHON_VERSION} mzbench-opt
```

Run the following commands whenever the contents of this folder change:

```bash
# activate mzbench-opt environment
pyenv virtualenv-delete mzbench-opt
pyenv virtualenv ${PYTHON_VERSION} mzbench-opt

# install package
pyenv activate mzbench-opt
pip install -e .

# install zsh shell completion
mzbench-opt --install-completion zsh
rm -f ~/.zcompdump; compinit
```

## Run Benchmarks

To run a benchmark you need to:

1. Intialize the database schema for your test scenario.
1. Run the workload of your test scenario.

These two steps are modeled by the `init` and `run` commands.
For example, for the `tpch` scenario, type

```bash
mzbench-opt init tpch
mzbench-opt run tpch --repository="$HOME/mzbench-opt"
```

The results of benchmark `run` are stored in the OS temp folder by default, but here we set the location to `$HOME/mzbench-opt` using the `--repository` option.
Result files have the form `mzbench-opt-${scenario}-${mz_version}.csv`, where

- `${scenario}` is the value of the `SCENARIO` argument passed to the `run` command, and
- `${mz_version}` is a suffix derived from the `SELECT mz_version()` result of the database instance under test.

For a full list of the available options, type the following:

```bash
mzbench-opt init --help
mzbench-opt run --help
```

## Comparing Two Benchmark Runs

To see the relative performance of a `diff` benchmark run against a `base` run, use the `compare` command.

For example, to compare the results of runs against database instances build from commits `07c937bee` and `3ab9c59b6` as described above, we will use the following command.

```bash
mzbench-opt compare \
  $HOME/mzbench-opt/mzbench-opt-tpch-v0.10.1-dev-07c937bee.csv \
  $HOME/mzbench-opt/mzbench-opt-tpch-v0.10.1-dev-3ab9c59b6.csv
```

As before, for a full list of the available options, type the following:

```bash
mzbench-opt compare --help
```

## Benchmark Scenarios

In the example above we used `tpch` as a benchmark scenario.

To add a new scenario `${scenario}`:

1. Add a file called `${scenario}.sql` in `mzbench_opt/schema`. Use `tpch.sql` as a reference.
1. Add a file called `${scenario}.sql` in `mzbench_opt/workload`. Use `tpch.sql` as a reference.

Queries in the workload file must be annotated with a comment of the form `-- name: ${name}`.
Prefer short names such as `Q01`, `Q02`, etc. in order to keep the width of the generated reports as narrow as possible.
