# mzbench

mzbench is a tool that wraps mzcompose in order to repeatedly run benchmarks against one or more
versions of Materialize, with a matrix of variables to test. Currently, only a varying number of
`MZ_WORKERS` is supported.

## Running a Benchmark

Benchmarks are identified by the name of their composition and compositions are expected to
expose workflows with specific names. To run a benchmark, run `mzbench` from the root of your
materialize git checkout:

    ./bin/mzbench <composition>

This script will choose the benchmark most suitable for your machine (`benchmark` for larger
machines, `benchmark-medium` for fast laptops) and will run through a permutation of worker counts
and git revisions. It will print a CSV that looks something like the following to your console:

    GIT_REVISION,MZ_WORKERS,INGEST_RATE,GRAFANA_DASHBOARD
    HEAD,16,522193,http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC

### Adding New Compositions

`mzbench` expects that benchmarks expose 3 different sizes of benchmarks:

- `benchmark-ci` - for tests that are intended to verify correctness, not performance.
- `benchmark-medium` - for tests that are intended for developers to understand the performance
  changes between two versions of the code.
- `benchmark` - for tests that are intended to verify performance in absolute terms. These are the
  tests that we run in our cloud based benchmarks.

To run the above benchmarks, `mzbench` assumes that there are 2 corresponding workflows for each
benchmark:

- `setup-<benchmark>` - this job is run once at mzbench startup to initialize any supporting
  resources, such as a Kafka cluster or other source of data.
- `run-<benchmark>` - this job is run once for iteration of the benchmark. This workflow is
  responsible for clearing any materialized state and making sure that materialized is restarted
  with the new parameters set by the benchmark iteration.
