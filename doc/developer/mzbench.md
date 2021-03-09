# mzbench

mzbench is a tool that wraps mzcompose in order to repeatedly run benchmarks against one or more
versions of Materialize, with a matrix of variables to test. Currently, mzbench explicitly varies
the value for `MZ_WORKERS` and allows for choosing which materialize versions to test. Users can
also select the number of times to test each version, as a means of improving confidence in the
results.

## Running a Benchmark

Benchmarks are identified by the name of their composition and compositions are expected to
expose workflows with specific names. To run a benchmark, run `mzbench` from the root of your
materialize git checkout:

    ./bin/mzbench <composition>

This script defaults to choosing the benchmark most suitable for developer machines
(`benchmark-medium` for fast laptops) and will run through a permutation of benchmark variables.
It will print a CSV that looks something like the following to your console:

    git_revision,num_workers,iteration,seconds_taken,rows_per_second,grafana_url
    NONE,1,0,3,333333,http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC

### Comparing Results Between Materialize Versions

Benchmark results can be considered either absolute (X operations per second) or relative
(10% faster than the previous release). Our current crop of benchmarks are written to test
relative performance. To help engineers better understand materialized performance, mzbench
supports running the benchmark over multiple versions of materialized:

    ./bin/mzbench <composition> <git_ref|release_tag> ...

For example, here's how to compare the performance of materialized, built from your local working
directory, against `v0.6.1` and `v0.7.0`:

    ./bin/mzbench <composition> v0.7.0 v0.6.1

If you don't want to benchmark the version in your local working directory, such as when
benchmarking in the cloud, you can supply the `--no-benchmark-this-checkout` flag:

    ./bin/mzbench --no-benchmark-this-checkout <composition> origin/main v0.7.0 v0.6.1

### Controlling the Number of Iterations

Our benchmarks are not sufficiently advanced to provide an accurate picture after only a single
run. By default, mzbench will run each measurement 6 times. This can be controlled via the
`--num-measurements` flag:

    ./bin/mzbench --num-measurements 3 <composition>

## Visualizing Results

If the benchmark is configured to start `perf-dash` services during the setup step, then you can
visualize results in your browser. These results are update in real-time, as the results are
written to the persistent datastore for perf-dash. To view the benchmark results in your browser,
you can run `mzbench` with the `--web` flag:

    ./bin/mzbench --web ...

This will open a new browser window, pointed at the web interface for perf-dash, once the services
have started up.

## Adding New Benchmarks (Compositions)

`mzbench` expects that benchmarks expose 3 different sizes of benchmarks:

- `benchmark-ci` - for tests that are intended to verify correctness, not performance.
- `benchmark-medium` - for tests that are intended for developers to understand the performance
  changes between two versions of the code.
- `benchmark-large` - for tests that are intended to verify performance in absolute terms. These
  are the tests that we run in our cloud based benchmarks.

Users of `mzbench` can choose which variant to run by using the `--size` flag.

To run a given benchmark, `mzbench` assumes that there are 2 corresponding workflows for each
benchmark:

- `setup-<benchmark>` - this job is run once at mzbench startup to initialize any supporting
  resources, such as a Kafka cluster or other source of data.
- `run-<benchmark>` - this job is run once for iteration of the benchmark. This workflow is
  responsible for clearing any materialized state and making sure that materialized is restarted
  with the new parameters set by the benchmark iteration.

This means that, to write a new benchmark that supports running in CI, on laptops and in the
cloud, 6 workflows must exist:

- `setup-benchmark-ci`
- `run-benchmark-ci`
- `setup-benchmark-medium`
- `run-benchmark-medium`
- `setup-benchmark-large`
- `run-benchmark-large`

## Worker Balance Visualization

If run with the `--web` flag, `mzbench` will open a browser to the results visualizations. In
addition to the results visualization, there is a demonstration visualization calling "Elapsed
Time Per Worker" (you may need to scroll down to see it). This visualization is a "technology
demostration" for replicating system tables to another materialized instance to power a real-time
visualization.

The code to make this all work lives under `misc/perf-dash` and consists of 3 major components:

1. `perf-dash-scraper`. A utility that periodically queries system tables, encodes them as Avro and
   writes them to a Kafka topic.
2. `perf-dash-metrics`: A materialized instanced configured to read metrics from Kafka and
   maintain a materialized view called `time_per_worker`.
3. `perf-dash-web`: A web server configured serve the visualization, pushing updates to the
   browser directly from tail.

Similar metrics can be inferred by using Prometheus / Grafana, as many of the system table
aggregations are also exported as prometheus metrics. So why go through the work of scraping
system tables, writing avro schemas, writing materialized views and custom visualizations? There
are a few motivations:

1. Demonstrate materialize feature parity with an existing metrics / reporting stack.
2. Demonstrate the value in replicating system tables as SQL to another materialized instance for
   telemetry purposes.
3. Demonstrate how to write an event-sourced application using materialized / using materialized
   as a denormalizatin engine.
4. Demonstrate the value of real-time visualizations over 5-second refresh, even for internal
   engineering projects.
5. Provide real data that can be used as a test-bed for features, particularly around temporal
   features (aggregations, sliding / tumbling windows, etc).
6. Lossless metrics collection. Instead of relying on Prometheus scrape intervals, metrics are
   persistently recorded in Kafka. No more gaps in our metrics.
