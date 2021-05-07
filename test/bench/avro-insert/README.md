# Avro Insert Benchmark

This benchmark tests our ability to ingest Avro messages without any envelope type. In it's
current configuration, it assumes that Kafka topics should have 30 partitions and that you want 16
worker threads.

This benchmark is largely identical to the `avro-upsert` benchmark and was originally developed to
compare the relative performance of the `upsert` operator.

## Running the Avro Insert Test

To run the spec benchmark, please ensure that your machine has 16 cores and at least 96GB of
memory. Then run the following command:

    ./mzcompose run benchmark

If you're running on a smaller machine, such as a laptop with 8 cores / 32GB of memory, you can
run the version of this benchmark designed for smaller machines:

    ./mzcompose run benchmark-medium

You can also use [mzbench](../../../doc/developer/mzbench.md) to run comparative benchmarks on
different versions. See the `mzbench` documentation for more details but here is an example
invocation (run from the repository root):

    ./bin/mzbench -w --size medium avro-insert

## Looking at Performance in Grafana

Each benchmark will output something like the following:

```sh
SUCCESS! seconds_taken=766, rows_per_sec=522193
Grafana URL: http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC
```

The Grafana link will display performance metrics for materialize during the entirety of the
benchmark run.
