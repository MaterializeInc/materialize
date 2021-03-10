# Avro Upsert Benchmark

This benchmark tests our ability to ingest Avro Upsert messages. In it's current configuration, it
assumes that Kafka topics should have 30 partitions and that you want 16 worker threads.

## Running the Avro Upsert Test

To run the spec benchmark, please ensure that your machine has 16 cores and at least 96GB of
memory. Then run the following command:

    ./mzcompose run benchmark

There is also a version of this benchmark that uses the same load generator, but a different set
of views that avoid exercising the upsert operator. This gives a good comparative benchmark to see
how much slower the upsert operator is:

    ./mzcompose run benchmark-insert-filter

If you're running on a smaller machine, such as a laptop with 8 cores / 32GB of memory, you can
run the version of this benchmark designed for smaller machines:

    ./mzcompose run benchmark-medium

## Looking at Performance

Each benchmark will output something like the following:

```sh
SUCCESS! seconds_taken=766, rows_per_sec=522193
Grafana URL: http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC
```

The Grafana link will display performance metrics for materialize during the entirety of the
benchmark run.
