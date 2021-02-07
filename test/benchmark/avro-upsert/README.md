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
SUCCESS! Benchmark finished in settled 766 seconds, 522193.21148825064 rows/sec
View performance metrics here: http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC
```

The Grafana link will display performance metrics for materialize during the entirety of the
benchmark run.

## Running Many Benchmarks

There is a script, `./matrix_bench`, that will repeatedly run the benchmark most suitable
for your machine (either `benchmark` or `benchmark-medium`), starting from 1 worker thread, to 16
worker threads. You can also supply multiple git revisions if you want to compare builds against
each other. This will take quite some time to run, but at the end, it will produce a CSV that
looks like the following:

    GIT_REVISION,MZ_WORKERS,INGEST_RATE,GRAFANA_DASHBOARD
    HEAD,16,522193,http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC

If you prefer to run with a different configuration, such as number of workers, you can provide
the `--worker-values` argument:

    ./matrix_bench --worker-values=16,8,4,2,1 --git-revisions=HEAD,origin/main
