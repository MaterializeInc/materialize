# Kafka Avro Debezium Sink Benchmark

This benchmark tests our ability to write messages out to Kafka. In its
current configuration, it uses a topic with only one partition as input and a
topic with only one partition as output. We use only one input topic because
that is enough to saturate writing. We use only one output topic because the
current Kafka sink will only ever write from one timely worker.

## Running the Kafka Avro Debezium Sink Test

To run the spec benchmark run the following command:

    ./mzcompose run benchmark

If you're running on a smaller machine, you can run the version of this
benchmark designed for smaller machines:

    ./mzcompose run benchmark-medium

You can also use `mzbench` to run comparative benchmarks on different versions.
See the `mzbench` documentation for more details but here is an example
invocation:

    ./bin/mzbench -w --size medium kafka-sink-avro-debezium

## Looking at Performance

Each benchmark will output something like the following:

```sh
SUCCESS! seconds_taken=766, rows_per_sec=522193
Grafana URL: http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC
```

The Grafana link will display performance metrics for materialize during the
entirety of the benchmark run.
