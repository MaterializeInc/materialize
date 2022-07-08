# Cloudbench

## Introduction

The Cloudbench tool, available at `bin/cloudbench` in the repository, launches one-off benchmarks in the AWS Scratch account.

Very little structure is imposed on the benchmarks scripts:
* they must write a CSV file (with header) containing the results to a file named `results.csv`
* they must (for now) be written in Python.
* they must follow the Unix convention of exiting with code 0 on success, and non-zero otherwise.

Each row of the produced CSV file is interpreted as one benchmark run. For an example, see
the [avro_ingest.py](https://github.com/MaterializeInc/materialize/blob/main/misc/python/materialize/benches/avro_ingest.py) script.

## Prerequisites

Follow the instructions in the [bin/scratch documentation](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/scratch.md) to
set up your machine for command-line AWS access to the Scratch account, and to learn about the basic commands for launching and querying
individual machines.

## Invocation

Launching benchmarks is done with the `bin/cloudbench start` subcommand. It takes the following options:

`--profile`: One of either `basic` or `confluent`. `basic` should be used if only the machine running the benchmark is required; `confluent` adds a separate machine running Kafka, Schema Registry, and Zookeeper.

`--trials`: How many separate clusters to run the benchmark on per Git revision

`--revs`: A comma-separated list of Git revisions to benchmark.

For example, the following command:
```
bin/cloudbench start --profile confluent --trials 3 --revs HEAD,'HEAD^' materialize.benches.avro_ingest -r 100000000 -n 10
```

launches 12 machines: two git revisions, three clusters per revision, and two machines per cluster. It then runs the module `misc.benches.avro_ingest`, defined in the file `misc/python/materialize/benches/avro_ingest.py`, with the arguments `-r 100000000 -n 10`
on 6 of those machines (one per cluster; the other machine is for the Confluent platform).

## Checking results

Because benchmarks may in general be long-running, `bin/cloudbench start` does not need to stay running for
the entire time it takes them to complete. After setting up the machines and launching the benchmark scripts, that tool
exists, printing out a "benchmark ID". That benchmark ID may in turn be passed to the `bin/benchmark check` utility, like so:

```
bin/cloudbench check f4765a9a
```

This command checks every 60 seconds whether a benchmark has completed. Once it does complete, assuming all runs were successful,
it gets their results, collates them into one CSV file, and prints it to `stdout`.

## Cleaning up

Benchmark machines are automatically cleaned up after one day. If you need to run something for longer, ask how to disable the automated cleanup in `#eng-infra` on Slack.

If you want to clean up machines sooner, you may use the `bin/scratch destroy` tool.
