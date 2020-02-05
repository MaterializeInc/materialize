# Billing Demo

This is a demonstration of Materialize used to create a billing microservice.
We believe that this is a neat illustration of Materialize's strengths in
real-time normalization of input data, aggregations, and joins.

## Overview

This demo simulates an imaginary cloud provider, with various clients that
consume compute resources with various CPU and memory allocations, and
each client has different prices for each resources. At startup, pricing data is
generated and written to a csv file, and then real-time usage data comes in via Kafka
(the schema for it is in [resources/billing.proto](resources/billing.proto). The
demo installs several sources and materialized views over this input data including:

* normalizations and numerical transformations over the raw inputs (`billing_records` and `billing_prices`)
* aggregations of resource utilization at various time scales (`billing_agg_by_*`)
* final itemized monthly statements (`billing_monthly statement`)

The views are all in [resources/views](resources/views).

## Running with Docker

This should be as simple as possible.

1. Move into the billing demo home directory. From the repository root run:

```shell session
$ cd src/billing-demo
```

2. Run `docker-compose` with:

```shell session
$ docker-compose up
```

## Running locally

1. The billing demo binary needs Materialize and Kafka to be running before it starts.
From the repository root start Materialize with:

```shell session
$ cargo run --release -- --threads 8
```

Note that this runs a release build of Materialize, which is essential for performance
testing but can be slow to compile. Also note that `--threads 8` starts Materialize with
8 worker threads which improves the throughput of update processing but loads the system
more. Generally its best to run with no more than `ncpus - 2` threads.

2. Start Kafka using your preferred invocation. If you don't have Kafka on your system
see the [developer guide](../../doc/developer/develop.md) for detailed instructions
on how to install Kafka.

3. Run the billing demo with:

```shell session
$ RUST_LOG==debug cargo run --release -- --message-count <number of messages to generate> --csv-file-name <absolute path to write price file to>
```

The `RUST_LOG` environment variable determines which log level is printed to stdout.
One small catch in running the demo manually is that the Kafka topic will persist data
across different runs of the demo and all of the data will be loaded into Materialize.
You can clear the Kafka topic by deleting it with:

```shell session
kafka-topics --zookeeper localhost:2181 --delete --topic billing
```

## Querying Materialize

You can query Materialize for current results of any of the computations at any point.
There's absolutely no need to wait for the demo to finish running. Also note that this
process is identical regardless of whether you choose to run the demo locally or with Docker.

First, connect to Materialize using your favorite Postgres CLI. For example with psql its as easy as:

```shell session
$ psql -h localhost -p 6875 -d materialize
```

After connecting you can run any query you can think of. For example to query all itemized bills
for client 1 you could run:

```sql
SELECT * FROM billing_monthly_statement WHERE client_id = 1;
```

Check out our [docs](https://materialize.io/docs/) for more info on what you can do!
