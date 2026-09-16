---
title: "Build your own sink"
description: "Stream a Materialize query into an external system using the Materialize sink SDK."
disable_list: true
robots: "noindex, nofollow"
build:
  render: always
  list: never
---

{{< private-preview enabled-by-default="true" />}}

When no native sink covers your destination, the [Materialize sink
SDK](https://github.com/MaterializeIncLabs/mz-sink-sdk) lets you build one. You
give the SDK a `SELECT` statement and a destination, and the destination tracks
that query's output exactly once: no duplicates after a crash, no partial
writes.

The SDK is a Python library. It runs [`SUBSCRIBE ... WITH
(PROGRESS)`](/sql/subscribe/) on your behalf, and writes each batch of changes
together with its checkpoint in a single destination transaction. A restarted
process resumes from the last committed checkpoint.

## Guides

- [PostgreSQL](/export-data/build-your-own-sink/postgres/)

## Other destinations

The SDK also ships sinks for Kafka, Apache Iceberg, and Redis, and a
`TransactionalSink` interface you can implement for any system that can make
data and a checkpoint durable together. For details, see the [SDK
README](https://github.com/MaterializeIncLabs/mz-sink-sdk).

For destinations that Materialize supports natively, prefer the native sink or
`COPY TO`. See [Sink results](/export-data/).
