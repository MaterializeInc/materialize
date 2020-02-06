---
title: "Get started"
description: "Get started with Materialize"
menu: "main"
weight: 2
---

To get started with Materialize, we'll cover:

- Conceptual framework, which explains how you should typically expect to
  interact with Materialize
- Hands-on experience, which will give you a chance to get some limited
  experience with a simple, local Materialize deployment

## Conceptual framework

Before trying to deploy Materialize, you should check out:

- [What is Materialize?](../overview/what-is-materialize)
- Our [architecture overview](../overview/architecture)

Below is a high-level overview of how Materialize works. This is simply an
overview and doesn't contain any actual instruction.

### Preparing Kafka

1. Materialize relies on getting data from your database through Kafka, so
   you'll need to have access to the typical suite of Kafka tools, including
   Kafka itself, Kafka Connect, and ZooKeeper.

1. To get data from your database into Kafka, you'll need to use a change data
   capture (CDC) tool. The only CDC tool that provides the requisite structure
   to Materialize is Debezium, so you'll need to configure that to publish a
   changefeed to your Kafka cluster.

1. Materialize needs to understand your Kafka topics' schemas, so you'll need to
   have your topics reports their schema to a Confluent Schema Registry, or know
   the topics' Avro schemas.

   Note that if you are using Debezium (and you should be), all of this happens
   automatically.

### Using Materialize

1. Start a Materialize server (through the `materialized` process) on a server,
   and connect to it through `mzcli`.

1. Using your `mzcli` connection, create a source for each Kafka topic/table
   from your database that you want to use.

   When creating sources, you also need to provide the topic's schema to
   Materialize by pointing Materialize to your Confluent Schema Registry or
   providing the Avro schema.

1. Ensure that your data is working as you expect by issuing some `SELECT`
   statements through Materialize.

1. Once you have queries that you want to materialize, create views of them.

   At this point, Materialize begins maintaining these views as new data
   streams in from Kafka.

1. Configure your application to connect to Materialize and read the results of
   your views, e.g. `SELECT * FROM some_view;`.

## Hands-on experience

To see what this feels like in action, we have a simple demo in
`materialize/demo/simple-demo`.

For something slightly more involved, check out `materialize/demo/chbench`.

## Related pages

- [What is Materialize?](../overview/what-is-materialize)
- [Architecture documentation](../overview/architecture)
- [`CREATE SOURCE`](../sql/create-source)
- [`CREATE VIEW`](../sql/create-view)
