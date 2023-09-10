---
title: "Key concepts"
description: "Understand Materialize's architecture."
pagerank: 30
menu:
  main:
    parent: 'get-started'
    weight: 10
aliases:
  - /overview/api-components/
  - /overview/key-concepts/
---

Materialize is a streaming database with a SQL API. However, despite the fact
that Materialize uses SQL idioms and can process data from databases, it
actually has very little in common with "databases" as most people think of them.

In this document, we'll sketch the conceptual framework expressed by Materialize's
Key Concepts, which might help you develop a mental model of how to work with
Materialize and how its components differ from traditional databases.

Materialize offers the following components through its SQL API:

Component                | Use
-------------------------|-----
**[Clusters]**           | Clusters provide isolated environment with dedicated compute resources to run your workloads.
**[Sources]**            | Sources describe an external system you want Materialize to read data from (e.g. Kafka).
**[Views]**              | Views represent queries of sources and other views that you want to save for repeated execution.
**[Indexes]**            | Indexes represent query results stored in memory.
**[Materialized views]** | Materialized views represent query results stored durably.
**[Sinks]**              | Sinks represent output streams or files that Materialize sends data to.

## Clusters

A _cluster_ is an isolated environment with dedicated compute resources (i.e. CPU
and memory) to run your workloads — similar to a virtual warehouse in
Snowflake. You can configure the size, replication factor, and other properties
of a cluster depending on the characteristics of the workload you plan to run
in it.

The following operations require compute resources in Materialize, and so need
to be associated with a cluster:

- Executing ad-hoc [SELECT](/sql/select/) statements.
- Maintaining [indexes](#indexes) and [materialized views](#materialized-views).
- Maintaining [sources](#sources), [sinks](#sinks), and [subscriptions](/sql/subscribe/).

You can gracefully resize clusters at any time (even while running) as your
workloads evolve, to accommodate the need for more or less compute resources.

### Cluster replicas

{{< info >}}
As a user, you do not directly interact with cluster replicas; their properties
are inherited from the properties you configure for the cluster.  Materialize
automatically manages them.
{{</ info >}}

The size and replication factor of a cluster determines the size and number
of _cluster replicas_ that are spawned under the hood to provision the
requested compute resources, respectively.

If a cluster has multiple replicas (i.e., replication factor > 1), all replicas
receive a copy of the data used in all dataflows running in the cluster, and
perform identical computations. This design allows Materialize to
provide **active replication**: in the case of failure, the cluster continues
making progress as long as at least one replica is still reachable.

[//]: # "TODO(morsapaes) Link to the sizing guide, once it's up (#18851)."

Because all dataflows running in a cluster contend for the same resources on
each replica, it's important to choose a workload distribution and cluster
sizing strategy that fits the specific computational needs of your use case.

## Sources

Sources describe external systems you want Materialize to read data from, and provide details about how to decode and interpret that data. A simplistic way to think of this is that sources represent streams and their schemas; this isn't entirely accurate, but provides an illustrative mental model.

In terms of SQL, sources are similar to a combination of tables and
clients.

- Like tables, sources are structured components that users can read from.
- Like clients, sources are responsible for reading data. External
  sources provide all of the underlying data to process.

By looking at what comprises a source, we can develop a sense for how this
combination works.

[//]: # "TODO(morsapaes) Add details about source persistence."

### Source components

Sources consist of the following components:

Component      | Use                                                                                               | Example
---------------|---------------------------------------------------------------------------------------------------|---------
**Connector**  | Provides actual bytes of data to Materialize                                                      | Kafka
**Format**     | Structures of the external source's bytes, i.e. its schema                                        | Avro
**Envelope**   | Expresses how Materialize should handle the incoming data + any additional formatting information | Upsert

#### Connectors

Materialize bundles native connectors for the following external systems:

- [Kafka](/sql/create-source/kafka)
- [Redpanda](/sql/create-source/kafka)
- [PostgreSQL](/sql/create-source/postgres)

For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.

#### Formats

Materialize can decode incoming bytes of data from several formats:

- Avro
- Protobuf
- Regex
- CSV
- Plain text
- Raw bytes
- JSON

#### Envelopes

What Materialize actually does with the data it receives depends on the
"envelope" your data provides:

Envelope | Action
---------|-------
**Append-only** | Inserts all received data; does not support updates or deletes.
**Debezium** | Treats data as wrapped in a "diff envelope" that indicates whether the record is an insertion, deletion, or update. The Debezium envelope is only supported by sources published to Kafka by [Debezium].<br/><br/>For more information, see [`CREATE SOURCE`: Kafka&mdash;Using Debezium](/sql/create-source/kafka/#using-debezium).
**Upsert** | Treats data as having a key and a value. New records with non-null value that have the same key as a preexisting record in the dataflow will replace the preexisting record. New records with null value that have the same key as preexisting record will cause the preexisting record to be deleted. <br/><br/>For more information, see [`CREATE SOURCE`: &mdash;Handling upserts](/sql/create-source/kafka/#handling-upserts)

## Sinks

Sinks are the inverse of sources and represent a connection to an external stream
where Materialize outputs data. When a user defines a sink over a materialized view,
source, or table, Materialize automatically generates the required schema and writes down
the stream of changes to that view or source. In effect, Materialize sinks act as
change data capture (CDC) producers for the given source or view.

Currently, Materialize only supports sending sink data to Kafka.

## Views

In SQL, views represent a query that you save with some given name. These are
used primarily as shorthand for some lengthy, complicated `SELECT` statement.
Materialize uses the idiom of views similarly, but the implication of views is
fundamentally different.

Materialize offers the following types of views:

Type | Use
-----|-----
**Materialized views** | Incrementally updated views whose results are persisted in durable storage
**Non-materialized views** | Queries saved under a name for reference, like traditional SQL views

All views in Materialize are built by reading data from sources and other views.

### Materialized views

Materialized views embed a query like a traditional SQL view, but&mdash;unlike a
SQL view&mdash;compute and incrementally update the results of this embedded
query. The results of a materialized view are persisted in durable storage,
which allows you to effectively decouple the computational resources used for
view maintenance from the resources used for query serving.

Materialize accomplishes incremental updates by creating a set of persistent
transformations&mdash;known as a "dataflow"&mdash;that represent the query's
output. As new data comes in from sources, it's fed into materialized views that
query the source. Materialize then incrementally updates the materialized view's
output by understanding what has changed in the data, based on the source's
envelope. Any changes to a view's output are then propagated to materialized
views that query it, and the process repeats.

When reading from a materialized view, Materialize simply returns the dataflow's
current result set from durable storage. To improve the speed of queries on
materialized views, we recommend creating [indexes] based on
common query patterns.

### Non-materialized views

Non-materialized views simply store a verbatim query and provide a shorthand
for performing the query.

Unlike materialized views, non-materialized views _do not_ store the results of
their embedded queries. The results of a view can be incrementally
maintained in memory within a [cluster][clusters] by
creating an index. This allows you to serve queries without
the overhead of materializing the view.

## Indexes

Indexes assemble and maintain a query's results in memory within a
[cluster][clusters], which provides future queries
the data they need in a format they can immediately use.

These continually updated indexes are known as
_arrangements_ within Materialize's dataflows. In the simplest case, the
arrangement is the last operator and simply stores the query's output in
memory. In more complex cases, arrangements let Materialize perform
sophisticated operations like joins more efficiently.

For a deeper dive into how indexes work, see [Arrangements](/overview/arrangements/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`CREATE VIEW`](/sql/create-view)
- [`CREATE INDEX`](/sql/create-index)
- [`CREATE SINK`](/sql/create-sink)
- [`CREATE CLUSTER`](/sql/create-cluster)

[Clusters]: #clusters
[Cluster replicas]: #cluster-replicas
[Indexes]: #indexes
[Materialized views]: #materialized-views
[Debezium]: http://debezium.io
[Sinks]: #sinks
[Sources]: #sources
[Views]: #views
