---
title: "Key concepts"
description: "Understand Materialize's architecture."
menu:
  main:
    parent: 'overview'
    weight: 10
aliases:
  - /overview/api-components/
---

Materialize is a streaming database with a SQL API. However, despite the fact
that Materialize uses SQL idioms and can process data from databases, it
actually has very little in common with "databases" as most people think of them.

In this document, we'll sketch the conceptual framework expressed by Materialize's
Key Concepts, which might help you develop a mental model of how to work with
Materialize and how its components differ from traditional databases.

Materialize offers the following components through its SQL API:

Component | Use
----------|-----
**Sources** | Sources describe an external system you want Materialize to read data from (e.g. Kafka).
**Views** | Views represent queries of sources and other views that you want to save for repeated execution.
**Indexes** | Indexes represent query results stored in memory.
**Sinks** | Sinks represent output streams or files that Materialize sends data to.
**Clusters** | Clusters represent a logical set of indexes that share physical resources.
**Cluster replicas** | Cluster replicas provide physical resources for a cluster's indexes.

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
materialized views, we recommend creating [indexes](#indexes) based on
common query patterns.

### Non-materialized views

Non-materialized views simply store a verbatim query and provide a shorthand
for performing the query.

Unlike materialized views, non-materialized views _do not_ store the results of
their embedded queries. The results of a view can be incrementally
maintained in memory within a [cluster](/overview/key-concepts/#clusters) by
creating an index. This allows you to serve queries without
the overhead of materializing the view.

## Indexes

Indexes assemble and maintain a query's results in memory within a
[cluster](/overview/key-concepts#clusters), which provides future queries
the data they need in a format they can immediately use.

These continually updated indexes are known as
_arrangements_ within Materialize's dataflows. In the simplest case, the
arrangement is the last operator and simply stores the query's output in
memory. In more complex cases, arrangements let Materialize perform
sophisticated operations like joins more efficiently.

For a deeper dive into how indexes work, see [Arrangements](/overview/arrangements/).

## Clusters

Clusters are logical components that let you express resource isolation for all
dataflow-powered objects, e.g. indexes. When creating dataflow-powered
objects, you must specify which cluster you want to use. (Not explicitly naming
a cluster uses your session's default cluster.)

Importantly, clusters are strictly a logical component; they rely on [cluster
replicas](#cluster-replicas) to run dataflows. Said a slightly different way, a
cluster with no replicas does no computation. For example, if you create an
index on a cluster with no replicas, you cannot select from that index because
there is no physical representation of the index to read from.

Though clusters only represent the logic of which objects you want to bundle
together, this impacts the performance characteristics once you provision
cluster replicas. Each object in a cluster gets instantiated on every replica,
meaning that on a given physical replica, objects in the cluster are in
contention for the same physical resources. To achieve the performance you need,
this might require setting up more than one cluster.

### Cluster deployment options

When building your Materialize deployment, you can change its performance
characteristics by...

Action | Outcome
-------|---------
Adding clusters + decreasing dataflow density | Reduced resource contention among dataflows, decoupled dataflow availability
Adding replicas to clusters | See [Cluster replica scaling](#cluster-replica-deployment-options)

## Cluster replicas

Where clusters represent the logical set of dataflows you want to maintain,
cluster replicas are their physical counterparts. Cluster replicas are where
Materialize actually creates and maintains dataflows.

Each cluster replica is essentially a clone, constructing the same dataflows.
Each cluster replica receives a copy of all data that comes in from sources its
dataflows use, and uses the data to perform identical computations. This design
provides Materialize with active replication, and so long as one replica is
still reachable, the cluster continues making progress.

This also means that all of a cluster's dataflows contend for the same resources
on each replica. This might mean, for instance, that instead of placing many
complex materialized views on the same cluster, you choose some other
distribution, or you replace all replicas in a cluster with more powerful
machines.

### Cluster replica deployment options

Materialize is an active-replication-based system, which means you expect each
cluster replica to have the same working set.

With this in mind, when building your Materialize deployment, you can change its
performance characteristics by...

Action | Outcome
---------|---------
Increase replicas' size | Ability to maintain more dataflows or more complex dataflows
Add replicas to a cluster | Greater tolerance to replica failure

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`CREATE VIEW`](/sql/create-view)
- [`CREATE INDEX`](/sql/create-index)
- [`CREATE SINK`](/sql/create-sink)
- [`CREATE CLUSTER`](/sql/create-cluster)
- [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica)

[Debezium]: http://debezium.io
