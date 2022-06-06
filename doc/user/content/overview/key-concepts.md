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
**Sources** | Sources represent streams (e.g. Kafka) or files that provide data to Materialize.
**Views** | Views represent queries of sources and other views that you want to save for repeated execution.
**Indexes** | Indexes represent query results stored in memory.
**Sinks** | Sinks represent output streams or files that Materialize sends data to.
**Clusters** | Clusters represent a logical set of indexes and sinks that share physical resources.
**Cluster replicas** | Cluster replicas provide physical resources for a cluster's indexes and sinks.

## Sources

Sources represent a connection to the data external to Materialize you want to process, as
well as details about the structure of that data. A simplistic way to think of
this is that sources represent streams and their schemas; this isn't entirely
accurate, but provides an illustrative mental model.

In terms of SQL, sources are similar to a combination of tables and
clients.

- Like tables, sources are structured components that users can read from.
- Like clients, sources are responsible for reading data. External
  sources provide all of the underlying data to process.

By looking at what comprises a source, we can develop a sense for how this
combination works.

### Source components

Sources consist of the following components:

Component | Use | Example
----------|-----|---------
**Connector** | Provides actual bytes of data to Materialize | Kafka
**Format** | Structures of the external source's bytes, i.e. its schema | Avro
**Envelope** | Expresses how Materialize should handle the incoming data + any additional formatting information | Append-only

#### Connectors

Materialize can connect to the following types of sources:

- Streaming sources like Kafka
- File sources like `.csv` or unstructured log files

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

### Materialized sources

You can materialize a source, which keeps data it receives in an in-memory
[index](#indexes) and makes the
source directly queryable. In contrast, non-materialized sources cannot process
queries directly; to access the data the source receives, you need to create
[materialized views](#materialized-views) that `SELECT` from the
source.

For a mental model, materializing the source is approximately equivalent to
creating a non-materialized source, and then creating a materialized view from
all of the source's columns. The actual implementation of materialized sources differs, though, by letting
you refer to the source's name directly in queries.

Because keeping the entire source in memory can be prohibitive for large sources, we recommend that you exercise caution before doing so. For more details about the impact of materializing sources (and implicitly
creating an index), see [`CREATE INDEX`: Details &mdash; Memory
footprint](/sql/create-index/#memory-footprint).

## Views

In SQL, views represent a query that you save with some given name. These are
used primarily as shorthand for some lengthy, complicated `SELECT` statement.
Materialize uses the idiom of views similarly, but the implication of views is
fundamentally different.

Materialize offers the following types of views:

Type | Use
-----|-----
**Materialized views** | Incrementally updated views whose results are maintained in memory
**Non-materialized views** | Queries saved under a name for reference, like traditional SQL views

All views in Materialize are built by reading data from sources and other views.

### Materialized views

Materialized views embed a query like a traditional SQL view, but&mdash;unlike a
SQL view&mdash;compute and incrementally update the results of the embedded
query. This lets users read from materialized views and receive fresh answers
with incredibly low latencies.

Materialize accomplishes incremental updates by creating a set of persistent
transformations&mdash;known as a "dataflow"&mdash;that represent the query's
output. As new data comes in from sources, it's fed into materialized views that
query the source. Materialize then incrementally updates the materialized view's
output by understanding what has changed in the data, based on the source's
envelope. Any changes to a view's output is then propagated to materialized
views that query it, and the process repeats.

When reading from a materialized view, Materialize simply returns the dataflow's
current result set.

You can find more information about how materialized views work in the
[Indexes](#indexes) section below.

### Non-materialized views

Non-materialized views simply store a verbatim query and provide a shorthand
for performing the query.

Unlike materialized views, non-materialized views _do not_ store the results of
their embedded queries. This means they take up very little memory, but also
provide very little benefit in terms of reducing the latency and computation
needed to answer queries.

If you plan on repeatedly reading from a view, we recommend using a materialized
view.

## Indexes

An index is the component that actually "materializes" a view by storing its
results in memory, though, more generally, indexes can simply store any subset of
a query's data.

Each materialized view contains at least one index, which both lets it maintain
the result set as new data streams in and provides low-latency reads.

If you add an index to a non-materialized view, it becomes a
materialized view, and starts incrementally updating the results of its embedded query.

### Interaction with materialized views

As mentioned before, each materialized view has at least one index that
maintains the embedded query's result in memory; the continually updated indexes are known as
"arrangements" within Materialize's dataflows.  In the simplest case, the arrangement is the
last operator and simply stores the query's output in memory. In more complex
cases, arrangements let Materialize perform more sophisticated aggregations like `JOIN`s more
quickly.

Creating additional indexes on materialized views lets you store some subset of a query's data in memory using a different structure, which can be useful if you want to perform a join over a view's data using non-primary keys (e.g. foreign keys).

For a deeper dive into arrangments, see [Arrangments](/overview/arrangements/).

## Sinks

Sinks are the inverse of sources and represent a connection to an external stream
where Materialize outputs data. When a user defines a sink over a materialized view
or source, Materialize automatically generates the required schema and writes down
the stream of changes to that view or source. In effect, Materialize sinks act as
change data capture (CDC) producers for the given source or view.

Currently, Materialize only supports sending sink data to Kafka,
encoded in Avro with the [Debezium diff envelope](/sql/create-sink#debezium-envelope-details).

## Clusters

Clusters are logical components that let you express resource isolation for all
dataflow-powered objects, e.g. indexes and sinks. When creating dataflow-powered
objects, you must specify which cluster you want to use. (Not explicitly naming
a cluster uses your session's default cluster.)

Importantly, clusters are strictly a logical component; they rely on  [cluster
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
