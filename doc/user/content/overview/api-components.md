---
title: "API Components"
description: "Understand Materialize's architecture."
menu:
  main:
    parent: 'overview'
    weight: 3
---

Materialize is a streaming database with a SQL API. However, despite the fact
that Materialize uses SQL idioms and can process data from databases, it
actually has very little in common with "databases" as most people think of them.

In this document, we'll sketch the conceptual framework expressed by Materialize's
API components, which might help you develop a mental model of how to work with
Materialize and how its components differ from traditional databases.

Materialize offers the following components through its SQL API:

Component | Use
----------|-----
**Sources** | Sources represent streams (e.g. Kafka) or files that provide data to Materialize.
**Views** | Views represent queries of sources and other views that you want to save for repeated execution.
**Indexes** | Indexes represent query results stored in memory.
**Sinks** | Sinks represent output streams or files that Materialize sends data to.

## Sources

Sources represent a connection to the data you want Materialize to process, as
well as details about the structure of that data. A simplistic way to think of
this is that sources represent streams and their schemas; this isn't entirely
accurate, but provides an illustrative mental model.

In terms of SQL, sources are similar to a combination of tables and
clients.

- Like tables, sources are structured components that users can read from.
- Like clients, sources are responsible for writing data. External
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

Currently, Materialize only supports sending sink data to Kafka or Avro OCFs,
encoded in Avro with the [Debezium diff envelope](/sql/create-sink#debezium-envelope-details).

## Clusters

{{< experimental >}}
The concept of a cluster
{{< /experimental >}}

A cluster is a set of compute resources that have been allocated to maintain
indexes and sinks. The `materialized` process provides one virtual cluster named
`default` that represents the compute resources of the local machine. In
a forthcoming version of [Materialize Cloud](/cloud), you will be able to
dynamically create and drop clusters to allocate and deallocate compute
resources on demand.

Clusters are still under active development and are not yet meant for general
use. They are described here because some non-experimental features (e.g.,
[`SHOW INDEX`](/sql/show-indexes)) already make reference to clusters.

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`CREATE VIEW`](/sql/create-view)
- [`CREATE INDEX`](/sql/create-index)
- [`CREATE SINK`](/sql/create-sink)

[Debezium]: http://debezium.io
