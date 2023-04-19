---
title: "Key concepts"
description: "Understand Materialize's components"
pagerank: 30
menu:
  main:
    parent: 'overview'
    weight: 10
aliases:
  - /overview/api-components/
---

Most physical and cloud-hosted database solutions require you to configure
resources and networking components to manage your data. Materialize takes care
of resource allocation for you and segments your data configuration into
separate components:

* Infrastructure
* Data connection
* Data computation

## Infrastructure components

Component                | Use 
-------------------------|-----
**Clusters**           | Isolated logical workspaces that host sources, sinks, indexes, and materialized views.
**Cluster replicas**   | Physical compute resources within a cluster.

### Clusters

Materialize isolates user workspaces into clusters. Each cluster allocates
compute resources to process your data. When you create a dataflow object like a
source or an index, you must specify a cluster or your resource will use the
`default` cluster.

Clusters rely on cluster replicas to run dataflows. Without a replica, clusters
cannot perform data operations. Clusters are similar to a VPC while cluster replicas are similar to compute
instances within a VPC. The cluster is the architectural foundation that
provides operational resources to objects and the
cluster replicas process data available in the cluster.

#### Cluster deployment options

You can control cluster performance in Materialize by scaling the amount of
clusters and distributing data amongst your clusters. This reduces the amount of
resources necessary in each cluster and gives you more availability within each
cluster.

You can also [add cluster replicas](#cluster-replica-deployment-options) within a cluster. The next section describes
cluster replicas in more detail.

### Cluster replicas

Cluster replicas are compute instances within clusters that create and maintain
your Materialize dataflows.

Each replica within a cluster is built with the same resource requirements you
set in your `CREATE CLUSTER` or `CREATE CLUSTER REPLICA` statement. Replicas
within a cluster receive a copy of all data from your sources and each perform
identical computations on the data. As long as one replica is available, the cluster continues operations and gives
you active replication in Materialize. 

All dataflows of a cluster share the same resources on each replica. Depending
on your dataflow needs, you may consider distributing replicas across multiple
clusters or provisioning more replicas within your clusters.

#### Cluster replica deployment options

For greater data throughput or for managing more complex dataflows consider
deploying larger sized replicas in your cluster.

You can also add more replicas to your clusters to increase fault tolerance if a
replica becomes unavailable.

## Data connection components

Component                | Use
-------------------------|-----
**Sources**            | An external system you want Materialize to read data from (e.g. Kafka).
**Sinks**              | Output streams or files that Materialize sends data to.

### Sources and sinks

Sources are external systems with data you want Materialize to compute.
Materialize uses these sources as data streams and schemas to interpret your data. 

Sources share some similarities with SQL tables and clients:

* Like tables, sources are structures of data components that you can query.
* Like clients, sources provide and read the underlying data.

Sinks are streams of outgoing data from Materialize to a receiving system. 

Sources and sinks consist of the following components:

Component      | Use                                                                                               | Example
---------------|---------------------------------------------------------------------------------------------------|---------
**Connector**  | Provides actual bytes of data to Materialize                                                      | Kafka
**Format**     | Structures of the external source's bytes, i.e. its schema                                        | Avro
**Envelope**   | Expresses how Materialize should handle the incoming data + any additional formatting information | Upsert

#### Connectors

Materialize includes embedded connectors for the following external systems:

- [Kafka](/sql/create-source/kafka)
- [Redpanda](/sql/create-source/kafka)
- [PostgreSQL](/sql/create-source/postgres)

For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.

#### Formats

Materialize decodes incoming bytes of data from several formats:

- Avro
- Protobuf
- Regex
- CSV
- Plain text
- Raw bytes
- JSON

#### Envelopes

An envelope is an attribute in a source statement that determines how
Materialize interacts with your data.

Envelope | Action
---------|-------
**Append-only** | Inserts all received data; does not support updates or deletes.
**Debezium** | Treats data as wrapped in a "diff envelope" that indicates whether the record is an insertion, deletion, or update. The Debezium envelope is only supported by sources published to Kafka by [Debezium].<br/><br/>For more information, see [`CREATE SOURCE`: Kafka&mdash;Using Debezium](/sql/create-source/kafka/#using-debezium).
**Upsert** | Treats data as having a key and a value. New records with non-null value that have the same key as a preexisting record in the dataflow will replace the preexisting record. New records with null value that have the same key as preexisting record will cause the preexisting record to be deleted. <br/><br/>For more information, see [`CREATE SOURCE`: &mdash;Handling upserts](/sql/create-source/kafka/#handling-upserts)

## Data compute components

Component                | Use
-------------------------|-----
**Views**              | Queries of sources and other views that you want to save for repeated execution.
**Indexes**            | Query results stored in memory.
**Materialized views** | Query results stored durably.

### Views

In SQL, views are queries you save as shortcuts for complex `SELECT` statements.
Materialize uses views in two ways:

Type | Use
-----|-----
**Materialized views** | Incrementally updated views with results saved durable storage
**Non-materialized views** | Queries saved under a name for reference, like traditional SQL views

Materialize builds all views by reading data from sources and other views.

#### Materialized views

Materialized views embed a query and then compute and incrementally update the
embedded query results. The materialized view results persist in durable storage
which reduces compute resources needed for the query.

Materialize uses "dataflows" for incremental updates. Dataflows are sets of
persistent transformations that represent the query's output. As Materialize
ingests new data from sources, the dataflow engine incrementally updates the
materialized view's output by computing the diff between the current state and
the received changes. Changes to the view's output stream to the materialized
views that query it.

Materialize returns the dataflow's current result set from storage when it
receives a read operation on a materialized view. To improve the speed of queries on
materialized views, we recommend creating indexes based on
common query patterns.

#### Non-materialized views

Non-materialized views store a query and provide a shortcut to execute the
query. Non-materialized views *do not* store results of embedded queries. You
can incrementally maintain the view's results in memory within a cluster with an
index. Indexes allow you to serve queries without materializing the view.

#### Indexes

Indexes assemble and maintain a query's results in memory in a cluster. The
index provides future queries with data they can use immediately.

In Materialize, *arrangements* are continually updated indexes. Arrangements
store query output in memory and allow Materialize to perform complex operations
more efficiently.

For more information on indexes, review [Arrangements](/overview/arrangements/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`CREATE VIEW`](/sql/create-view)
- [`CREATE INDEX`](/sql/create-index)
- [`CREATE SINK`](/sql/create-sink)
- [`CREATE CLUSTER`](/sql/create-cluster)
- [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica)

[Clusters]: #clusters [Cluster replicas]: #cluster-replicas [Indexes]: #indexes
[Materialized views]: #materialized-views [Debezium]: http://debezium.io
[Sinks]: #sinks [Sources]: #sources [Views]: #views
