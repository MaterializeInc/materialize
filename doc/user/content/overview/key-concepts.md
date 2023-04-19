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

This document captures the unique components in Materialize to help you
understand how Materialize functions differently from traditional databases.

Materialize uses the components below to compute and build your data queries.

Component                | Use
-------------------------|-----
**[Sources]**            | An external system you want Materialize to read data from (e.g. Kafka).
**[Views]**              | Queries of sources and other views that you want to save for repeated execution.
**[Indexes]**            | Query results stored in memory.
**[Materialized views]** | Query results stored durably.
**[Sinks]**              | Output streams or files that Materialize sends data to.
**[Clusters]**           | Logical compute resources that can be used by sources, sinks, indexes, and materialized views.
**[Cluster replicas]**   | Allocate physical compute resources for a cluster.

## Clusters

Clusters are logical components that describe how Materialize allocates compute
resources for your dataflow objects. When you create a dataflow object like a
source or an index, you must specify a cluster or your resource will use the
`default` cluster.

Clusters rely on cluster replicas to run dataflows. Without a replica, clusters
cannot perform data operations. For example, if you create an index on a cluster
without a replica, you cannot select from that index because there is
no index architecture to read.

Clusters are similar to a VPC while cluster replicas are similar to compute
instances within a VPC. The cluster is the architectural foundation that
provides the operational resources to objects within and the
cluster replicas process data available to them in the cluster.

### Cluster deployment options

You can control cluster performance in Materialize by scaling the amount of
clusters and distributing data amongst your clusters. This reduces the amount of
resources necessary in each cluster and gives you more availability within each
cluster.

You can also [add cluster replicas](#cluster-replica-deployment-options) within a cluster. The next section describes
cluster replicas in more detail.


## Cluster replicas

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

### Cluster replica deployment options


Materialize is an active-replication-based system and assumes each cluster replica has the same working set.

When planning your Materialize deployment one option to consider is the size of
your replicas within your clusters. Clusters with larger sized replicas have greater
dataflow throughput and can maintain more complex dataflows. 

You can also add more replicas to your clusters to increase fault tolerance if a
replica becomes unavailable.

## Views

In SQL, views are queries you save as shortcuts for complex `SELECT` statements.
Materialize uses views in two ways:

Type | Use
-----|-----
**Materialized views** | Incrementally updated views with results saved durable storage
**Non-materialized views** | Queries saved under a name for reference, like traditional SQL views

Materialize builds all views by reading data from sources and other views.

### Materialized views

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
materialized views, we recommend creating [indexes] based on
common query patterns.

### Non-materialized views

Non-materialized views store a query and provide a shortcut to execute the
query. Non-materialized views *do not* store results of embedded queries. You
can incrementally maintain the view's results in memory within a cluster with an
index. Indexes allow you to serve queries without materializing the view.

## Indexes

Indexes assemble and maintain a query's results in memory in a cluster. The
index provides future queries with data they can use immediately.

In Materialize, *arrangements* are continually updated indexes. Arrangements
store query output in memory and allow Materialize to perform complex operations
more efficiently.

For more information on indexes, review [Arrangements](/overview/arrangements/).

## Clusters

Tenants are logical components that describe how Materialize alloctates compute
resources for your dataflow objects. When you create a dataflow object like a
source or an index, you must specify a cluster or your resource will use the
`default` cluster.

Clusters rely on cluster replicas to run dataflows. Without a replica, clusters
cannot perform data operations. For example, if you create an index on a cluster
without a replica, you cannot select from that index because there is
no index architecture to read.

Clusters represent the logic of your dataflow objects and how they work together
which impact their performance once you provision replicas. Materialize
creates an instance of every object on each replica which means objects
in your tenant share the same compute resources. You may need to configure
multiple clusters to optimize performance in more resource intense operations.

### Cluster deployment options

When building your Materialize deployment, you can change its performance
characteristics by...

Action | Outcome
-------|---------
Adding clusters + decreasing dataflow density | Reduced resource contention among dataflows, decoupled dataflow availability
Adding replicas to clusters | See [Cluster replica scaling](#cluster-replica-deployment-options)

## Cluster replicas

Cluster replicas are the physical architecture within clusters that create and
maintain dataflows.


Each cluster replica within a cluster is a clone and construct the same
dataflows. Each replica within a cluster receives a copy of all data from
sources the dataflow uses and then performs identical computations on that data.
As long as one replica is available, the cluster continues operations and gives
you active replication in Materialize. 

All dataflows of a cluster share the same resources on each replica. Depending
on your dataflow needs, you may consider distributing replicas across multiple
clusters or provisioning more compute resources within your clusters.

### Cluster replica deployment options


Materialize is an active-replication-based system. An active-replication-based
system assumes each cluster replica has the same working set.

When planning your Materialize deployment, consider these options for your
cluster and replica architecture:

Action | Outcome ---------|--------- Increase replicas' size | Ability to
maintain more dataflows or more complex dataflows Add replicas to a cluster |
Greater tolerance to replica failure

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
