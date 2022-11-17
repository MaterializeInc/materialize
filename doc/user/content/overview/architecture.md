---
title: "Architecture overview"
description: "Understand the different components that make Materialize a horizontally scalable, highly available distributed system."
menu:
  main:
    parent: overview
    weight: 10
---

Materialize is a streaming database designed to provide correct, low latency answers to queries that need to be answered repeatedly by **incrementally maintaining answers** to those queries. Materialize enables users to ingest as much data as they want (by separating compute and storage into decoupled layers), perform as many queries as they want (by being horizontally and vertically scalable), while being highly available and flexible to operate (by offering active-active replication).

The Materialize architecture is broken into three separate layers:
* Storage: which is responsible for reading from and writing to external sources and sinks.
* Compute: which is responsible for incrementally maintaining the results of user requested queries.
* Adapter: which is responsible for actually responding to user requests, and serving results back to users.

{{<
    figure src="/images/architecture_overview.jpg"
    alt="Materialize architecture overview"
    width="700"
>}}

Each of these layers performs their work independently and can be **scaled independently**.

This page will describe the responsibilities of these three layers, and how users can interact with them in SQL.

## Storage

Storage is responsible for reading data from external systems (e.g. a Kafka cluster) and maintaining explicitly timestamped histories of changes to that data. Each such explicitly timestamped history is called a collection. Storage maintains these collections in persistent storage (S3) so that they can be used by the Compute layer to maintain views over those stored collections. Storage is also responsible for writing stored collections back to external systems (e.g. a Kafka cluster).

The key concepts and nouns in storage that users have to interact with are `CONNECTION`s, `SOURCE`s, and `SINK`s.

### Connections

A connection describes how to connect and authenticate to an external system that Materialize will read data from or write data to. A connection is a logical entity. Materialize will actually connect to the external system specified in a connection when a source or sink is created using that connection.

### Sources

A source is the SQL respresentation of an input data collection from an external system (e.g. a Kafka topic). A source describes a location within an external system (e.g. a Kafka topic on a specified Kafka cluster) that Materialize will read data from, and provides details on how to decode and interpret the incoming data from that particular location. Materialize will start reading and maintaining a stored collection (an explicitly timestamped history of changes to the input data) as soon as the source is created.

The computational resources available to decode and interpret the input data, and maintain the stored collection can be scaled up or down by modifying the source's size with `ALTER SOURCE`.

### Sinks

Sinks are the inverse of sources. A sink describes a location within an external system (e.g. a Kafka topic on a specified Kafka cluster) that Materialize will write data to and provides details on how to encode the written data. Sinks can be defined over existing sources, materialized views, or tables. When a sink is defined, Materialize automatically generates the required schema, and writes the stream of changes to the specified source, materialized view, or table in the specified format.

The computational resources available to read from persistent storage and encode data before writing it out to the external system can be scaled up or down by modifying the sink's size with `ALTER SINK`.

## Compute

The compute layer is responsible for efficiently computing and maintaining views over data collections maintained by the Storage layer.

The key concepts and nouns that users have to interact with are `VIEW`s, `CLUSTER`s, `CLUSTER REPLICA`s, `INDEX`es, and `MATERIALIZED VIEW`s.

### (Unmaterialized) Views

Unmaterialized views represent a query that a user can save and associate with a name. These are used primarily as shorthand for some lengthy, complicated `SELECT` statement. Materialize will not compute or maintain an unmaterialized view when it is created. Materialize will compute the view (i.e. compute the underlying `SELECT` statement) if the view is `SELECT`ed.

Repeatedly querying an unmaterialized view without any indexes is discouraged because Materialize will repeatedly compute and then discardiscard the output collections. It is recommended in that case to either create indexes based on the repeated query or a materialized view instead.

Unmaterialized views can be referenced by any cluster in the region.

### Clusters

Clusters are logical components that enable users to express resource isolation for objects responsible for computing and maintaining views (indexes and materialized views). A cluster specifies which indexes and materialized views share the same computational resources (CPU and RAM).

However, a cluster is a logical respresentation of this relationship. Materialize will not actually spin up any computational resources when a cluster is created. Materialize will do so when a cluster is endowed with cluster replicas. A cluster can be active-active replicated by endowing it with multiple replicas. As long as one of the replicas is making progress, the cluster is making progress and can service reads and writes.

### Cluster replicas

Cluster replicas are the physical incarnations of a given cluster. A cluster replica represents a physical set of computational resources (CPU and RAM) that is responsible for maintaining the indexes and materialized views defined on the parent cluster.

Each cluster replica associated with a given cluster contains all of the indexes and materialized views associated with that cluster. However, different cluster replicas can be differently sized, even if they are associated with the same cluster. Each replica is guaranteed to produce the same outputs as all of its peers for all times, because all cluster replicas read the same input data collections from the storage layer, and they all perform the same deterministic computations.

### Indexes

Indexes assemble and maintain source, view or materialized view output data in-memory on all cluster replicas associated with a given cluster. Creating an index enables future `SELECT` queries against the given source, view or materialized view to return rapidly because the requested data can be read without interacting with persistent storage, or incurring (much) additional computation.

Users can create indexes on any source, unmaterialized view, or materialized view present in the region. However indexes are associated with a particular cluster, and when an index is created, altered, or destroyed, the corresponding change is replicated on all current and future replicas associated with that cluster. This setup lets users decouple changes to the computation (which views are defined, which indexes are created) from how that computation is physically deployed (how many replicas, how large is each replica).

Note however, that unlike (materialized) views, indexes are local to a cluster and can can only be used on `SELECT` queries on the same cluster.

For a deeper dive into how indexes work see [here](/ops/optimization/#indexes) and [here](/sql/create-index).

### Materialized views

Materialized views compute and then maintain views as data collections stored in persistent storage (S3). Subsequent `SELECT` queries can then read results from the stored data collection without having to perform (much) additional computation.

If `SELECT` queries against a materialized view are too slow, it is recommended to create an index based on common query patterns.

Materialized views are associated with a particular cluster but they can be read from any cluster in the region.

### Adapter

Adapter is responsible for handling users input SQL statements, and assigning timestamps to users read (`SELECT`, `SUBSCRIBE`, `COPY ... FROM`) queries and write (`INSERT`, `UPDATE`, `DELETE`, `COPY ... TO`) queries to ensure that they receive outputs corresponding to those timestamps. The Adapter is also responsible for ensuring that [transactions](/overview/isolation-level) work correctly.

## Learn more

- [Key Concepts](/overview/key-concepts/) to better understand Materialize's API
- [Get started](/get-started) to try out Materialize
