---
title: "Ingest data"
description: "Best practices for ingesting data into Materialize from external systems."
disable_list: true
menu:
  main:
    identifier: "ingest-data"
    name: "Ingest data"
    weight: 11
---

You can ingest data into Materialize from various external systems:

{{< multilinkbox >}}
{{< linkbox title="Databases (CDC)" >}}
- [PostgreSQL](/ingest-data/postgres/)
- [MySQL](/ingest-data/mysql/)
- [SQL Server](/ingest-data/cdc-sql-server/)
- [MongoDB](https://github.com/MaterializeIncLabs/materialize-mongodb-debezium)
- [CockroachDB](/ingest-data/cdc-cockroachdb/)
- [Other databases](/integrations/#databases)
{{</ linkbox >}}
{{< linkbox title="Message Brokers" >}}
- [Kafka](/ingest-data/kafka/)
- [Redpanda](/sql/create-source/kafka)
- [Other message brokers](/integrations/#message-brokers)
{{</ linkbox >}}
{{< linkbox title="Webhooks" >}}
- [Amazon EventBridge](/ingest-data/webhooks/amazon-eventbridge/)
- [Segment](/ingest-data/webhooks/segment/)
- [Other webhooks](/sql/create-source/webhook)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Sources and clusters

Materialize ingests data from external systems using
[sources](/concepts/sources/). For the sources, you need to associate a
[cluster](/concepts/clusters/) to provide the compute resources needed to ingest
data.

{{% tip %}}

- If possible, dedicate a cluster just for sources.

- When you create a new source, Materialize performs a one-time [snapshotting
  operation](#snapshotting) to initially populate the source in Materialize.
  Snapshotting is a resource-intensive operation that can require a significant
  amount of CPU and memory. Consider using a larger cluster size during
  snapshotting. Once the snapshotting operation is complete, you can downsize
  the cluster to align with the steady-state ingestion.

- See also [Best practices](#best-practices).

{{% /tip %}}

## Snapshotting

When a new source is created, Materialize performs a sync of all data available
in the external system before it starts ingesting new data — an operation known
as _snapshotting_. Because the initial snapshot is persisted in the storage
layer atomically (i.e., at the same ingestion timestamp), you are **not able to
query the source until snapshotting is complete**.

Depending on the volume of data in the initial snapshot and the size of the
cluster the source is hosted in, this operation can take anywhere from a few
minutes up to several hours, and might require more compute resources than
steady-state.

### Duration

The snapshotting operation duration depends on the snapshot dataset size and the
size of the Materialize cluster that is hosting the source. For very large
sources that contain hundreds of GB of data, it can take up to an hour or even
several hours to complete.

{{% tip %}}

- If possible, schedule creating new sources during off-peak hours to mitigate
  the impact of snapshotting on both the upstream system and the Materialize
  cluster.

- Consider using a larger cluster size during snapshotting. Once the
  snapshottingis complete, you can downsize the cluster to align with the volume
  of changes being replicated from your upstream in steady-state.

- See also [Best practices](#best-practices).

{{% /tip %}}

If you create your source from the Materialize Console, the overview page for
the source displays the snapshotting progress. Alternatively, you can run a
query to monitor its progress. See [Monitoring the snapshotting progress](/ingest-data/monitoring-data-ingestion/#monitoring-the-snapshotting-progress).

### Monitoring progress

While snapshotting is taking place, you can monitor the progress of the
operation in the **overview page** for the source in the [Materialize Console]
(https://console.materialize.com/). Alternatively, you can manually keep track
of using information from the system catalog. See [Monitoring the snapshotting
progress](/ingest-data/monitoring-data-ingestion/#monitoring-the-snapshotting-progress)
for guidance.

It's also important to **monitor CPU and memory utilization** for the cluster
hosting the source during snapshotting. If there are signs of resource
exhaustion, you may need to [resize the cluster](#use-a-larger-cluster-for-snapshotting).

### Queries during snapshotting

While a source is snapshotting, the source (and the associated subsources)
cannot serve queries. That is, queries issued to the snapshotting source (and
its subsources) will return after the snapshotting completes (unless the user
breaks out of the query). If the user does not break out of the query, the
returned query results will reflect the data from the snapshot.

## Running/steady-state

Once snapshotting completes, Materialize transitions to Running state. During
this state, Materialize continually ingests changes from the upstream system.

### Queries during steady-state

Although Materialize is continually ingesting changes from the upstream system,
depending on the volume of the upstream changes, Materialize may lag behind the
upstream system. If the lag is significant, queries may block until Materialize
has caught up sufficiently with the upstream system when using the default
[isolation level](/get-started/isolation-level/) of [strict
serializability](/get-started/isolation-level/#strict-serializable).

In the Materialize Console, you can see a source's data freshness from the
**Data Explorer** screen. Alternatively, you can run a query to monitor the lag.
See [Monitoring the snapshotting progress](/ingest-data/monitoring-data-ingestion).

## Rehydration

When a cluster is restarted (such as after resizing), sources undergo
rehydration.[^1] Rehydration refers to the reconstruction of in-memory state by
reading data from the storage layer; rehydration does not require reading data
from the upstream system.

{{% tip %}}

If possible, use a dedicated cluster just for sources. That is, avoid
using the same cluster for sources and other objects, such as sinks, etc. See [Best practices](#best-practices) for more details.

{{% /tip %}}

### Process

During rehydration, data from the storage layer is read to reconstruct the
in-memory state of the object. As part of the rehydration process:

- Internal data structures are re-created.

- Various processes are re-initiated. These processes may also require
  re-reading of their in-memory state.

### Duration

For a source, the duration of its rehydration depends on the type and the size
of the source; e.g., large `UPSERT` sources can take hours to complete.

### Queries during rehydration

During rehydration, queries usually block until the process has been completed.

## Best practices

#### Scheduling

If possible, schedule creating new sources during off-peak hours to mitigate the
impact of snapshotting on both the upstream system and the Materialize cluster.

#### Dedicate a cluster for the sources

If possible, dedicate a cluster just for sources. That is, avoid using the same
cluster for sources and sinks/indexes/materialized views (and other compute objects).

#### Use a larger cluster for snapshotting

Consider using a larger cluster size during snapshotting. Once the snapshotting
operation is complete, you can downsize the cluster to align with the volume of
changes being replicated from your upstream in steady-state.

If the cluster hosting the source restarts during snapshotting (e.g., because it
ran out of memory), you can scale up to a larger
[size](/sql/alter-cluster/#alter-cluster-size) to complete the
operation.

```sql
ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
```

{{% note %}}

Resizing a cluster with sources requires the cluster to restart. This operation
incurs downtime for the duration it takes for all objects in the cluster to
[rehydrate](#rehydration).

{{% /note %}}

Once the initial snapshot has completed, you can resize the cluster.

#### Limit the volume of data

If possible, limit the volume of data that needs to be synced into Materialize
on source creation. This will help speed up snapshotting as well as make data
exploration more lightweight.

For example, when creating a PostgreSQL source, you may want to create a
publication with specific tables rather than for all tables in the database.

#### Right-size the cluster for steady-state

Once the initial snapshot has completed, you can
[resize](/sql/alter-cluster/#alter-cluster-size)  the cluster
to align with the volume of changes being replicated from your upstream in
steady-state.

```sql
ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
```

{{% note %}}

Resizing a cluster with sources requires the cluster to restart. This operation
incurs downtime for the duration it takes for all objects in the cluster to
[rehydrate](#rehydration).

{{% /note %}}

## See also

- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion)
- [Troubleshooting data ingestion](/ingest-data/troubleshooting)

[^1]: Other objects, such as sinks, indexes, materialized views, etc., also
undergo rehydration if their cluster is restarted.  If possible, use a dedicated
cluster just for sources.
