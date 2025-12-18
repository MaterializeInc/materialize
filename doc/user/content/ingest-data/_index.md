---
title: "Ingest data"
description: "Best practices for ingesting data into Materialize from external systems."
disable_list: true
menu:
  main:
    identifier: "ingest-data"
    name: "Ingest data"
    weight: 11
aliases:
  - /self-managed/v25.1/ingest-data/
---

You can ingest data into Materialize from various external systems:

{{< include-md file="shared-content/multilink-box-native-connectors.md" >}}

## Sources and clusters

Materialize ingests data from external systems using
[sources](/concepts/sources/). For the sources, you need to associate a
[cluster](/concepts/clusters/) to provide the compute resources needed to ingest
data.

{{% tip %}}

If possible, dedicate a cluster just for sources.

{{% /tip %}}

## Snapshotting

When a new source is created, Materialize performs a sync of all data available
in the external system before it starts ingesting new data — an operation known
as _snapshotting_. Because the initial snapshot is persisted in the storage
layer atomically (i.e., at the same ingestion timestamp), you are **not able to
query the source until snapshotting is complete**.

### Duration

The duration of the snapshotting operation depends on the volume of data in the
initial snapshot and the size of the cluster where the source is hosted. To
reduce the operational burden of snapshotting on the upstream system and ensure
you are only bringing in the volume of data that you need in Materialize, we
recommend:

- If possible, running source creation operations during **off-peak hours** to
  minimize operational risk in both the upstream system and Materialize.

- **Limiting the volume of data** that is synced into Materialize on source
  creation. This will help speed up snapshotting, as well as make data
  exploration more lightweight. See [Limit the volume of
  data](#limit-the-volume-of-data) for best practices.

- **For upsert sources**, overprovisioning the source cluster for snapshotting,
  then right-sizing once the snapshot is complete and you have a better grasp on
  the steady-state resource needs of your upsert source(s). See [Best practices:
  Upsert sources](#upsert-sources).

### Monitoring progress

While snapshotting is taking place, you can monitor the progress of the
operation in the **overview page** for the source in the [Materialize
Console](/console/data/#sample-source-overview). Alternatively, you can manually
keep track of using information from the system catalog. See [Monitoring the
snapshotting
progress](/ingest-data/monitoring-data-ingestion/#monitoring-the-snapshotting-progress)
for guidance.

It's also important to **monitor CPU and memory utilization** for the cluster
hosting the source during snapshotting. If there are signs of resource
exhaustion, you may need to [resize the cluster](#use-a-larger-cluster-for-upsert-source-snapshotting).

### Queries during snapshotting

Because the initial snapshot is persisted atomically, you are **not able to
query the source until snapshotting is complete**. This means that queries
issued against (sub)sources undergoing snapshotting will hang until the
operation completes. Once the initial snapshot has been ingested, you can start
querying your (sub)sources and Materialize will continue ingesting any new data
as it arrives, in real time.

### Modifying an existing source

{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}

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
See [Monitoring hydration/data freshness status](/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status).

## Hydration

When a cluster is restarted (such as after resizing), certain objects on that
cluster  (such as sources, indexes, materialized views, and sinks) undergo
hydration. Hydration refers to the reconstruction of in-memory state by reading
data from Materialize's storage layer; hydration **does not** require reading
data from the upstream system.

{{% tip %}}

If possible, use a dedicated cluster just for sources. That is, avoid
using the same cluster for sources and other objects, such as sinks, etc.

See [Best practices](#best-practices) for more details.

{{% /tip %}}

### Process

During hydration, data from Materialize's storage layer is read to reconstruct
the in-memory state of the object. As part of the hydration process:

- Internal data structures are re-created.

- Various processes are re-initiated. These processes may also require
  re-reading of their in-memory state.

### Duration

For a source, the duration of its hydration depends on the type and the size
of the source; e.g., large `UPSERT` sources can take hours to complete.

### Queries during hydration

During hydration, queries usually block until the process has been completed.

## Best practices

The following lists some general best practice guidelines as well as additional
guidelines for upsert sources.

### Scheduling

{{% best-practices/ingest-data/scheduling %}}

### Dedicate a cluster for the sources

If possible, dedicate a cluster just for sources. That is, avoid using the same
cluster for sources and sinks/indexes/materialized views (and other compute objects).

### Limit the volume of data

If possible, limit the volume of data that needs to be synced into Materialize
on source creation. This will help speed up snapshotting as well as make data
exploration more lightweight.

For example, when creating a PostgreSQL source, you may want to create a
publication with specific tables rather than for all tables in the database.

### Upsert sources

In addition to the general best practices, the following additional best
practices apply to upsert sources.

#### Use a larger cluster for upsert source snapshotting

When you create a new source, Materialize performs a one-time [snapshotting
operation](#snapshotting) to initially populate the source in Materialize. For
upsert sources, snapshotting is a resource-intensive operation that can require
a significant amount of CPU and memory.

Consider using a [larger cluster size](/sql/alter-cluster/#alter-cluster-size)
during snapshotting for upsert sources. Once the snapshotting operation is
complete, you can downsize the cluster to align with the steady-state ingestion.

If the cluster hosting the source restarts during snapshotting (e.g., because it
ran out of memory), you can scale up to a [larger
size](/sql/alter-cluster/#alter-cluster-size) to complete the operation.

{{< include-md file="shared-content/resize-cluster-for-snapshotting.md" >}}

#### Right-size the cluster for steady-state

Once the initial snapshot has completed, you can
[resize](/sql/alter-cluster/#resizing)  the cluster
to align with the volume of changes being replicated from your upstream in
steady-state.

```sql
ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
```

{{% note %}}

Resizing a cluster with sources requires the cluster to restart. This operation
incurs downtime for the duration it takes for all objects in the cluster to
[hydrate](#hydration).

You might want to let the new-sized replica hydrate before shutting down the
current replica. See [zero-downtime cluster
resizing](/sql/alter-cluster/#zero-downtime-cluster-resizing) about automating
this process.

{{% /note %}}


## See also

- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion)
- [Troubleshooting data ingestion](/ingest-data/troubleshooting)
