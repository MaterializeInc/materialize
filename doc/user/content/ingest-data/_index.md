---
title: "Ingest data"
description: "Best practices for ingesting data into Materialize from external systems."
disable_list: true
menu:
  main:
    identifier: "ingest-data"
    name: "Ingest data"
    weight: 30
aliases:
  - /self-managed/v25.1/ingest-data/
  - /self-managed/v25.2/ingest-data/kafka/amazon-msk/
  - /self-managed/v25.2/ingest-data/kafka/kafka-self-hosted/
  - /self-managed/v25.2/ingest-data/troubleshooting/
---

You can ingest data into Materialize from various external systems:

{{% include-headless "/headless/ingest-connectors-table" %}}

## Sources and clusters

Materialize ingests data from external systems using
[sources](/concepts/sources/). For the sources, you need to associate a
[cluster](/concepts/clusters/) to provide the compute resources needed to ingest
data.

{{% tip %}}

If possible, dedicate a cluster just for sources.

{{% /tip %}}

## Snapshotting

{{% include-headless "/headless/ingestion/snapshotting-definition" %}}

### When snapshotting occurs

{{% include-headless "/headless/ingestion/snapshotting-occurrence" %}}

### Duration

{{% include-headless "/headless/ingestion/snapshotting-duration" %}}

To reduce the operational burden of snapshotting on the upstream system and
ensure you are only bringing in the volume of data that you need in Materialize,
we recommend:

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

### Parallelism

{{% include-headless "/headless/ingestion/snapshotting-parallelism" %}}

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

{{% include-headless "/headless/ingestion/snapshotting-queries" %}}

### Modifying an existing source

{{% include-headless "/headless/ingestion/snapshotting-ingestion" %}}

If possible, resize the cluster to speed up the snapshot, then right-size it once
snapshotting completes.

## Running/steady-state

Once snapshotting completes, Materialize transitions to Running state. During
this state, Materialize continually ingests changes from the upstream system.

### Queries during steady-state

Although Materialize is continually ingesting changes from the upstream system,
depending on the volume of the upstream changes, Materialize may lag behind the
upstream system. If the lag is significant, queries may block until Materialize
has caught up sufficiently with the upstream system when using the default
[isolation level](/reference/isolation-level/) of [strict
serializability](/reference/isolation-level/#strict-serializable).

In the Materialize Console, you can see a source's data freshness from the
**Data Explorer** screen. Alternatively, you can run a query to monitor the lag.
See [Monitoring hydration/data freshness status](/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status).

## Hydration

{{< include-from-yaml data="hydration-details" name="definition" >}}

When a cluster is restarted (such as after resizing), certain objects on that
cluster  (such as Kafka upsert sources, indexes, materialized views, and sinks)
undergo hydration. For the full list of events that trigger hydration and the
affected objects, see [Hydration](/concepts/hydration/).

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

{{% include-from-yaml data="best_practices_details"
name="ingest-data-scheduling" %}}

### Dedicate a cluster for the sources

If possible, dedicate a cluster just for sources. That is, avoid using the same
cluster for sources and sinks/indexes/materialized views (and other compute objects).

### Limit the volume of data

If possible, limit the volume of data that needs to be synced into Materialize
on source creation. This will help speed up snapshotting as well as make data
exploration more lightweight.

For example, when creating a PostgreSQL source, you may want to create a
publication with specific tables rather than for all tables in the database.

Once the data is in Materialize, you can further [reduce the size of the
data](/transform-data/optimization/#reduce-the-size-of-the-data) maintained by
your view definitions.

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

{{% include-headless "/headless/resize-cluster-for-snapshotting" %}}

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
current replica. See the [resizing
process](/sql/alter-cluster/#resizing-process) about automating
this process.

{{% /note %}}


## See also

- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion)
- [Troubleshooting data ingestion](/ingest-data/troubleshooting)
