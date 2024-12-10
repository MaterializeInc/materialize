---
title: "Ingesting data"
description: "How to ingest data into Materialize from external systems."
disable_toc: true
---

a source [](). 

Sources are used to ingest data into Materialize. When a new source is created, it needs to transition through different stages until it becomes ready to be queried. For large datasets it can take some time until the initial data load has been completed and the source can be queried.

To avoid bad surprises during source creation, like blocking queries, it's essential to follow a few steps and monitor that one step completed before going to the next. There are three main stages a source needs to go through before it becomes healthy: initial data load, rehydration, and catching up with new data. Unless all three steps have completed successfully, queries reading from a source may just hang and appear to do nothing. These steps usually complete within a few minutes. But for very large sources that contain hundreds of GB of data, it can take up to an hour or even several hours to complete.

## Snapshotting

[//]: # "TODO(morsapaes) If we decide to include screenshots of the console in
this section, we should also adapt ingest-data/troubleshooting for
consistency."

When a new source is created, Materialize performs a sync of all data available
in the external system before it starts ingesting new data — an operation known
as _snapshotting_. Because the initial snapshot is persisted in the storage
layer atomically (i.e., at the same ingestion timestamp), you are **not able to
query the source until snapshotting is complete**. Depending on the volume of
data in the initial snapshot and the size of the cluster the source is hosted
in, this operation can take anywhere from a few minutes up to several hours,
and might require more compute resources than steady-state.

### Monitoring the snapshot

While snapshotting is taking place, you can monitor how many records have
already been read by the source (`snapshot_records_staged`) and how many
records are part of the initial snapshot (`snapshot_records_known`).

```sql
SELECT
	o.name,
	s.snapshot_records_staged,
	s.snapshot_records_known,
	round(100.0 * s.snapshot_records_staged / NULLIF(s.snapshot_records_known, 0), 2) AS snapshot_completed_pct
FROM mz_internal.mz_source_statistics AS s
INNER JOIN mz_objects AS o ON (s.id = o.id)
WHERE NOT s.snapshot_committed;
```

It's also important to monitor CPU and memory utilization for the cluster
hosting the source during snapshotting. If there are signs of resource
exhaustion, you may need to [resize the cluster](#resizing-the-cluster).

### Best practices

#### Limiting the volume of data

When possible, you should limit the volume of data that needs to be synced into
Materialize on source creation. This will help speed up snapshotting, as well
as make data exploration more lightweight.

* [Kafka source]():

* [PostgreSQL source]():

* [MySQL source]():

#### Resizing the cluster

If the cluster hosting the source restarts during snapshotting (e.g., because it
ran out of memory), you must temporarily scale it up to a larger [size](https://materialize.com/docs/sql/create-cluster/#size),
so the operation can complete.

```sql
ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
```

Once the initial snapshot has completed, you can right-size the cluster.

## Rehydration

When the source has completed the initial snapshot and every time the source cluster is restarted, it reads the data from the storage backend. This process is called rehydration. Depending on the type and the size of the source, this can be a very lightweight process that takes seconds, but for large `UPSERT` sources it can also be hours. During rehydration, queries usually block until the process has been completed. So it's best to monitor the source status and wait until rehydration has completed.

```sql
SELECT
	s.name,
	h.hydrated
FROM mz_sources AS s
INNER JOIN mz_internal.mz_hydration_statuses AS h ON (s.id = h.object_id);
```

Because the initial data load is more resource intensive than rehydration and consuming data from the external system, there may be opportunity to scale down the cluster for the steady state workload. Because the taking initial snapshot is resource intensive but happens only once, knowing the resource requirements during steady state is much more important to correctly size the source cluster for normal operation. Restarting the cluster is achieved by removing all compute resources from the underlying cluster and subsequently adding it back.

```sql
ALTER CLUSTER sources SET (REPLICATION FACTOR 0);
ALTER CLUSTER sources SET (REPLICATION FACTOR 1);
```

## Catching up with most recent changes

During the initial snapshot and rehydration, new changes may happen in the external system. After the source has completed the first two stages, it consumes new changes from the external system and might need to catch up with the backlog of outstanding changes. The source is aware of any outstanding changes and queries may once again block until the source is caught up enough with the backlog.

The following query indicates the difference between the largest offset that is known from the external system and the last offset that has been processed (committed) by the source. The units depend on the source type. For Kafka sources, it's the number of offsets, for MySQL sources, it's the number of transactions, and for Postgres sources, it's the number of bytes in its replication stream. But in any case, you want `offset_delta` to be close to 0.

```sql
SELECT
	o.name,
	s.offset_committed,
	s.offset_known,
	s.offset_known - s.offset_committed AS offset_delta
FROM mz_internal.mz_source_statistics AS s
INNER JOIN mz_objects AS o ON (s.id = o.id)
WHERE s.snapshot_committed;
```

Once the source has caught up consuming from the external upstream system, it should be ready to be queried.

## Putting it all together

In general, sources can only be queried when they have completed their initial snapshot, completed hydration, and are caught up with the external upstream system they are consuming from. Unless all these three conditions are met, queries will likely block and wait until the conditions are met. So it’s recommended that you verify your source is healthy before you run any query against it.

```sql
SELECT
	o.name,
	s.snapshot_committed,
	CASE WHEN NOT s.snapshot_committed THEN round(100.0 * s.snapshot_records_staged / NULLIF(s.snapshot_records_known, 0)) ELSE 100 END AS snapshot_completed_pct,
	h.hydrated,
	s.offset_known - s.offset_committed AS offset_delta
FROM mz_internal.mz_source_statistics AS s
INNER JOIN mz_objects AS o ON (s.id = o.id)
INNER JOIN mz_internal.mz_hydration_statuses AS h ON (s.id = h.object_id);
```
