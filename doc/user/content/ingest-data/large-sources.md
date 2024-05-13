---
title: "Creating Large Sources"
description: "Creating Large Sources in Materialize"
menu:
  main:
    name: "Overview"
    identifier: ingest-overview
    parent: ingest-data
    weight: 39
---

Sources are used to ingest data into Materialize. New sources transition through different stages until they become ready to be queried. For large datasets it can take some time until the initial data load of a new source has been completed and the source is ready to be queried. During that time, queries may need to wait until the source is ready before they can start computing answers. Moreover, for very large sources containing hundreds of GB of data, not only the initial data load, but also ad-hoc queries and maintaining views can take a significant amount of time to complete and require significant resources.

Let’s take a look at how to know when a source is ready to be queries and how to reduce the size of a source during the initial prototyping stage.

## Creating new sources

To avoid any bad surprises during source creation, like blocking queries, it's essential to follow a few steps and monitor that one step completed before going to the next. There are three main stages a source needs to go through before it becomes healthy: initial data load, rehydration, and catching up with new data. Unless all three steps have completed successfully, queries reading from a source may just hang and appear to do nothing.

These steps usually complete within a few minutes. But for very large sources that contain hundreds of GB of data, it can take up to an hour or even several hours to complete.
Initial data load
It all starts with the creation of a new (sub)source, which triggers an initial snapshot for the source. The initial snapshot contains all historic data that is available in the external system, which is atomically committed to the storage layer at a specific timestamp. Because of that, you are not able to query your source until Materialize has finished ingesting the initial snapshot.

Depending on the size of the source, this progress can take a few minutes up to several hours. It’s also a resource intensive task (in particular if you are using `ENVELOPE UPSERT`). If your source cluster restarts while taking the initial snapshot, e.g., because it runs out of memory, you need to scale up the cluster of the source. Keep in mind that the initial snapshot needs to be restarted from scratch in case there is a failure. So your source may seem busy, whereas the cluster is constantly restarting and the initial snapshot cannot progress beyond a certain point. So it’s worthwhile to also keep an eye on the CPU and memory utilization during this phase.

While the snapshot is taking place, you can monitor how many records have already been read by the source (`snapshot_records_staged`) and how many records are part of the initial snapshot (`snapshot_records_known`).

```
SELECT
	o.name,
	s.snapshot_records_staged,
	s.snapshot_records_known,
	round(100.0 * s.snapshot_records_staged / NULLIF(s.snapshot_records_known, 0), 2) AS snapshot_completed_pct
FROM mz_internal.mz_source_statistics AS s
INNER JOIN mz_objects AS o ON (s.id = o.id)
WHERE NOT s.snapshot_committed;
```

Once the initial snapshot has completed, the data has been durably stored in the storage backend.

Because the initial data load is more resource intensive than consuming change data from the external system, you might have needed to size up the source cluster during that process. Now is a good time to restart the cluster to see if you can scale back down to understand the resource requirements during rehydration and steady state operations. Because the initial snapshot happens only once, knowing the resource requirements during steady state is much more important to correctly size the source cluster for normal operation. Restarting is achieved by removing all compute resources from the underlying cluster and subsequently adding it back. 

```
ALTER CLUSTER sources SET (REPLICATION FACTOR 0);
ALTER CLUSTER sources SET (REPLICATION FACTOR 1);
```

## Rehydration

When the source is restarted and after it has completed the initial snapshot, it reads the data from storage. This process is called rehydration. Depending on the type and the size of the source, this can be a very lightweight process that takes seconds, but for large UPSERT sources it can also be hours. During rehydration, queries are likely to block until the process has been completed. So it's best to monitor the source status and wait until rehydration has completed.

```
SELECT
	s.name,
	h.hydrated
FROM mz_sources AS s
INNER JOIN mz_internal.mz_hydration_statuses AS h ON (s.id = h.object_id);
```

## Catching up with most recent changes

During the initial snapshot and rehydration, new changes may happen in the external system. After the source has completed the first two stages, it consumes new changes from the external system and might need to catch up with the backlog of outstanding changes. The source is aware of any outstanding changes and queries may once again block until the source is caught up enough with the backlog.

The following query indicates the difference between the largest offset that is known from the external system and the last offset that has been processed (committed) by the source. The units depend on the source type. For Kafka sources, it's the number of offsets, for MySQL sources, it's the number of transactions, and for Postgres sources, it's the number of bytes in its replication stream. But in any case, you want offset_delta to be close to 0.

```
SELECT
	o.name,
	s.offset_committed,
	s.offset_known,
	s.offset_known - s.offset_committed AS offset_delta
FROM mz_internal.mz_source_statistics AS s
INNER JOIN mz_objects AS o ON (s.id = o.id)
WHERE s.snapshot_committed;
```

## Putting it all together

In general, sources can only be queried when they have completed their initial snapshot, completed hydration, and are caught up with the external upstream system they are consuming from. Unless all these three conditions are met, queries will likely block and wait until the conditions are met. So it’s recommended that you verify your source is healthy before you run any query against it.

```
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
