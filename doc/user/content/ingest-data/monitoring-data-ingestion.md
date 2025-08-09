---
title: "Monitoring data ingestion"
description: "How to monitor the snapshotting progress and data lag for your sources."
menu:
  main:
    identifier: ingest-monitoring
    parent: ingest-data
    weight: 49
---

### Monitoring the snapshotting progress

In the Materialize Console, the Overview page for the source displays the
snapshotting progress.

![Source overview page](/images/monitoring/snapshot-monitoring.png
"Materialize Console - Overview page displays snapshotting progress")

Alternatively, you can run a query to monitor its progress.

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
exhaustion, you may need to [resize the cluster](/sql/alter-cluster/#alter-cluster-size).

In the Materialize Console, the Overview page for the source displays the CPU
and memory utilization. See image above.

## Monitoring hydration/data freshness status

To monitor the hydration/data freshness status of a source (and its
sub-sources), in the Materialize Console, you can go to the Workflow page of a
source (or its sub-sources) to check for data freshness status; that is, whether
the source is **Up to date** or **Lagging**. If lagging, the page also displays
the lag amount.

![Source workflow page](/images/monitoring/source-data-freshness-status.png
"Materialize Console - Workflow page displays data freshness of a source")

Alternatively, you can run the following query:

```sql
SELECT
	s.name,
	h.hydrated
FROM mz_sources AS s
INNER JOIN mz_internal.mz_hydration_statuses AS h ON (s.id = h.object_id);
```

## Monitoring data lag

In the Materialize Console, you can go to the Workflow page of a
source (or its sub-sources) to check for data freshness status. If the source
(or its sub-sources) is lagging, its Workflow page displays **Lagging** status
as well as the lag amount.

Alternatively, the following query indicates the difference between the largest offset that is known from the external system and the last offset that has been processed (committed) by the source. The units depend on the source type.  you want `offset_delta` to be close to 0.

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
