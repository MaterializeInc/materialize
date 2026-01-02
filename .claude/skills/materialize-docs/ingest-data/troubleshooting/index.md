---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/troubleshooting/
complexity: advanced
description: How to troubleshoot common data ingestion scenarios where Materialize
  is not working as expected.
doc_type: troubleshooting
keywords:
- SELECT SNAPSHOT_COMMITTED
- 'Tip:'
- Troubleshooting
- CREATE A
- ALTER CLUSTER
- Sources
- 'Note:'
product_area: Sources
status: stable
title: Troubleshooting
---

# Troubleshooting

## Purpose
How to troubleshoot common data ingestion scenarios where Materialize is not working as expected.

This page provides detailed documentation for this topic.


How to troubleshoot common data ingestion scenarios where Materialize is not working as expected.


As you wire up data ingestion in Materialize, you might run into some snags or
unexpected scenarios. This guide collects common questions around data ingestion
to help you troubleshoot your sources. See also [Monitoring data
ingestion](/ingest-data/monitoring-data-ingestion/)

If you're looking for troubleshooting guidance for slow or unresponsive queries,
check out the [`Transform data`
troubleshooting](/transform-data/troubleshooting) guide instead.

> **Tip:** 


## Why isn't my source ingesting data?

First, check the status of your source in the Materialize console by navigating
to https://console.materialize.com/, clicking the **Sources** tab in the
navigation bar, and clicking the affected source.

Alternatively, you can get this information from the system catalog by querying
the [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
table:

```mzsql
SELECT * FROM mz_internal.mz_source_statuses
WHERE name = <SOURCE_NAME>;
```text


| Status        | Description/recommendation                                                                                                                                             |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `paused`      | Source is running on a cluster with 0 replicas. To resolve this, [increase the replication factor](/sql/alter-cluster/#replication-factor-1) of the cluster.                                                                                                          |
| `stalled` | You likely have a configuration issue. The returned `error` field will provide more details.                                                     |
| `failed` | You likely have a configuration issue. The returned `error` field will provide more details.                                                     |
| `starting`    | If this status persists for more than a few minutes, [reach out to our team](http://materialize.com/convert-account/) for support.      |
| `running`     | If your source is in a `running` state but you are not receiving data when you query the source, the source may still be ingesting its initial snapshot. See [Has my source ingested its initial snapshot?](#has-my-source-ingested-its-initial-snapshot). |

## Has my source ingested its initial snapshot?

[//]: # "This page as a whole (as well as some of our other troubleshooting
    pages) can undergo a rewrite since the page is a bit of troubleshooting/faq.
    That is, the troubleshooting might relate to 'why is my query not returning'
    and the answer is check if the source is still snapshotting.  For now, just
    tweaking the changes made for this PR and will address how to rework this
    page in the future."

While a source is [snapshotting](/ingest-data/#snapshotting), the source (and the associated subsources)
cannot serve queries. That is, queries issued to the snapshotting source (and
its subsources) will return after the snapshotting completes (unless the user
breaks out of the query).

Snapshotting can take anywhere from a few minutes to several hours, depending on the size of your dataset,
the upstream database, the number of tables (more tables can be parallelized in Postgres), and the [size of your ingestion cluster](/sql/create-cluster/#size).

We've observed the following approximate snapshot rates from PostgreSQL:
| Cluster Size | Snapshot Rate |
|--------------|---------------|
| 25 cc | ~20 MB/s |
| 100 cc | ~50 MB/s |
| 800 cc | ~200 MB/s |


To determine whether your source has completed ingesting the initial snapshot,
you can query the [`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
system catalog table:

```mzsql
SELECT snapshot_committed
FROM mz_internal.mz_source_statistics
WHERE id = <SOURCE_ID>;
```text

You generally want to aggregate the `snapshot_committed` field across all worker
threads, as done in the above query. The snapshot is only considered committed
for the source as a whole once all worker threads have committed their
components of the snapshot.

Even if your source has not yet committed its initial snapshot, you can still
monitor its progress. See [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/).

## How do I speed up the snapshotting process?

Snapshotting can take anywhere from a few minutes to several hours, depending on the size of your dataset,
the upstream database, the number of tables (more tables can be parallelized in Postgres), and the [size of your ingestion cluster](/sql/create-cluster/#size).

We've observed the following approximate snapshot rates from PostgreSQL:
| Cluster Size | Snapshot Rate |
|--------------|---------------|
| 25 cc | ~20 MB/s |
| 100 cc | ~50 MB/s |
| 800 cc | ~200 MB/s |


To speed up the snapshotting process, you can scale up the [size of the cluster
](/sql/alter-cluster/#alter-cluster-size) used for snapshotting, then scale it
back down once the snapshot completes.

```sql
ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
```

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Unresolved shortcode: > **Note:**  --> --> -->

Resizing a cluster with sources requires the cluster to restart. This operation
incurs downtime for the duration it takes for all objects in the cluster to
[hydrate](/ingest-data/#hydration).

You might want to let the new-sized replica hydrate before shutting down the
current replica. See [zero-downtime cluster
resizing](/sql/alter-cluster/#zero-downtime-cluster-resizing) about automating
this process.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- Unresolved shortcode:  --> --> -->

Once the initial snapshot has completed, you can resize the cluster for steady
state.


For upsert sources, a larger cluster can not only speed up snapshotting, but may
also be necessary to support increased memory usage during the process. For more
information, see [Use a larger cluster for upsert source
snapshotting](/ingest-data/#use-a-larger-cluster-for-upsert-source-snapshotting).

## Adding a new subsource to an existing source blocks replication. Should I just create a new source instead?

It depends. Materialize provides transactional guarantees for subsource of the
same source, not across different sources. So, if you need transactional
guarantees across the tables between the two sources, you cannot use a new
source. In addition, creating a new source means that you are reading the
replication stream twice.

To use the same source, consider resizing the cluster to speed up the
snapshotting process for the new subsource and once the process finishes, resize
the cluster for steady-state.

## See also

- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/)