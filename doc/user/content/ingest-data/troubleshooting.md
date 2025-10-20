---
title: "Troubleshooting"
description: "How to troubleshoot common data ingestion scenarios where Materialize is not working as expected."
menu:
  main:
    name: "Troubleshooting"
    identifier: ingest-troubleshooting
    parent: ingest-data
    weight: 40
aliases:
  - /ops/diagnosing-using-sql/
  - /ops/troubleshooting/
---

As you wire up data ingestion in Materialize, you might run into some snags or
unexpected scenarios. This guide collects common questions around data ingestion
to help you troubleshoot your sources. See also [Monitoring data
ingestion](/ingest-data/monitoring-data-ingestion/)

If you're looking for troubleshooting guidance for slow or unresponsive queries,
check out the [`Transform data`
troubleshooting](/transform-data/troubleshooting) guide instead.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Why isn't my source ingesting data?

First, check the status of your source in the Materialize console by navigating
to /console/, clicking the **Sources** tab in the
navigation bar, and clicking the affected source.

Alternatively, you can get this information from the system catalog by querying
the [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
table:

```mzsql
SELECT * FROM mz_internal.mz_source_statuses
WHERE name = <SOURCE_NAME>;
```


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

{{< include-md file="shared-content/snapshotting-cluster-size-postgres.md" >}}

To determine whether your source has completed ingesting the initial snapshot,
you can query the [`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
system catalog table:

```mzsql
SELECT snapshot_committed
FROM mz_internal.mz_source_statistics
WHERE id = <SOURCE_ID>;
```

You generally want to aggregate the `snapshot_committed` field across all worker
threads, as done in the above query. The snapshot is only considered committed
for the source as a whole once all worker threads have committed their
components of the snapshot.

Even if your source has not yet committed its initial snapshot, you can still
monitor its progress. See [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/).

## How do I speed up the snapshotting process?

{{< include-md file="shared-content/snapshotting-cluster-size-postgres.md" >}}

To speed up the snapshotting process, you can scale up the [size of the cluster
](/sql/alter-cluster/#alter-cluster-size) used for snapshotting, then scale it
back down once the snapshot completes.

{{< include-md file="shared-content/resize-cluster-for-snapshotting.md" >}}

For upsert sources, a larger cluster can not only speed up snapshotting, but may
also be necessary to support increased memory usage during the process. For more
information, see [Use a larger cluster for upsert source
snapshotting](/ingest-data/#use-a-larger-cluster-for-upsert-source-snapshotting).

## See also

- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/)
