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
unexpected scenarios. This guide collects common questions around data
ingestion to help you troubleshoot your sources. If you're looking for
troubleshooting guidance for slow or unresponsive queries, check out the
[`Transform data` troubleshooting](/transform-data/troubleshooting) guide
instead.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Why isn't my source ingesting data?

First, check the status of your source in the Materialize Console by clicking the **Sources** tab in the
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
monitor its progress. See [How do I monitor source ingestion progress?](#how-do-i-monitor-source-ingestion-progress).

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

## How do I monitor source ingestion progress?

Repeatedly query the
[`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
table and look for ingestion statistics that advance over time:

```mzsql
SELECT
    bytes_received,
    messages_received,
    updates_staged,
    updates_committed
FROM mz_internal.mz_source_statistics
WHERE id = <SOURCE_ID>;
```

You can also look at statistics for individual worker threads to evaluate
whether ingestion progress is skewed, but it's generally simplest to start
by looking at the aggregate statistics for the whole source.

The `bytes_received` and `messages_received` statistics should roughly match the
external system's measure of progress. For example, the `bytes_received` and
`messages_received` fields for a Kafka source should roughly match the upstream
Kafka broker reports as the number of bytes (including the key) and number of
messages transmitted, respectively.

During the initial snapshot, `updates_committed` will remain at zero until all
messages in the snapshot have been staged. Only then will `updates_committed`
advance. This is expected, and not a cause for concern.

After the initial snapshot, there should be relatively little skew between
`updates_staged` and `updates_committed`. A large gap is usually an indication
that the source has fallen behind, and that you likely need to scale it up.

`messages_received` does not necessarily correspond with `updates_staged`
and `updates_commmited`. For example, a source with `ENVELOPE UPSERT` can have _more_
updates than messages, because messages can cause both deletions and insertions
(i.e. when they update a value for a key), which are both counted in the
`updates_*` statistics. There can also be _fewer_ updates than messages, as
many messages for a single key can be consolidated if they occur within a (small)
internally configured window. That said, `messages_received` making
steady progress while `updates_staged`/`updates_committed` doesn't is also
evidence that a source has fallen behind, and may need to be scaled up.

Beware that these statistics periodically reset to zero, as internal components
of the system restart. This is expected behavior. As a result, you should
restrict your attention to how these statistics evolve over time, and not their
absolute values at any moment in time.
