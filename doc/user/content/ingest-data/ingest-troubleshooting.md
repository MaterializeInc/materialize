---
title: "Troubleshooting ingest"
description: ""
menu:
  main:
    name: "Troubleshooting ingest"
    identifier: ingest-troubleshooting
    parent: ingest-data
    weight: 40
---

If you're looking for somewhere to start for troubleshooting slow or unresponsive queries, start at the [troubleshooting](troubleshooting) page.

<!-- Copied over from the old manage/troubleshooting guide -->
## Why isn't my source ingesting data?
First check the status of your source in the Materialize console by going to https://console.materialize.com/, clicking the "Sources" tab in the nav bar, and clicking into the source.


You can also query for this information via SQL in the [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses) table:

```sql
SELECT * FROM mz_internal.mz_source_statuses
WHERE name = <SOURCE_NAME>;
```

If your source reports a status of `stalled` or `failed`, you likely have a
configuration issue. The returned `error` field will provide details.

If your source reports a status of `starting` for more than a few minutes,
[contact support](/support).

If your source reports a status of `running`, but you are not receiving data
when you query the source, the source may still be ingesting its initial
snapshot. See the next section.

## Has my source ingested its initial snapshot?
When you create a source, before it starts consuming the replication stream, Materialize takes an initial snapshot of the relevant tables in your publication. The initial snapshot is committed at one timestamp atomically to ensure correct and consistent results. As such, you will not be able to query data from your source until the initial snapshot is complete.

Snapshotting can take between a few minutes to several hours, depending on the size of your dataset and the size of your ingestion cluster.

To determine whether your source has completed initial snapshot ingestion, query the `snapshot_committed` field of the
[`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics) table:

```sql
SELECT snapshot_committed
FROM mz_internal.mz_source_statistics
WHERE id = <SOURCE ID>;
```

You generally want to aggregate the `snapshot_committed` field across all worker
threads, as done in the above query. The snapshot is only considered committed
for the source as a whole once all worker threads have committed their
components of the snapshot.

Even if your source has not yet committed its initial snapshot, you can still
monitor its progress. See the next section.

## How do I monitor source ingestion progress?

Repeatedly query the
[`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
table and look for ingestion statistics that advance over time:

```sql
SELECT
    bytes_received,
    messages_received,
    updates_staged,
    updates_committed
FROM mz_internal.mz_source_statistics
WHERE id = <SOURCE ID>;
```

(You can also look at statistics for individual worker threads to evaluate
whether ingestion progress is skewed, but it's generally simplest to start
by looking at the aggregate statistics for the whole source.)

The `bytes_received` and `messages_received` statistics should roughly
correspond with the external system's measure of progress. For example, the
`bytes_received` and `messages_received` fields for a Kafka source should
roughly correspond to what the upstream Kafka broker reports as the number of
bytes (including the key) and number of messages transmitted, respectively.

During the initial snapshot, `updates_committed` will remain at zero until all
messages in the snapshot have been staged. Only then will `updates_committed`
advance. This is expected, and not a cause for concern.

After the initial snapshot, there should be relatively little skew between
`updates_staged` and `updates_committed`. A large gap is usually an indication
that the source has fallen behind, and that you likely need to scale up your
source.

`messages_received` does not necessarily correspond with `updates_staged`
and `updates_commmited`. For example, an `UPSERT` envelope can have _more_
updates than messages, because messages can cause both deletions and insertions
(i.e. when they update a value for a key), which are both counted in the
`updates_*` statistics. There can also be _fewer_ updates than messages, as
many messages for a single key can be consolidated if they occur within a (small)
internally configured window. That said, `messages_received` making
steady progress, while `updates_staged`/`updates_committed` doesn't is also
evidence that a source has fallen behind, and may need to be scaled up.

Beware that these statistics periodically reset to zero, as internal components
of the system restart. This is expected behavior. As a result, you should
restrict your attention to how these statistics evolve over time, and not their
absolute values at any moment in time.
