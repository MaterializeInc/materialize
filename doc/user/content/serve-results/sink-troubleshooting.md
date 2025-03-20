---
title: "Troubleshooting sinks"
description: ""
menu:
  main:
    parent: sink
    name: "Troubleshooting sinks"
    identifier: sink-troubleshooting
    weight: 50

---

<!-- Copied over from the old manage/troubleshooting guide -->
## Why isn't my sink exporting data?
First, look for errors in [`mz_sink_statuses`](/sql/system-catalog/mz_internal/#mz_sink_statuses):

```mzsql
SELECT * FROM mz_internal.mz_sink_statuses
WHERE name = <SINK_NAME>;
```

If your sink reports a status of `stalled` or `failed`, you likely have a
configuration issue. The returned `error` field will provide details.

If your sink reports a status of `starting` for more than a few minutes,
[contact support](/support).

## How do I monitor sink ingestion progress?

Repeatedly query the
[`mz_sink_statistics`](/sql/system-catalog/mz_internal/#mz_sink_statistics)
table and look for ingestion statistics that advance over time:

```mzsql
SELECT
    messages_staged,
    messages_committed,
    bytes_staged,
    bytes_committed
FROM mz_internal.mz_sink_statistics
WHERE id = <SINK ID>;
```

(You can also look at statistics for individual worker threads to evaluate
whether ingestion progress is skewed, but it's generally simplest to start
by looking at the aggregate statistics for the whole source.)

The `messages_staged` and `bytes_staged` statistics should roughly correspond
with what materialize has written (but not necessarily committed) to the
external service. For example, the `bytes_staged` and `messages_staged` fields
for a Kafka sink should roughly correspond with how many messages materialize
has written to the Kafka topic, and how big they are (including the key), but
the Kafka transaction for those messages might not have been committed yet.

`messages_committed` and `bytes_committed` correspond to the number of messages
committed to the external service. These numbers can be _smaller_ than the
`*_staged` statistics, because Materialize might fail to write transactions and
retry them.

If any of these statistics are not making progress, your sink might be stalled
or need to be scaled up.

If the `*_staged` statistics are making progress, but the `*_committed` ones
are not, there may be a configuration issues with the external service that is
preventing Materialize from committing transactions. Check the `reason`
column in `mz_sink_statuses`, which can provide more information.
