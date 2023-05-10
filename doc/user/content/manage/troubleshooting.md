---
title: "Troubleshooting"
description: "Troubleshoot issues."
menu:
  main:
    parent: manage
    weight: 5
aliases:
  - /ops/diagnosing-using-sql/
  - /ops/troubleshooting/
---

This page describes several SQL queries you can run to diagnose performance
issues with Materialize.

{{< warning >}}
The queries below reference [introspection relations](/sql/system-catalog/mz_internal/#replica-introspection-relations), which are subject to future changes without notice.

Due to the potential changes in introspection relations all relations below are created in the `mz_internal` schema, and it is not possible to create higher-level views dependent on them.
{{< /warning >}}

## Limitations

The contents of introspection relations differ depending on the selected cluster and replica.
As a consequence, you should expect the answers to the queries below to vary depending on which cluster you are working in.
In particular, indexes and dataflows are local to a cluster, so their introspection information will vary across clusters.

<!--
[//]: # "TODO(joacoc) We should share ways for the user to diagnose and troubleshoot if and how fast a source is consuming."
``` -->

## Why is Materialize running so slowly?

Materialize spends time in various dataflow operators maintaining
materialized views or indexes. If Materialize is taking more time to update
results than you expect, you can identify which operators
take the largest total amount of time.

```sql
-- Extract raw elapsed time information, summed across workers
SELECT mdo.id, mdo.name, mse.elapsed_ns
FROM mz_internal.mz_scheduling_elapsed AS mse,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mse.id = mdo.id
ORDER BY elapsed_ns DESC;
```

```sql
-- Extract raw elapsed time information, by worker
SELECT mdo.id, mdo.name, mse.worker_id, mse.elapsed_ns
FROM mz_internal.mz_scheduling_elapsed_per_worker AS mse,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mse.id = mdo.id
ORDER BY elapsed_ns DESC;
```

## Why is Materialize unresponsive for seconds at a time?

Materialize operators get scheduled and try to
behave themselves by returning control promptly, but
that doesn't always happen. These queries
reveal how many times each operator was scheduled for each
power-of-two elapsed time: high durations indicate an event
that took roughly that amount of time before it yielded,
and incriminate the subject.

```sql
-- Extract raw scheduling histogram information, summed across workers.
SELECT mdo.id, mdo.name, mcodh.duration_ns, mcodh.count
FROM mz_internal.mz_compute_operator_durations_histogram AS mcodh,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mcodh.id = mdo.id
ORDER BY mcodh.duration_ns DESC;
```

```sql
-- Extract raw scheduling histogram information, by worker.
SELECT mdo.id, mdo.name, mcodh.worker_id, mcodh.duration_ns, mcodh.count
FROM mz_internal.mz_compute_operator_durations_histogram_per_worker AS mcodh,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mcodh.id = mdo.id
ORDER BY mcodh.duration_ns DESC;
```

## Why is Materialize using so much memory?

Differential data flow structures called [arrangements](/overview/arrangements)
take up most of Materialize's memory use. Arrangements maintain indexes for data
as it changes. These queries extract the numbers of records and
batches backing each of the arrangements. The reported records may
exceed the number of logical records; the report reflects the uncompacted
state. The number of batches should be logarithmic-ish in this
number, and anything significantly larger is probably a bug.

```sql
-- Extract arrangement records and batches, summed across workers.
SELECT mdo.id, mdo.name, mas.records, mas.batches
FROM mz_internal.mz_arrangement_sizes AS mas,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mas.operator_id = mdo.id
ORDER BY mas.records DESC;
```

```sql
-- Extract arrangement records and batches, by worker.
SELECT mdo.id, mdo.name, mas.worker_id, mas.records, mas.batches
FROM mz_internal.mz_arrangement_sizes_per_worker AS mas,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mas.operator_id = mdo.id
ORDER BY mas.records DESC;
```

We've also bundled an interactive, web-based memory usage visualization tool to
aid in debugging. The memory visualization tool shows all user-created arrangements,
grouped by dataflow. The amount of memory used
by Materialize should correlate with the number of arrangement records that are
displayed by either the visual interface or the SQL queries.

You can access the memory usage visualization for your Materialize region at
`https://<region host>/memory`.

## Is work distributed equally across workers?

Work is distributed across workers by the hash of their keys. Thus, work can
become skewed if situations arise where Materialize needs to use arrangements
with very few or no keys. Example situations include:

* Views that maintain order by/limit/offset
* Cross joins
* Joins where the join columns have very few unique values

Additionally, the operators that implement data sources may demonstrate skew, as
they (currently) have a granularity determined by the source itself. For
example, Kafka topic ingestion work can become skewed if most of the data is in
only one out of multiple partitions.

```sql
-- Get operators where one worker has spent more than 2 times the average
-- amount of time spent. The number 2 can be changed according to the threshold
-- for the amount of skew deemed problematic.
SELECT
    mse.id,
    dod.name,
    mse.worker_id,
    elapsed_ns,
    avg_ns,
    elapsed_ns/avg_ns AS ratio
FROM
    mz_internal.mz_scheduling_elapsed_per_worker mse,
    (
        SELECT
            id,
            avg(elapsed_ns) AS avg_ns
        FROM
            mz_internal.mz_scheduling_elapsed_per_worker
        GROUP BY
            id
    ) aebi,
    mz_internal.mz_dataflow_operator_dataflows dod
WHERE
    mse.id = aebi.id AND
    mse.elapsed_ns > 2 * aebi.avg_ns AND
    mse.id = dod.id
ORDER BY ratio DESC;
```

## I found a problematic operator. Where did it come from?

Look up the operator in `mz_dataflow_addresses`. If an operator has
value `x` at position `n`, then it is part of the `x` subregion of the region
defined by positions `0..n-1`. The example SQL query and result below shows an
operator whose `id` is 515 that belongs to "subregion 5 of region 1 of dataflow
21".
```sql
SELECT * FROM mz_internal.mz_dataflow_addresses WHERE id=515;
```
```
 id  | worker_id | address
-----+-----------+----------
 515 |      0    | {21,1,5}
```

Usually, it is only important to know the name of the dataflow a problematic
operator comes from. Once the name is known, the dataflow can be correlated to
an index or materialized view in Materialize.

Each dataflow has an operator representing the entire dataflow. The address of
said operator has only a single entry. For the example operator 515 above, you
can find the name of the dataflow if you can find the name of the operator whose
address is just "dataflow 21."

```sql
-- get id and name of the operator representing the entirety of the dataflow
-- that a problematic operator comes from
SELECT
    mdo.id AS id,
    mdo.name AS name
FROM
    mz_internal.mz_dataflow_addresses mda,
    -- source of operator names
    mz_internal.mz_dataflow_operators mdo,
    -- view containing operators representing entire dataflows
    (SELECT
      mda.id AS dataflow_operator,
      mda.address[1] AS dataflow_address
    FROM
      mz_internal.mz_dataflow_addresses mda
    WHERE
      list_length(mda.address) = 1) dataflows
WHERE
    mda.id = 515
    AND mda.address[1] = dataflows.dataflow_address
    AND mdo.id = dataflows.dataflow_operator;
```

## How many `SUBSCRIBE` commands are running?

Materialize creates a dataflow using the `Dataflow: subscribe` prefix with a unique identifier **for each subscription running**.
Query the number of active `SUBSCRIBE` dataflows in Materialize by using the following statement:

```sql
SELECT count(1) FROM (
    SELECT id
    FROM mz_internal.mz_dataflows
    WHERE substring(name, 0, 20) = 'Dataflow: subscribe'
    GROUP BY id
);
```

## Which objects is a `SUBSCRIBE` command reading?

Each `SUBSCRIBE` command reads data from **objects** such as indexes, tables, sources, or materialized views.
Query the **object name and type** by issuing the following statement:

```sql
-- Subscriptions in execution:
WITH subscriptions AS (
    SELECT name, split_part(name, '-', 2) as dataflow
    FROM mz_internal.mz_dataflows
    WHERE substring(name, 0, 20) = 'Dataflow: subscribe'
),
-- Object dependency:
first_level_dependencies AS (
	SELECT name, dataflow, export_id, import_id
	FROM mz_internal.mz_compute_dependencies D
	JOIN subscriptions S ON (D.export_id = S.dataflow)
),
-- Second-level object dependency. In case the first dependency is an index:
second_level_dependencies AS (
	SELECT name, dataflow, D.export_id, D.import_id
	FROM mz_internal.mz_compute_dependencies D
	JOIN first_level_dependencies F ON (F.import_id = D.export_id)
),
-- All dependencies together but prioritizing second-level dependencies:
dependencies AS (
	SELECT *
	FROM first_level_dependencies
	WHERE first_level_dependencies.name NOT IN (SELECT name FROM second_level_dependencies)
	UNION ALL (
        SELECT *
        FROM
        second_level_dependencies
    )
)
-- Join the dependency id with the object name.
SELECT
    D.export_id,
    D.import_id,
    D.name,
    O.name as object_name,
    O.type
FROM dependencies D
JOIN mz_objects O ON (D.import_id = O.id);
```

## Why isn't my source ingesting data?

First, look for errors in [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses):

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

Query the `snapshot_comitted` field of the
[`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics) table:

```sql
SELECT bool_and(snapshot_committed) as snapshot_committed
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
    SUM(bytes_received) AS bytes_received,
    SUM(messages_received) AS messages_received,
    SUM(updates_staged) AS updates_staged,
    SUM(updates_committed) AS updates_committed
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

## Why isn't my sink exporting data?
First, look for errors in [`mz_sink_statuses`](/sql/system-catalog/mz_internal/#mz_sink_statuses):

```sql
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

```sql
SELECT
    SUM(messages_staged) AS messages_staged,
    SUM(messages_committed) AS messages_committed,
    SUM(bytes_staged) AS bytes_staged,
    SUM(bytes_committed) AS bytes_committed
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
