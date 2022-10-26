---
title: "Troubleshooting"
description: "Troubleshoot issues."
menu:
  main:
    parent: ops
    weight: 70
aliases:
  - /ops/diagnosing-using-sql
---

This page describes several SQL queries you can run to diagnose performance
issues with Materialize.

{{< warning >}}
The introspection sources and views used in the queries below are subject to
future changes without notice.

This is why these sources and views are all created in the `mz_internal` schema,
and why it is not possible to create higher-level views depending on them.
{{< /warning >}}

## Limitations

Introspection sources are maintained by independently collecting internal logging
information within each of the replicas of a cluster. Thus, in a multi-replica
cluster, the queries below need to be directed to a specific replica by issuing
the command `SET cluster_replica = <replica_name>`. Note that once this command
is issued, all subsequent `SELECT` queries, for introspection sources or not, will
be directed to the targeted replica. The latter selection can be cancelled by
issuing the command `RESET cluster_replica`.

As a consequence of the above, you should expect the answers to the queries below
to vary dependending on which cluster you are working in. In particular, indexes
and dataflows are local to a cluster, so their introspection information will
vary across clusters.

<!--
[//]: # "TODO(joacoc) We should share ways for the user to diagnose and troubleshoot if and how fast a source is consuming."
``` -->

## Why is Materialize running so slowly?

Materialize spends time in various dataflow operators maintaining
materialized views or indexes. If Materialize is taking more time to update
results than you expect, you can identify which operators
take the largest total amount of time.

```sql
-- Extract raw elapsed time information, by worker
SELECT mdo.id, mdo.name, mdo.worker_id, mse.elapsed_ns
FROM mz_internal.mz_scheduling_elapsed AS mse,
     mz_internal.mz_dataflow_operators AS mdo
WHERE
    mse.id = mdo.id AND
    mse.worker_id = mdo.worker_id
ORDER BY elapsed_ns DESC;
```

```sql
-- Extract raw elapsed time information, summed across workers
SELECT mdo.id, mdo.name, sum(mse.elapsed_ns) AS elapsed_ns
FROM mz_internal.mz_scheduling_elapsed AS mse,
     mz_internal.mz_dataflow_operators AS mdo
WHERE
    mse.id = mdo.id AND
    mse.worker_id = mdo.worker_id
GROUP BY mdo.id, mdo.name
ORDER BY elapsed_ns DESC;
```

<!-- mz_raw_compute_operator_durations is not available yet. -->
<!-- ### Why is Materialize unresponsive for seconds at a time?

Materialize operators get scheduled and try to
behave themselves by returning control promptly, but
that doesn't always happen. These queries
reveal how many times each operator was scheduled for each
power-of-two elapsed time: high durations indicate an event
that took roughly that amount of time before it yielded,
and incriminate the subject.

```sql
-- Extract raw scheduling histogram information, by worker.
SELECT mdo.id, mdo.name, mdo.worker_id, mrcod.duration_ns, count
FROM mz_internal.mz_raw_compute_operator_durations AS mrcod,
     mz_internal.mz_dataflow_operators AS mdo
WHERE
    mrcod.id = mdo.id AND
    mrcod.worker_id = mdo.worker_id
ORDER BY mrcod.duration_ns DESC;
```

```sql
-- Extract raw scheduling histogram information, summed across workers.
SELECT mdo.id, mdo.name, mrcod.duration_ns, sum(mrcod.count) count
FROM mz_internal.mz_raw_compute_operator_durations AS mrcod,
     mz_internal.mz_dataflow_operators AS mdo
WHERE
    mrcod.id = mdo.id AND
    mrcod.worker_id = mdo.worker_id
GROUP BY mdo.id, mdo.name, mrcod.duration_ns
ORDER BY mrcod.duration_ns DESC;
``` -->

## Why is Materialize using so much memory?

The majority of Materialize's memory use is taken up by "arrangements", which
are differential dataflow structures that maintain indexes for data
as it changes. These queries extract the numbers of records and
batches backing each of the arrangements. The reported records may
exceed the number of logical records; the report reflects the uncompacted
state. The number of batches should be logarithmic-ish in this
number, and anything significantly larger is probably a bug.

```sql
-- Extract arrangement records and batches, by worker.
SELECT mdo.id, mdo.name, mdo.worker_id, mas.records, mas.batches
FROM mz_internal.mz_arrangement_sizes AS mas,
     mz_internal.mz_dataflow_operators AS mdo
WHERE
    mas.operator_id = mdo.id AND
    mas.worker_id = mdo.worker_id
ORDER BY mas.records DESC;
```

```sql
-- Extract arrangement records and batches, summed across workers.
SELECT mdo.id, mdo.name, sum(mas.records) AS records, sum(mas.batches) AS batches
FROM mz_internal.mz_arrangement_sizes AS mas,
     mz_internal.mz_dataflow_operators AS mdo
WHERE
    mas.operator_id = mdo.id AND
    mas.worker_id = mdo.worker_id
GROUP BY mdo.id, mdo.name
ORDER BY sum(mas.records) DESC;
```

We've also bundled an interactive, web-based memory usage visualization tool to
aid in debugging. The memory visualization tool shows all user-created arrangements,
grouped by dataflow. The amount of memory used
by Materialize should correlate with the number of arrangement records that are
displayed by either the visual interface or the SQL queries.

The memory usage visualization is available at `http://<materialized
host>:6876/memory`.

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
    mz_internal.mz_scheduling_elapsed mse,
    (
        SELECT
            id,
            avg(elapsed_ns) AS avg_ns
        FROM
            mz_internal.mz_scheduling_elapsed
        GROUP BY
            id
    ) aebi,
    mz_internal.mz_dataflow_operator_dataflows dod
WHERE
    mse.id = aebi.id AND
    mse.elapsed_ns > 2 * aebi.avg_ns AND
    mse.id = dod.id AND
    mse.worker_id = dod.worker_id
ORDER BY ratio DESC;
```

## I found a problematic operator. Where did it come from?

Look up the operator in `mz_dataflow_addresses`. If an operator has
value `x` at position `n`, then it is part of the `x` subregion of the region
defined by positions `0..n-1`. The example SQL query and result below shows an
operator whose `id` is 515 that belongs to "subregion 5 of region 1 of dataflow
21".
```sql
SELECT * FROM mz_internal.mz_dataflow_addresses WHERE id=515 AND worker_id=0;
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
      mda.worker_id = 0
      AND list_length(mda.address) = 1) dataflows
WHERE
    mda.worker_id = 0
    AND mda.id = <problematic_operator_id>
    AND mda.address[1] = dataflows.dataflow_address
    AND mdo.id = dataflows.dataflow_operator
    AND mdo.worker_id = 0;
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
	SELECT name, dataflow, export_id, import_id, worker_id
	FROM mz_internal.mz_worker_compute_dependencies D
	JOIN subscriptions S ON (D.export_id = S.dataflow)
),
-- Second-level object dependency. In case the first dependency is an index:
second_level_dependencies AS (
	SELECT name, dataflow, D.export_id, D.import_id, D.worker_id
	FROM mz_internal.mz_worker_compute_dependencies D
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
SELECT DISTINCT
    D.export_id,
    D.import_id,
    D.name,
    O.name as object_name,
    O.type
FROM dependencies D
JOIN mz_objects O ON (D.import_id = O.id);
```
