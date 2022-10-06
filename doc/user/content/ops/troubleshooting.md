---
title: "Troubleshooting"
description: "Troubleshoot performance issues."
aliases:
  - /ops/diagnosing-using-sql
---

You can use the queries below for spot debugging, but it may also be
helpful to monitor their output by [`SUBSCRIBE`ing](/sql/subscribe) to their change stream, e.g.,
by issuing a command `COPY (SUBSCRIBE (<query>)) TO stdout`.
Note that this additional monitoring may, however, itself affect performance.

### Limitations on Introspection Sources and Views for Troubleshooting

Importantly, the introspection sources and views used in the queries below
should be considered subject to changes in the future without further notice.
This is why these sources and views are all created in the `mz_internal`
schema, and it is not possible to create higher-level views depending on them.

Introspection sources are maintained by independently collecting internal logging
information within each of the replicas of a cluster. Thus, in a multi-replica
cluster, the queries below need to be directed to a specific replica by issuing
the command `SET cluster_replica = <replica_name>`. Note that once this command
is issued, all subsequent `SELECT` queries, for introspection sources or not, will
be directed to the targeted replica. The latter selection can be cancelled by
issuing the command `RESET cluster_replica`.

Materialize also directly exposes replica-specific introspection sources by
suffixing the respective catalog relation names with a replica ID that is unique
across clusters. For example, `mz_internal.mz_compute_frontiers_1` corresponds to
the introspection source `mz_internal.mz_compute_frontiers` in the replica with
the unique ID of `1`. A mapping of replica IDs to clusters and replica names is
provided by the [`mz_cluster_replicas`] system table.

As a consequence of the above, you should expect the answers to the queries below
to vary dependending on which cluster you are working in. In particular, indexes
and dataflows are local to a cluster, so their introspection information will
vary across clusters.

### How fast are my sources loading data?

You can count the number of records accepted in a source, materialized view,
or indexed view. Note that this makes less sense for a non-materialized,
non-indexed view, as invoking it will create a new dataflow and run it to
the point that it is caught up with its sources; that elapsed time may be
informative, but it tells you something other than how fast a collection
is populated.

```sql
-- Report the number of records available from the materialization.
SELECT count(*) FROM my_source_or_materialization;
```

The following introspection source indicates the upper frontier of materializations.

This source provides timestamp-based progress, which reveals not the
volume of data, but how closely the contents track source timestamps.
```sql
-- For each materialization, the next timestamp to be added.
SELECT * FROM mz_internal.mz_compute_frontiers;
```

You can also contrast the materialization's upper frontier with the
corresponding source object frontiers known by Materialize's compute
layer.
```sql
-- For each materialization, the next timestamps of the materialization and
-- its source objects.
SELECT *
FROM (
  SELECT mcd.export_id, mcd.import_id, mcd.worker_id,
         mfe.time AS export_time, mfi.time AS import_time
  FROM mz_internal.mz_worker_compute_dependencies AS mcd
       JOIN mz_internal.mz_worker_compute_frontiers AS mfe
           ON (mfe.export_id = mcd.export_id AND mfe.worker_id = mcd.worker_id)
       JOIN mz_internal.mz_worker_compute_frontiers AS mfi
           ON (mfi.export_id = mcd.import_id AND mfi.worker_id = mcd.worker_id)
  UNION
  SELECT mcd.export_id, mcd.import_id, mcd.worker_id,
         mfe.time AS export_time, mfi.time AS import_time
  FROM mz_internal.mz_worker_compute_dependencies AS mcd
       JOIN mz_internal.mz_worker_compute_frontiers AS mfe
           ON (mfe.export_id = mcd.export_id AND mfe.worker_id = mcd.worker_id)
       JOIN mz_internal.mz_worker_compute_import_frontiers AS mfi
           ON (mfi.import_id = mcd.import_id AND mfi.worker_id = mcd.worker_id
               AND mfi.export_id = mcd.export_id)
)
ORDER BY export_id, import_id;
```

The above query shows the currently known frontiers, but not the wall-clock
delays of propagating information within Materialize's compute layer once it
it read out from storage. For the latter, the following introspection source
is useful:
```sql
-- Histogram of wall-clock delays in propagating data obtained from storage
-- through materializations
SELECT * FROM mz_internal.mz_worker_compute_delays;
```

### Why is Materialize running so slowly?

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

### Why is Materialize unresponsive for seconds at a time?

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
```

### Why is Materialize using so much memory?

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

### Is work distributed equally across workers?

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

### I found a problematic operator. Where did it come from?

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

### How many `SUBSCRIBE` processes are running?

You can get the number of active `SUBSCRIBE` processes in Materialize using the statement below, or another `SUBSCRIBE` statement.
Every time `SUBSCRIBE` is invoked, a dataflow using the `Dataflow: subscribe` prefix is created.

```sql
-- Report the number of `SUBSCRIBE` queries running
SELECT count(1) FROM (
    SELECT id
    FROM mz_internal.mz_dataflows
    WHERE substring(name, 0, 20) = 'Dataflow: subscribe'
    GROUP BY id
);
```

[`mz_cluster_replicas`]: /sql/system-catalog/mz_catalog/#mz_cluster_replicas
