---
title: "Troubleshooting"
description: "Troubleshoot performance issues."
menu:
  main:
    parent: ops
    weight: 80
aliases:
  - /ops/diagnosing-using-sql
---

You can use the queries below for spot debugging, but it may also be
helpful to make them more permanent tools by creating them as views for `TAIL`ing or materialized views to read more efficiently. Note that the
existence of these additional views may itself affect performance.

### How fast are my sources loading data?

You can count the number of records accepted in a source or materialized view.
Note that this makes less sense for a non-materialized view,
as invoking it will create a new dataflow and run it to the point that
it is caught up with its sources; that elapsed time may be informative,
but it tells you something other than how fast a collection is populated.

```sql
-- Report the number of records available from the materialization.
select count(*) from my_source_or_materialized_view;
```

This logging source indicates the upper frontier of materializations.

This source provides timestamp-based progress, which reveals not the
volume of data, but how closely the contents track source timestamps.
```sql
-- For each materialization, the next timestamp to be added.
select * from mz_materialization_frontiers;
```

### Why is Materialize running so slowly?

Materialize spends time in various dataflow operators maintaining
materialized views. If Materialize is taking more time to update results than you expect, you can identify which operators
take the largest total amount of time.

```sql
-- Extract raw elapsed time information, by worker
select mdo.id, mdo.name, mdo.worker, mse.elapsed_ns
from mz_scheduling_elapsed as mse,
     mz_dataflow_operators as mdo
where
    mse.id = mdo.id and
    mse.worker = mdo.worker
order by elapsed_ns desc;
```

```sql
-- Extract raw elapsed time information, summed across workers
select mdo.id, mdo.name, sum(mse.elapsed_ns) as elapsed_ns
from mz_scheduling_elapsed as mse,
     mz_dataflow_operators as mdo
where
    mse.id = mdo.id and
    mse.worker = mdo.worker
group by mdo.id, mdo.name
order by elapsed_ns desc;
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
select mdo.id, mdo.name, mdo.worker, msh.duration_ns, count
from mz_scheduling_histogram as msh,
     mz_dataflow_operators as mdo
where
    msh.id = mdo.id and
    msh.worker = mdo.worker
order by msh.duration_ns desc;
```

```sql
-- Extract raw scheduling histogram information, summed across workers.
select mdo.id, mdo.name, msh.duration_ns, sum(msh.count) count
from mz_scheduling_histogram as msh,
     mz_dataflow_operators as mdo
where
    msh.id = mdo.id and
    msh.worker = mdo.worker
group by mdo.id, mdo.name, msh.duration_ns
order by msh.duration_ns desc;
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
select mdo.id, mdo.name, mdo.worker, mas.records, mas.batches
from mz_arrangement_sizes as mas,
     mz_dataflow_operators as mdo
where
    mas.operator = mdo.id and
    mas.worker = mdo.worker
order by mas.records desc;
```

```sql
-- Extract arrangement records and batches, summed across workers.
select mdo.id, mdo.name, sum(mas.records) as records, sum(mas.batches) as batches
from mz_arrangement_sizes as mas,
     mz_dataflow_operators as mdo
where
    mas.operator = mdo.id and
    mas.worker = mdo.worker
group by mdo.id, mdo.name
order by sum(mas.records) desc;
```

We've also bundled an interactive, web-based memory usage visualization tool to
aid in debugging. The SQL queries above show all arrangements in Materialize
(including system arrangements), whereas the memory visualization tool shows
only user-created arrangements, grouped by dataflow. The amount of memory used
by Materialize should correlate with the number of arrangement records that are
displayed by either the visual interface or the SQL queries.

The memory usage visualization is available at `http://<materialized
host>:6875/memory`.

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
-- Average the total time spent by each operator across all workers.
create view avg_elapsed_by_id as
select
    id,
    avg(elapsed_ns) as avg_ns
from
    mz_scheduling_elapsed
group by
    id;

-- Get operators where one worker has spent more than 2 times the average
-- amount of time spent. The number 2 can be changed according to the threshold
-- for the amount of skew deemed problematic.
select
    mse.id,
    dod.name,
    mse.worker,
    elapsed_ns,
    avg_ns,
    elapsed_ns/avg_ns as ratio
from
    mz_scheduling_elapsed mse,
    avg_elapsed_by_id aebi,
    mz_dataflow_operator_dataflows dod
where
    mse.id = aebi.id and
    mse.elapsed_ns > 2 * aebi.avg_ns and
    mse.id = dod.id and
    mse.worker = dod.worker
order by ratio desc;
```

### I found a problematic operator. Where did it come from?

Look up the operator in `mz_dataflow_operator_addresses`. If an operator has
value `x` at position `n`, then it is part of the `x` subregion of the region
defined by positions `0..n-1`. The example SQL query and result below shows an
operator whose `id` is 515 that belongs to "subregion 5 of region 1 of dataflow
21".
```sql
select * from mz_dataflow_operator_addresses where id=515 and worker=0;
```
```
 id  | worker | address
-----+--------+----------
 515 |      0 | {21,1,5}
```

Usually, it is only important to know the name of the dataflow a problematic
operator comes from. Once the name is known, the dataflow can be correlated to
an index or view in Materialize.

Each dataflow has an operator representing the entire dataflow. The address of
said operator has only a single entry. For the example operator 515 above, you
can find the name of the dataflow if you can find the name of the operator whose
address is just "dataflow 21."

```sql
-- get id and name of the operator representing the entirety of the dataflow
-- that a problematic operator comes from
SELECT
    mdo.id as id,
    mdo.name as name
FROM
    mz_dataflow_operator_addresses mdoa,
    -- source of operator names
    mz_dataflow_operators mdo,
    -- view containing operators representing entire dataflows
    (SELECT
      mdoa.id as dataflow_operator,
      mdoa.address[1] as dataflow_address
    FROM
      mz_dataflow_operator_addresses mdoa
    WHERE
      mdoa.worker = 0
      AND list_length(mdoa.address) = 1) dataflows
WHERE
    mdoa.worker = 0
    AND mdoa.id = <problematic_operator_id>
    AND mdoa.address[1] = dataflows.dataflow_address
    AND mdo.id = dataflows.dataflow_operator
    AND mdo.worker = 0;
```

### How much disk space is Materialize using?

To see how much disk space a Materialize installation is using, open a terminal and enter:

```nofmt
$ du -h -d 1 /path/to/materialize/mzdata
```
`materialize` is the directory for the Materialize installation, and  `materialize/mzdata` is the directory where Materialize stores its log file and the [system catalog](/sql/system-catalog).

The response lists the disk space for the data directory and any subdirectories:

```nofmt
2.8M	mzdata/persist
2.9M	mzdata
```

The `mzdata` directory is typically less than 10MB in size.

### How many `TAIL` processes are running?

You can get the number of active `TAIL` processes in Materialize using the statement below, or another `TAIL` statement.
Every time `TAIL` is invoked, a dataflow using the `Dataflow: tail` prefix is created.

```sql
-- Report the number of tails running
SELECT count(1) FROM (
    SELECT id
    FROM mz_dataflow_names
    WHERE substring(name, 0, 15) = 'Dataflow: tail'
    GROUP BY id
);
```
