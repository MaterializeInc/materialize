---
title: "Diagnosing Using SQL"
description: "Use the SQL Interface to understand performance defects"
menu:
  main:
    parent: operations
---

## Diagnostic queries that can help understand apparent performance defects.

Sections are meant to be thematically related based on what problems are
experienced.

The queries may be useful for spot debugging, but it may also be smart
to create them as views and TAIL them, or as materialized views to read
them more efficiently (understand that any of these have the potential
to impact the system itself).

### Are my sources loading data in a reasonable fashion?

Count the number of records accepted in a materialized source or view.
Note that this makes less sense for a non-materialized source or view,
as invoking it will create a new dataflow and run it to the point that
it is caught up with its sources; that elapsed time may be informative,
but it tells you something other than how fast a collection is populated.

```sql
-- Report the number of records available from the materialization.
select count(*) from my_materialized_source_or_view;
```

This logging source indicates the upper frontier of materializations.

This source provides timestamp-based progress, which reveals not the
volume of data, but how closely the contents track source timestamps.
```sql
-- For each materialization, the next timestamp to be added.
select * from mz_materialization_frontiers;
```

### It seems like things aren't getting done as fast as I would like!

Materialize spends time in various dataflow operators maintaining
materialized views. The amount of time may be more than one expects,
either because Materialize is behaving badly or because expectations
aren't aligned with reality. These queries reveal which operators
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

### Materialize becomes unresponsive for seconds at a time!

What causes Materialize to take control away for seconds
at a time? Materialize operators get scheduled and try to
behave themselves by returning control promptly, but for
various reasons that doesn't always happen. These queries
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

### Materialize is using lots of memory! What gives?

The majority of Materialize's memory live in "arrangements", which
are differential dataflow structures that maintain indexes for data
as they change. These queries extract the numbers of records and
batches backing each of the arrangements. The reported records may
exceed the number of logical records, as they reflect un-compacted
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

We've also bundled a [memory usage visualization tool](https://materialize.com/docs/ops/monitoring/#memory-usage-visualization)
to aid in debugging. The sql queries above show all arrangements in Materialize
(including system arrangements), whereas the memory visualization tool shows
only user-created arrangements, and it shows them by dataflow. The amount of
memory used by Materialize should correlate with the number of arrangement
records that are displayed by either the visual interface or the sql queries.

### How can I check whether work is distributed equally across workers?

Work is distributed across workers by the hash of their keys. Thus, work can
become skewed if situations arise where Materialize needs to use arrangements
with very few or no keys. Example situations include:
* views that maintain order by/limit/offset
* cross joins
* joins where the join columns have very few unique values.

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
    mse.id = dod.id
order by ratio desc;
```

### I found a problematic operator! How do I find where it came from?

Look up the operator in `mz_dataflow_operator_addresses`. If an operator has
value `x` at position `n`, then it is part of the `x` subregion of the region
defined by positions `0..n-1`. The example SQL query and result below shows an
operator whose id is 515 that belongs to "subregion 5 of region 1 of dataflow
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
