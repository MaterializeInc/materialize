## Diagnostic queries that can help understand apparent performance defects.

This is a work in progress, and sections are meant to be thematically
related based on what problems are experienced.

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