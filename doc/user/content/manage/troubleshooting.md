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

This page describes several SQL queries you can run to diagnose performance issues with Materialize.

## Mental model and basic terminology

When you create a materialized view and issue a query, Materialize creates a dataflow. A dataflow consists of instructions on how to respond to data input and to changes to that data. Once executed, the dataflow computes the result of the SQL query, polls the source for updates, and then incrementally updates the query results when new data arrives.

Materialize dataflows act on collections of data. A collection provides a data stream of updates as they happen. To provide fast access to the changes to individual records, the collection can be represented in an alternate form, indexed on data to present the sequence of changes the collection has undergone. This indexed representation is called an arrangement.


### Translating SQL to Differential Dataflow

To make these concepts a bit more tangible, let's look at the example from the getting started guide.

```sql
SELECT auctions.item, avg(bids.amount) AS average_bid
FROM bids
JOIN auctions ON bids.auction_id = auctions.id
WHERE bids.bid_time < auctions.end_time
GROUP BY auctions.item
```

This query from the guide joins the relations `bids` and `auctions`, groups by `auctions.item` and determines the average `bids.amount` per auction. To understand how this SQL query is translated to differential dataflow, we can use `EXPLAIN` to display the plan used to evaluate the join.

```sql
EXPLAIN 
SELECT auctions.item, avg(bids.amount) AS average_bid
FROM bids
JOIN auctions ON bids.auction_id = auctions.id
WHERE bids.bid_time < auctions.end_time
GROUP BY auctions.item
```

```noftm
                                        Optimized Plan
-----------------------------------------------------------------------------------------------
 Explained Query:                                                                             +
   Project (#0, #3)                                                                           +
     Map ((bigint_to_double(#1) / bigint_to_double(case when (#2 = 0) then null else #2 end)))+
       Reduce group_by=[#1] aggregates=[sum(#0), count(*)]                                    +
         Project (#1, #4)                                                                     +
           Filter (#2 < #5)                                                                   +
             Join on=(#0 = #3) type=differential                                              +
               ArrangeBy keys=[[#0]]                                                          +
                 Project (#2..=#4)                                                            +
                   Get materialize.qck.bids                                                   +
               ArrangeBy keys=[[#0]]                                                          +
                 Project (#0, #2, #3)                                                         +
                   Get materialize.qck.auctions                                               +

(1 row)
```

The plan describes the specific Differential Dataflow operators that are used to evaluate the query.
Some of these operators resemble relational algebra or map reduce style operators (`Filter`, `Join on`, `Map`). 
Others are specific to differential dataflow (`Get`, `ArrangeBy`).

In general, a high level understanding of what these operators do is sufficient for effective debugging:
`Filter` filters records, `Join on` joins records from two or more inputs, `Map` applies a function to transform records, etc.
The `ArrangeBy` operator indexes data and stores it in memory for fast point lookups.
It's not important to have a deep understanding of all these operators for effective debugging, though.

Behind the scenes, the operator graph is turned into a dataflow that can be evaluated by the Differential Dataflow runtime.
The dataflow is organized in a hierarchical structure. 
It contains operators and so-called regions, which in turn contain operators and (sub)regions.

In our example, the dataflow contains a InputRegion and a BuildRegion.
And the BuildRegion contains the operators from the above plan.

![Regions and operator visualization](/images/regions-and-operators.png)

Again, it's not too important for our purposes to understand what these regions do and how they are used to structure the operator graph.
For our purposes it's merely important to know than that they define a hierarchy on the operators.



## The system catalog and introspection relations

Materialize collects a lot of useful information about the dataflows and operators in the System Catalog in [introspection relations](/sql/system-catalog/mz_internal/#replica-introspection-relations).
The introspection relations are extremely valuable to troubleshoot and to understand what is happening under the hood,
in particular if Materialize is not behaving in the expected way.
However, it is important to understand that most of the statistics we need for troubleshooting purposes are specific to the cluster that is running the queries we want to debug.

{{< warning >}}
The contents of introspection relations differ depending on the selected cluster and replica.
As a consequence, you should expect the answers to the queries below to vary depending on which cluster you are working in.
In particular, indexes and dataflows are local to a cluster, so their introspection information will vary across clusters.
{{< /warning >}}

<!--
[//]: # "TODO(joacoc) We should share ways for the user to diagnose and troubleshoot if and how fast a source is consuming."
``` -->

## Where is Materialize spending time on computations?

Materialize spends time in various dataflow operators maintaining
materialized views or indexes. If Materialize is taking more time to update
results than you expect, you can identify which operators
take the largest total amount of time.

### Finding expensive queries/dataflows

To understand which query or dataflow, respectively, is taking the most time we can query the `mz_scheduling_elapsed` relation.
The `elapsed_time` metric shows the absolute time the dataflows was busy since the system started and the dataflows was created.

```sql
-- Extract raw elapsed time information for dataflows
SELECT
    mdo.id,
    mdo.name,
    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
FROM mz_internal.mz_scheduling_elapsed AS mse,
    mz_internal.mz_dataflow_operators AS mdo,
    mz_internal.mz_dataflow_addresses AS mda
WHERE mse.id = mdo.id AND mdo.id = mda.id AND list_length(address) = 1
ORDER BY elapsed_ns DESC
```
<p></p>
```noftm
 id  |                  name                  |  elapsed_time
-----+----------------------------------------+-----------------
 354 | Dataflow: materialize.qck.num_bids     | 00:11:05.107351
 180 | Dataflow: materialize.qck.avg_bids_idx | 00:09:34.29102
 578 | Dataflow: materialize.qck.num_bids_idx | 00:01:25.363355
(3 rows)
```

These results show that the system spend the most time keeping the materialized view `num_bids` (from the quick start) up to date.
Followed by the work on the index `avg_bids_idx` that has been defined on a view from the query above.

### Finding expensive operators within a dataflow

This result only shows dataflows and is a good starting point to get an overview of the work happening in the cluster.
The `mz_scheduling_elapsed` relation also contains details for regions and operators.
Be aware that every row shows aggregated times for all the element it contains.
The `elapsed_time` of the dataflows above also includes the time of all the regions and operators they contain.

Removing the condition `list_length(address) = 1` will include the regions and operators in the result.
But because the parent child relationship is not always obvious, the results can be a bit hard to interpret.
It can therefore make sense to only focus on operators and filter for a specific dataflow.

```sql
-- Extract raw elapsed time information for operators
WITH leaves AS (
    SELECT *
    FROM mz_internal.mz_dataflow_addresses AS outer
    WHERE
        outer.address NOT IN (
            SELECT inner.address[:list_length(outer.address)]
            FROM mz_internal.mz_dataflow_addresses AS inner
            WHERE inner.id != outer.id
        )
)

SELECT
    mdo.id,
    mdo.name,
    mdod.dataflow_name,
    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
FROM mz_internal.mz_scheduling_elapsed AS mse,
    mz_internal.mz_dataflow_operators AS mdo,
    mz_internal.mz_dataflow_operator_dataflows AS mdod
WHERE mse.id = mdo.id AND mdo.id = mdod.id AND mdo.id IN (SELECT id FROM leaves)
ORDER BY elapsed_ns DESC
```
<p></p>
```noftm
 id  |               name               |             dataflow_name              |  elapsed_time
-----+----------------------------------+----------------------------------------+-----------------
 431 | ArrangeBy[[Column(0)]]           | Dataflow: materialize.qck.num_bids     | 00:06:17.074208
 257 | ArrangeBy[[Column(0)]]           | Dataflow: materialize.qck.avg_bids_idx | 00:06:16.302804
 268 | ArrangeBy[[Column(0)]]           | Dataflow: materialize.qck.avg_bids_idx | 00:00:34.975043
 442 | ArrangeBy[[Column(0)]]           | Dataflow: materialize.qck.num_bids     | 00:00:34.403058
 528 | shard_source_fetch(u517)         | Dataflow: materialize.qck.num_bids     | 00:00:29.972871
 594 | shard_source_fetch(u517)         | Dataflow: materialize.qck.num_bids_idx | 00:00:27.517982
 230 | shard_source_fetch(u510)         | Dataflow: materialize.qck.avg_bids_idx | 00:00:25.494895
 404 | shard_source_fetch(u510)         | Dataflow: materialize.qck.num_bids     | 00:00:22.286794
 566 | persist_sink u517 append_batches | Dataflow: materialize.qck.num_bids     | 00:00:17.657125
 196 | shard_source_fetch(u509)         | Dataflow: materialize.qck.avg_bids_idx | 00:00:08.692289
 ...
```

## Why is Materialize unresponsive and where is it currently spending time?

The `elapsed_time` is a nice indicator for the most expensive dataflows and operators.
A large class of problems can be identified by just looking at this metric.
However, `elapsed_time` contains all work since the operator or dataflow was first created.
Sometimes, a lot of work happens initially when the operator is created, but later on it takes only little continuous effort.
If you are interested in what operator is taking the most time right now, 
it can be a bit challenging to get that information from the `elapsed_time` metric.

The relation `mz_compute_operator_durations_histogram` also tracks the time operators are busy,
but instead of aggregating `elapsed_time` since an operator got created,
it tracks how long each operator was scheduled at a time in a histogram.
This information can show you two things: operators that block progress for others and operators that are currently doing work.

If there is a very expensive operator that blocks progress for all other operators,
it will become visible in the histogram.
The offending operator will be scheduled much longer at a time compared to other operators.
Therefore the values of the time buckets in the histogram will be much higher
compared to the other operators.

```sql
-- Extract raw scheduling histogram information for operators
WITH leaves AS (
    SELECT *
    FROM mz_internal.mz_dataflow_addresses AS outer
    WHERE
        outer.address NOT IN (
            SELECT inner.address[:list_length(outer.address)]
            FROM mz_internal.mz_dataflow_addresses AS inner
            WHERE inner.id != outer.id
        )
),

histograms AS (
    SELECT
        mdo.id,
        mdo.name,
        mdod.dataflow_name,
        mcodh.count,
        mcodh.duration_ns / 1000 * '1 MICROSECONDS'::interval AS duration
    FROM mz_internal.mz_compute_operator_durations_histogram AS mcodh,
        mz_internal.mz_dataflow_operators AS mdo,
        mz_internal.mz_dataflow_operator_dataflows AS mdod
    WHERE
        mcodh.id = mdo.id
        AND mdo.id = mdod.id
        AND mdo.id IN (SELECT id FROM leaves)
)

SELECT *
FROM histograms
WHERE duration > '100 millisecond'::interval
ORDER BY duration DESC
```
<p></p>
```noftm
 id  |                 name                 |             dataflow_name              | count |    duration
-----+--------------------------------------+----------------------------------------+-------+-----------------
 234 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.avg_bids_idx |     1 | 00:00:01.073741
 234 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.avg_bids_idx |     1 | 00:00:00.53687
 255 | FormArrangementKey                   | Dataflow: materialize.qck.avg_bids_idx |     1 | 00:00:00.268435
 200 | persist_source::decode_and_mfp(u509) | Dataflow: materialize.qck.avg_bids_idx |     1 | 00:00:00.268435
 234 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.avg_bids_idx |     2 | 00:00:00.268435
 255 | FormArrangementKey                   | Dataflow: materialize.qck.avg_bids_idx |     2 | 00:00:00.134217
 408 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.num_bids     |     1 | 00:00:01.073741
 408 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.num_bids     |     1 | 00:00:00.53687
 374 | persist_source::decode_and_mfp(u509) | Dataflow: materialize.qck.num_bids     |     1 | 00:00:00.268435
 408 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.num_bids     |     2 | 00:00:00.268435
 429 | FormArrangementKey                   | Dataflow: materialize.qck.num_bids     |     2 | 00:00:00.134217
(11 rows)
```

Note that this relation contains a lot of information.
The query therefore filters all duration below `100 millisecond`.

This is still reporting aggregated values since the operator has been created.
To get a feeling for which operators are currently doing work,
you can subscribe to the changes of the relation.

```sql
-- Observe changes to the raw scheduling histogram information
COPY(SUBSCRIBE(
    WITH leaves AS (
        SELECT *
        FROM mz_internal.mz_dataflow_addresses AS outer
        WHERE
            outer.address NOT IN (
                SELECT inner.address[:list_length(outer.address)]
                FROM mz_internal.mz_dataflow_addresses AS inner
                WHERE inner.id != outer.id
            )
    ),

    histograms AS (
        SELECT
            mdo.id,
            mdo.name,
            mdod.dataflow_name,
            mcodh.count,
            mcodh.duration_ns / 1000 * '1 microseconds'::interval AS duration
        FROM mz_internal.mz_compute_operator_durations_histogram AS mcodh,
            mz_internal.mz_dataflow_operators AS mdo,
            mz_internal.mz_dataflow_operator_dataflows AS mdod
        WHERE
            mcodh.id = mdo.id
            AND mdo.id = mdod.id
            AND mdo.id IN (SELECT id FROM leaves)
    )

    SELECT *
    FROM histograms
    WHERE duration > '100 milliseconds'::interval
    ORDER BY dataflow_name ASC, duration DESC
) WITH (SNAPSHOT = false, PROGRESS)) TO STDOUT;
```
<p></p>
```noftm
1691667343000	t	\N	\N	\N	\N	\N	\N
1691667344000	t	\N	\N	\N	\N	\N	\N
1691667344000	f	-1	431	ArrangeBy[[Column(0)]]	Dataflow: materialize.qck.num_bids	7673	00:00:00.104800
1691667344000	f	1	431	ArrangeBy[[Column(0)]]	Dataflow: materialize.qck.num_bids	7674	00:00:00.104800
1691667345000	t	\N	\N	\N	\N	\N	\N...
```

In this way you can see that currently the only operator that is doing more than 100 milliseconds worth of work
is the `ArrangeBy` operator form the materialized view `num_bids`.


## Why is Materialize using so much memory?

Differential data flow structures called [arrangements](/overview/arrangements)
take up most of Materialize's memory use. Arrangements maintain indexes for data
as it changes. These queries extract the numbers of records and
batches backing each of the arrangements. The reported records may
exceed the number of logical records; the report reflects the uncompacted
state.

```sql
-- Extract arrangement records and batches
SELECT
    mdo.id,
    mdo.name,
    mdod.dataflow_name,
    mas.records
FROM mz_internal.mz_arrangement_sizes AS mas,
    mz_internal.mz_dataflow_operators AS mdo,
    mz_internal.mz_dataflow_operator_dataflows AS mdod
WHERE mas.operator_id = mdo.id AND mdo.id = mdod.id
ORDER BY mas.records DESC
```
<p></p>
```noftm
 id  |          name          |             dataflow_name              | records
-----+------------------------+----------------------------------------+---------
 431 | ArrangeBy[[Column(0)]] | Dataflow: materialize.qck.num_bids     |  748128
 257 | ArrangeBy[[Column(0)]] | Dataflow: materialize.qck.avg_bids_idx |  748128
 442 | ArrangeBy[[Column(0)]] | Dataflow: materialize.qck.num_bids     |  135715
 268 | ArrangeBy[[Column(0)]] | Dataflow: materialize.qck.avg_bids_idx |  135715
 340 | ArrangeBy[[Column(0)]] | Dataflow: materialize.qck.avg_bids_idx |      17
 619 | ArrangeBy[[Column(0)]] | Dataflow: materialize.qck.num_bids_idx |      17
 486 | ReduceAccumulable      | Dataflow: materialize.qck.num_bids     |       5
 484 | ArrangeAccumulable     | Dataflow: materialize.qck.num_bids     |       5
 312 | ReduceAccumulable      | Dataflow: materialize.qck.avg_bids_idx |       5
 310 | ArrangeAccumulable     | Dataflow: materialize.qck.avg_bids_idx |       5
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
