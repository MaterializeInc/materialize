# Overview

Learn how to efficiently transform data using Materialize SQL.



With Materialize, you can use SQL to transform, deliver, and act on
fast-changing data.

<p>Materialize follows the SQL standard (SQL-92) implementation and aims for
compatibility with the PostgreSQL dialect. It <strong>does not</strong> aim for
compatibility with a specific version of PostgreSQL. This means that
Materialize might support syntax from any released PostgreSQL version, but does
not provide full coverage of the PostgreSQL dialect. The implementation and
performance of specific features (like <a href="/transform-data/idiomatic-materialize-sql/appendix/window-function-to-materialize" >window functions</a>)
might also differ, because Materialize uses an entirely different database
engine based on <a href="/get-started/#incremental-updates" >Timely and Differential Dataflow</a>.</p>
<p>If you need specific syntax or features that are not currently supported in
Materialize, please submit a <a href="/support/#share-your-feedback" >feature request</a>.</p>


### SELECT statement

To build your transformations, you can [`SELECT`](/sql/select/) from
[sources](/concepts/sources/), tables, [views](/concepts/views/#views), and
[materialized views](/concepts/views/#materialized-views).

```mzsql
SELECT [ ALL | DISTINCT [ ON ( col_ref [, ...] ) ] ]
    [ { * | projection_expr [ [ AS ] output_name ] } [, ...] ]
    [ FROM table_expr [ join_expr | , ] ... ]
    [ WHERE condition_expr ]
    [ GROUP BY grouping_expr [, ...] ]
    [ OPTIONS ( option = val[, ...] ) ]
    [ HAVING having_expr ]
    [ ORDER BY projection_expr [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ] [, ...] ]
    [ LIMIT { integer  } [ OFFSET { integer } ] ]
    [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] { SELECT ...} ]
```

In Materialize, the [`SELECT`](/sql/select/) statement supports (among others):

- [JOINS (inner, left outer, right outer, full outer,
  cross)](/sql/select/join/) and [lateral
  subqueries](/sql/select/join/#lateral-subqueries)

- [Common Table Expressions (CTEs)](/sql/select/#common-table-expressions-ctes)
  and [Recursive CTEs](/sql/select/recursive-ctes/)

- [Query hints (`AGGREGATE INPUT GROUP SIZE`, `DISTINCT ON INPUT GROUP SIZE`,
  `LIMIT INPUT GROUP SIZE`)](/sql/select/#query-hints)

- [SQL functions](/sql/functions/) and [operators](/sql/functions/#operators)

For more information, see:

- [`SELECT` reference page](/sql/select/)

- [Query optimization](/transform-data/optimization/)

### Views and materialized views

A view represent queries that are saved under a name for reference.

```mzsql
CREATE VIEW my_view_name AS
SELECT ...   ;
```

In Materialize, you can create [indexes](/concepts/indexes/#indexes-on-views) on
views. When you to create an index on a view, the underlying query is executed
and the results are stored in memory within the [cluster](/concepts/clusters/)
you create the index. As new data arrives, Materialize incrementally updates the
view results.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

You can also create materialized views. A materialized view is a view whose
results are persisted in durable storage. As new data arrives, Materialize
incrementally updates the view results.

```mzsql
CREATE MATERIALIZED VIEW my_mat_view_name AS
SELECT ...  ;
```

You can also create an index on a materialized view to make the results
available in memory within the cluster you create the index.

For more information, see:

- [Views](/concepts/views/)
- [Indexes](/concepts/indexes/)
- [Indexed views vs materialized
  views](/concepts/views/#indexed-views-vs-materialized-views)

### Indexes

In Materialize, [indexes](/concepts/indexes/) represent query results stored in
memory within a cluster. By making up-to-date view results available in memory,
indexes can help improve performance within the cluster. Indexes can also help
[optimize query performance](/transform-data/optimization/).

For more information, see:

- [Indexes](/concepts/indexes)
- [Indexed views vs materialized
  views](/concepts/indexes/#indexes-on-views-vs-materialized-views)
- [Indexes: Best practices](/concepts/indexes/#best-practices)



---

## Dataflow troubleshooting


If you're unable to troubleshoot your issue using the [`Ingest data`](/ingest-data/troubleshooting/)
and [`Transform data`](/transform-data/troubleshooting/) troubleshooting guides,
going a level deeper in the stack might be needed. This guide collects common
questions around dataflows to help you troubleshoot your queries.

<!-- Copied over from the old manage/troubleshooting guide -->
## Dataflows: mental model and basic terminology

When you create a materialized view, an index, or issue an ad-hoc query,
Materialize creates a so-called dataflow. A dataflow consists of instructions
on how to respond to data input and to changes to that data. Once executed, the
dataflow computes the result of the SQL query, waits for updates from the
sources, and then incrementally updates the query results when new data
arrives.

Materialize dataflows act on collections of data. To provide fast access to the
changes to individual records, the records can be stored in an indexed
representation called [arrangements](/get-started/arrangements/#arrangements).
Arrangements can be manually created by users on views by creating an index on
the view. But they are also used internally in dataflows, for instance, when
joining relations.

### Translating SQL to dataflows

To make these concepts a bit more tangible, let's look at the example from the
[getting started guide](/get-started/quickstart/).

```mzsql
CREATE SOURCE auction_house
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '1s', AS OF 100000)
  FOR ALL TABLES;

CREATE MATERIALIZED VIEW num_bids AS
  SELECT auctions.item, count(bids.id) AS number_of_bids
  FROM bids
  JOIN auctions ON bids.auction_id = auctions.id
  WHERE bids.bid_time < auctions.end_time
  GROUP BY auctions.item;

CREATE INDEX num_bids_idx ON num_bids (item);
```

The query of the materialized view joins the relations `bids` and `auctions`,
groups by `auctions.item` and determines the number of bids per auction. To
understand how this SQL query is translated to a dataflow, we can use
[`EXPLAIN PLAN`](/sql/explain-plan/) to display the
plan used to evaluate the join.

```mzsql
EXPLAIN MATERIALIZED VIEW num_bids;
```
```
                               Optimized Plan
-----------------------------------------------------------------------------
 materialize.public.num_bids:                                               +
   Reduce group_by=[#0{item}] aggregates=[count(*)] // { arity: 2 }         +
     Project (#3) // { arity: 1 }                                           +
       Filter (#1{bid_time} < #4{end_time}) // { arity: 5 }                 +
         Join on=(#0{auction_id} = #2{id}) type=differential // { arity: 5 }+
           ArrangeBy keys=[[#0{auction_id}]] // { arity: 2 }                +
             Project (#2, #4) // { arity: 2 }                               +
               ReadStorage materialize.public.bids // { arity: 5 }          +
           ArrangeBy keys=[[#0{id}]] // { arity: 3 }                        +
             Project (#0{id}, #2{end_time}, #3) // { arity: 3 }             +
               ReadStorage materialize.public.auctions // { arity: 4 }      +
                                                                            +
 Source materialize.public.auctions                                         +
 Source materialize.public.bids                                             +
                                                                            +
 Target cluster: quickstart                                                 +

(1 row)
```

The plan describes the specific operators that are used to evaluate the query.
Some of these operators resemble relational algebra or map reduce style
operators (`Filter`, `Join`, `Project`). Others are specific to Materialize
(`ArrangeBy`, `ReadStorage`).

In general, a high level understanding of what these operators do is sufficient
for effective debugging: `Filter` filters records, `Join` joins records from
two or more inputs, `Map` applies a function to transform records, etc. You can
find more details on these operators in the [`EXPLAIN PLAN` documentation](/sql/explain-plan/#reference-plan-operators).
But it's not important to have a deep understanding of all these operators for
effective debugging.

Behind the scenes, the operator graph is turned into a dataflow. The dataflow is
organized in a hierarchical structure that contains operators and _regions_,
which define a hierarchy on the operators. In our example, the dataflow
contains an `InputRegion`, a `BuildRegion`, and a region for the sink.

![Regions and operator visualization](/images/regions-and-operators-hierarchy.png)

Again, it's not too important for our purposes to understand what these regions
do and how they are used to structure the operator graph. For our purposes it's
just important to know than that they define a hierarchy on the operators.


## The system catalog and introspection relations

Materialize collects a lot of useful information about the dataflows and
operators in the system catalog in [introspection relations](/sql/system-catalog/mz_introspection).
The introspection relations are useful to troubleshoot and understand what is
happening under the hood when Materialize is not behaving as expected. However,
it is important to understand that most of the statistics we need for
troubleshooting purposes are specific to the cluster that is running the
queries we want to debug.

> **Warning:** Indexes and dataflows are local to a cluster, so their introspection information
> will vary across clusters depending on the active cluster and replica. As a
> consequence, you should expect the results of the queries below to vary
> depending on the values set for the `cluster` and `cluster_replica`
> [configuration parameters](/sql/set/#other-configuration-parameters).


<!--
[//]: # "TODO(joacoc) We should share ways for the user to diagnose and troubleshoot if and how fast a source is consuming."
``` -->

## Where is Materialize spending compute time?

Materialize spends time in various dataflow operators. Dataflows are created to
maintain materialized views or indexes. In addition, temporary dataflow will be
created for ad-hoc queries that don't just make a lookup to an existing index.

If Materialize is taking more time to update results than you expect, you can
identify which operators take the largest total amount of time.

### Identifying expensive dataflows

To understand which dataflow is taking the most time we can query the
`mz_scheduling_elapsed` relation. The `elapsed_time` metric shows the absolute
time the dataflows was busy since the system started and the dataflow was
created.

```mzsql
-- Extract raw elapsed time information for dataflows
SELECT
    mdo.id,
    mdo.name,
    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
FROM mz_introspection.mz_scheduling_elapsed AS mse,
    mz_introspection.mz_dataflow_operators AS mdo,
    mz_introspection.mz_dataflow_addresses AS mda
WHERE mse.id = mdo.id AND mdo.id = mda.id AND list_length(address) = 1
ORDER BY elapsed_ns DESC;
```
```
 id  |                  name                  |  elapsed_time
-----+----------------------------------------+-----------------
 354 | Dataflow: materialize.qck.num_bids     | 02:05:25.756836
 578 | Dataflow: materialize.qck.num_bids_idx | 00:15:04.838741
 (2 rows)
```

These results show that Materialize spends the most time keeping the
materialized view `num_bids` up to date, followed by the work on the index
`avg_bids_idx`.

### Identifying expensive operators in a dataflow

The previous query is a good starting point to get an overview of the work
happening in the cluster because it only returns dataflows.

The `mz_scheduling_elapsed` relation also contains details for regions and
operators. Removing the condition `list_length(address) = 1` will include the
regions and operators in the result. But be aware that every row shows
aggregated times for all the elements it contains. The `elapsed_time` reported
for the dataflows above also includes the elapsed time for all the regions and
operators they contain.

But because the parent-child relationship is not always obvious, the results
containing a mixture of dataflows, regions, and operators can be a bit hard to
interpret. The following query therefore only returns operators from the
`mz_scheduling_elapsed` relation. You can further drill down by adding a filter
condition that matches the name of a specific dataflow.

```mzsql
SELECT
    mdod.id,
    mdod.name,
    mdod.dataflow_name,
    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
FROM mz_introspection.mz_scheduling_elapsed AS mse,
    mz_introspection.mz_dataflow_addresses AS mda,
    mz_introspection.mz_dataflow_operator_dataflows AS mdod
WHERE
    mse.id = mdod.id AND mdod.id = mda.id
    -- exclude regions and just return operators
    AND mda.address NOT IN (
        SELECT DISTINCT address[:list_length(address) - 1]
        FROM mz_introspection.mz_dataflow_addresses
    )
ORDER BY elapsed_ns DESC;
```
```
 id  |                      name                       |             dataflow_name              |  elapsed_time
-----+-------------------------------------------------+----------------------------------------+-----------------
 431 | ArrangeBy[[Column(0)]]                          | Dataflow: materialize.qck.num_bids     | 01:12:58.964875
 442 | ArrangeBy[[Column(0)]]                          | Dataflow: materialize.qck.num_bids     | 00:06:06.080178
 528 | shard_source_fetch(u517)                        | Dataflow: materialize.qck.num_bids     | 00:04:04.076344
 594 | shard_source_fetch(u517)                        | Dataflow: materialize.qck.num_bids_idx | 00:03:34.803234
 590 | persist_source_backpressure(backpressure(u517)) | Dataflow: materialize.qck.num_bids_idx | 00:03:33.626036
 400 | persist_source_backpressure(backpressure(u510)) | Dataflow: materialize.qck.num_bids     | 00:03:03.575832
...
```

From the results of this query we can see that most of the elapsed time of the
dataflow is caused by the time it takes to maintain the updates in one of the
arrangements of the dataflow of the materialized view.

### Debugging expensive dataflows and operators

A large class of problems can be identified by using [`elapsed_time`](#identifying-expensive-operators-in-a-dataflow)
to estimate the most expensive dataflows and operators. However, `elapsed_time`
contains all work since the operator or dataflow was first created. Sometimes,
a lot of work happens initially when the operator is created, but later on it
takes only little continuous effort. If you want to see what operator is taking
the most time **right now**, the `elapsed_time` metric is not enough.

The relation `mz_compute_operator_durations_histogram` also tracks the time
operators are busy, but instead of aggregating `elapsed_time` since an operator
got created, it tracks how long each operator was scheduled at a time in a
histogram. This information can show you two things: operators that block
progress for others and operators that are currently doing work.

If there is a very expensive operator that blocks progress for all other
operators, it will become visible in the histogram. The offending operator will
be scheduled in much longer intervals compared to other operators, which
reflects in the histogram as larger time buckets.

```mzsql
-- Extract raw scheduling histogram information for operators
WITH histograms AS (
    SELECT
        mdod.id,
        mdod.name,
        mdod.dataflow_name,
        mcodh.count,
        mcodh.duration_ns / 1000 * '1 MICROSECONDS'::interval AS duration
    FROM mz_introspection.mz_compute_operator_durations_histogram AS mcodh,
        mz_introspection.mz_dataflow_addresses AS mda,
        mz_introspection.mz_dataflow_operator_dataflows AS mdod
    WHERE
        mcodh.id = mdod.id
        AND mdod.id = mda.id
        -- exclude regions and just return operators
        AND mda.address NOT IN (
            SELECT DISTINCT address[:list_length(address) - 1]
            FROM mz_introspection.mz_dataflow_addresses
        )

)

SELECT *
FROM histograms
WHERE duration > '100 millisecond'::interval
ORDER BY duration DESC;
```
```
 id  |                 name                 |             dataflow_name              | count |    duration
-----+--------------------------------------+----------------------------------------+-------+-----------------
 408 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.num_bids     |     1 | 00:00:01.073741
 408 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.num_bids     |     1 | 00:00:00.53687
 374 | persist_source::decode_and_mfp(u509) | Dataflow: materialize.qck.num_bids     |     1 | 00:00:00.268435
 408 | persist_source::decode_and_mfp(u510) | Dataflow: materialize.qck.num_bids     |     2 | 00:00:00.268435
 429 | FormArrangementKey                   | Dataflow: materialize.qck.num_bids     |     2 | 00:00:00.134217
 (5 rows)
```

Note that this relation contains a lot of information. The query therefore
filters all durations below `100 millisecond`. In this example, the result of
the query shows that the longest an operator got scheduled is just over one
second. So it's unlikely that Materialize is unresponsive because of a single
operator blocking progress for others.

The reported duration is still reporting aggregated values since the operator
has been created. To get a feeling for which operators are currently doing
work, you can subscribe to the changes of the relation.

```mzsql
-- Observe changes to the raw scheduling histogram information
COPY(SUBSCRIBE(
    WITH histograms AS (
        SELECT
            mdod.id,
            mdod.name,
            mdod.dataflow_name,
            mcodh.count,
            mcodh.duration_ns / 1000 * '1 MICROSECONDS'::interval AS duration
        FROM mz_introspection.mz_compute_operator_durations_histogram AS mcodh,
            mz_introspection.mz_dataflow_addresses AS mda,
            mz_introspection.mz_dataflow_operator_dataflows AS mdod
        WHERE
            mcodh.id = mdod.id
            AND mdod.id = mda.id
            -- exclude regions and just return operators
            AND mda.address NOT IN (
                SELECT DISTINCT address[:list_length(address) - 1]
                FROM mz_introspection.mz_dataflow_addresses
            )

    )

    SELECT *
    FROM histograms
    WHERE duration > '100 millisecond'::interval
) WITH (SNAPSHOT = false, PROGRESS)) TO STDOUT;
```
```
1691667343000	t	\N	\N	\N	\N	\N	\N
1691667344000	t	\N	\N	\N	\N	\N	\N
1691667344000	f	-1	431	ArrangeBy[[Column(0)]]	Dataflow: materialize.qck.num_bids	7673	00:00:00.104800
1691667344000	f	1	431	ArrangeBy[[Column(0)]]	Dataflow: materialize.qck.num_bids	7674	00:00:00.104800
1691667345000	t	\N	\N	\N	\N	\N	\N
...
```

In this way you can see that currently the only operator that is doing more than
100 milliseconds worth of work is the `ArrangeBy` operator from the
materialized view `num_bids`.


## Why is Materialize using so much memory?

[Arrangements](/overview/arrangements) take up most of Materialize's memory use.
Arrangements maintain indexes for data as it changes. These queries extract the
numbers of records and the size of the arrangements. The reported records may
exceed the number of logical records; the report reflects the uncompacted
state.

```mzsql
-- Extract dataflow records and sizes
SELECT
    s.id,
    s.name,
    s.records,
    pg_size_pretty(s.size) AS size
FROM mz_introspection.mz_dataflow_arrangement_sizes AS s
ORDER BY s.size DESC;
```
```
  id   |                   name                    | records  |    size
-------+-------------------------------------------+----------+------------
 49030 | Dataflow: materialize.public.num_bids     | 10000135 | 165 MB
 49031 | Dataflow: materialize.public.num_bids_idx |       33 | 1661 bytes
```

If you need to drill down into individual operators, you can query
`mz_arrangement_sizes` instead.

```mzsql
-- Extract operator records and sizes
SELECT
    mdod.id,
    mdod.name,
    mdod.dataflow_name,
    mas.records,
    pg_size_pretty(mas.size) AS size
FROM mz_introspection.mz_arrangement_sizes AS mas,
    mz_introspection.mz_dataflow_operator_dataflows AS mdod
WHERE mas.operator_id = mdod.id
ORDER BY mas.size DESC;
```
```
    id    |              name               |               dataflow_name               | records |    size
----------+---------------------------------+-------------------------------------------+---------+------------
 16318351 | ArrangeBy[[Column(0)]]          | Dataflow: materialize.public.num_bids     | 8462247 | 110 MB
 16318370 | ArrangeBy[[Column(0)]]          | Dataflow: materialize.public.num_bids     | 1537865 | 55 MB
 16318418 | ArrangeAccumulable [val: empty] | Dataflow: materialize.public.num_bids     |       5 | 1397 bytes
 16318550 | ArrangeBy[[Column(0)]]          | Dataflow: materialize.public.num_bids_idx |      13 | 1277 bytes
 16318422 | ReduceAccumulable               | Dataflow: materialize.public.num_bids     |       5 | 1073 bytes
 16318426 | AccumulableErrorCheck           | Dataflow: materialize.public.num_bids     |       0 | 256 bytes
 16318559 | ArrangeBy[[Column(0)]]-errors   | Dataflow: materialize.public.num_bids_idx |       0 | 0 bytes
```

In the [Materialize Console](https://console.materialize.com),

- The [**Cluster Overview**](/console/clusters/) page displays the cluster
  resource utilization for a selected cluster as well as the resource intensive
  objects in the cluster.

- The [**Environment Overview**](/console/monitoring/) page displays the
  resource utilization for all your clusters. You can select a specific cluster
  to view its **Overview** page.

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

```mzsql
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
    mz_introspection.mz_scheduling_elapsed_per_worker mse,
    (
        SELECT
            id,
            avg(elapsed_ns) AS avg_ns
        FROM
            mz_introspection.mz_scheduling_elapsed_per_worker
        GROUP BY
            id
    ) aebi,
    mz_introspection.mz_dataflow_operator_dataflows dod
WHERE
    mse.id = aebi.id AND
    mse.elapsed_ns > 2 * aebi.avg_ns AND
    mse.id = dod.id
ORDER BY ratio DESC;
```

## I found a problematic operator. Where did it come from?

Look up the operator in `mz_dataflow_addresses`. If an operator has value `x` at
position `n`, then it is part of the `x` subregion of the region defined by
positions `0..n-1`. The example SQL query and result below shows an operator
whose `id` is 515 that belongs to "subregion 5 of region 1 of dataflow 21".

```mzsql
SELECT * FROM mz_introspection.mz_dataflow_addresses WHERE id=515;
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
can find the name of the dataflow if you can find the name of the operator
whose address is just "dataflow 21."

```mzsql
-- get id and name of the operator representing the entirety of the dataflow
-- that a problematic operator comes from
SELECT
    mdo.id AS id,
    mdo.name AS name
FROM
    mz_introspection.mz_dataflow_addresses mda,
    -- source of operator names
    mz_introspection.mz_dataflow_operators mdo,
    -- view containing operators representing entire dataflows
    (SELECT
      mda.id AS dataflow_operator,
      mda.address[1] AS dataflow_address
    FROM
      mz_introspection.mz_dataflow_addresses mda
    WHERE
      list_length(mda.address) = 1) dataflows
WHERE
    mda.id = 515
    AND mda.address[1] = dataflows.dataflow_address
    AND mdo.id = dataflows.dataflow_operator;
```

## I dropped an index, why haven't my plans and dataflows changed?

It's likely that your index has **downstream dependencies**. If an index has
dependent objects downstream, its underlying dataflow will continue to be
maintained and take up resources until all dependent object are dropped or
altered, or Materialize is restarted.

[//]: # "TODO(chaas) Add reference to the console once it's available."

To check if there are residual dataflows on a specific cluster, run the
following query:

```mzsql
SET CLUSTER TO <cluster_name>;

SELECT ce.export_id AS dropped_index_id,
       s.name AS dropped_index_name,
       s.id AS dataflow_id
FROM mz_internal.mz_dataflow_arrangement_sizes s
JOIN mz_internal.mz_compute_exports ce ON ce.dataflow_id = s.id
LEFT JOIN mz_catalog.mz_objects o ON o.id = ce.export_id
WHERE o.id IS NULL;
```

You can then use the `dropped_index_id` object identifier to list the downstream
dependencies of the residual dataflow, using:

```mzsql
SELECT do.id AS dependent_object_id,
       do.name AS dependent_object_name,
       db.name AS dependent_object_database,
       s.name AS dependent_object_schema
FROM mz_internal.mz_compute_dependencies cd
LEFT JOIN mz_catalog.mz_objects do ON cd.object_id = do.id
LEFT JOIN mz_catalog.mz_schemas s ON do.schema_id = s.id
LEFT JOIN mz_catalog.mz_databases db ON s.database_id = db.id
WHERE cd.dependency_id = <dropped_index_id>;
```

To force a re-plan of the downstream objects that doesn't consider the dropped
index, you have to drop and recreate all downstream dependencies.

> **Warning:** Forcing a re-plan using the approach above **will trigger hydration**,
> which incurs downtime while the objects are recreated and backfilled with
> pre-existing data. We recommend doing a [blue/green deployment](/manage/dbt/blue-green-deployments/)
> to handle these changes in production environments.



---

## FAQ: Indexes


## Do indexes in Materialize support `ORDER BY`?

No. Indexes in Materialize do not support `ORDER BY` clauses.

Indexes in Materialize do not order their keys using the data type's natural
ordering and instead orders by its internal representation of the key (the tuple
of key length and value).

As such, indexes in Materialize currently do not provide optimizations for:

- Range queries; that is queries using `>`, `>=`,
  `<`, `<=`, `BETWEEN` clauses (e.g., `WHERE
  quantity > 10`,  `price >= 10 AND price <= 50`, and `WHERE quantity
  BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.

## Do indexes in Materialize support range queries?

No. Indexes in Materialize do not support range queries.

Indexes in Materialize do not order their keys using the data type's natural
ordering and instead orders by its internal representation of the key (the tuple
of key length and value).

As such, indexes in Materialize currently do not provide optimizations for:

- Range queries; that is queries using `>`, `>=`,
  `<`, `<=`, `BETWEEN` clauses (e.g., `WHERE
  quantity > 10`,  `price >= 10 AND price <= 50`, and `WHERE quantity
  BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.


---

## Idiomatic Materialize SQL


Materialize follows the SQL standard (SQL-92) implementation and strives for
compatibility with the PostgreSQL dialect. However, for some use cases,
Materialize provides its own idiomatic query patterns that can provide better
performance.

## Window functions


| Window Function | Idiomatic Materialize |
| --- | --- |
| <a href="/transform-data/idiomatic-materialize-sql/first-value/" >First value within groups</a> | <a href="/transform-data/idiomatic-materialize-sql/first-value/" >Use <code>MIN/MAX ... GROUP BY</code> subquery</a>. |
| <a href="/transform-data/idiomatic-materialize-sql/lag/" >Lag over a regularly increasing field</a> | <a href="/transform-data/idiomatic-materialize-sql/lag/" >Use self join or a self <code>LEFT JOIN/LEFT OUTER JOIN</code> by an <strong>equality match</strong> on the regularly increasing field</a>. |
| <a href="/transform-data/idiomatic-materialize-sql/last-value/" >Last value within groups</a> | <a href="/transform-data/idiomatic-materialize-sql/last-value/" >Use <code>MIN/MAX ... GROUP BY</code> subquery</a> |
| <a href="/transform-data/idiomatic-materialize-sql/lead/" >Lead over a regularly increasing field</a> | <a href="/transform-data/idiomatic-materialize-sql/lead/" >Use self join or a self <code>LEFT JOIN/LEFT OUTER JOIN</code> by an <strong>equality match</strong> on the regularly increasing field</a>. |
| <a href="/transform-data/idiomatic-materialize-sql/top-k/" >Top-K</a> | <a href="/transform-data/idiomatic-materialize-sql/top-k/" >Use an <code>ORDER BY ... LIMIT</code> subquery with a <code>LATERAL JOIN</code> on a <code>DISTINCT</code> subquery (or, for K=1,  a <code>SELECT DISTINCT ON ... ORDER BY ... LIMIT</code> query)</a> |


## General query patterns


| Query Pattern | Idiomatic Materialize |
| --- | --- |
| <a href="/transform-data/idiomatic-materialize-sql/any/" >ANY() Equi-join condition</a> | <a href="/transform-data/idiomatic-materialize-sql/any/" >Use <code>UNNEST()</code> or <code>DISTINCT UNNEST()</code> to expand the values and join</a>. |
| <a href="/transform-data/idiomatic-materialize-sql/mz_now/#mz_now-expressions-to-calculate-past-or-future-timestamp" ><code>mz_now()</code> with date/time operators</a> | <a href="/transform-data/idiomatic-materialize-sql/mz_now/#mz_now-expressions-to-calculate-past-or-future-timestamp" >Move the operation to the other side of the comparison</a>: |
| <a href="/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or" ><code>mz_now()</code> with disjunctions (<code>OR</code>) in materialized/indexed view definitions and <code>SUBSCRIBE</code> statements</a>: | <a href="/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or" >Rewrite using <code>UNION ALL</code> or <code>UNION</code> (deduplicating as necessary) expression</a> |



---

## Optimization


## Indexes

Indexes in Materialize maintain the complete up-to-date query results in memory
(and not just the index keys and the pointers to data rows). Unlike some other
databases, Materialize can use an index to serve query results even if the query
does not specify a `WHERE` condition on the index keys. Serving queries from
an index is fast since the results are already up-to-date and in memory.

Materialize can use [indexes](/concepts/indexes/) to further optimize query
performance in Materialize. Improvements can be significant, reducing some query
times down to single-digit milliseconds.

Building an efficient index depends on the clauses used in your queries as well
as your expected access patterns. Use the following as a guide:

* [WHERE point lookups](#where-point-lookups)
* [JOIN](#join)
* [DEFAULT](#default-index)

### `WHERE` point lookups

Unlike some other databases, Materialize can use an index to serve query results
even if the query does not specify a `WHERE` condition on the index keys. For
some queries, Materialize can perform [**point
lookups**](/concepts/indexes/#point-lookups) on the index (as opposed to an
index scan) if the query's `WHERE` clause:

- Specifies equality (`=` or `IN`) condition on **all** the indexed fields. The
  equality conditions must specify the **exact** index key expression (including
  type).

- Only uses `AND` (conjunction) to combine conditions for **different** fields.

Depending on your query pattern, you may want to build indexes to support point
lookups.

#### Create an index to support point lookups

To [create an index](/sql/create-index/) to support [**point
lookups**](/concepts/indexes/#point-lookups):

```mzsql
CREATE INDEX ON obj_name (<keys>);
```

- Specify **only** the keys that are constrained in the query's `WHERE` clause.
  If your index contains keys not specified in the query's `WHERE` clause, then
  Materialize performs a full index scan.

- Specify all (or a subset of) keys that are constrained in the query pattern's
  `WHERE` clause. If the index specifies all the keys, Materialize performs a
  point lookup only. If the index specifies a subset of keys, then Materialize
  performs a point lookup on the index keys and then filters these results using
  the conditions on the non-indexed fields.

- Specify index keys that **exactly match** the column expressions in the
  `WHERE` clause. For example, if the query specifies `WHERE quantity * price =
  100`, the index key should be `quantity * price` and not `price * quantity`.

- If the `WHERE` uses `OR` clauses and:

  - The `OR` arguments constrain all the same fields (e.g., `WHERE (quantity = 5
    AND price = 1.25) OR (quantity = 10 AND price = 1.25)`), create an index for
    the constrained fields (e.g., `quantity` and `price`).

  - The `OR` arguments constrain some of the same fields (e.g., `WHERE (quantity
    = 5 AND price = 1.25) OR (quantity = 10 AND item = 'brownie)`), create an
    index for the intersection of the constrained fields (e.g., `quantity`).
    Materialize performs a point lookup on the indexed key and then filters the
    results using the conditions on the non-indexed fields.

  - The `OR` arguments constrain completely disjoint sets of fields (e.g.,
    `WHERE quantity = 5 OR item = 'brownie'`), try to rewrite your query using a
    `UNION` (or `UNION ALL`), where each argument of the `UNION` has one of the
    original `OR` arguments.

    For example, the query can be rewritten as:

    ```mzsql
    SELECT * FROM orders_view WHERE quantity = 5
    UNION
    SELECT * FROM orders_view WHERE item = 'brownie';
    ```

    Depending on your usage pattern, you may want point-lookup indexes on both
    `quantity` and `item` (i.e., create two indexes, one on `quantity` and one
    on `item`). However, since each index will hold a copy of the data, consider
    the tradeoff between speed and memory usage. If the memory impact of having
    both indexes is too high, you might want to take a more global look at all
    of your queries to determine which index to build.

#### Examples

| WHERE clause of your query patterns    | Index for point lookups                                 |
|---------------------------------------------------|------------------------------------------|
| `WHERE x = 42`                                    | `CREATE INDEX ON obj_name (x);`        |
| `WHERE x IN (1, 2, 3)`                            | `CREATE INDEX ON obj_name (x);`        |
| `WHERE x = 1 OR x = 2`                            | `CREATE INDEX ON obj_name (x);`        |
| `WHERE (x, y) IN ((1, 'a'), (7, 'b'), (8, 'c'))`  | `CREATE INDEX ON obj_name (x, y);` or <br/> `CREATE INDEX ON obj_name (y, x);`  |
| `WHERE x = 1 AND y = 'abc'`                       | `CREATE INDEX ON obj_name (x, y);` or <br/> `CREATE INDEX ON obj_name (y, x);` |
| `WHERE (x = 5 AND y = 'a') OR (x = 7 AND y = ''`) | `CREATE INDEX ON obj_name (x, y);` or <br/> `CREATE INDEX ON obj_name (y, x);`     |
| `WHERE y * x = 64`                                | `CREATE INDEX ON obj_name (y * x);`    |
| `WHERE upper(y) = 'HELLO'`                        | `CREATE INDEX ON obj_name (upper(y));` |

You can verify that Materialize is accessing the input by an index lookup using [`EXPLAIN`](/sql/explain-plan/).

```mzsql
CREATE INDEX ON foo (x, y);
EXPLAIN SELECT * FROM foo WHERE x = 42 AND y = 50;
```

In the [`EXPLAIN`](/sql/explain-plan/) output, check for `lookup_value` after
the index name to confirm that Materialize will use a point lookup; i.e., that
Materialize will only read the matching records from the index instead of
scanning the entire index:

```
 Explained Query (fast path):
   Project (#0{x}, #1{y})
     ReadIndex on=materialize.public.foo foo_x_y_idx=[lookup value=(42, 50)]

 Used Indexes:
   - materialize.public.foo_x_y_idx (lookup)
```

### `JOIN`

In general, you can [improve the performance of your joins](https://materialize.com/blog/maintaining-joins-using-few-resources) by creating indexes on the columns occurring in join keys. (When a relation is joined with different relations on different keys, then separate indexes should be created for these keys.) This comes at the cost of additional memory usage. Materialize's in-memory [arrangements](/overview/arrangements) (the internal data structure of indexes) allow the system to share indexes across queries: **for multiple queries, an index is a fixed upfront cost with memory savings for each new query that uses it.**

Let's create a few tables to work through examples.

```mzsql
CREATE TABLE teachers (id INT, name TEXT);
CREATE TABLE sections (id INT, teacher_id INT, course_id INT, schedule TEXT);
CREATE TABLE courses (id INT, name TEXT);
```

#### Multiple Queries Join On the Same Collection

Let's consider two queries that join on a common collection. The idea is to create an index that can be shared across the two queries to save memory.

Here is a query where we join a collection `teachers` to a collection `sections` to see the name of the teacher, schedule, and course ID for a specific section of a course.

```mzsql
SELECT
    t.name,
    s.schedule,
    s.course_id
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id;
```

Here is another query that also joins on `teachers.id`. This one counts the number of sections each teacher teaches.

```mzsql
SELECT
    t.id,
    t.name,
    count(*)
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id
GROUP BY t.id, t.name;
```

We can eliminate redundant memory usage for these two queries by creating an index on the common column being joined, `teachers.id`.

```mzsql
CREATE INDEX pk_teachers ON teachers (id);
```

#### Joins with Filters

If your query filters one or more of the join inputs by a literal equality (e.g., `WHERE t.name = 'Escalante'`), place one of those input collections first in the `FROM` clause. In particular, this can speed up [ad hoc `SELECT` queries](/sql/select/#ad-hoc-queries) by accessing collections using index lookups rather than full scans.

Note that when the same input is being used in a join as well as being constrained by equalities to literals, _either_ the join _or_ the literal equalities can be sped up by an index (possibly the same index, but usually different indexes). Which of these will perform better depends on the characteristics of your data. For example, the following query can make use of _either_ of the following two indexes, but not both at the same time:
- on `teachers(name)` to perform the `t.name = 'Escalante'` point lookup before the join,
- on `teachers(id)` to speed up the join and then perform the `WHERE t.name = 'Escalante'`.

```mzsql
SELECT
    t.name,
    s.schedule,
    s.course_id
FROM teachers t
INNER JOIN sections s ON t.id = s.teacher_id
WHERE t.name = 'Escalante';
```

In this case, the index on `teachers(name)` might work better, as the `WHERE t.name = 'Escalante'` can filter out a very large percentage of the `teachers` table before the table is fed to the join. You can see an example `EXPLAIN` command output for the above query [here](#use-explain-to-verify-index-usage).

#### Optimize Multi-Way Joins with Delta Joins

Materialize has access to a join execution strategy we call **delta joins**, which aggressively re-uses indexes and maintains no intermediate results in memory. Materialize considers this plan only if all the necessary indexes already exist, in which case the additional memory cost of the join is zero. This is typically possible when you index all the join keys (including primary keys and foreign keys that are involved in the join). Delta joins are relevant only for joins of more than 2 inputs.

Let us extend the previous example by also querying for the name of the course rather than just the course ID, needing a 3-input join.

```mzsql
CREATE VIEW course_schedule AS
  SELECT
      t.name AS teacher_name,
      s.schedule,
      c.name AS course_name
  FROM teachers t
  INNER JOIN sections s ON t.id = s.teacher_id
  INNER JOIN courses c ON c.id = s.course_id;
```

In this case, we create indexes on the join keys to optimize the query:

```mzsql
CREATE INDEX pk_teachers ON teachers (id);
CREATE INDEX sections_fk_teachers ON sections (teacher_id);
CREATE INDEX pk_courses ON courses (id);
CREATE INDEX sections_fk_courses ON sections (course_id);
```

```mzsql
EXPLAIN SELECT * FROM course_schedule;
```

```
Optimized Plan
Explained Query:
  Project (#1, #5, #7)
    Filter (#0) IS NOT NULL AND (#4) IS NOT NULL
      Join on=(#0 = #3 AND #4 = #6) type=delta                 <---------- Delta join
        ArrangeBy keys=[[#0]]
          ReadIndex on=teachers pk_teachers=[delta join 1st input (full scan)]
        ArrangeBy keys=[[#1], [#2]]
          ReadIndex on=sections sections_fk_teachers=[delta join lookup] sections_fk_courses=[delta join lookup]
        ArrangeBy keys=[[#0]]
          ReadIndex on=courses pk_courses=[delta join lookup]

Used Indexes:
  - materialize.public.pk_teachers (delta join 1st input (full scan))
  - materialize.public.sections_fk_teachers (delta join lookup)
  - materialize.public.pk_courses (delta join lookup)
  - materialize.public.sections_fk_courses (delta join lookup)
```

For [ad hoc `SELECT` queries](/sql/select/#ad-hoc-queries) with a delta join, place the smallest input (taking into account predicates that filter from it) first in the `FROM` clause. (This is only relevant for joins with more than two inputs, because two-input joins are always Differential joins.)

It is important to note that often more than one index is needed on a single input of a multi-way join. In the above example, `sections` needs an index on the `teacher_id` column and another index on the `course_id` column. Generally, when a relation is joined with different relations on different keys, then separate indexes should be created for each of these keys.

#### Further Optimize with Late Materialization

Materialize can further optimize memory usage when joining collections with primary and foreign key constraints using a pattern known as **late materialization**.

To understand late materialization, you need to know about primary and foreign keys. In our example, the `teachers.id` column uniquely identifies all teachers. When a column or set of columns uniquely identifies each record, it is called a **primary key**. We also have `sections.teacher_id`, which is not the primary key of `sections`, but it *does* correspond to the primary key of `teachers`. Whenever we have a column that is a primary key of another collection, it is called a [**foreign key**](https://en.wikipedia.org/wiki/Foreign_key).

In many relational databases, indexes don't replicate the entire collection of data. Rather, they maintain just a mapping from the indexed columns back to a primary key. These few columns can take substantially less space than the whole collection, and may also change less as various unrelated attributes are updated. This is called **late materialization**, and it is possible to achieve in Materialize as well. Here are the steps to implementing late materialization along with examples.

1. Create indexes on the primary key column(s) for your input collections.
    ```mzsql
    CREATE INDEX pk_teachers ON teachers (id);
    CREATE INDEX pk_sections ON sections (id);
    CREATE INDEX pk_courses ON courses (id);
    ```


2. For each foreign key in the join, create a "narrow" view with just two columns: foreign key and primary key. Then create two indexes: one for the foreign key and one for the primary key. In our example, the two foreign keys are `sections.teacher_id` and `sections.course_id`, so we do the following:
    ```mzsql
    -- Create a "narrow" view containing primary key sections.id
    -- and foreign key sections.teacher_id
    CREATE VIEW sections_narrow_teachers AS SELECT id, teacher_id FROM sections;
    -- Create indexes on those columns
    CREATE INDEX sections_narrow_teachers_0 ON sections_narrow_teachers (id);
    CREATE INDEX sections_narrow_teachers_1 ON sections_narrow_teachers (teacher_id);
    ```
    ```mzsql
    -- Create a "narrow" view containing primary key sections.id
    -- and foreign key sections.course_id
    CREATE VIEW sections_narrow_courses AS SELECT id, course_id FROM sections;
    -- Create indexes on those columns
    CREATE INDEX sections_narrow_courses_0 ON sections_narrow_courses (id);
    CREATE INDEX sections_narrow_courses_1 ON sections_narrow_courses (course_id);
    ```
    > **Note:** In this case, because both foreign keys are in `sections`, we could have gotten away with one narrow collection `sections_narrow_teachers_and_courses` with indexes on `id`, `teacher_id`, and `course_id`. In general, we won't be so lucky to have all the foreign keys in the same collection, so we've shown the more general pattern of creating a narrow view and two indexes for each foreign key.


3. Rewrite your query to use your narrow collections in the join conditions. Example:

    ```mzsql
    SELECT
      t.name AS teacher_name,
      s.schedule,
      c.name AS course_name
    FROM sections_narrow_teachers s_t
    INNER JOIN sections s ON s_t.id = s.id
    INNER JOIN teachers t ON s_t.teacher_id = t.id
    INNER JOIN sections_narrow_courses s_c ON s_c.id = s.id
    INNER JOIN courses c ON s_c.course_id = c.id;
    ```

### Default index

Create a default index when there is no particular `WHERE` or `JOIN` clause that would fit the above cases. This can still speed up your query by reading the input from memory.

Clause                                               | Index                               |
-----------------------------------------------------|-------------------------------------|
`SELECT x, y FROM obj_name`                          | `CREATE DEFAULT INDEX ON obj_name;` |

### Use `EXPLAIN` to verify index usage

Use `EXPLAIN` to verify that indexes are used as you expect. For example:

```mzsql
CREATE TABLE teachers (id INT, name TEXT);
CREATE TABLE sections (id INT, teacher_id INT, course_id INT, schedule TEXT);
CREATE TABLE courses (id INT, name TEXT);

CREATE INDEX pk_teachers ON teachers (id);
CREATE INDEX teachers_name ON teachers (name);
CREATE INDEX sections_fk_teachers ON sections (teacher_id);
CREATE INDEX pk_courses ON courses (id);
CREATE INDEX sections_fk_courses ON sections (course_id);

EXPLAIN
  SELECT
      t.name AS teacher_name,
      s.schedule,
      c.name AS course_name
  FROM teachers t
  INNER JOIN sections s ON t.id = s.teacher_id
  INNER JOIN courses c ON c.id = s.course_id
  WHERE t.name = 'Escalante';
```

```
                                                  Optimized Plan
------------------------------------------------------------------------------------------------------------------
 Explained Query:                                                                                                +
   Project (#1, #6, #8)                                                                                          +
     Filter (#0) IS NOT NULL AND (#5) IS NOT NULL                                                                +
       Join on=(#0 = #4 AND #5 = #7) type=delta                                                                  +
         ArrangeBy keys=[[#0]]                                                                                   +
           ReadIndex on=materialize.public.teachers teachers_name=[lookup value=("Escalante")]                   +
         ArrangeBy keys=[[#1], [#2]]                                                                             +
           ReadIndex on=sections sections_fk_teachers=[delta join lookup] sections_fk_courses=[delta join lookup]+
         ArrangeBy keys=[[#0]]                                                                                   +
           ReadIndex on=courses pk_courses=[delta join lookup]                                                   +
                                                                                                                 +
 Used Indexes:                                                                                                   +
   - materialize.public.teachers_name (lookup)                                                                   +
   - materialize.public.sections_fk_teachers (delta join lookup)                                                 +
   - materialize.public.pk_courses (delta join lookup)                                                           +
   - materialize.public.sections_fk_courses (delta join lookup)                                                  +
```

You can see in the above `EXPLAIN` printout that the system will use `teachers_name` for a point lookup, and use three other indexes for the execution of the delta join. Note that the `pk_teachers` index is not used, as explained [above](#joins-with-filters).

The following are the possible index usage types:
- `*** full scan ***`: Materialize will read the entire index.
- `lookup`: Materialize will look up only specific keys in the index.
- `differential join`: Materialize will use the index to perform a _differential join_. For a differential join between two relations, the amount of memory required is proportional to the sum of the sizes of each of the input relations that are **not** indexed. In other words, if an input is already indexed, then the size of that input won't affect the memory usage of a differential join between two relations. For a join between more than two relations, we recommend aiming for a delta join instead of a differential join, as explained [above](#optimize-multi-way-joins-with-delta-joins). A differential join between more than two relations will perform a series of binary differential joins on top of each other, and each of these binary joins (except the first one) will use memory proportional to the size of the intermediate data that is fed into the join.
- `delta join 1st input (full scan)`: Materialize will use the index for the first input of a [delta join](#optimize-multi-way-joins-with-delta-joins). Note that the first input of a delta join is always fully scanned. However, executing the join won't require additional memory if the input is indexed.
- `delta join lookup`: Materialize will use the index for a non-first input of a [delta join](#optimize-multi-way-joins-with-delta-joins). This means that, in an ad hoc query, the join will perform only lookups into the index.
- `fast path limit`: When a [fast path](/sql/explain-plan/#fast-path-queries) query has a `LIMIT` clause but no `ORDER BY` clause, then Materialize will read from the index only as many records as required to satisfy the `LIMIT` (plus `OFFSET`) clause.

### Limitations

Indexes in Materialize do not order their keys using the data type's natural
ordering and instead orders by its internal representation of the key (the tuple
of key length and value).

As such, indexes in Materialize currently do not provide optimizations for:

- Range queries; that is queries using `>`, `>=`,
  `<`, `<=`, `BETWEEN` clauses (e.g., `WHERE
  quantity > 10`,  `price >= 10 AND price <= 50`, and `WHERE quantity
  BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.

## Query hints

Materialize has at present three important [query hints]: `AGGREGATE INPUT GROUP SIZE`, `DISTINCT ON INPUT GROUP SIZE`, and `LIMIT INPUT GROUP SIZE`. These hints apply to indexed or materialized views that need to incrementally maintain [`MIN`], [`MAX`], or [Top K] queries, as specified by SQL aggregations, `DISTINCT ON`, or `LIMIT` clauses. Maintaining these queries while delivering low latency result updates is demanding in terms of main memory. This is because Materialize builds a hierarchy of aggregations so that data can be physically partitioned into small groups. By having only small groups at each level of the hierarchy, we can make sure that recomputing aggregations is not slowed down by skew in the sizes of the original query groups.

The number of levels needed in the hierarchical scheme is by default set assuming that there may be large query groups in the input data. By specifying the query hints, it is possible to refine this assumption, allowing Materialize to build a hierarchy with fewer levels and lower memory consumption without sacrificing update latency.

Consider the previous example with the collection `sections`. Maintenance of the maximum `course_id` per `teacher` can be achieved with a materialized view:

```mzsql
CREATE MATERIALIZED VIEW max_course_id_per_teacher AS
SELECT teacher_id, MAX(course_id)
FROM sections
GROUP BY teacher_id;
```

If the largest number of `course_id` values that are allocated to a single `teacher_id` is known, then this number can be provided as the `AGGREGATE INPUT GROUP SIZE`. For the query above, it is possible to get an estimate for this number by:

```mzsql
SELECT MAX(course_count)
FROM (
  SELECT teacher_id, COUNT(*) course_count
  FROM sections
  GROUP BY teacher_id
);
```

However, the estimate is based only on data that is already present in the system. So taking into account how much this largest number could expand is critical to avoid issues with update latency after tuning the query hint.

For our example, let's suppose that we determined the largest number of courses per teacher to be `1000`. Then, the original definition of `max_course_id_per_teacher` can be revised to include the `AGGREGATE INPUT GROUP SIZE` query hint as follows:

```mzsql
CREATE MATERIALIZED VIEW max_course_id_per_teacher AS
SELECT teacher_id, MAX(course_id)
FROM sections
GROUP BY teacher_id
OPTIONS (AGGREGATE INPUT GROUP SIZE = 1000)
```

The other two hints can be provided in [Top K] query patterns specified by `DISTINCT ON` or `LIMIT`. As examples, consider that we wish not to compute the maximum `course_id`, but rather the `id` of the section of this top course. This computation can be incrementally maintained by the following materialized view:

```mzsql
CREATE MATERIALIZED VIEW section_of_top_course_per_teacher AS
SELECT DISTINCT ON(teacher_id) teacher_id, id AS section_id
FROM sections
OPTIONS (DISTINCT ON INPUT GROUP SIZE = 1000)
ORDER BY teacher_id ASC, course_id DESC;
```

In the above examples, we see that the query hints are always positioned in an `OPTIONS` clause after a `GROUP BY` clause, but before an `ORDER BY`, as captured by the [`SELECT` syntax]. However, in the case of Top K using a `LATERAL` subquery and `LIMIT`, it is important to note that the hint is specified in the subquery. For instance, the following materialized view illustrates how to incrementally maintain the top-3 section `id`s ranked by `course_id` for each teacher:

```mzsql
CREATE MATERIALIZED VIEW sections_of_top_3_courses_per_teacher AS
SELECT id AS teacher_id, section_id
FROM teachers grp,
     LATERAL (SELECT id AS section_id
              FROM sections
              WHERE teacher_id = grp.id
              OPTIONS (LIMIT INPUT GROUP SIZE = 1000)
              ORDER BY course_id DESC
              LIMIT 3);
```

For indexed and materialized views that have already been created without specifying query hints, Materialize includes an introspection view, [`mz_introspection.mz_expected_group_size_advice`], that can be used to query, for a given cluster, all incrementally maintained [dataflows] where tuning of the above query hints could be beneficial. The introspection view also provides an advice value based on an estimate of how many levels could be cut from the hierarchy. The following query illustrates how to access this introspection view:

```mzsql
SELECT dataflow_name, region_name, levels, to_cut, hint
FROM mz_introspection.mz_expected_group_size_advice
ORDER BY dataflow_name, region_name;
```

The column `hint` provides the estimated value to be provided to the `AGGREGATE INPUT GROUP SIZE` in the case of a `MIN` or `MAX` aggregation or to the `DISTINCT ON INPUT GROUP SIZE` or `LIMIT INPUT GROUP SIZE` in the case of a Top K pattern.

## Learn more

Check out the blog post [Delta Joins and Late Materialization](https://materialize.com/blog/delta-joins/) to go deeper on join optimization in Materialize.

[query hints]: /sql/select/#query-hints
[arrangements]: /get-started/arrangements/#arrangements
[`MIN`]: /sql/functions/#min
[`MAX`]: /sql/functions/#max
[Top K]: /transform-data/patterns/top-k
[`mz_introspection.mz_expected_group_size_advice`]: /sql/system-catalog/mz_introspection/#mz_expected_group_size_advice
[dataflows]: /get-started/arrangements/#dataflows
[`SELECT` syntax]: /sql/select/#syntax


---

## Patterns


The following section provides examples of implementing some common query
patterns in Materialize:


---

## Troubleshooting


Once data is flowing into Materialize and you start modeling it in SQL, you
might run into some snags or unexpected scenarios. This guide collects common
questions around data transformation to help you troubleshoot your queries.

## Why is my query slow?

<!-- TODO: update this to use the query history UI once it's available -->
The most common reasons for query execution taking longer than expected are:

* Processing lag in upstream dependencies, like materialized views and indexes
* Index design
* Query design

Each of these reasons requires a different approach for troubleshooting. Follow
the guidance below to first detect the source of slowness, and then address it
accordingly.

### Lagging materialized views or indexes

#### Detect

When a materialized view or index upstream of your query is behind on
processing, your query must wait for it to catch up before returning results.
This is how Materialize ensures consistent results for all queries.

To check if any materialized views or indexes are lagging, use the workflow
graphs in the Materialize console.

1. Go to https://console.materialize.com/.
2. Click on the **"Clusters"** tab in the side navigation bar.
3. Click on the cluster that contains your upstream materialized view or index.
4. Go to the **"Materialized Views"** or **"Indexes"** section, and click on the
object name to access its workflow graph.

If you find that one of the upstream materialized views or indexes is lagging,
this could be the cause of your query slowness.

#### Address

To troubleshoot and fix a lagging materialized view or index, follow the steps
in the [dataflow troubleshooting](/transform-data/dataflow-troubleshooting) guide.

*Do you have multiple materialized views chained on top of each other? Are you
seeing small amounts of lag?*<br>
Tip: avoid intermediary materialized views where not necessary. Each chained
materialized view incurs a small amount of processing lag from the previous
one.
<!-- TODO add more guidance on avoiding chained mat views-->

Other options to consider:

* If you've gone through the dataflow troubleshooting and do not want to make
  any changes to your query, consider [sizing up your cluster](/sql/create-cluster/#size).
* You can also consider changing your [isolation level](/get-started/isolation-level/),
  depending on the consistency guarantees that you need. With a lower isolation
  level, you may be able to query stale results out of lagging indexes and
  materialized views.
* You can also check whether you're using a [transaction](#transactions) and
  follow the guidance there.

### Slow query execution

Query execution time largely depends on the amount of on-the-fly work that needs
to be done to compute the result. You can cut back on execution time in a few
ways:

#### Indexing and query optimization

Like in any other database, index design affects query performance. If the
dependencies of your query don't have [indexes](/sql/create-index/) defined,
you should consider creating one (or many). Check out the [optimization guide](/transform-data/optimization)
for guidance on how to optimize query performance. For information on when
to use a materialized view versus an index, check out the
[materialized view reference documentation](/sql/create-materialized-view/#details) .

If the dependencies of your query are indexed, you should confirm that the query
is actually using the index! This information is available in the query plan,
which you can view using the [`EXPLAIN PLAN`](/sql/explain-plan/) command. If
you run `EXPLAIN PLAN` for your query and see the index(es) under `Used indexes`,
this means that the index was correctly used. If that's not the case, consider:

* Are you running the query in the same cluster which contains the index? You
  must do so in order for the index to be used.
* Does the index's indexed expression (key) match up with how you're querying
  the data?

#### Result filtering

If you are just looking to validate data and don't want to deal with query
optimization at this stage, you can improve the efficiency of validation
queries by reducing the amount of data that Materialize needs to read. You can
achieve this by adding `LIMIT` clauses or [temporal filters](/transform-data/patterns/temporal-filters/)
to your queries.

**`LIMIT` clause**

Use the standard `LIMIT` clause to return at most the specified number of rows.
It's important to note that this only applies to basic queries against **a
single** source, materialized view or table, with no ordering, filters or
offsets.

```mzsql
SELECT <column list or *>
FROM <source, materialized view or table>
LIMIT <25 or less>;
```

To verify whether the query will return quickly, use [`EXPLAIN PLAN`](/sql/explain-plan/)
to get the execution plan for the query, and validate that it starts with
`Explained Query (fast path)`.

**Temporal filters**

Use temporal flters to filter results on a timestamp column that correlates with
the insertion or update time of each row. For example:

```mzsql
WHERE mz_now() <= event_ts + INTERVAL '1hr'
```

Materialize is able to “push down” temporal filters all the way down to its
storage layer, skipping over old data that isn't relevant to the query. For
more details on temporal filter pushdown, see the [reference documentation](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

### Other things to consider

#### Transactions
<!-- Copied from doc/user/content/manage/troubleshooting.md#Transactions -->
Transactions are a database concept for bundling multiple query steps into a
single, all-or-nothing operation. You can read more about them in the
[transactions](/sql/begin) section of our docs.

In Materialize, `BEGIN` starts a transaction block. All statements in a
transaction block will be executed in a single transaction until an explicit
`COMMIT` or `ROLLBACK` is given. All statements in that transaction happen at
the same timestamp, and that timestamp must be valid for all objects the
transaction may access.

What this means for latency: Materialize may delay queries against "slow"
tables, materialized views, and indexes until they catch up to faster ones in
the same schema. We recommend you avoid using transactions in contexts where
you require low latency responses and are not certain that all objects in a
schema will be equally current.

What you can do:

- Avoid using transactions where you don’t need them. For example, if you’re
  only executing single statements at a time.
- Double check whether your SQL library or ORM is wrapping all queries in
  transactions on your behalf, and disable that setting, only using
  transactions explicitly when you want them.

#### Client-side latency
<!-- Copied from doc/user/content/manage/troubleshooting.md#client-side-latency -->
To minimize the roundtrip latency associated with making requests from your
client to Materialize, make your requests as physically close to your
Materialize region as possible. For example, if you use the AWS `us-east-1`
region for Materialize, your client server would ideally also be running in AWS
`us-east-1`.

#### Result size
<!-- TODO: Use the query history UI to fetch result size -->
Smaller results lead to less time spent transmitting data over the network. You
can calculate your result size as `number of rows returned x byte size of each
row`, where `byte size of each row = sum(byte size of each column)`. If your
result size is large, this will be a factor in query latency.

#### Cluster CPU
Another thing to check is how busy the cluster you're issuing queries on is. A
busy cluster means your query might be blocked by some other processing going
on, taking longer to return. As an example, if you issue a lot of
resource-intensive queries at once, that might spike the CPU.

The measure of cluster busyness is CPU. You can monitor CPU usage in the
[Materialize console](/console/) by clicking
the **"Clusters"** tab in the navigation bar, and clicking into the cluster.
You can also grab CPU usage from the system catalog using SQL:

```mzsql
SELECT cru.cpu_percent
FROM mz_internal.mz_cluster_replica_utilization cru
LEFT JOIN mz_catalog.mz_cluster_replicas cr ON cru.replica_id = cr.id
LEFT JOIN mz_catalog.mz_clusters c ON cr.cluster_id = c.id
WHERE c.name = <CLUSTER_NAME>;
```

## Why is my query not responding?

The most common reasons for query hanging are:

* An upstream source is stalled
* An upstream object is still hydrating
* Your cluster is unhealthy

Each of these reasons requires a different approach for troubleshooting. Follow
the guidance below to first detect the source of the hang, and then address it
accordingly.

> **Note:** Your query may be running, just slowly. If none of the reasons below detects
> your issue, jump to [Why is my query slow?](#why-is-my-query-slow) for further
> guidance.


### Stalled source

<!-- TODO: update this to use the query history UI once it's available -->
To detect and address stalled sources, follow the [`Ingest data` troubleshooting](/ingest-data/troubleshooting)
guide.

### Hydrating upstream objects

When a source, materialized view, or index is created or updated, it must first
be backfilled with any pre-existing data — a process known as _hydration_.

Queries that depend objects that are still hydrating will **block until
hydration is complete**. To see whether an object is still hydrating, navigate
to the [workflow graph](#detect) for the object in the Materialize console.

Hydration time is proportional to data volume and query complexity. This means
that you should expect objects with large volumes of data and/or complex queries
to take longer to hydrate. For Cloud, you should also expect hydration to be
triggered every time a cluster is restarted or sized up, including during
[Materialize Cloud's routine maintenance
window](/releases/schedule/#cloud-upgrade-schedule).

### Unhealthy cluster

#### Detect

If your cluster replica reaches its capacity (i.e., it OOMs at 100% Memory Utilization), this will result in a crash. After a crash, the cluster replica has to restart, which can take a few seconds. On cluster restart, your query will also automatically restart execution from the beginning.

If your cluster replica is CPU-maxed out (~100% CPU usage), your query may be blocked while the cluster processes the other activity. It may eventually complete, but it will continue to be slow and potentially blocked until the CPU usage goes down. As an example, if you issue a lot of resource-intensive queries at once, that might spike the CPU.

We recommend setting [Alerting thresholds](https://materialize.com/docs/manage/monitor/alerting/#thresholds) to notify your team when a cluster is reaching its capacity. Please note that these are recommendations, and some configurations may reach unstable memory utilization levels sooner than the thresholds.

To see Memory Utilization and CPU usage for your cluster replica in the [Materialize console](https://materialize.com/docs/console/clusters/), go to [https://console.materialize.com/](/console/), click the **“Clusters”** tab in the navigation bar, and click on the cluster name.

#### Address

Your query may have been the root cause of the increased Memory Utilization and CPU usage, or it may have been something else happening on the cluster at the same time. To troubleshoot and fix Memory Utilization and CPU usage, follow the steps in the [dataflow troubleshooting](https://materialize.com/docs/transform-data/dataflow-troubleshooting) guide.

For guidance on how to reduce Memory Utilization and CPU usage for this or another query, take a look at the [indexing and query optimization](https://materialize.com/docs/transform-data/troubleshooting/#indexing-and-query-optimization) and result filtering sections above.

If your query was the root cause, you’ll need to kill it for the cluster replica’s Memory Utilization or CPU to go down. If your query was causing an OOM, the cluster replica will continue to be in an “OOM loop” - every time the replica restarts, the query restarts executing automatically then causes an OOM again - until you kill the query.

If your query was not the root cause, you can wait for the other activity on the cluster to stop and Memory Utilization/CPU to go down, or switch to a different cluster.

If you’ve gone through the dataflow troubleshooting and do not want to make any changes to your query, consider [sizing up your cluster](https://materialize.com/docs/sql/create-cluster/#size). A larger size cluster will provision more resources.


## Which part of my query runs slowly or uses a lot of memory?

You can [`EXPLAIN`](/sql/explain-plan/) a query to see how it will be run as a
dataflow. In particular, `EXPLAIN PHYSICAL PLAN` (the default) will show the concrete, fully
optimized plan that Materialize will run. That plan is written in our "low-level
intermediate representation" (LIR).

You can [`EXPLAIN ANALYZE`](/sql/explain-analyze) an index or materialized view to
attribute performance information to each LIR operator.

## How do I troubleshoot slow queries?

Materialize stores a (sampled) log of the SQL statements that are issued against
your Materialize region in the last **three days**, along with various metadata
about these statements. You can access this log via the **"Query history"** tab
in the [Materialize console](/console/). You can filter
and sort statements by type, duration, and other dimensions.

This data is also available via the
[mz_internal.mz_recent_activity_log](/sql/system-catalog/mz_internal/#mz_recent_activity_log)
catalog table.

It's important to note that the default (and max) sample rate for most
Materialize organizations is 99%, which means that not all statements will be
captured in the log. The sampling rate is not user-configurable, and may change
at any time.

If you're looking for a complete audit history, use the [mz_audit_events](/sql/system-catalog/mz_catalog/#mz_audit_events)
catalog table, which records all DDL commands issued against your Materialize
region.
