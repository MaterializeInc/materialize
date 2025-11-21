<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Overview](/docs/transform-data/)

</div>

# Dataflow troubleshooting

If you’re unable to troubleshoot your issue using the
[`Ingest data`](/docs/ingest-data/troubleshooting/) and
[`Transform data`](/docs/transform-data/troubleshooting/)
troubleshooting guides, going a level deeper in the stack might be
needed. This guide collects common questions around dataflows to help
you troubleshoot your queries.

## Dataflows: mental model and basic terminology

When you create a materialized view, an index, or issue an ad-hoc query,
Materialize creates a so-called dataflow. A dataflow consists of
instructions on how to respond to data input and to changes to that
data. Once executed, the dataflow computes the result of the SQL query,
waits for updates from the sources, and then incrementally updates the
query results when new data arrives.

Materialize dataflows act on collections of data. To provide fast access
to the changes to individual records, the records can be stored in an
indexed representation called
[arrangements](/docs/get-started/arrangements/#arrangements).
Arrangements can be manually created by users on views by creating an
index on the view. But they are also used internally in dataflows, for
instance, when joining relations.

### Translating SQL to dataflows

To make these concepts a bit more tangible, let’s look at the example
from the [getting started guide](/docs/get-started/quickstart/).

<div class="highlight">

``` chroma
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

</div>

The query of the materialized view joins the relations `bids` and
`auctions`, groups by `auctions.item` and determines the number of bids
per auction. To understand how this SQL query is translated to a
dataflow, we can use [`EXPLAIN PLAN`](/docs/sql/explain-plan/) to
display the plan used to evaluate the join.

<div class="highlight">

``` chroma
EXPLAIN MATERIALIZED VIEW num_bids;
```

</div>

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

The plan describes the specific operators that are used to evaluate the
query. Some of these operators resemble relational algebra or map reduce
style operators (`Filter`, `Join`, `Project`). Others are specific to
Materialize (`ArrangeBy`, `ReadStorage`).

In general, a high level understanding of what these operators do is
sufficient for effective debugging: `Filter` filters records, `Join`
joins records from two or more inputs, `Map` applies a function to
transform records, etc. You can find more details on these operators in
the [`EXPLAIN PLAN`
documentation](/docs/sql/explain-plan/#reference-plan-operators). But
it’s not important to have a deep understanding of all these operators
for effective debugging.

Behind the scenes, the operator graph is turned into a dataflow. The
dataflow is organized in a hierarchical structure that contains
operators and *regions*, which define a hierarchy on the operators. In
our example, the dataflow contains an `InputRegion`, a `BuildRegion`,
and a region for the sink.

![Regions and operator
visualization](/docs/images/regions-and-operators-hierarchy.png)

Again, it’s not too important for our purposes to understand what these
regions do and how they are used to structure the operator graph. For
our purposes it’s just important to know than that they define a
hierarchy on the operators.

## The system catalog and introspection relations

Materialize collects a lot of useful information about the dataflows and
operators in the system catalog in [introspection
relations](/docs/sql/system-catalog/mz_introspection). The introspection
relations are useful to troubleshoot and understand what is happening
under the hood when Materialize is not behaving as expected. However, it
is important to understand that most of the statistics we need for
troubleshooting purposes are specific to the cluster that is running the
queries we want to debug.

<div class="warning">

**WARNING!** Indexes and dataflows are local to a cluster, so their
introspection information will vary across clusters depending on the
active cluster and replica. As a consequence, you should expect the
results of the queries below to vary depending on the values set for the
`cluster` and `cluster_replica` [configuration
parameters](/docs/sql/set/#other-configuration-parameters).

</div>

## Where is Materialize spending compute time?

Materialize spends time in various dataflow operators. Dataflows are
created to maintain materialized views or indexes. In addition,
temporary dataflow will be created for ad-hoc queries that don’t just
make a lookup to an existing index.

If Materialize is taking more time to update results than you expect,
you can identify which operators take the largest total amount of time.

### Identifying expensive dataflows

To understand which dataflow is taking the most time we can query the
`mz_scheduling_elapsed` relation. The `elapsed_time` metric shows the
absolute time the dataflows was busy since the system started and the
dataflow was created.

<div class="highlight">

``` chroma
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

</div>

```
 id  |                  name                  |  elapsed_time
-----+----------------------------------------+-----------------
 354 | Dataflow: materialize.qck.num_bids     | 02:05:25.756836
 578 | Dataflow: materialize.qck.num_bids_idx | 00:15:04.838741
 (2 rows)
```

These results show that Materialize spends the most time keeping the
materialized view `num_bids` up to date, followed by the work on the
index `avg_bids_idx`.

### Identifying expensive operators in a dataflow

The previous query is a good starting point to get an overview of the
work happening in the cluster because it only returns dataflows.

The `mz_scheduling_elapsed` relation also contains details for regions
and operators. Removing the condition `list_length(address) = 1` will
include the regions and operators in the result. But be aware that every
row shows aggregated times for all the elements it contains. The
`elapsed_time` reported for the dataflows above also includes the
elapsed time for all the regions and operators they contain.

But because the parent-child relationship is not always obvious, the
results containing a mixture of dataflows, regions, and operators can be
a bit hard to interpret. The following query therefore only returns
operators from the `mz_scheduling_elapsed` relation. You can further
drill down by adding a filter condition that matches the name of a
specific dataflow.

<div class="highlight">

``` chroma
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

</div>

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

From the results of this query we can see that most of the elapsed time
of the dataflow is caused by the time it takes to maintain the updates
in one of the arrangements of the dataflow of the materialized view.

### Debugging expensive dataflows and operators

A large class of problems can be identified by using
[`elapsed_time`](#identifying-expensive-operators-in-a-dataflow) to
estimate the most expensive dataflows and operators. However,
`elapsed_time` contains all work since the operator or dataflow was
first created. Sometimes, a lot of work happens initially when the
operator is created, but later on it takes only little continuous
effort. If you want to see what operator is taking the most time **right
now**, the `elapsed_time` metric is not enough.

The relation `mz_compute_operator_durations_histogram` also tracks the
time operators are busy, but instead of aggregating `elapsed_time` since
an operator got created, it tracks how long each operator was scheduled
at a time in a histogram. This information can show you two things:
operators that block progress for others and operators that are
currently doing work.

If there is a very expensive operator that blocks progress for all other
operators, it will become visible in the histogram. The offending
operator will be scheduled in much longer intervals compared to other
operators, which reflects in the histogram as larger time buckets.

<div class="highlight">

``` chroma
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

</div>

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

Note that this relation contains a lot of information. The query
therefore filters all durations below `100 millisecond`. In this
example, the result of the query shows that the longest an operator got
scheduled is just over one second. So it’s unlikely that Materialize is
unresponsive because of a single operator blocking progress for others.

The reported duration is still reporting aggregated values since the
operator has been created. To get a feeling for which operators are
currently doing work, you can subscribe to the changes of the relation.

<div class="highlight">

``` chroma
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

</div>

```
1691667343000  t   \N  \N  \N  \N  \N  \N
1691667344000   t   \N  \N  \N  \N  \N  \N
1691667344000   f   -1  431 ArrangeBy[[Column(0)]]  Dataflow: materialize.qck.num_bids  7673    00:00:00.104800
1691667344000   f   1   431 ArrangeBy[[Column(0)]]  Dataflow: materialize.qck.num_bids  7674    00:00:00.104800
1691667345000   t   \N  \N  \N  \N  \N  \N
...
```

In this way you can see that currently the only operator that is doing
more than 100 milliseconds worth of work is the `ArrangeBy` operator
from the materialized view `num_bids`.

## Why is Materialize using so much memory?

[Arrangements](/docs/overview/arrangements) take up most of
Materialize’s memory use. Arrangements maintain indexes for data as it
changes. These queries extract the numbers of records and the size of
the arrangements. The reported records may exceed the number of logical
records; the report reflects the uncompacted state.

<div class="highlight">

``` chroma
-- Extract dataflow records and sizes
SELECT
    s.id,
    s.name,
    s.records,
    pg_size_pretty(s.size) AS size
FROM mz_introspection.mz_dataflow_arrangement_sizes AS s
ORDER BY s.size DESC;
```

</div>

```
  id   |                   name                    | records  |    size
-------+-------------------------------------------+----------+------------
 49030 | Dataflow: materialize.public.num_bids     | 10000135 | 165 MB
 49031 | Dataflow: materialize.public.num_bids_idx |       33 | 1661 bytes
```

If you need to drill down into individual operators, you can query
`mz_arrangement_sizes` instead.

<div class="highlight">

``` chroma
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

</div>

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

- The [**Cluster Overview**](/docs/console/clusters/) page displays the
  cluster resource utilization for a selected cluster as well as the
  resource intensive objects in the cluster.

- The [**Environment Overview**](/docs/console/monitoring/) page
  displays the resource utilization for all your clusters. You can
  select a specific cluster to view its **Overview** page.

## Is work distributed equally across workers?

Work is distributed across workers by the hash of their keys. Thus, work
can become skewed if situations arise where Materialize needs to use
arrangements with very few or no keys. Example situations include:

- Views that maintain order by/limit/offset
- Cross joins
- Joins where the join columns have very few unique values

Additionally, the operators that implement data sources may demonstrate
skew, as they (currently) have a granularity determined by the source
itself. For example, Kafka topic ingestion work can become skewed if
most of the data is in only one out of multiple partitions.

<div class="highlight">

``` chroma
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

</div>

## I found a problematic operator. Where did it come from?

Look up the operator in `mz_dataflow_addresses`. If an operator has
value `x` at position `n`, then it is part of the `x` subregion of the
region defined by positions `0..n-1`. The example SQL query and result
below shows an operator whose `id` is 515 that belongs to “subregion 5
of region 1 of dataflow 21”.

<div class="highlight">

``` chroma
SELECT * FROM mz_introspection.mz_dataflow_addresses WHERE id=515;
```

</div>

```
 id  | worker_id | address
-----+-----------+----------
 515 |      0    | {21,1,5}
```

Usually, it is only important to know the name of the dataflow a
problematic operator comes from. Once the name is known, the dataflow
can be correlated to an index or materialized view in Materialize.

Each dataflow has an operator representing the entire dataflow. The
address of said operator has only a single entry. For the example
operator 515 above, you can find the name of the dataflow if you can
find the name of the operator whose address is just “dataflow 21.”

<div class="highlight">

``` chroma
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

</div>

## I dropped an index, why haven’t my plans and dataflows changed?

It’s likely that your index has **downstream dependencies**. If an index
has dependent objects downstream, its underlying dataflow will continue
to be maintained and take up resources until all dependent object are
dropped or altered, or Materialize is restarted.

To check if there are residual dataflows on a specific cluster, run the
following query:

<div class="highlight">

``` chroma
SET CLUSTER TO <cluster_name>;

SELECT ce.export_id AS dropped_index_id,
       s.name AS dropped_index_name,
       s.id AS dataflow_id
FROM mz_internal.mz_dataflow_arrangement_sizes s
JOIN mz_internal.mz_compute_exports ce ON ce.dataflow_id = s.id
LEFT JOIN mz_catalog.mz_objects o ON o.id = ce.export_id
WHERE o.id IS NULL;
```

</div>

You can then use the `dropped_index_id` object identifier to list the
downstream dependencies of the residual dataflow, using:

<div class="highlight">

``` chroma
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

</div>

To force a re-plan of the downstream objects that doesn’t consider the
dropped index, you have to drop and recreate all downstream
dependencies.

<div class="warning">

**WARNING!** Forcing a re-plan using the approach above **will trigger
hydration**, which incurs downtime while the objects are recreated and
backfilled with pre-existing data. We recommend doing a [blue/green
deployment](/docs/manage/dbt/blue-green-deployments/) to handle these
changes in production environments.

</div>

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/dataflow-troubleshooting.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
