---
audience: developer
canonical_url: https://materialize.com/docs/transform-data/patterns/
complexity: advanced
description: Learn about common Materialize query patterns.
doc_type: reference
keywords:
- 'Important:'
- SELECT D
- Patterns
- CREATE MATERIALIZED
- SELECT AUCTION_ID
- 'For sources, tables and materialized views:'
- retain history
- 'Private Preview:'
- ALTER MATERIALIZED
product_area: SQL
status: experimental
title: Patterns
---

# Patterns

## Purpose
Learn about common Materialize query patterns.

If you need to understand the syntax and options for this command, you're in the right place.


Learn about common Materialize query patterns.


The following section provides examples of implementing some common query
patterns in Materialize:


---

## Durable subscriptions


[//]: # "TODO: Move to Serve results section"

[Subscriptions](/sql/subscribe/) allow you to stream changing results from
Materialize to an external application programatically. Like any connection over
the network, subscriptions might get disrupted for both expected and unexpected
reasons. In such cases, it can be useful to have a mechanism to gracefully
recover data processing.

To avoid the need for re-processing data that was already sent to your external
application following a connection disruption, you can:

- Adjust the [history retention period](#history-retention-period) for the
  objects that a subscription depends on, and

- [Access past versions of this
  data](#enabling-durable-subscriptions-in-your-application) at specific points
  in time to pick up data processing where you left off.

## History retention period

> **Private Preview:** This feature is in private preview.

By default, all user-defined sources, tables, materialized views, and indexes
keep track of the most recent version of their underlying data. To gracefully
recover from connection disruptions and enable lossless, _durable
subscriptions_, you can configure the sources, tables, and materialized views
that the subscription depends on to **retain history**.

> **Important:** 

Configuring indexes to retain history is not recommended. Instead, consider
creating a materialized view for your subscription query and configuring the
history retention period on that view.


To configure the history retention period for sources, tables and materialized
views, use the `RETAIN HISTORY` option in its `CREATE` statement. This value can
also be adjusted at any time using the object-specific `ALTER` statement.

### Semantics

#### Increasing the history retention period

When you increase the history retention period for an object, both the existing
historical data and any subsequently produced historical data are retained for
the specified time period.

**For sources, tables and materialized views:** increasing the history retention
  period will not restore older historical data that was already outside the
  previous history retention period before the change.

Configuring indexes to retain history is not recommended. Instead, consider
creating a materialized view for your subscription query and configuring the
history retention period on that view.

See also [Considerations](#considerations).

#### Decreasing the history retention period

When you decrease the history retention period for an object:

* Newly produced historical data is retained for the new, shorter history
  retention period.

* Historical data outside the new, shorter history retention period is no longer
  retained. If you subsequently increase the history retention period again,
  the older historical data may already be unavailable.

See also [Considerations](#considerations).

### Set history retention period

> **Important:** 

Setting the history retention period for an object will lead to increased
resource utilization. Moreover, for indexes, setting history retention period is
not recommended. Instead, consider creating a materialized view for your
subscription query and configuring the history retention period on that view.
See [Considerations](#considerations).


To set the history retention period for [sources](/sql/create-source/),
[tables](/sql/create-table/), and [materialized
views](/sql/create-materialized-view/), you can either:

- Specify the `RETAIN HISTORY` option in the `CREATE` statement. The `RETAIN
   HISTORY` option accepts positive [interval](/sql/types/interval/)
   values (e.g., `'1hr'`). For example:

   ```mzsql
   CREATE MATERIALIZED VIEW winning_bids
   WITH (RETAIN HISTORY FOR '1hr') AS
   SELECT auction_id,
         bid_id,
         item,
         amount
   FROM highest_bid_per_auction
   WHERE end_time < mz_now();
   ```text

- Specify the `RETAIN HISTORY` option in the `ALTER` statement. The `RETAIN
  HISTORY` option accepts positive [interval](/sql/types/interval/) values
  (e.g., `'1hr'`). For example:

  ```mzsql
  ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '2hr');
  ```bash

### View history retention period for an object

To see what history retention period has been configured for an object, look up
the object in the
[`mz_internal.mz_history_retention_strategies`](/sql/system-catalog/mz_internal/#mz_history_retention_strategies)
catalog table. For example:

```mzsql
SELECT
    d.name AS database_name,
    s.name AS schema_name,
    mv.name,
    hrs.strategy,
    hrs.value
FROM
    mz_catalog.mz_materialized_views AS mv
        LEFT JOIN mz_schemas AS s ON mv.schema_id = s.id
        LEFT JOIN mz_databases AS d ON s.database_id = d.id
        LEFT JOIN mz_internal.mz_history_retention_strategies AS hrs ON mv.id = hrs.id
WHERE mv.name = 'winning_bids';
```text

If set, the returning result includes the value (in milliseconds) of the history
retention period:

```nofmt

 database_name | schema_name |     name     | strategy |  value
---------------+-------------+--------------+----------+---------
 materialize   | public      | winning_bids | FOR      | 7200000
```bash

### Unset/reset history retention period

To disable history retention, reset the history retention period; i.e., specify
the `RESET (RETAIN HISTORY)` option in the `ALTER` statement. For example:

```mzsql
ALTER MATERIALIZED VIEW winning_bids RESET (RETAIN HISTORY);
```bash

### Considerations

#### Resource utilization

Increasing the history retention period for an object will lead to increased
resource utilization in Materialize.

**For sources, tables and materialized views:**  Increasing the history
retention period for these objects increases the amount of historical data that
is retained in the storage layer. You can expect storage resource utilization to
increase, which may incur additional costs.

**For indexes:** Configuring indexes to retain history is not recommended.
Instead, consider creating a materialized view for your subscription query and
configuring the history retention period on that view.

#### Best practices

- Because of the increased storage costs and processing time for the additional
  historical data, consider configuring history retention period on the object
  directly powering the subscription, rather than all the way through the
  dependency chain from the source to the materialized view.

- Configuring indexes to retain history is not recommended. Instead, consider
  creating a materialized view for your subscription query and configuring the
  history retention period on that view.

#### Clean-up

The history retention period represents the minimum amount of historical data
guaranteed to be retained by Materialize. History clean-up is processed in the
background, so older history may be accessible for the period of time between
when it falls outside the retention period and when it is cleaned up.

## Enabling durable subscriptions in your application

1. In Materialize, configure the history retention period for the object(s)
queried in the `SUBSCRIBE`. Choose a duration you expect will allow you to
recover in case of connection drops. One hour (`1h`) is a good place to start,
though you should be mindful of the [impact](#considerations) of increasing an
object's history retention period.

1. In order to restart your application without losing or re-snapshotting data
after a connection drop, you need to store the latest timestamp processed for
the subscription (either in Materialize, or elsewhere in your application
state). This will allow you to resume using the retained history upstream.

1. The first time you start the subscription, run the following
continuous query against Materialize in your application code:

   ```mzsql
   SUBSCRIBE (<your query>) WITH (PROGRESS, SNAPSHOT true);
   ```text

   If you do not need a full snapshot to bootstrap your application,  change
   this to `SNAPSHOT false`.

1. As results come in continuously, buffer the latest results in memory until
you receive a [progress](/sql/subscribe#progress) message. At that point, the
data up until the progress message is complete, so you can:

   1. Process all the buffered data in your application.
   1. Persist the `mz_timestamp` of the progress message.

1. To resume the subscription in subsequent restarts, use the following
continuous query against Materialize in your application code:

   ```mzsql
   SUBSCRIBE (<your query>) WITH (PROGRESS, SNAPSHOT false) AS OF <last_progress_mz_timestamp>;
   ```text

   In a similar way, as results come in continuously, buffer the latest results
   in memory until you receive a [progress](/sql/subscribe#progress) message. At that point,
   the data up until the progress message is complete, so you can:

   1. Process all the buffered data in your application.
   1. Persist the `mz_timestamp` of the progress message.

   You can tweak the flush interval at which you durably record the latest
   progress timestamp, if every progress message is too frequent.

### Note about idempotency

The guidance above recommends you buffer data in memory until receiving a
progress message, and then persist the data and progress message `mz_timestamp`
at the same time. This is to ensure data is processed **exactly once**.

In the case that your application crashes and you need to resume your subscription using the
persisted progress message `mz_timestamp`:
* If you were processing data in your application before persisting the subsequent progress message's `mz_timestamp`: you may end up processing duplicate data.
* If you were persisting the progress message's `mz_timestamp` before processing all the
buffered data from before that progress message: you may end up dropping some data.

As a result, to guarantee that the data processing occurs only once after your
application crashes, you must write the progress message `mz_timestamp` and all
buffered data **together in a single transaction**.


---

## Partitioning and filter pushdown


[//]: # "TODO link to the source table docs once that feature is documented."

A few types of Materialize collections are durably written to storage: [materialized views](/sql/create-materialized-view/), [tables](/sql/create-table), and [sources](/sql/create-source).

Internally, each collection is stored as a set of **runs** of data, each of which is sorted and then partitioned up into individual **parts**, and those parts are written to object storage and fetched only when necessary to satisfy a query. Materialize will also periodically **compact** the data it stores, to consolidate small parts into larger ones or discard deleted rows.

Using the `PARTITION BY` option, you can specify the internal ordering that
Materialize will use to sort, partition, and store these runs of data.
A well-chosen partitioning can unlock optimizations like [filter pushdown](#filter-pushdown), which in turn can make queries and other operations more efficient.

> **Note:** 
The `PARTITION BY` option has no impact on the order in which records are returned by queries.
If you want to return results in a specific order, use an `ORDER BY` clause on your [`SELECT` statement](/sql/select/).


## Syntax

The option `PARTITION BY <column list>` declares that a [materialized view](/sql/create-materialized-view/#with_options) or [table](/sql/create-table/#syntax) should be partitioned by the listed columns.
For example, a table that stores an append-only collection of events may want to partition the data by time:

```mzsql
CREATE TABLE events (event_ts timestamptz, body jsonb)
WITH (
    PARTITION BY (event_ts)
);
```text

This `PARTITION BY` clause declares that events with similar `event_ts` timestamps should be stored together.

When multiple columns are specified, rows are partitioned lexicographically.
For example, `PARTITION BY (event_date, event_time)` would partition first by the created date;
if many rows have the same `event_date`, those rows would be partitioned by the `event_time` column.
Durable collections without a `PARTITION BY` option can be partitioned arbitrarily.

> **Note:** 
The `PARTITION BY` option does not mean that rows with different values for the specified columns will be stored in different parts, only that rows with similar values for those columns should be stored together.


## Requirements

Materialize currently imposes some restrictions on the list of columns in the `PARTITION BY` clause.

- This clause must list a prefix of the columns in the collection. For example:
  - if you're creating a table that partitions by a single column, that column must be the first column in the table's schema definition;
  - if you're creating a table that partitions by two columns, those columns must be the first two columns in the table's schema definition and listed in the same order.
- Only certain types of columns are supported. This includes:
    - all fixed-width integer types, including `smallint`, `integer`, and `bigint`;
    - date and time types, including `date`, `time`, `timestamp`, `timestamptz`, and `mz_timestamp`;
    - string types like `text` and `bytea`;
    - `boolean` and `uuid`;
    - `record` types where all fields types are supported.


## Filter pushdown

Suppose that our example `events` table has accumulated years' worth of data, but we're running a query with a [temporal filter](/transform-data/patterns/temporal-filters/) that matches only rows with recent timestamps.

```mzsql
SELECT * FROM events WHERE mz_now() <= event_ts + INTERVAL '5min';
```text

This query returns only rows with similar values for `event_ts`: timestamps in the last five minutes.
Since we declared that our `events` table is partitioned by `event_ts`, that means all the rows that pass this filter will be stored in the same small subset of parts.

Materialize tracks a small amount of metadata for every part, including the range of possible values for many columns. When it can determine that none of the data in a part will match a filter, it will skip fetching that data from object storage. This optimization is called _filter pushdown_, and when you're querying with a selective filter against a large collection, it can save a great deal of time and computation.

Materialize will always try to apply filter pushdown to your query, but that filtering is usually only effective when similar rows are stored together.
If you want to make sure that the filter pushdown optimization is effective for your query, you can:

- Use a `PARTITION BY` clause on the relevant column to ensure that data with similar values for that column are stored close together.
- Add a filter to your query that only returns true for a narrow range of values in that column.

Filters that consist of arithmetic, date math, and comparisons are generally eligible for pushdown, including all the examples in this page. However, more complex filters might not be. You can check whether the filters in your query can be pushed down using [an `EXPLAIN` statement](/sql/explain-plan/). In the following example, we can be confident our temporal filter will be pushed down because it's present in the `pushdown` list at the bottom of the output.

```mzsql
EXPLAIN SELECT * FROM events WHERE mz_now() <= event_ts + INTERVAL '5min';
----
Explained Query:
[...]
Source materialize.public.events
  [...]
  pushdown=((mz_now() <= timestamp_to_mz_timestamp((#0 + 00:05:00))))
```text

Some common functions, such as casting from a string to a timestamp, can prevent filter pushdown for a query. For similar functions that _do_ allow pushdown, see [the pushdown functions documentation](/sql/functions/pushdown/).

## Examples

These examples create real objects. After you have tried the examples, make sure to drop these objects and spin down any resources you may have created.

### Partitioning by timestamp

For timeseries or "event"-type collections, it's often useful to partition the data by timestamp.

1. First, create a table called `events`.
    ```mzsql
    -- Create a table of timestamped events. Note that the `event_ts` column is
    -- first in the column list and in the parition-by clause.
    CREATE TABLE events (
        event_ts timestamptz,
        content text
    ) WITH (
        PARTITION BY (event_ts)
    );
    ```text

1. Insert a few records, one "older" record and one more recent.
    ```mzsql
    INSERT INTO events VALUES (now()::timestamp - '5 minutes', 'hello');
    INSERT INTO events VALUES (now(), 'world');
    ```text

1. Run a select statement against the data within the next five minutes. This should return only the more recent of the two rows.
    ```mzsql
    SELECT * FROM events WHERE event_ts + '2 minutes' > mz_now();
    ```text

1. To verify that Materialize fetched only the parts that contain data with the
   recent timestamps, run an `EXPLAIN FILTER PUSHDOWN` statement.
    ```mzsql
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM events WHERE event_ts + '2 minutes' > mz_now();
    ```text

If you wait a few minutes longer until there are no events that match the temporal filter, you'll notice that not only does the query return zero rows, but the explain shows that we fetched zero parts.

> **Note:** 

The exact numbers you see here may vary: parts can be much larger than a single row, and the actual level of filtering may fluctuate for small datasets as data is compacted together internally. However, datasets of a few gigabytes or larger should reliably see benefits from this optimization.


### Partitioning by category

Other datasets don't have a strong timeseries component, but they do have a clear notion of type or category. For example, suppose you have a collection of music venues spread across the world that you regularly query by a single country.

1. First, create a table called `venues`, partitioned by country.
    ```mzsql
    -- Create a table for our venue data.
    -- Once again, the partition column is listed first.
    CREATE TABLE venues (
        country_code text,
        id bigint,
        name text
    ) WITH (
        PARTITION BY (country_code)
    );
    ```text

1. Insert a few records with different country codes.
    ```mzsql
    INSERT INTO venues VALUES ('US', 1, 'Rock World');
    INSERT INTO venues VALUES ('CA', 2, 'Friendship Cove');
    ```text

1. Query for venues in particular countries.
    ```mzsql
    SELECT * FROM venues WHERE country_code IN ('US', 'MX');
    ```text

1. Run `EXPLAIN FILTER PUSHDOWN` to check that we're filtering out parts that don't include data that's relevant to the query.
    ```mzsql
    EXPLAIN FILTER PUSHDOWN FOR
    SELECT * FROM venues WHERE country_code IN ('US', 'MX');
    ```text

> **Note:** 

As before, we're not guaranteed to see much or any benefit from filter pushdown on small collections... but for datasets of over a few gigabytes, we should reliably be able to filter down to a subset of the parts we'd otherwise need to fetch.


---

## Percentile calculation


Percentiles are a useful statistic to understand and interpret data distribution. This pattern covers how to use histograms to efficiently calculate percentiles in Materialize.

One way to compute percentiles is to order all values and pick the value at the
position of the corresponding percentile. However, this approach requires
storing all values, causing memory to grow linearly with the number of tracked
values.

Instead, more memory efficient alternatives are to use **histograms** and **High
Dynamic Range (HDR) histograms**:

- [Histograms](#using-histograms-to-compute-exact-percentiles) have a lower
  memory footprint that is linear to the number of _unique_ values and can
  compute precise percentiles. However, for domains with _high_ cardinality,
  calculating precise percentiles may be computationally expensive.

- [HDR histograms](#using-hdr-histograms-to-compute-approximate-percentiles)
  further reduce the memory footprint but computes approximate percentiles.
  Depending on the precision needed for the percentiles, HDR histograms may be
  preferred for domains with a high cardinality and dynamic range of values.

## Using histograms to compute exact percentiles

Histograms summarize data sets by grouping values into ranges and counting how
many elements fall into each range. From this summary, you can get the
percentile information by identifying the range where the cumulative count
crosses the desired percentile threshold. By grouping each distinct value into
its own range, you can get exact percentiles; however, this can be
computationally expensive if there are large number of distinct values.
Alternatively, you can get an approximate percentiles by using [HDR
histograms](#using-hdr-histograms-to-compute-approximate-percentiles).


To use histograms to compute exact percentiles:

- First, create a histogram view that groups each distinct value into its own
  bucket and counts the number of each distinct value.

- Then, using a cross join on the histogram view, create a distribution view
  that calculates the cumulative density for a bucket by dividing the cumulative
  counts (sum of the counts for all buckets up to and including that bucket) by
  the total count.

  > **Note:** 

  The use of the cross join produces a number of outputs that is quadratic in
  the input. And, while the results will only be linear in size, it may take a
  disproportionate amount of time to produce and maintain.

  

### Example

1. Create a table `input`:

   ```mzsql
   CREATE TABLE input (value BIGINT);
   ```text

2. Insert into the `input` table values `1` to `10`.

   ```mzsql
   INSERT INTO input
   SELECT n FROM generate_series(1,10) AS n;
   ```text

1. Create a `histogram` view to track unique values from the
   `input` table and their count:

   ```mzsql
   CREATE VIEW histogram AS
   SELECT
     value AS bucket,
     count(*) AS count_of_bucket_values
   FROM input
   GROUP BY value;
   ```text

1. Create a view `distribution` to calculate the cumulative count and the
   cumulative density for each bucket. The cumulative density is calculated by
   dividing the cumulative count for a bucket (i.e., count for all bucket values
   up to and including that bucket) by the total count.

   ```mzsql
   CREATE VIEW distribution AS
   SELECT
     h.bucket,
     h.count_of_bucket_values,
     sum(g.count_of_bucket_values) AS cumulative_count,
     sum(g.count_of_bucket_values) / (SELECT sum(count_of_bucket_values) FROM histogram) AS cumulative_density
   FROM histogram g, histogram h
   WHERE g.bucket <= h.bucket
   GROUP BY h.bucket, h.count_of_bucket_values
   ORDER BY cumulative_density;
   ```text

   > **Note:** 

   The use of the cross join produces a number of outputs that is quadratic in
   the input. And, while the results will only be linear in size, it may take a
   disproportionate amount of time to produce and maintain.

   

1. You can then query `distribution` by the `cumulative_density` field to
   return specific percentiles. For example, the following query returns the
   90-th percentile.

   ```mzsql
   SELECT bucket AS percentile90
   FROM distribution
   WHERE cumulative_density >= 0.9
   ORDER BY cumulative_density
   LIMIT 1;
   ```bash


## Using HDR histograms to compute approximate percentiles

[HDR histograms](https://github.com/HdrHistogram/HdrHistogram) can be used to approximate percentiles in a space efficient manner that scales well even for large domains with many distinct values. HDR histograms reduce the precision of values that are tracked and use buckets with variable width. Buckets that are closer to 0 are smaller whereas buckets far away from 0 are wider. This works particularly well for data that exhibits a long tail of large values, e.g., latency measurements.

HDR histograms are related to how [floating point numbers are
represented](https://en.wikipedia.org/wiki/Double-precision_floating-point_format)
as integers. The underlying assumption is that smaller numbers require a higher
precision to be distinguishable (e.g. 5 ms and 6 ms are different and should be
in different buckets) whereas larger numbers can be rounded more aggressively as
their relative error becomes less relevant (e.g. 10000 ms and 10001 ms are
almost the same and can reside in the same bucket).

In the example below, to reduce the number of buckets, the values are first
decomposed into `significand * 2^exponent`, and then with the precision of the
significand lowered, reconstructed for the respective bucket value.

- With higher precisions, fewer items are kept in the same bucket and thus, more
  memory is required, but the approximate percentile becomes more precise.

- With lower precisions, more items are kept in the same bucket, and thus, the
  less memory is required, but the approximate percentile becomes less precise.

Except for the bucket calculation, the basic ideas of [using histograms to
compute percentiles](#using-histograms-to-compute-exact-percentiles) remains the
same for HDR histograms.

### Example

> **Tip:** 

The following example assumes you have not previously created and populated the
`input` table from the [Using histograms to compute exact percentiles
example](#example). If you have created and populated the table, skip the
corresponding steps.


1. Create a table `input`:

   ```mzsql
   CREATE TABLE input (value BIGINT);
   ```text

2. Insert into the `input` table values `1` to `10`.

   ```mzsql
   INSERT INTO input
   SELECT n FROM generate_series(1,10) AS n;
   ```text

1. Create a `hdr_histogram` view. To reduce the number of buckets, the values
   are rounded down to the nearest multiple of 1/16. Specifically, the values
   are first decomposed into `significand * 2^exponent`. Then by reducing the
   precision of the significand to 1/16 (4 bits), the value is reconstructed to
   an approximated value.

   

   #### Materialize Console


```mzsql
CREATE VIEW hdr_histogram AS
WITH
  input_parts AS (
    SELECT
      CASE WHEN value = 0 THEN NULL
          ELSE trunc(log(2, abs(value)))::int
      END AS exponent,
      CASE WHEN value = 0 THEN NULL
          ELSE value / pow(2.0, trunc(log(2, abs(value)))::int)
      END AS significand
    FROM input
  ),
  buckets AS (
    -- reduce precision by 4 bits to round down the value to the nearest multiple of 1/16
    SELECT
      trunc(significand * pow(2.0, 4)) / pow(2.0, 4)
        * pow(2.0, exponent)
        AS bucket
    FROM input_parts
  )
SELECT
  COALESCE(bucket, 0) AS bucket,
  count(*) AS count_of_bucket_values
FROM buckets
GROUP BY bucket;
```json

   

   #### psql


```mzsql
-- precision for the representation of the significand in bits
\set precision 4

CREATE VIEW hdr_histogram AS
WITH
  input_parts AS (
    SELECT
      CASE WHEN value = 0 THEN NULL
          ELSE trunc(log(2, abs(value)))::int
      END AS exponent,
      CASE WHEN value = 0 THEN NULL
          ELSE value / pow(2.0, trunc(log(2, abs(value)))::int)
      END AS significand
    FROM input
  ),
  buckets AS (
    -- reduce precision by 4 bits to round down the value to the nearest multiple of 1/16
    SELECT
      trunc(significand * pow(2.0, :precision)) / pow(2.0, :precision)
        * pow(2.0, exponent)
        AS bucket
    FROM input_parts
  )
SELECT
  COALESCE(bucket, 0) AS bucket,
  count(*) AS count_of_bucket_values
FROM buckets
GROUP BY bucket;
```json
   
   

1. Create a view `hdr_distribution` to calculate the cumulative count and the
   cumulative density for each bucket. The cumulative density is calculated by
   dividing the cumulative count for a bucket (i.e., count for all bucket values
   up to and including that bucket) by the total count.

   ```mzsql
   CREATE VIEW hdr_distribution AS
   SELECT
     h.bucket,
     h.count_of_bucket_values,
     sum(g.count_of_bucket_values) AS cumulative_count,
     sum(g.count_of_bucket_values) / (SELECT sum(count_of_bucket_values) FROM hdr_histogram) AS  cumulative_density
   FROM hdr_histogram g, hdr_histogram h
   WHERE g.bucket <= h.bucket
   GROUP BY h.bucket, h.count_of_bucket_values;
   ```text

1. You can then query `hdr_distribution` by the `cumulative_density` field
   to return _approximate_ percentiles. More precisely, the query returns the
   lower bound for the percentile (the next larger bucket represents the upper
   bound).

   For example, the following query returns the lower bound for the 90-th
   percentile.

   ```mzsql
   SELECT bucket AS approximate_percentile
   FROM hdr_distribution
   WHERE cumulative_density >= 0.9
   ORDER BY cumulative_density
   LIMIT 1;
   ```bash

### HDR Histograms and approximate values

For small numbers, `distribution` and `hdr_distribution` are identical. Even in
`hdr_distribution`, all numbers from 1 to 10 are stored in their own buckets. To
verify, query `hdr_distribution`:

```mzsql
SELECT * FROM hdr_distribution;
```text

The query returns the following:

```none
 bucket | frequency | cumulative_count     | cumulative_density
--------+-----------+----------------------+-------------------------
      1 |         1 |                    1 |                     0.1
      2 |         1 |                    2 |                     0.2
      3 |         1 |                    3 |                     0.3
      4 |         1 |                    4 |                     0.4
      5 |         1 |                    5 |                     0.5
      6 |         1 |                    6 |                     0.6
      7 |         1 |                    7 |                     0.7
      8 |         1 |                    8 |                     0.8
      9 |         1 |                    9 |                     0.9
     10 |         1 |                   10 |                       1
(10 rows)
```text

But if values grow larger, buckets can contain more than one value. Let's see what happens if more values are added to the `input` table.

```mzsql
INSERT INTO input SELECT n FROM generate_series(11,10001) AS n;
```text

Unlike the `distribution` view (used in the histogram approach) where each
bucket contains only a single value and has 10001 rows, a single bucket in
`hdr_distribution` can represent up to 512 distinct values and has 163 rows:

```mzsql
SELECT * FROM hdr_distribution ORDER BY cumulative_density;
```text

The query returns the following:

```none
 bucket | frequency | cumulative_count     | cumulative_density
--------+-----------+----------------------+-------------------------------------------
      1 |         1 |                    1 |     0.00000999990000099999000009999900001
      2 |         1 |                    2 |     0.00001999980000199998000019999800002
      3 |         1 |                    3 |     0.00002999970000299997000029999700003
      4 |         1 |                    4 |     0.00003999960000399996000039999600004
      5 |         1 |                    5 |     0.00004999950000499995000049999500005
...skipping...
   7424 |       256 |                 7679 | 0.767823217678232176782321767823217678232
   7680 |       256 |                 7935 | 0.793420657934206579342065793420657934207
   7936 |       256 |                 8191 | 0.819018098190180981901809819018098190181
   8192 |       512 |                 8703 |  0.87021297870212978702129787021297870213
   8704 |       512 |                 9215 | 0.921407859214078592140785921407859214079
   9216 |       512 |                 9727 | 0.972602739726027397260273972602739726027
   9728 |       274 |                10001 |                                         1
(163 rows)
```text

When querying `hdr_distribution`  for the 90-th percentile value:

```mzsql
SELECT bucket AS approximate_percentile
FROM hdr_distribution
WHERE cumulative_density >= 0.9
ORDER BY cumulative_density
LIMIT 1;
```text

The query returns an approximate
percentile of `8704` (or more precisely between `8704`and `9216`) whereas the
precise percentile is `9001`.

```none
 approximate_percentile
------------------------
                   8704
(1 row)
```text

The precision of the approximation can be adapted by changing the `precision` in the definition of `hdr_histogram`. The higher the `precision`, the fewer items are kept in the same bucket and therefore the more precise the approximate percentile becomes. The lower the `precision`, the more items are kept in the same bucket and therefore the less memory is required.


---

## Rules execution engine


A rules engine is a powerful way to make decisions based on data.
With Materialize, you can execute those rules continuously.

Rule execution use cases can have many thousands of rules, so it's sometimes impractical to have a separate SQL view for each one.
Fortunately there is a pattern to address this need without having to create and manage many separate views.
If your rules are simple enough to be expressed as data (i.e. not arbitrary SQL), then you can use `LATERAL` joins to implement a rules execution engine.

## Hands-on Example

In this example, we have a dataset about birds. We need to subscribe to all birds in the dataset that satisfy a set of rules.
Instead of creating separate views for each rule, you can encode the rules **as data** and use a `LATERAL` join to execute them.

A `LATERAL` join is essentially a `for` loop; for each element of one dataset, do something with another dataset.
In our example, for each rule in a `bird_rules` dataset, we filter the `birds` dataset according to the rule.

### Create Resources

1. Create the `birds` table and insert some birds.
    ```mzsql
    CREATE TABLE birds (
    id INT,
    name VARCHAR(50),
    wingspan_cm FLOAT,
    colors jsonb
    );

    INSERT INTO birds (id, name, wingspan_cm, colors) VALUES
    (1, 'Sparrow', 15.5, '["Brown"]'),
    (2, 'Blue Jay', 20.2, '["Blue"]'),
    (3, 'Cardinal', 22.1, '["Red"]'),
    (4, 'Robin', 18.7, '["Red","Brown"]'),
    (5, 'Hummingbird', 8.2, '["Green"]'),
    (6, 'Penguin', 99.5, '["Black", "White"]'),
    (7, 'Eagle', 200.8, '["Brown"]'),
    (8, 'Owl', 105.3, '["Gray"]'),
    (9, 'Flamingo', 150.6, '["Pink"]'),
    (10, 'Pelican', 180.4, '["White"]');
    ```text
1. Create the `bird_rules` table and insert a few rules.
    ```mzsql
    CREATE TABLE bird_rules (
    id INT,
    starts_with CHAR(1),
    wingspan_operator VARCHAR(3),
    wingspan_cm FLOAT,
    colors JSONB
    );

    INSERT INTO bird_rules (id, starts_with, wingspan_operator, wingspan_cm, colors)
    VALUES
    (1, 'P', 'GTE', 50.0, '["Blue"]'),
    (2, 'P', 'LTE', 100.0, '["Black","White"]'),
    (3, 'R', 'GTE', 20.0, '["Red"]');
    ```text
    Each rule has a unique `id` and encodes filters on starting letter, wingspan, and color. For `wingspan_operator`, `'GTE'` means "greater than or equal" and `'LTE'` means "less than or equal". For more complicated rules with varying schemas, consider using the [`jsonb` type](/sql/types/jsonb) and adjust the logic in the upcoming `LATERAL` join to suit your needs.

### Create the View

Here is the view that will execute our bird rules:

```mzsql
CREATE VIEW birds_filtered AS
SELECT r.id AS rule_id, b.name, b.colors, b.wingspan_cm
FROM
-- retrieve bird rules
(SELECT id, starts_with, wingspan_operator, wingspan_cm, colors FROM bird_rules) AS r,
-- for each bird rule, find the birds who satisfy it
LATERAL (
    SELECT *
    FROM birds
    WHERE r.starts_with = SUBSTRING(birds.name, 1, 1)
        AND (
            (r.wingspan_operator = 'GTE' AND birds.wingspan_cm >= r.wingspan_cm)
            OR
            (r.wingspan_operator = 'LTE' AND birds.wingspan_cm <= r.wingspan_cm)
        )
        AND r.colors <@ birds.colors
) AS b;
```bash

### Subscribe to Changes

1. Subscribe to the changes of `birds_filtered`.
    ```mzsql
    SUBSCRIBE TO birds_filtered;
    ```text

   > **Tip:** 
   If running this example in a client, use `COPY(SUBSCRIBE...) TO STDOUT;`.
   

    ```nofmt
    mz_timestamp  | mz_diff | rule_id |   name   |      colors         | wingspan_cm
    --------------|---------|---------|----------|---------------------|------------
    1688673701670      1         2       Penguin     ["Black","White"]       99.5
    ```text
    Notice that the majestic penguin satisfies rule 2. None of the other birds satisfy any of the rules.
1. In a separate session, insert a new bird that satisfies rule 3. Rule 3 requires a bird whose first letter is 'R', with a wingspan greater than or equal to 20 centimeters, and whose colors contain "Red". We will insert a "Really big robin" that satisfies this rule.
    ```mzsql
    INSERT INTO birds VALUES (11, 'Really big robin', 25.0, '["Red"]');
    ```text
    Back in the `SUBSCRIBE` terminal, notice the output was immediately updated.
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |       name         |  colors   | wingspan_cm
    --------------|---------|---------|--------------------|-----------|------------
    1688674195279      1         3       Really big robin     ["Red"]        25
    ```text
1. For fun, let's delete rule 3 and see what happens.
    ```mzsql
    DELETE FROM bird_rules WHERE id = 3;
    ```text
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |       name         |  colors   | wingspan_cm
    --------------|---------|---------|--------------------|-----------|------------
    1688674195279     -1         3       Really big robin     ["Red"]        25
    ```text
    Notice the bird was removed because the rule no longer exists.
1. Now let's update an existing bird so that it satisfies a new rule. It turns out our penguin also has some blue coloration we didn't notice before.
    ```mzsql
    UPDATE birds SET colors = '["Black","White","Blue"]' WHERE name = 'Penguin';
    ```text
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |   name   |      colors              | wingspan_cm
    --------------|---------|---------|----------|--------------------------|------------
    1688675781416     -1         2       Penguin   ["Black","White"]               99.5
    1688675781416      1         2       Penguin   ["Black","White","Blue"]        99.5
    1688675781416      1         1       Penguin   ["Black","White","Blue"]        99.5
    ```text
    First there was an update to the row corresponding to the penguin's adherence to rule 2: a diff of -1 to delete the old value with just the black and white colors, and a diff of +1 to add the new value with black, white, and blue colors. Then there was a new record showing that the penguin now also adheres to rule 1.

### Clean Up

Press `Ctrl+C` to stop your `SUBSCRIBE` query and then drop the tables to clean up.

```mzsql
DROP TABLE birds CASCADE;
DROP TABLE bird_rules CASCADE;
```bash

## Conclusion

Rule execution engines can be much more complex than the minimal example presented here, but the underlying principle is the same; define the rules as **data** and use a `LATERAL` join to apply each rule to the dataset. Once you materialize the view, either by creating an index or creating it as a materialized view, the results will be kept up to date automatically as the dataset changes and as the rules change.


---

## Temporal filters (time windows)


A **temporal filter** is a query condition/predicate that uses the
[`mz_now()`](/sql/functions/now_and_mz_now) function to filter data based on a
time-related condition. Using a temporal filter reduces the working dataset,
saving memory resources and focusing on results that meet the condition.

In Materialize, you implement temporal filters using the
[`mz_now()`](/sql/functions/now_and_mz_now) function (which returns
Materialize's current virtual timestamp) in a `WHERE` or `HAVING` clause;
specifically, you compare [`mz_now()`](/sql/functions/now_and_mz_now) to a
numeric or timestamp column expression. As
[`mz_now()`](/sql/functions/now_and_mz_now) progresses (every millisecond),
records for which the condition is no longer true are retracted from the working
dataset while records for which the condition becomes true are included in the
working dataset. When using temporal filters, Materialize must be prepared to
retract updates in the near future and will need resources to maintain these
retractions.

For example, the following temporal filter reduces the working dataset to those
records whose event timestamp column (`event_ts`) is no more than 5 minutes ago:

```mzsql
WHERE mz_now() <= event_ts + INTERVAL '5min'
```text

> **Note:** 
It may feel more natural to write this filter as the equivalent `WHERE event_ts >= mz_now() - INTERVAL '5min'`.
However, there are currently no valid operators for the [`mz_timestamp`
type](/sql/types/mz_timestamp) that would allow this.  See [`mz_now()` requirements and restrictions](#mz_now-requirements-and-restrictions).


The following diagram shows record `B` falling out of the result set as time
moves forward:

- In the first timeline, record `B` occurred less than 5 minutes ago (occurred
  less than 5 minutes from `mz_now()`).

- In the second timeline, as `mz_now()` progresses, record `B` occurred more
  than 5 minutes from `mz_now()`.

![temporal filter diagram](/images/temporal-filter.svg)

## `mz_now()` requirements and restrictions

This section covers `mz_now()` requirements and restrictions.

### `mz_now()` requirements

> **Tip:** 

When possible, prefer materialized views when using temporal filter to take
advantage of custom consolidation.


When creating a temporal filter using
[`mz_now()`](/sql/functions/now_and_mz_now) in a `WHERE` or `HAVING` clause, the
clause has the following shape:

```mzsql
mz_now() <comparison_operator> <numeric_expr | timestamp_expr>
```text

- `mz_now()` must be used with one of the following comparison operators: `=`,
`<`, `<=`, `>`, `>=`, or an operator that desugars to them or to a conjunction
(`AND`) of them (for example, `BETWEEN...AND...`). That is, you cannot use
date/time operations directly on  `mz_now()` to calculate a timestamp in the
past or future. Instead, rewrite the query expression to move the operation to
the other side of the comparison.


- `mz_now()` can only be compared to either a
  [`numeric`](/sql/types/numeric) expression or a
  [`timestamp`](/sql/types/timestamp) expression not containing `mz_now()`.


### `mz_now()` restrictions

The [`mz_now()`](/sql/functions/now_and_mz_now) clause has the following
restrictions:

- When used in a materialized view definition, a view definition that is being
indexed (i.e., although you can create the view and perform ad-hoc query on
the view, you cannot create an index on that view), or a `SUBSCRIBE`
statement:

- `mz_now()` clauses can only be combined using an `AND`, and

- All top-level `WHERE` or `HAVING` conditions must be combined using an `AND`,
  even if the `mz_now()` clause is nested.


  To rewrite the query, see [Disjunction (OR)
  alternatives](http://localhost:1313/docs/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or).

- If part of a  `WHERE` clause, the `WHERE` clause cannot be an [aggregate
 `FILTER` expression](/sql/functions/filters).

## Examples

These examples create real objects.
After you have tried the examples, make sure to drop these objects and spin down any resources you may have created.

> **Tip:** 

When possible, prefer materialized views when using temporal filter to take
advantage of custom consolidation.


### Sliding window

<!-- This example also appears in now_and_mz_now -->
It is common for real-time applications to be concerned with only a recent period of time.
We call this a **sliding window**.
Other systems use this term differently because they cannot achieve a continuously sliding window.

In this case, we will filter a table to only include only records from the last 30 seconds.

1. First, create a table called `events` and a view of the most recent 30 seconds of events.
    ```mzsql
    --Create a table of timestamped events.
    CREATE TABLE events (
        content TEXT,
        event_ts TIMESTAMP
    );

    -- Create a view of events from the last 30 seconds.
    CREATE VIEW last_30_sec AS
    SELECT event_ts, content
    FROM events
    WHERE mz_now() <= event_ts + INTERVAL '30s';
    ```text

1. Next, subscribe to the results of the view.
    ```mzsql
    COPY (SUBSCRIBE (SELECT ts, content FROM last_30_sec)) TO STDOUT;
    ```text

1. In a separate session, insert a record.
    ```mzsql
    INSERT INTO events VALUES ('hello', now());
    ```text

1. Back in the first session, watch the record expire after 30 seconds.
    ```nofmt
    1686868190714   1       2023-06-15 22:29:50.711 hello
    1686868220712   -1      2023-06-15 22:29:50.711 hello
    ```text
    Press `Ctrl+C` to quit the `SUBSCRIBE` when you are ready.

You can materialize the `last_30_sec` view by [recreating it as a `MATERIALIZED
VIEW`](/sql/create-materialized-view/) (results persisted to storage). When
you do so, Materialize will keep the results up to date with records expiring
automatically according to the temporal filter.


### Time-to-Live (TTL)

The **time to live (TTL)** pattern helps to filter rows with user-defined expiration times.
This example uses a `tasks` table with a time to live for each task.
Materialize then helps perform actions according to each task's expiration time.

1. First, create a table:
    ```mzsql
    CREATE TABLE tasks (name TEXT, created_ts TIMESTAMP, ttl INTERVAL);
    ```text

1. Add some tasks to track:
    ```mzsql
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 minutes');
    INSERT INTO tasks VALUES ('time_to_eat', now(), INTERVAL '1 hour');
    INSERT INTO tasks VALUES ('security_block', now(), INTERVAL '1 day');
    ```text

1. Create a view using a temporal filter **over the expiration time**. For our example, the expiration time represents the sum between the task's `created_ts` and its `ttl`.
    ```mzsql
    CREATE MATERIALIZED VIEW tracking_tasks AS
    SELECT
      name,
      created_ts + ttl as expiration_time
    FROM tasks
    WHERE mz_now() < created_ts + ttl;
    ```text
    The moment `mz_now()` crosses the expiration time of a record, that record is retracted (removed) from the result set.

You can now:

- Query the remaining time for a row:
  ```mzsql
    SELECT expiration_time - now() AS remaining_ttl
    FROM tracking_tasks
    WHERE name = 'time_to_eat';
  ```text

- Check if a particular row is still available:
  ```mzsql
  SELECT true
  FROM tracking_tasks
  WHERE name = 'security_block';
  ```text

- Trigger an external process when a row expires:
  ```mzsql
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 seconds');
    COPY( SUBSCRIBE tracking_tasks WITH (SNAPSHOT = false) ) TO STDOUT;

  ```text
  ```nofmt
  mz_timestamp | mz_diff | name       | expiration_time |
  -------------|---------|------------|-----------------|
  ...          | -1      | send_email | ...             | <-- Time to send the email!
  ```bash

### Periodically emit results

Suppose you want to count the number of records in each 1 minute time window, grouped by an `id` column.
You don't care to receive every update as it happens; instead, you would prefer Materialize to emit a single result at the end of each window.
Materialize [date functions](/sql/functions/#date-and-time-functions) are helpful for use cases like this where you want to bucket records into time windows.

The strategy for this example is to put an initial temporal filter on the input (say, 30 days) to bound it, use the [`date_bin` function](/sql/functions/date-bin) to bin records into 1 minute windows, use a second temporal filter to emit results at the end of the window, and finally apply a third temporal filter shorter than the first (say, 7 days) to set how long results should persist in Materialize.

1. First, create a table for the input records.
    ```mzsql
    CREATE TABLE input (id INT, event_ts TIMESTAMP);
    ```text
1. Create a view that filters the input for the most recent 30 days and buckets records into 1 minute windows.
    ```mzsql
    CREATE VIEW
        input_recent_bucketed
        AS
            SELECT
                id,
                date_bin(
                        '1 minute',
                        event_ts,
                        '2000-01-01 00:00:00+00'
                    )
                    + INTERVAL '1 minute'
                    AS window_end
            FROM input
            WHERE mz_now() <= event_ts + INTERVAL '30 days';
    ```text
1. Create the final output view that does the aggregation and maintains 7 days worth of results.
    ```mzsql
    CREATE MATERIALIZED VIEW output
        AS
            SELECT
              id,
              count(id) AS count,
              window_end
            FROM input_recent_bucketed
            WHERE
                mz_now() >= window_end
                    AND
                mz_now() < window_end + INTERVAL '7 days'
            GROUP BY window_end, id;
    ```text
    This `WHERE` clause means "the result for a 1-minute window should come into effect when `mz_now()` reaches `window_end` and be removed 7 days later". Without the latter constraint, records in the result set would receive strange updates as records expire from the initial 30 day filter on the input.
1. Subscribe to the `output`.
    ```mzsql
    COPY (SUBSCRIBE (SELECT * FROM output)) TO STDOUT;
    ```text
1. In a different session, insert some records.
    ```mzsql
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (2, now());
    ```text
1. Back at the `SUBSCRIBE`, wait about a minute for your final aggregation result to show up the moment the 1 minute window ends.
    ```nofmt
     mz_timestamp | mz_diff |  id   | count |      window_end
    --------------|---------|-------|-------|----------------------
    1686889140000       1       1       3       2023-06-16 04:19:00
    1686889140000       1       2       1       2023-06-16 04:19:00
    ```text
    If you are very patient, you will see these results retracted in 7 days.
    Press `Ctrl+C` to exit the `SUBSCRIBE` when you are finished playing.

From here, you could create a [Kafka sink](/sql/create-sink/) and use Kafka Connect to archive the historical results to a data warehouse (ignoring Kafka tombstone records that represent retracted results).

## Late arriving events

For various reasons, it's possible for records to arrive out of order.
For example, network connectivity issues may cause a mobile device to emit data with a timestamp from the relatively distant past.
How can you account for late arriving data in Materialize?

Consider the temporal filter for the most recent hour's worth of records.

```mzsql
WHERE mz_now() <= event_ts + INTERVAL '1hr'
```text

Suppose a record with a timestamp `11:00:00` arrives "late" with a virtual timestamp of `11:59:59` and you query this collection at a virtual timestamp of `12:00:00`.
According to the temporal filter, the record is included for results as of virtual time `11:59:59` and retracted just after `12:00:00`.

Let's say another record comes in with a timestamp of `11:00:00`, but `mz_now()` has marched forward to `12:00:01`.
Unfortunately, this record does not pass the filter and is excluded from processing altogether.

In conclusion: if you want to account for late arriving data up to some given time duration, you must adjust your temporal filter to allow for such records to make an appearance in the result set.
This is often referred to as a **grace period**.

## Temporal filter pushdown

All of the queries in the previous examples only return results based on recently-added events.
Materialize can "push down" filters that match this pattern all the way down to its storage layer, skipping over old data that’s not relevant to the query.
Here are the key benefits of this optimization:
- For ad-hoc `SELECT` queries, temporal filter pushdown can substantially improve query latency.
- When a materialized view is created or the cluster maintaining it restarts, temporal filter pushdown can substantially reduce the time it takes to start serving results.

The columns filtered should correlate with the insertion or update time of the row.
In the examples above, the `event_ts` value in each event correlates with the time the event was inserted, so filters that reference these columns should be pushed down to the storage layer.
However, the values in the `content` column are not correlated with insertion time in any way, so filters against `content` will probably not be pushed down to the storage layer.

Temporal filters that consist of arithmetic, date math, and comparisons are eligible for pushdown, including all the examples in this page.
However, more complex filters might not be. You can check whether the filters in your query can be pushed down by using [the `filter_pushdown` option](/sql/explain-plan/#output-modifiers) in an `EXPLAIN` statement. For example:

```mzsql
EXPLAIN WITH(filter_pushdown)
SELECT count(*)
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30s';
----
Explained Query:
[...]
Source materialize.public.events
  filter=((mz_now() <= timestamp_to_mz_timestamp((#1 + 00:00:30))))
  pushdown=((mz_now() <= timestamp_to_mz_timestamp((#1 + 00:00:30))))
```

The filter in our query appears in the `pushdown=` list at the bottom of the output, so the filter pushdown optimization will be able to filter out irrelevant ranges of data in that source and make the overall query more efficient.

Some common functions, such as casting from a string to a timestamp, can prevent filter pushdown for a query. For similar functions that _do_ allow pushdown, see [the pushdown functions documentation](/sql/functions/pushdown/).

> **Note:** 
See the guide on [partitioning and filter pushdown](/transform-data/patterns/partition-by/) for a **private preview** feature that can make the filter pushdown optimization more predictable.