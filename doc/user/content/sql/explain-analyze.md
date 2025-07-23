---
title: "EXPLAIN ANALYZE"
description: "Reference page for `EXPLAIN ANALYZE`. `EXPLAIN ANALYZE` is used to understand the performance of indexes and materialized views."
menu:
  main:
    parent: commands
---

`EXPLAIN ANALYZE`:

- Reports on the performance of indexes and materialized views.
- Provide the execution plan annotated with TopK hints. The TopK
  query pattern groups by some key and return the first K elements within each
  group according to some ordering.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

```mzsql
EXPLAIN ANALYZE
      CPU [, MEMORY] [WITH SKEW]
    | MEMORY [, CPU] [WITH SKEW]
    | HINTS
FOR INDEX <name> | MATERIALIZED VIEW <name>
[ AS SQL ]
;
```

{{< tip >}}
If you want to specify both `CPU` or `MEMORY`, they may be listed in any order;
however, each may appear only once.
{{</ tip >}}

Parameter    | Description
-------------|-----
**CPU**      | Annotates the LIR plan with the consumed CPU time information `total_elapsed` for each operator (not inclusive of its child operators).
**MEMORY**   | Annotates the LIR plan with the consumed memory information `total_memory` and number of records `total_records` for each operator.
**WITH SKEW** | *Optional.* If specified, includes additional information about average and per-worker consumption and ratios (of `CPU` and/or `MEMORY`).
**HINTS**    | Annotates the LIR plan with [TopK hints].
**AS SQL**   | *Optional.* If specified, returns the SQL associated with the specified `EXPLAIN ANALYZE` command without executing it. You can modify this SQL as a starting point to create customized queries.

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee are contained in.

## Examples

`EXPLAIN ANALYZE` attributes runtime metrics to [`PHYSICAL PLAN` operators](/sql/explain-plan#reference-plan-operators).

The attribution examples in this
section reference the `wins_by_item` index (and the underlying `winning_bids`
view) from the [quickstart
guide](/get-started/quickstart/#step-2-create-the-source):

```sql
CREATE SOURCE auction_house
FROM LOAD GENERATOR AUCTION
(TICK INTERVAL '1s', AS OF 100000)
FOR ALL TABLES;

CREATE VIEW winning_bids AS
  SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
    FROM auctions AS a
    JOIN bids AS b
      ON a.id = b.auction_id
   WHERE b.bid_time < a.end_time
     AND mz_now() >= a.end_time
   ORDER BY a.id, b.amount DESC, b.bid_time, b.buyer;

CREATE INDEX wins_by_item ON winning_bids (item);
```

### `EXPLAIN ANALYZE MEMORY`

The following examples reports on the memory usage of the index `wins_by_item`:

```mzsql
EXPLAIN ANALYZE MEMORY FOR INDEX wins_by_item;
```

For the index, `EXPLAIN ANALYZE MEMORY` reports on the memory usage and the
number of records for each operator in the dataflow:

|         operator            | total_memory | total_records |
|-----------------------------|-------------:|--------------:|
| Arrange                     | 386 kB       |         15409 |
|   Stream u8                 |              |               |
| **Non-monotonic TopK**      | **36 MB**    |    **731975** |
|   Differential Join %0 » %1 |              |               |
|     Arrange                 | 2010 kB      |         84622 |
|       Stream u5             |              |               |
|     Arrange                 | 591 kB       |         15410 |
|       Read u4               |              |               |

The results show the `TopK` operator is overwhelmingly responsible for memory
usage.

### `EXPLAIN ANALYZE CPU`

The following examples reports on the cpu usage of the index `wins_by_item`:

```mzsql
EXPLAIN ANALYZE CPU FOR INDEX wins_by_item;
```

For the index, `EXPLAIN ANALYZE CPU` reports on total time spent in each
operator (not inclusive of its child operators) in the dataflow:

|         operator            |  total_elapsed  |
|:----------------------------|----------------:|
| Arrange                     | 00:00:00.161341 |
|   Stream u8                 |                 |
| Non-monotonic TopK          | 00:00:15.153963 |
|   Differential Join %0 » %1 | 00:00:00.978381 |
|     Arrange                 | 00:00:00.536282 |
|       Stream u5             |                 |
|     Arrange                 | 00:00:00.171586 |
|       Read u4               |                 |


### `EXPLAIN ANALYZE CPU, MEMORY`

You can report on both CPU and memory usage simultaneously:

```mzsql
EXPLAIN ANALYZE CPU, MEMORY FOR INDEX wins_by_item;
```

You can specify both `CPU` or `MEMORY` in any order; however, each may appear
only once. The order of `CPU` and `MEMORY` in the statement determines the order
of the output columns

For example, in the above example where the `CPU` was listed before `MEMORY`,
the CPU time (`total_elasped`) column is listed before the `MEMORY` information
`total_memory` and `total_records`.

|         operator            |  total_elapsed  | total_memory | total_records |
|:----------------------------|----------------:|-------------:|--------------:|
| Arrange                     | 00:00:00.190801 | 389 kB       |         15435 |
|   Stream u8                 |                 |              |               |
| Non-monotonic TopK          | 00:00:16.193381 | 36 MB        |        733457 |
|   Differential Join %0 » %1 | 00:00:01.107056 |              |               |
|     Arrange                 | 00:00:00.592818 | 2017 kB      |         84793 |
|       Stream u5             |                 |              |               |
|     Arrange                 | 00:00:00.214064 | 595 kB       |         15436 |
|       Read u4               |                 |              |               |

### `EXPLAIN ANALYZE ... WITH SKEW`

In clusters with more than one worker, [worker
skew](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers)
can occur when data is unevenly distributed across workers. Extreme cases of
skew can seriously impact performance. You can use `EXPLAIN ANALYZE ... WITH
SKEW` to identify this scenario. The `WITH SKEW` option includes the per worker
and average worker performance numbers for each operator, along with each
worker's ratio compared to the average.

For the below example, assume there are 2 workers in the cluster.

{{< tip >}}

To determine how many workers a given cluster size has, you can query
[`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes).

{{</ tip >}}

You can explain `MEMORY` and/or `CPU` with the `WITH SKEW` option. For example,
the following runs `EXPLAIN ANALYZE MEMORY WITH SKEW`:

```mzsql
EXPLAIN ANALYZE MEMORY WITH SKEW FOR INDEX wins_by_item;
```

The results include the per worker and average worker performance numbers for
each operator, along with each worker's ratio compared to the average:

|         operator            | worker_id | memory_ratio | worker_memory | avg_memory | total_memory | records_ratio | worker_records | avg_records | total_records |
|:----------------------------|----------:|-------------:|--------------:|-----------:|-------------:|--------------:|---------------:|------------:|--------------:|
| Arrange                     | 0         |          0.8 | 78 kB         | 97 kB      | 389 kB       |           0.8 |           3099 |        3862 |         15448 |
| Arrange                     | 1         |         1.59 | 154 kB        | 97 kB      | 389 kB       |          1.58 |           6113 |        3862 |         15448 |
| Arrange                     | 2         |         1.61 | 157 kB        | 97 kB      | 389 kB       |          1.61 |           6236 |        3862 |         15448 |
| **Arrange**                 | **3**     |        **0** | **272 bytes** | **97 kB**  | **389 kB**   |         **0** |          **0** |    **3862** |     **15448** |
|   Stream u8                 |           |              |               |            |              |               |                |             |               |
| Non-monotonic TopK          | 0         |            1 | 9225 kB       | 9261 kB    | 36 MB        |             1 |         183148 |   183486.75 |        733947 |
| Non-monotonic TopK          | 1         |            1 | 9222 kB       | 9261 kB    | 36 MB        |             1 |         183319 |   183486.75 |        733947 |
| Non-monotonic TopK          | 2         |            1 | 9301 kB       | 9261 kB    | 36 MB        |             1 |         183585 |   183486.75 |        733947 |
| Non-monotonic TopK          | 3         |            1 | 9293 kB       | 9261 kB    | 36 MB        |             1 |         183895 |   183486.75 |        733947 |
|   Differential Join %0 » %1 |           |              |               |            |              |               |                |             |               |
|     Arrange                 | 0         |         0.97 | 487 kB        | 505 kB     | 2019 kB      |             1 |          21165 |     21213.5 |         84854 |
|     Arrange                 | 1         |         0.97 | 489 kB        | 505 kB     | 2019 kB      |             1 |          21274 |     21213.5 |         84854 |
|     Arrange                 | 2         |          1.1 | 555 kB        | 505 kB     | 2019 kB      |             1 |          21298 |     21213.5 |         84854 |
|     Arrange                 | 3         |         0.96 | 487 kB        | 505 kB     | 2019 kB      |             1 |          21117 |     21213.5 |         84854 |
|       Stream u5             |           |              |               |            |              |               |                |             |               |
|     Arrange                 | 0         |            1 | 149 kB        | 149 kB     | 595 kB       |             1 |           3862 |      3862.5 |         15450 |
|     Arrange                 | 1         |            1 | 148 kB        | 149 kB     | 595 kB       |             1 |           3862 |      3862.5 |         15450 |
|     Arrange                 | 2         |            1 | 149 kB        | 149 kB     | 595 kB       |             1 |           3863 |      3862.5 |         15450 |
|     Arrange                 | 3         |            1 | 149 kB        | 149 kB     | 595 kB       |             1 |           3863 |      3862.5 |         15450 |
|       Read u4               |           |              |               |            |              |               |                |             |               |

The `ratio` column tells you whether a worker is particularly over- or
under-loaded:

- a `ratio` below 1 indicates a worker doing a below average amount of work.

- a `ratio` above 1 indicates a worker doing an above average amount of work.

While there will always be some amount of variation, very high ratios indicate a
skewed workload. Here the memory ratios are mostly close to 1, indicating there is very
little worker skew everywhere but at the top level arrangement, where worker 3 has no records.

### `EXPLAIN ANALYZE HINTS`

`EXPLAIN ANALYZE HINTS` can annotate your plan (specifically, each TopK
operator) with suggested [TopK hints]; i.e., [`DISTINCT ON INPUT GROUP SIZE=`
value](/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1).

For example, the following runs `EXPLAIN ANALYZE HINTS` on the `wins_by_item`
index:

```mzsql
EXPLAIN ANALYZE HINTS FOR INDEX wins_by_item;
```

The result shows that the `wins_by_item` index has only one `TopK` operator and
suggests the hint (i.e, the `DISTINCT ON INPUT GROUP SIZE=` value) of `255.0`.

|         operator            | levels | to_cut | hint | savings |
|:----------------------------|-------:|-------:|-----:|--------:|
| Arrange                     |        |        |      |         |
|   Stream u8                 |        |        |      |         |
| Non-monotonic TopK          |      8 |      6 |  255 | 26 MB   |
|   Differential Join %0 » %1 |        |        |      |         |
|     Arrange                 |        |        |      |         |
|       Stream u5             |        |        |      |         |
|     Arrange                 |        |        |      |         |
|       Read u4               |        |        |      |         |


With the hint information, you can recreate the view and index to improve memory
usage:

```sql
DROP VIEW winning_bids CASCADE;

CREATE VIEW winning_bids AS
    SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
      FROM auctions AS a
      JOIN bids AS b
        ON a.id = b.auction_id
     WHERE b.bid_time < a.end_time
       AND mz_now() >= a.end_time
   OPTIONS (DISTINCT ON INPUT GROUP SIZE = 255) -- use hint!
  ORDER BY a.id,
    b.amount DESC,
    b.bid_time,
    b.buyer;

CREATE INDEX wins_by_item ON winning_bids (item);
```

Re-running the `TopK`-hints query will show only `null` hints; i.e., there are
no hints because our `TopK` is now appropriately sized.

To see if the indexe's memory usage has improved with the hint, rerun the
following `EXPLAIN ANALYZE MEMORY` command:

```mzsql
EXPLAIN ANALYZE MEMORY FOR INDEX wins_by_item;
```

The results show that the `TopK` operator uses `11MB` of memory, less than a third of the
[~36MB of memory it was using before](#explain-analyze-memory):

|         operator            | total_memory | total_records |
|:----------------------------|-------------:|--------------:|
| Arrange                     | 391 kB       |         15501 |
|   Stream u10                |              |               |
| **Non-monotonic TopK**      | **11 MB**    |    **226706** |
|   Differential Join %0 » %1 |              |               |
|     Arrange                 | 1994 kB      |         85150 |
|       Stream u5             |              |               |
|     Arrange                 | 601 kB       |         15502 |
|       Read u4               |              |               |

### `EXPLAIN ANALYZE ... AS SQL`

Under the hood:

- For returning Memory/CPU information, `EXPLAIN ANALYZE` runs SQL queries that
correlate [`mz_introspection` performance
information](https://materialize.com/docs/sql/system-catalog/mz_introspection/)
with the LIR operators in
[`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

- For TopK hints, `EXPLAIN ANALYZE` uses
[`mz_introspection.mz_expected_group_size_advice`](/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice)
introspection source to offer hints on sizing `TopK` operators.

You can append `AS SQL` to any `EXPLAIN ANALYZE` statement to see the SQL that
would be run (without running it). You can then customize this SQL to report
finer grained or other information. For example:

```mzsql
EXPLAIN ANALYZE HINTS FOR INDEX wins_by_item AS SQL;
```

The results show the SQL that `EXPLAIN ANALYZE` would run to get the TopK hints
for the `wins_by_items` index:

```none
SELECT
    repeat(' ', nesting * 2) || operator AS operator,
    megsa.levels AS levels,
    megsa.to_cut AS to_cut,
    megsa.hint AS hint,
    pg_size_pretty(savings) AS savings
FROM
    mz_introspection.mz_lir_mapping AS mlm
        JOIN
            mz_introspection.mz_dataflow_global_ids AS mdgi
            ON (mlm.global_id = mdgi.global_id)
        LEFT JOIN
            mz_introspection.mz_expected_group_size_advice AS megsa
            ON
                (
                    megsa.dataflow_id = mdgi.id
                        AND
                    mlm.operator_id_start <= megsa.region_id
                        AND
                    megsa.region_id < mlm.operator_id_end
                )
        JOIN
            mz_introspection.mz_mappable_objects AS mo
            ON (mlm.global_id = mo.global_id)
WHERE mo.name = 'materialize.public.wins_by_item'
ORDER BY mlm.lir_id DESC;
```

[TopK hints]: /transform-data/idiomatic-materialize-sql/top-k/#query-hints-1
