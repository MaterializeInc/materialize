<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# EXPLAIN ANALYZE

`EXPLAIN ANALYZE`:

- Reports on the performance of indexes and materialized views.
- Provide the execution plan annotated with TopK hints. The TopK query
  pattern groups by some key and return the first K elements within each
  group according to some ordering.

<div class="warning">

**WARNING!** `EXPLAIN` is not part of Materialize’s stable interface and
is not subject to our backwards compatibility guarantee. The syntax and
output of `EXPLAIN` may change arbitrarily in future versions of
Materialize.

</div>

## Syntax

<div class="highlight">

``` chroma
EXPLAIN ANALYZE
      CPU [, MEMORY] [WITH SKEW]
    | MEMORY [, CPU] [WITH SKEW]
    | HINTS
FOR INDEX <name> | MATERIALIZED VIEW <name>
[ AS SQL ]
;
```

</div>

<div class="tip">

**💡 Tip:** If you want to specify both `CPU` or `MEMORY`, they may be
listed in any order; however, each may appear only once.

</div>

| Parameter | Description |
|----|----|
| **CPU** | Annotates the LIR plan with the consumed CPU time information `total_elapsed` for each operator (not inclusive of its child operators). |
| **MEMORY** | Annotates the LIR plan with the consumed memory information `total_memory` and number of records `total_records` for each operator. |
| **WITH SKEW** | *Optional.* If specified, includes additional information about average and per-worker consumption and ratios (of `CPU` and/or `MEMORY`). |
| **HINTS** | Annotates the LIR plan with [TopK hints](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1). |
| **AS SQL** | *Optional.* If specified, returns the SQL associated with the specified `EXPLAIN ANALYZE` command without executing it. You can modify this SQL as a starting point to create customized queries. |

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee
  are contained in.

## Examples

The attribution examples in this section reference the `wins_by_item`
index (and the underlying `winning_bids` view) from the [quickstart
guide](/docs/self-managed/v25.2/get-started/quickstart/#step-2-create-the-source):

<div class="highlight">

``` chroma
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

</div>

### `EXPLAIN ANALYZE MEMORY`

The following examples reports on the memory usage of the index
`wins_by_item`:

<div class="highlight">

``` chroma
EXPLAIN ANALYZE MEMORY FOR INDEX wins_by_item;
```

</div>

For the index, `EXPLAIN ANALYZE MEMORY` reports on the memory usage and
the number of records for each operator in the dataflow:

| operator                    | total_memory | total_records |
|-----------------------------|-------------:|--------------:|
| Arrange                     |       386 kB |         15409 |
|   Stream u8                 |              |               |
| **Non-monotonic TopK**      |    **36 MB** |    **731975** |
|   Differential Join %0 » %1 |              |               |
|     Arrange                 |      2010 kB |         84622 |
|       Stream u5             |              |               |
|     Arrange                 |       591 kB |         15410 |
|       Read u4               |              |               |

The results show the `TopK` operator is overwhelmingly responsible for
memory usage.

### `EXPLAIN ANALYZE CPU`

The following examples reports on the cpu usage of the index
`wins_by_item`:

<div class="highlight">

``` chroma
EXPLAIN ANALYZE CPU FOR INDEX wins_by_item;
```

</div>

For the index, `EXPLAIN ANALYZE CPU` reports on total time spent in each
operator (not inclusive of its child operators) in the dataflow:

| operator                    |   total_elapsed |
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

<div class="highlight">

``` chroma
EXPLAIN ANALYZE CPU, MEMORY FOR INDEX wins_by_item;
```

</div>

You can specify both `CPU` or `MEMORY` in any order; however, each may
appear only once. The order of `CPU` and `MEMORY` in the statement
determines the order of the output columns

For example, in the above example where the `CPU` was listed before
`MEMORY`, the CPU time (`total_elasped`) column is listed before the
`MEMORY` information `total_memory` and `total_records`.

| operator                    |   total_elapsed | total_memory | total_records |
|:----------------------------|----------------:|-------------:|--------------:|
| Arrange                     | 00:00:00.190801 |       389 kB |         15435 |
|   Stream u8                 |                 |              |               |
| Non-monotonic TopK          | 00:00:16.193381 |        36 MB |        733457 |
|   Differential Join %0 » %1 | 00:00:01.107056 |              |               |
|     Arrange                 | 00:00:00.592818 |      2017 kB |         84793 |
|       Stream u5             |                 |              |               |
|     Arrange                 | 00:00:00.214064 |       595 kB |         15436 |
|       Read u4               |                 |              |               |

### `EXPLAIN ANALYZE ... WITH SKEW`

In clusters with more than one worker, [worker
skew](/docs/self-managed/v25.2/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers)
can occur when data is unevenly distributed across workers. Extreme
cases of skew can seriously impact performance. You can use
`EXPLAIN ANALYZE ... WITH SKEW` to identify this scenario. The
`WITH SKEW` option includes the per worker and average worker
performance numbers for each operator, along with each worker’s ratio
compared to the average.

For the below example, assume there are 2 workers in the cluster.

<div class="tip">

**💡 Tip:** To determine how many workers a given cluster size has, you
can query
[`mz_catalog.mz_cluster_replica_sizes`](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes).

</div>

You can explain `MEMORY` and/or `CPU` with the `WITH SKEW` option. For
example, the following runs `EXPLAIN ANALYZE MEMORY WITH SKEW`:

<div class="highlight">

``` chroma
EXPLAIN ANALYZE MEMORY WITH SKEW FOR INDEX wins_by_item;
```

</div>

The results include the per worker and average worker performance
numbers for each operator, along with each worker’s ratio compared to
the average:

| operator | worker_id | memory_ratio | worker_memory | avg_memory | total_memory | records_ratio | worker_records | avg_records | total_records |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Arrange | 0 | 0.8 | 78 kB | 97 kB | 389 kB | 0.8 | 3099 | 3862 | 15448 |
| Arrange | 1 | 1.59 | 154 kB | 97 kB | 389 kB | 1.58 | 6113 | 3862 | 15448 |
| Arrange | 2 | 1.61 | 157 kB | 97 kB | 389 kB | 1.61 | 6236 | 3862 | 15448 |
| **Arrange** | **3** | **0** | **272 bytes** | **97 kB** | **389 kB** | **0** | **0** | **3862** | **15448** |
|   Stream u8 |  |  |  |  |  |  |  |  |  |
| Non-monotonic TopK | 0 | 1 | 9225 kB | 9261 kB | 36 MB | 1 | 183148 | 183486.75 | 733947 |
| Non-monotonic TopK | 1 | 1 | 9222 kB | 9261 kB | 36 MB | 1 | 183319 | 183486.75 | 733947 |
| Non-monotonic TopK | 2 | 1 | 9301 kB | 9261 kB | 36 MB | 1 | 183585 | 183486.75 | 733947 |
| Non-monotonic TopK | 3 | 1 | 9293 kB | 9261 kB | 36 MB | 1 | 183895 | 183486.75 | 733947 |
|   Differential Join %0 » %1 |  |  |  |  |  |  |  |  |  |
|     Arrange | 0 | 0.97 | 487 kB | 505 kB | 2019 kB | 1 | 21165 | 21213.5 | 84854 |
|     Arrange | 1 | 0.97 | 489 kB | 505 kB | 2019 kB | 1 | 21274 | 21213.5 | 84854 |
|     Arrange | 2 | 1.1 | 555 kB | 505 kB | 2019 kB | 1 | 21298 | 21213.5 | 84854 |
|     Arrange | 3 | 0.96 | 487 kB | 505 kB | 2019 kB | 1 | 21117 | 21213.5 | 84854 |
|       Stream u5 |  |  |  |  |  |  |  |  |  |
|     Arrange | 0 | 1 | 149 kB | 149 kB | 595 kB | 1 | 3862 | 3862.5 | 15450 |
|     Arrange | 1 | 1 | 148 kB | 149 kB | 595 kB | 1 | 3862 | 3862.5 | 15450 |
|     Arrange | 2 | 1 | 149 kB | 149 kB | 595 kB | 1 | 3863 | 3862.5 | 15450 |
|     Arrange | 3 | 1 | 149 kB | 149 kB | 595 kB | 1 | 3863 | 3862.5 | 15450 |
|       Read u4 |  |  |  |  |  |  |  |  |  |

The `ratio` column tells you whether a worker is particularly over- or
under-loaded:

- a `ratio` below 1 indicates a worker doing a below average amount of
  work.

- a `ratio` above 1 indicates a worker doing an above average amount of
  work.

While there will always be some amount of variation, very high ratios
indicate a skewed workload. Here the memory ratios are mostly close to
1, indicating there is very little worker skew everywhere but at the top
level arrangement, where worker 3 has no records.

### `EXPLAIN ANALYZE HINTS`

`EXPLAIN ANALYZE HINTS` can annotate your plan (specifically, each TopK
operator) with suggested [TopK
hints](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1);
i.e., [`DISTINCT ON INPUT GROUP SIZE=`
value](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1).

For example, the following runs `EXPLAIN ANALYZE HINTS` on the
`wins_by_item` index:

<div class="highlight">

``` chroma
EXPLAIN ANALYZE HINTS FOR INDEX wins_by_item;
```

</div>

The result shows that the `wins_by_item` index has only one `TopK`
operator and suggests the hint (i.e, the `DISTINCT ON INPUT GROUP SIZE=`
value) of `255.0`.

| operator                    | levels | to_cut | hint | savings |
|:----------------------------|-------:|-------:|-----:|--------:|
| Arrange                     |        |        |      |         |
|   Stream u8                 |        |        |      |         |
| Non-monotonic TopK          |      8 |      6 |  255 |   26 MB |
|   Differential Join %0 » %1 |        |        |      |         |
|     Arrange                 |        |        |      |         |
|       Stream u5             |        |        |      |         |
|     Arrange                 |        |        |      |         |
|       Read u4               |        |        |      |         |

With the hint information, you can recreate the view and index to
improve memory usage:

<div class="highlight">

``` chroma
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

</div>

Re-running the `TopK`-hints query will show only `null` hints; i.e.,
there are no hints because our `TopK` is now appropriately sized.

To see if the indexe’s memory usage has improved with the hint, rerun
the following `EXPLAIN ANALYZE MEMORY` command:

<div class="highlight">

``` chroma
EXPLAIN ANALYZE MEMORY FOR INDEX wins_by_item;
```

</div>

The results show that the `TopK` operator uses `11MB` of memory, less
than a third of the [~36MB of memory it was using
before](#explain-analyze-memory):

| operator                    | total_memory | total_records |
|:----------------------------|-------------:|--------------:|
| Arrange                     |       391 kB |         15501 |
|   Stream u10                |              |               |
| **Non-monotonic TopK**      |    **11 MB** |    **226706** |
|   Differential Join %0 » %1 |              |               |
|     Arrange                 |      1994 kB |         85150 |
|       Stream u5             |              |               |
|     Arrange                 |       601 kB |         15502 |
|       Read u4               |              |               |

### `EXPLAIN ANALYZE ... AS SQL`

Under the hood:

- For returning Memory/CPU information, `EXPLAIN ANALYZE` runs SQL
  queries that correlate [`mz_introspection` performance
  information](https://materialize.com/docs/sql/system-catalog/mz_introspection/)
  with the LIR operators in
  [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

- For TopK hints, `EXPLAIN ANALYZE` uses
  [`mz_introspection.mz_expected_group_size_advice`](/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice)
  introspection source to offer hints on sizing `TopK` operators.

You can append `AS SQL` to any `EXPLAIN ANALYZE` statement to see the
SQL that would be run (without running it). You can then customize this
SQL to report finer grained or other information. For example:

<div class="highlight">

``` chroma
EXPLAIN ANALYZE HINTS FOR INDEX wins_by_item AS SQL;
```

</div>

The results show the SQL that `EXPLAIN ANALYZE` would run to get the
TopK hints for the `wins_by_items` index:

```
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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/explain-analyze.md"
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
