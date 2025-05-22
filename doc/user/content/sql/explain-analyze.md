---
title: "EXPLAIN ANALYZE"
description: "Reference page for `EXPLAIN ANALYZE`. `EXPLAIN ANALYZE` is used to understand the performance of indexes and materialized views."
menu:
  main:
    parent: commands
---

`EXPLAIN ANALYZE` uses [`mz_introspection`](https://materialize.com/docs/sql/system-catalog/mz_introspection/) sources to report on the performance of indexes and materialized views.

When optimizing a query, it helps to be able to attribute 'cost' to its parts,
starting with how much time is spent computing in each part overall. For example, Materialize
reports the time spent in each _dataflow operator_ in
[`mz_introspection.mz_compute_operator_durations_histogram`](/sql/system-catalog/mz_introspection/#mz_compute_operator_durations_histogram).
Using `EXPLAIN ANALYZE`, we can attribute the time spent in each operator to the higher-level, more
intelligible LIR operators.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

```mzsql
EXPLAIN ANALYZE
    [ HINTS | [<property> [, <property ...>] [WITH SKEW]]]
    FOR [INDEX ... | MATERIALIZED VIEW ...]
    [ AS SQL ]
;
```

Here `<property>` is either `CPU` or `MEMORY`; you may list them in any order, but each may appear only once.

### Explained object

The following object types can be explained.

Explained object | Description
------|-----
**INDEX name** | Display information for an existing index.
**MATERIALIZED VIEW name** | Display information for an existing materialized view.

### Properties to analyze


Property | Description
------|-----
**`HINTS`** | Annotates the LIR plan with TopK hints.
**`CPU`** | Annotates the LIR plan with information about CPU time consumed.
**`MEMORY`** | Annotates the LIR plan with information about memory consumed.
**`WITH SKEW`** | Adds additional information about average and per-worker consumption (of `CPU` or `MEMORY`).

Note that `CPU` and `MEMORY` can be combined, separated by commas.
When running `EXPLAIN ANALYZE` for `CPU` or `MEMORY`, you can specify `WITH SKEW` to observe average and per worker information, as well.

## Details

Under the hood, `EXPLAIN ANALYZE` runs SQL queries that correlate [`mz_introspection` performance information](https://materialize.com/docs/sql/system-catalog/mz_introspection/) with the LIR operators in [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

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

When we run `EXPLAIN ANALYZE MEMORY FOR INDEX wins_by_item`, we get back a report on each operator in the dataflow for `wins_by_item`, annotated with memory usage and the number of records:

| operator                         | total_memory | total_records |
| :------------------------------- | -----------: | ------------: |
| Arrange 7                        | 1116 kB      | 39976         |
|   Get::PassArrangements u274     | null         | null          |
| TopK::Basic 5                    | 100 MB       | 1899733       |
|   Join::Differential 2 » 4       | null         | null          |
|     Arrange 3                    | 5672 kB      | 220375        |
|       Get::PassArrangements u271 | null         | null          |
|     Arrange 1                    | 3501 kB      | 188785        |
|       Get::Collection u270       | null         | null          |

The results show the [`TopK`](/transform-data/idiomatic-materialize-sql/top-k/) is overwhelmingly responsible for memory usage.


### `EXPLAIN ANALYZE CPU`

When we run `EXPLAIN ANALYZE CPU FOR INDEX wins_by_item`, we get back a report on each operator in the dataflow for `wins_by_item`, annotated with total time spent in each operator (not inclusive of its child operators):

| operator                         | total_elapsed   |
| :------------------------------- | --------------: |
| Arrange 7                        | 00:00:03.14266  |
|   Get::PassArrangements u274     | null            |
| TopK::Basic 5                    | 00:00:44.079592 |
|   Join::Differential 2 » 4       | 00:00:06.372705 |
|     Arrange 3                    | 00:00:21.49465  |
|       Get::PassArrangements u271 | 00:00:00.066236 |
|     Arrange 1                    | 00:00:11.212331 |
|       Get::Collection u270       | 00:00:00.190449 |

We can see CPU and memory usage simultaneously by running `EXPLAIN ANALYZE CPU, MEMORY FOR INDEX wins_by_item`:

| operator                         | total_elapsed   | total_memory | total_records |
| :------------------------------- | --------------: | -----------: | ------------: |
| Arrange 7                        | 00:00:03.151386 | 1234 kB      | 42359         |
|   Get::PassArrangements u274     | null            | null         |               |
| TopK::Basic 5                    | 00:00:44.347959 | 105 MB       | 2013547       |
|   Join::Differential 2 » 4       | 00:00:06.389385 | null         | null          |
|     Arrange 3                    | 00:00:21.558754 | 5431 kB      | 233533        |
|       Get::PassArrangements u271 | 00:00:00.06644  | null         | null          |
|     Arrange 1                    | 00:00:11.246103 | 3546 kB      | 191168        |
|       Get::Collection u270       | 00:00:00.190935 | null         | null          |

The order of `CPU` and `MEMORY` in the statement determines the order of the output columns.

### `EXPLAIN ANALYZE ... WITH SKEW`

[Worker skew](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers) occurs when your data do not end up getting evenly
partitioned between workers.
Worker skew can only happen when your cluster has more than one worker.
You can query
[`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
to determine how many workers a given cluster size has; in our example, there are 2 workers.
Extreme cases of skew can cause serious performance issues; `EXPLAIN ANALYZE` can help you identify this scenario.

You can explain `MEMORY`, `CPU`, or both with `WITH SKEW`, which adds per worker and average worker performance numbers for each operator, along with each worker's ratio compared to the average. Here is an example of `EXPLAIN ANALYZE MEMORY WITH SKEW FOR INDEX wins_by_item`:

| operator                         | worker_id | memory_ratio | worker_memory | avg_memory | total_memory | records_ratio | worker_records | avg_records | total_records |
| :------------------------------- | --------: | -----------: | ------------: | ---------: | -----------: | ------------: | -------------: | ----------: | ------------: |
| Arrange 7                        | 0         | 1.24         | 768 kB        | 619 kB     | 1238 kB      | 1.2           | 25485          | 21246       | 42492         |
| Arrange 7                        | 1         | 0.76         | 470 kB        | 619 kB     | 1238 kB      | 0.8           | 17007          | 21246       | 42492         |
|   Get::PassArrangements u274     | null      | null         | null          | null       | null         | null          | null           | null        | null          |
| TopK::Basic 5                    | 0         | 1            | 53 MB         | 53 MB      | 105 MB       | 1             | 1011624        | 1010259.5   | 2020519       |
| TopK::Basic 5                    | 1         | 1            | 52 MB         | 53 MB      | 105 MB       | 1             | 1008895        | 1010259.5   | 2020519       |
|   Join::Differential 2 » 4       | null      | null         | null          | null       | null         | null          | null           | null        | null          |
|     Arrange 3                    | 0         | 1            | 2726 kB       | 2724 kB    | 5448 kB      | 1             | 117297         | 117167.5    | 234335        |
|     Arrange 3                    | 1         | 1            | 2722 kB       | 2724 kB    | 5448 kB      | 1             | 117038         | 117167.5    | 234335        |
|       Get::PassArrangements u271 | null      | null         | null          | null       | null         | null          | null           | null        | null          |
|     Arrange 1                    | 0         | 1            | 1779 kB       | 1778 kB    | 3556 kB      | 1             | 95955          | 95750       | 191500        |
|     Arrange 1                    | 1         | 1            | 1778 kB       | 1778 kB    | 3556 kB      | 1             | 95545          | 95750       | 191500        |
|       Get::Collection u270       | null      | null         | null          | null       | null         | null          | null           | null        | null          |

The `ratio` column tells you whether a worker is particularly over- or
under-loaded:

- a `ratio` below 1 indicates a worker doing a below average amount of work.

- a `ratio` above 1 indicates a worker doing an above average amount of work.

While there will always be some amount of variation, very high ratios indicate a
skewed workload.
Here the memory ratios are close to 1, indicating there is very little worker skew.

### `EXPLAIN ANALYZE HINTS`

The [`mz_introspection.mz_expected_group_size_advice`](/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice) introspection source offers hints on sizing `TopK` operators. `EXPLAIN ANALYZE HINTS` can annotate your query with these hints, which can be helpful for identifying which `TopK` should get which hint. Each `TopK` operator will have an [associated `DISTINCT ON INPUT GROUP SIZE`
query hint](/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1):
Here is an example of `EXPLAIN ANALYZE HINTS FOR INDEX wins_by_item` (which has only one `TopK`):

| operator                         | levels | to_cut | hint  | savings |
| :------------------------------- | -----: | -----: | ----: | ------: |
| Arrange 7                        | null   | null   | null  | null    |
|   Get::PassArrangements u274     | null   | null   | null  | null    |
| TopK::Basic 5                    | 8      | 6      | 255.0 | 75 MB   |
|   Join::Differential 2 » 4       | null   | null   | null  | null    |
|     Arrange 3                    | null   | null   | null  | null    |
|       Get::PassArrangements u271 | null   | null   | null  | null    |
|     Arrange 1                    | null   | null   | null  | null    |
|       Get::Collection u270       | null   | null   | null  | null    |

Here, the hinted `DISTINCT ON INPUT GROUP SIZE` is `255.0`. We can re-create our view and index using the hint as follows:

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

Re-running the `TopK`-hints query will show only `null` hints; there are no
hints because our `TopK` is now appropriately sized. But if we re-run our [query
for attributing memory usage](#attributing-memory-usage), we can see that our
`TopK` operator uses a third of the 100MB of memory it was using before:

| operator                         | total_memory | total_records |
| -------------------------------- | ------------ | ------------- |
| Arrange 7                        | 1093 kB      | 42720         |
|   Get::PassArrangements u286     | null         | null          |
| TopK::Basic 5                    | 30 MB        | 625638        |
|   Join::Differential 2 » 4       | null         | null          |
|     Arrange 3                    | 5447 kB      | 235570        |
|       Get::PassArrangements u271 | null         | null          |
|     Arrange 1                    | 3485 kB      | 191730        |
|       Get::Collection u270       | null         | null          |

### `EXPLAIN ANALYZE ... AS SQL`

Under the hood, `EXPLAIN ANALYZE` issues SQL queries against [`mz_introspection`](https://materialize.com/docs/sql/system-catalog/mz_introspection/) sources.
You can append `AS SQL` to any `EXPLAIN ANALYZE` statement to see the SQL that would be run (without running it).
You can then customize this SQL to report finer grained or other information.

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee are contained in.
