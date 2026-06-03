---
title: "CHANGES function"
description: "Reads a collection's change stream as a relation with per-update timestamp and diff columns."
menu:
  main:
    parent: 'sql-functions'
---

{{< private-preview />}}

`CHANGES` reads the change stream of a table, source, or materialized view as a relation.
Each update to the input — an insertion or a deletion of a row at a specific time — becomes a row in the output, annotated with the time it happened and whether it was an insertion or a deletion.
Unlike [`SUBSCRIBE`](/sql/subscribe), which streams updates directly to a client, `CHANGES` produces an ordinary relation: you can filter it, aggregate it, join it, and maintain it in a materialized view.

## Syntax

```mzsql
CHANGES (<relation> AS OF [AT LEAST] <bound>)
```

Parameter | Description
----------|------------
_relation_ | A [table](/sql/create-table), [source](/sql/create-source), or [materialized view](/sql/create-materialized-view), named directly or wrapped in a parenthesized `SELECT` that reads it unchanged (e.g. `(SELECT * FROM t)`). Views and queries that filter or transform the input are not supported.
_bound_ | The changelog start: changes at times greater than the bound appear as individual updates; the state of the input at the bound appears as a collapsed snapshot. A constant [`mz_timestamp`](/sql/types/mz_timestamp) expression is a fixed start. An expression of the form `mz_now() - <interval>` is a sliding window that trails the current time.

`AS OF` is strict: if the input no longer retains history back to the bound, the query fails.
`AS OF AT LEAST` is advisory: the bound is moved up to the earliest history the input still retains.

### Return value

`CHANGES` returns the columns of the input relation, followed by:

Column | Type | Description
-------|------|------------
`mz_timestamp` | [`mz_timestamp`](/sql/types/mz_timestamp) | The logical time of the update.
`mz_diff` | [`bigint`](/sql/types/integer) | `1` if the update inserted the row, `-1` if it deleted it.

The output is append-only: every update to the input, including a deletion, appears as a new row in the changelog.
An `UPDATE` to the input appears as two rows, a deletion of the old row and an insertion of the new one, at the same `mz_timestamp`.

## Details

### History and compaction

`CHANGES` can only replay history the input still retains; it cannot manufacture history that was already compacted away.
By default Materialize compacts historical detail quickly, so a fixed bound in the past is only readable if the input retains history, for example with the [`RETAIN HISTORY` option](/transform-data/patterns/durable-subscriptions/#history-retention-period).
The state of the input at the changelog start appears as a set of insertions at the start time; changes after it appear at the times they occurred.

### Where CHANGES is allowed

Context | Fixed bound | Sliding bound
--------|-------------|--------------
One-off `SELECT` | Supported | Supported (the window ends at the query time)
Materialized view | Not supported (it would pin the input's history forever) | Supported
View, index, `SUBSCRIBE` | Not supported | Not supported

A single query cannot read a collection both directly and via `CHANGES`.
Move one of the reads into a separate query, or read `CHANGES` of a materialized view over the collection.

### Maintained sliding windows

A materialized view over `CHANGES` with a sliding bound maintains a rolling window of changes: new updates enter the changelog as they happen, and updates leave it once they age past the window.
Aggregations over the view are continuously correct over the window.

With an advisory bound (`AS OF AT LEAST`), the view starts from whatever history the input retains and ages in from there.
With a strict bound (`AS OF`), creation fails unless the input already retains a full window — the view never silently serves a partial window.
The strict check applies at creation; a view that exists keeps maintaining its window across restarts.

The window determines how much history of the input Materialize must retain, so its size is capped by the `changes_max_window` system parameter (default: 1 day).

## Examples

### Reading recent changes

The table retains an hour of history, so a look-back over the last ten minutes is fully available:

```mzsql
CREATE TABLE t (a int) WITH (RETAIN HISTORY FOR '1 hour');
INSERT INTO t VALUES (1), (2);
-- some time later
INSERT INTO t VALUES (3);
DELETE FROM t WHERE a = 1;
```

Read the changelog over the last ten minutes:

```mzsql
SELECT a, mz_diff FROM CHANGES (t AS OF AT LEAST mz_now() - INTERVAL '10 minutes')
ORDER BY a, mz_diff;
```
```nofmt
 a | mz_diff
---+---------
 1 |      -1
 1 |       1
 2 |       1
 3 |       1
(4 rows)
```

The deletion of `1` appears as a row with `mz_diff = -1`, not as a retraction: the changelog records that the deletion happened.

### Aggregating changes

`CHANGES` composes with ordinary SQL.
The net change per key over the window:

```mzsql
SELECT a, sum(mz_diff) FROM CHANGES (t AS OF AT LEAST mz_now() - INTERVAL '10 minutes')
GROUP BY a ORDER BY a;
```
```nofmt
 a | sum
---+-----
 1 |   0
 2 |   1
 3 |   1
(3 rows)
```

### Maintaining a rolling window of changes

```mzsql
CREATE MATERIALIZED VIEW t_changes AS
  SELECT a, mz_timestamp, mz_diff
  FROM CHANGES (t AS OF AT LEAST mz_now() - INTERVAL '1 hour');
```

`t_changes` contains the changes to `t` from the last hour, continuously updated as changes happen and age out.
For example, the number of deletions in the last hour:

```mzsql
SELECT count(*) FROM t_changes WHERE mz_diff < 0;
```

## Related pages

* [`SUBSCRIBE`](/sql/subscribe)
* [`mz_timestamp` type](/sql/types/mz_timestamp)
* [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
