---
title: "AS OF"
description: "`AS OF` specifies timestamps in `SELECT` or `TAIL`."
menu:
  main:
    parent: commands
---

The `AS OF` clause specifies or configures how query timestamps are chosen.

All read operations ([`SELECT`](/sql/select), [`TAIL`](/sql/tail)) happen at or start at a specific timestamp.
Materialize will choose correct and recent timestamps if no `AS OF` is specified.
In some cases, users need to specify the time in order to observe data as it appeared in the past, or to resume an interrupted `TAIL`. For example:

- A `TAIL` was interrupted or needs to be restarted at a specific timestamp based on the previous `TAIL` responses.
- A previous query was run, and its timestamp was exposed with `mz_logical_timestamp()`.
  Further queries need to be run at exactly that same timestamp.
  In most cases, using a [transaction](/sql/begin) is the best way to achieve this, however.

A given timestamp will be rejected if data it would report has already been compacted by Materialize.
Use the `logical_compaction_window` option when [creating](/sql/create-index) or [altering](/sql/alter-index) indexes.
This will instruct Materialize to keep historical data for longer than its default (1 millisecond), at the [expense of memory](/ops/optimization/#compaction).

## Syntax

{{< diagram "as-of.svg" >}}

| Field | Use |
| ----- | --- |
| timestamp_expression | The timestamp (or minimum timestamp) for the query in milliseconds since the Unix epoch. |

[Unmaterializable functions](/sql/functions#unmaterializable-functions) are not supported in `timestamp_expression`.
This includes `now()` and `current_timestamp()`, which may seem like useful functions in order to reflect the most recent data in a query.
However due to how Materialize assigns timestamps to data, those functions do not achieve that goal.
Instead, it is best to not specify `AS OF` at all, allowing Materialize to choose a correct and recent timestamp on its own.

### `AS OF`

`AS OF <timestamp_expression>` is used when an exact timestamp for a query is needed.
An error will occur if the source data have been compacted past the specified timestamp.

### `AS OF AT LEAST`

`AS OF AT LEAST <timestamp_expression>` is used to ensure a specific timestamp will be included in the results,
but also allowing Materialize to choose a timestamp after the compaction time.
This is useful when some data are known to have occurred at a specific timestamp, and the query should reflect at least those data.
This mode will choose the most recent valid timestamp that is at least the specified timestamp.

## Using `AS OF` with `TAIL`

```shell
docker run -p 6875:6875 materialize/materialized:{{< version >}}
```

Create a non-materialized view:

```sql
CREATE VIEW most_scheduled_worker AS
  SELECT
    worker,
    SUM(elapsed_ns) as time_working
  FROM mz_scheduling_elapsed
  GROUP BY worker
  ORDER BY time_working DESC
  LIMIT 1;
```

Create an index and set the compaction window:

```sql
CREATE INDEX most_scheduled_worker_idx
  ON most_scheduled_worker (worker, time_working)
  WITH (logical_compaction_window = '1 minute');
```

Stream changes, then cancel with Ctrl-C after a few seconds:

```sql
COPY (TAIL most_scheduled_worker) TO STDOUT;
```

```shell
1651279771999   1       0       292816684
1651279772000   -1      0       292816684
1651279772000   1       0       367471179
1651279773000   1       0       443165347
1651279773000   -1      0       367471179
^C Cancel request sent
ERROR:  canceling statement due to user request
```

Then start again at the last produced timestamp:

```sql
COPY (TAIL most_scheduled_worker AS OF 1651279773000) TO STDOUT;
```

```shell
1651279773000   1       0       443165347
1651279774000   1       0       519431779
1651279774000   -1      0       443165347
1651279775000   -1      0       519431779
1651279775000   1       0       592491548
...
```

If we wait for over a minute (the configured compaction time) and then try again, we will see:

```sql
COPY (TAIL most_scheduled_worker AS OF 1651279773000) TO STDOUT;
```

```shell
ERROR:  Timestamp (1651279773000) is not valid for all inputs: [Antichain { elements: [1651279800000] }]
```

If we switch to `AT LEAST`, the timestamp as allowed to advance:

```sql
COPY (TAIL most_scheduled_worker AS OF AT LEAST 1651279773000) TO STDOUT
```

```shell
1651279926999   1       0       11717911854
1651279927000   -1      0       11717911854
1651279927000   1       0       11794768474
1651279928000   1       0       11869810717
1651279928000   -1      0       11794768474
...
```

## Using `AS OF` with `SELECT`

Generally, using [`BEGIN`](/sql/begin) to start a read transaction is sufficient to allow multiple queries to be run at the same timestamp.
Transactions also prevent compaction for involved sources, so no compaction window needs to be configured.
Some use cases could benefit from using `AS OF` (separate SQL sessions needing the same view, sources in different schemas).

With the same view and index from above, we can use `mz_logical_timestamp()` to observe the timestamp at which a query was run, then run other queries at the same timestamp.

```sql
SELECT mz_logical_timestamp(), COUNT(*) FROM most_scheduled_worker;
```

```shell
 mz_logical_timestamp | count
----------------------+-------
        1651280104999 |     1
```

Then we can use that timestamp to continue looking at the state as of the first query:

```sql
SELECT * FROM most_scheduled_worker AS OF 1651280104999;
```

```shell
 worker | time_working
--------+--------------
      0 |  24845638312
```
