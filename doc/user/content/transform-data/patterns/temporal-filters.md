---
title: "Temporal filters (time windows)"
description: "Perform time-windowed computation over temporal data."
aliases:
  - /guides/temporal-filters/
  - /sql/patterns/temporal-filters/
menu:
  main:
    parent: 'sql-patterns'
---

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
```

{{< note >}}
It may feel more natural to write this filter as the equivalent `WHERE event_ts >= mz_now() - INTERVAL '5min'`.
However, there are currently no valid operators for the [`mz_timestamp`
type](/sql/types/mz_timestamp) that would allow this.  See [`mz_now()` requirements and restrictions](#mz_now-requirements-and-restrictions).
{{< /note >}}

The following diagram shows record `B` falling out of the result set as time
moves forward:

- In the first timeline, record `B` occurred less than 5 minutes ago (occurred
  less than 5 minutes from `mz_now()`).

- In the second timeline, as `mz_now()` progresses, record `B` occurred more
  than 5 minutes from `mz_now()`.

![temporal filter diagram](/images/temporal-filter.svg)

## `mz_now()` requirements and restrictions

### `mz_now()` requirements

{{< tip >}}

When possible, prefer materialized views when using temporal filter to take
advantage of custom consolidation.

{{</ tip >}}

When creating a temporal filter using
[`mz_now()`](/sql/functions/now_and_mz_now) in a `WHERE` or `HAVING` clause, the
clause has the following shape:

{{< include-md file="shared-content/mz_now_clause_requirements.md" >}}

### `mz_now()` restrictions

The [`mz_now()`](/sql/functions/now_and_mz_now) clause has the following
restrictions:

- {{< include-md file="shared-content/mz_now_clause_disjunction_restrictions.md" >}}

  To rewrite the query, see [Disjunction (OR)
  alternatives](http://localhost:1313/docs/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or).

- If part of a  `WHERE` clause, the `WHERE` clause cannot be an [aggregate
 `FILTER` expression](/sql/functions/filters).

## Examples

These examples create real objects.
After you have tried the examples, make sure to drop these objects and spin down any resources you may have created.

{{< tip >}}

When possible, prefer materialized views when using temporal filter to take
advantage of custom consolidation.

{{</ tip >}}

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
    ```

1. Next, subscribe to the results of the view.
    ```mzsql
    COPY (SUBSCRIBE (SELECT ts, content FROM last_30_sec)) TO STDOUT;
    ```

1. In a separate session, insert a record.
    ```mzsql
    INSERT INTO events VALUES ('hello', now());
    ```

1. Back in the first session, watch the record expire after 30 seconds.
    ```nofmt
    1686868190714   1       2023-06-15 22:29:50.711 hello
    1686868220712   -1      2023-06-15 22:29:50.711 hello
    ```
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
    ```

1. Add some tasks to track:
    ```mzsql
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 minutes');
    INSERT INTO tasks VALUES ('time_to_eat', now(), INTERVAL '1 hour');
    INSERT INTO tasks VALUES ('security_block', now(), INTERVAL '1 day');
    ```

1. Create a view using a temporal filter **over the expiration time**. For our example, the expiration time represents the sum between the task's `created_ts` and its `ttl`.
    ```mzsql
    CREATE MATERIALIZED VIEW tracking_tasks AS
    SELECT
      name,
      created_ts + ttl as expiration_time
    FROM tasks
    WHERE mz_now() < created_ts + ttl;
    ```
    The moment `mz_now()` crosses the expiration time of a record, that record is retracted (removed) from the result set.

You can now:

- Query the remaining time for a row:
  ```mzsql
    SELECT expiration_time - now() AS remaining_ttl
    FROM tracking_tasks
    WHERE name = 'time_to_eat';
  ```

- Check if a particular row is still available:
  ```mzsql
  SELECT true
  FROM tracking_tasks
  WHERE name = 'security_block';
  ```

- Trigger an external process when a row expires:
  ```mzsql
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 seconds');
    COPY( SUBSCRIBE tracking_tasks WITH (SNAPSHOT = false) ) TO STDOUT;

  ```
  ```nofmt
  mz_timestamp | mz_diff | name       | expiration_time |
  -------------|---------|------------|-----------------|
  ...          | -1      | send_email | ...             | <-- Time to send the email!
  ```

### Periodically emit results

Suppose you want to count the number of records in each 1 minute time window, grouped by an `id` column.
You don't care to receive every update as it happens; instead, you would prefer Materialize to emit a single result at the end of each window.
Materialize [date functions](/sql/functions/#date-and-time-functions) are helpful for use cases like this where you want to bucket records into time windows.

The strategy for this example is to put an initial temporal filter on the input (say, 30 days) to bound it, use the [`date_bin` function](/sql/functions/date-bin) to bin records into 1 minute windows, use a second temporal filter to emit results at the end of the window, and finally apply a third temporal filter shorter than the first (say, 7 days) to set how long results should persist in Materialize.

1. First, create a table for the input records.
    ```mzsql
    CREATE TABLE input (id INT, event_ts TIMESTAMP);
    ```
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
    ```
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
    ```
    This `WHERE` clause means "the result for a 1-minute window should come into effect when `mz_now()` reaches `window_end` and be removed 7 days later". Without the latter constraint, records in the result set would receive strange updates as records expire from the initial 30 day filter on the input.
1. Subscribe to the `output`.
    ```mzsql
    COPY (SUBSCRIBE (SELECT * FROM output)) TO STDOUT;
    ```
1. In a different session, insert some records.
    ```mzsql
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (1, now());
    -- wait a moment
    INSERT INTO input VALUES (2, now());
    ```
1. Back at the `SUBSCRIBE`, wait about a minute for your final aggregation result to show up the moment the 1 minute window ends.
    ```nofmt
     mz_timestamp | mz_diff |  id   | count |      window_end
    --------------|---------|-------|-------|----------------------
    1686889140000       1       1       3       2023-06-16 04:19:00
    1686889140000       1       2       1       2023-06-16 04:19:00
    ```
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
```

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

{{< note >}}
See the guide on [partitioning and filter pushdown](/transform-data/patterns/partition-by/) for a **private preview** feature that can make the filter pushdown optimization more predictable.
{{< /note >}}
