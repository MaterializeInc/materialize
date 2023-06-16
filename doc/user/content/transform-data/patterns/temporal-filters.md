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

A "temporal filter" is a `WHERE` or `HAVING` clause which uses the [`mz_now`](/sql/functions/now_and_mz_now) function.
This function returns Materialize's current virtual timestamp, which attempts to keep up with real time as data is processed.
Applying a temporal filter reduces the working dataset, saving memory resources and focusing results on the recent past.

Here is a typical temporal filter that considers records whose timestamps are within the last 5 minutes.

```sql
WHERE mz_now() <= event_ts + INTERVAL '5min'
```

Consider this diagram that shows an record `E` falling out of the result set as time moves forward:

![temporal filter diagram](/images/temporal-filter.jpg)

{{< note >}}
It may feel more natural to write this filter as `WHERE event_ts >= mz_now() - INTERVAL '5min'`.
For reasons having to do with the internal implementation of `mz_now()`, there are currently no valid operators for the resulting [`mz_timestamp` type](/sql/types/mz_timestamp) that would allow this.
{{< /note >}}

## Requirements

You can only use `mz_now()` to establish a temporal filter under the following conditions:

- `mz_now` appears in a `WHERE` or `HAVING` clause.
- The clause must directly compare `mz_now()` to a [`numeric`](/sql/types/numeric) expression not containing `mz_now()`,
    or be part of a conjunction phrase (`AND`) which directly compares `mz_now()` to a [`numeric`](/sql/types/numeric) expression not containing `mz_now()`.
- The comparison must be one of `=`, `<`, `<=`, `>`, or `>=`, or operators that desugar to them or a conjunction of them (for example, `BETWEEN`).
    At the moment, you can't use the `!=` operator with `mz_now()`.

## Examples

### Sliding Window

<!-- This example also appears in now_and_mz_now -->
It is common for real-time applications to be concerned with only a recent period of time.
We call this a **sliding window**.
Other systems use this term differently because they cannot achieve a continuously sliding window.

In this case, we will filter a table to only include only records from the last 30 seconds.

```sql
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

Next, subscribe to the results of the view.

```sql
COPY (SUBSCRIBE (SELECT ts, content FROM last_30_sec)) TO STDOUT;
```

In a separate session, insert a record.

```sql
INSERT INTO events VALUES (
    'hello',
    now()
);
```

Back in the first session, watch the record expire after 30 seconds. Press `Ctrl+C` to quit the `SUBSCRIBE` when you are ready.

```nofmt
1686868190714   1       2023-06-15 22:29:50.711 hello
1686868220712   -1      2023-06-15 22:29:50.711 hello
```

You can materialize the `last_30_sec` view by creating an index on it (results stored in memory) or by recreating it as a `MATERIALIZED VIEW` (results persisted to storage). When you do so, Materialize will keep the results up to date with records expiring automatically according to the temporal filter.

### Time to Live (TTL)

The **time to live (TTL)** pattern helps to filter rows using expiration times.
This example uses a `tasks` table and adds a time to live for each task.
Materialize then helps perform actions according to each task's expiration time.

1.  First, create a table:
    ```sql
      CREATE TABLE tasks (name TEXT, created_ts TIMESTAMP, ttl INTERVAL);
    ```
1.  Add some tasks to track:
    ```sql
      INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 minutes');
      INSERT INTO tasks VALUES ('time_to_eat', now(), INTERVAL '1 hour');
      INSERT INTO tasks VALUES ('security_block', now(), INTERVAL '1 day');
    ```
1. Create a view using a temporal filter **over the expiration time**. For our example, the expiration time represents the sum between the task's `created_ts` and its `ttl`.
    ```sql
      CREATE MATERIALIZED VIEW tracking_tasks AS
      SELECT
        name,
        created_ts + ttl as expiration_time
      FROM tasks
      WHERE mz_now() < created_ts + ttl;
    ```
  The moment `mz_now()` crosses the expiration time of a record, it is removed from the result set.

- Query the remaining time for a row:
  ```sql
    SELECT expiration_time - now() AS remaining_ttl
    FROM tracking_tasks
    WHERE name = 'time_to_eat';
  ```

- Check if a particular row is still available:
  ```sql
  SELECT true
  FROM tracking_tasks
  WHERE name = 'security_block';
  ```

- Trigger an external process when a row expires:
  ```sql
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 seconds');
    COPY( SUBSCRIBE tracking_tasks WITH (SNAPSHOT = false) ) TO STDOUT;

  ```
  ```nofmt
  mz_timestamp | mz_diff | name       | expiration_time |
  -------------|---------|------------|-----------------|
  ...          | -1      | send_email | ...             | <-- Time to send the email!
  ```

### Periodically Emit Results

Suppose you want to count the number of records in each 1 minute time window, grouped by an `id` column, and emit the result at the end of each window.
Materialize [date functions](/sql/functions/#date-and-time-func) are helpful when bucketing records into time windows.

First, create a table for the input records.

```sql

```


