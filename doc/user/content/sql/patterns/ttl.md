---
title: "Time-To-Live"
description: "Use the time-to-live pattern and assign an expiration time for a particular row."
menu:
  main:
    parent: 'sql-patterns'
---

The time-to-live (TTL) pattern helps assign expiration times to rows while processing. Before continuing, make sure to understand how [temporal filters](/guides/temporal-filters/) work.

A few examples of the use cases that you can achieve are:

- Free-up memory
- Schedule tasks, like emails or messages
- Assign temporal offers for an e-commerce
- Run time-sensitive security blockers

## How-to

1. Identify the row, its initial timestamp, and the TTL. In the example, the `event_timestamp` and `event_ttl` represent those times. The sum of these values is when the row needs to drop, known as **expiring time**.
    ```sql
      CREATE TABLE events (event_name TEXT, event_timestamp TIMESTAMP, event_ttl INTERVAL);

      -- Sample (One minute TTL):
      INSERT INTO events VALUES ('send_email', now(), INTERVAL '5 minutes');
    ```

    The fields of `event_timestamp` and `event_ttl`, or the resulting expiring time, could come from sources or views building them on the fly. The table is just one of the many options.

1. Create a view with a temporal filter **filtering by the expiring time**.
    ```sql
      CREATE MATERIALIZED VIEW ttl_view AS
      SELECT *
      FROM events
      WHERE mz_logical_timestamp() < extract(epoch from (event_timestamp + event_ttl)) * 1000;
    ```

    The filter clause will discard any row with an **expiring time** less or equal to `mz_logical_timestamp()`.
1. That's it! Use it in the way that best fits your use case.

### Usage examples

- Run a query to know the time to live for a row:
  ```sql
    SELECT
      (extract(epoch from (event_timestamp + event_ttl))) -
      (mz_logical_timestamp() / 1000) AS remaining_time
    FROM TTL_VIEW
    WHERE event_name = 'send_email';
  ```

- Just check if a particular row is still available:
  ```sql
  SELECT event_name
  FROM TTL_VIEW
  WHERE event_name = 'ban_over_ip_address';
  ```

- Run a tail and react over any expiring row:
  ```sql
    INSERT INTO events VALUES ('send_email', now(), INTERVAL '5 seconds');
    COPY( TAIL TTL_VIEW WITH (SNAPSHOT = false) ) TO STDOUT;

  ```
  ```nofmt
  mz_timestamp | mz_diff | event_name | event_timestamp | event_ttl |
  -------------|---------|------------|-----------------|-----------|
  ...          | -1      | send_email | ...             | 00:00:10  | <-- Time to send the email!
  ```

<!-- ## Expiring time

One can simplify the case by summing the `event_time` and `event_ttl` to get the expiry date at the beginning:

```sql
  CREATE TABLE events (event_name TEXT, event_expiration TIMESTAMP);

  -- Sample (One minute time to live):
  INSERT INTO events VALUES ('send_email', now() + INTERVAL '1 minute');
```

But if there is a need in the future for more details, then **it will be a disadvantage**. Granularity is absent. -->
