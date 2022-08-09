---
title: "Time-To-Live"
description: "Use the time-to-live pattern and assign an expiration time for a particular row."
draft: true
menu:
  main:
    parent: 'sql-patterns'
---

The time-to-live pattern (TTL) helps you to build expiration times mechanisms using SQL. Before continuing, make sure to understand how [temporal filters](/guides/temporal-filters/) work.

A few examples of the use cases that you can achieve are:

- Free memory
- Schedule events, like emails or messages
- Temporal offers for an e-commerce
- Time-sensitive security blockers

## How-to

1. Identify the row, its initial time, and the TTL. In the example, the `event_time` and `event_ttl` represent those times. The sum of these values represents the **expiring time**.
    ```sql
      CREATE TABLE events (event_name TEXT, event_time TIMESTAMP, event_ttl INTERVAL);

      -- Sample (One minute TTL):
      INSERT INTO events VALUES ('send_email', now(), INTERVAL '1 minute');
    ```

    For the example, we use a table, but the time fields could come from a source or views building them on the fly.

1. Create a view with a temporal filter **filtering by the expiring time**.
    ```sql
      CREATE MATERIALIZED VIEW ttl_view AS
      SELECT *
      FROM events
      WHERE mz_logical_timestamp() < extract(epoch from (event_time + event_ttl)) * 1000;
    ```

    The filter discards rows with **expiring time** less or equal to the  `mz_logical_timestamp()`.
1. That's it! Use it in the way that best fits your use case.

### Usage examples

- Run a query to know the remaining living time for a row:
  ```sql
    SELECT
      (extract(epoch from (event_time + event_ttl))) -
      (mz_logical_timestamp() / 1000) AS remaining_block_time
    FROM TTL_VIEW
    WHERE event_name = 'security_block_ban';
  ```

- Check if a particular row is still available:
  ```sql
  SELECT event_name
  FROM TTL_VIEW
  WHERE event_name = 'special_offer';
  ```

- Run a tail and see when the filter reaches an expiration date:
  ```sql
    INSERT INTO events VALUES ('send_email', now(), INTERVAL '10 seconds');
    COPY( TAIL TTL_VIEW WITH (SNAPSHOT = false) ) TO STDOUT;

    -- It is time to send the email when:
  ```
  ```nofmt
  mz_timestamp | mz_diff | event_name | event_time | event_ttl |
  -------------|---------|------------|------------|-----------|
  ...          | -1      | send_email | ...        | 00:00:10  |
  ```

<!-- ## Expiring time

One can simplify the case by summing the `event_time` and `event_ttl` to get the expiry date at the beginning:

```sql
  CREATE TABLE events (event_name TEXT, event_expiration TIMESTAMP);

  -- Sample (One minute time to live):
  INSERT INTO events VALUES ('send_email', now() + INTERVAL '1 minute');
```

But if there is a need in the future for more details, then **it will be a disadvantage**. Granularity is absent. -->
