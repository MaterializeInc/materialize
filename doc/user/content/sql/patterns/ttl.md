---
title: "Time-To-Live"
description: "Use the time-to-live pattern and assign an expiration time for a particular row."
draft: true
menu:
  main:
    parent: 'sql-patterns'
---

The TTL pattern helps assign an expiration time for a particular row. Before you dive in, make sure you understand how [temporal filters](/guides/temporal-filters/) work.

A few examples of the use cases that you can achieve:

- Free up memory
- Schedule events, like emails or messages
- Temporal offers for a store
- Spam counters and blockers

## How-to

1. Identify the row, its initial time, and the time to live. The sum of these two values, `event_time` and `event_ttl`, returns the **expiry date**.
    ```sql
      CREATE TABLE events (event_name TEXT, event_time TIMESTAMP, event_ttl INTERVAL);

      -- Sample (One minute time to live):
      INSERT INTO events VALUES ('send_email', now(), INTERVAL '1 minute');
    ```

1. Create a view with a temporal filter filtering by the expiring date. Only **rows with a expiry date greater than** `mz_logical_timestamp()` **are made available**; otherwise, they get discarded.
    ```sql
      CREATE MATERIALIZED VIEW ttl_view AS
      SELECT *
      FROM events
      WHERE mz_logical_timestamp() < extract(epoch from (event_time + event_ttl)) * 1000;
    ```

1. Use it in the way that best fits your use case.

Here are some examples:

- Run a query to know the remaining living time:
  ```sql
    SELECT
      (extract(epoch from (event_time + event_ttl))) -
      (mz_logical_timestamp() / 1000) AS remaining_block_time
    FROM TTL_VIEW
    WHERE event_name = 'send_email';
  ```

- Check if a particular row is still available:
  ```sql
  SELECT event_name as email_offer_is_available
  FROM TTL_VIEW
  WHERE event_name = 'special_offer';
  ```

- Run a tail and listen when the time to live is over:
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

## Expiring time

One can simplify the case by summing the `event_time` and `event_ttl` to get the expiry date at the beginning:

```sql
  CREATE TABLE events (event_name TEXT, event_expiration TIMESTAMP);

  -- Sample (One minute time to live):
  INSERT INTO events VALUES ('send_email', now() + INTERVAL '1 minute');
```

But if there is a need in the future for more details, then **it will be a disadvantage**. Granularity is absent.