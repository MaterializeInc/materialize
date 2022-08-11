---
title: "Time-To-Live"
description: "Use the time-to-live pattern and assign an expiration time for a particular row."
menu:
  main:
    parent: 'sql-patterns'
---

The time to live (TTL) pattern helps to remove rows in views or tails using expiration times. You can use it to keep rows in views or tails until they are no longer useful or to trigger certain use cases.

The following examples are possible TTL use cases that you could achieve:

- Trigger schedule tasks, like emails or messages
- Store temporal offers for an e-commerce
- Maintain time-sensitive security blockers
- Saving-up memory

Before continuing, make sure to understand how [temporal filters](/guides/temporal-filters/) work.

## Pattern

The pattern uses a temporal filter over a row’s **expiring time**.
This time indicates when the query should drop the row.

```sql
  CREATE VIEW TTL_VIEW
  SELECT *
  FROM events
  WHERE mz_logical_timestamp() < expiring_time;
```

To know the remaining time to live:

```sql
  SELECT (expiring_time - mz_logical_timestamp()) AS remaining_ttl
  FROM TTL_VIEW;
```

## Example

To have a crystal clear understanding of the pattern, let's build a task scheduling system. It will consist on a view filtering rows from a table. Each row in the table represents a task, and it contains its name, creation time, and TTL.

1.  First, we need to set up the table with its corresponding fields.
    ```sql
      CREATE TABLE tasks (name TEXT, created_date TIMESTAMP, ttl INTERVAL);
    ```
1.  Add a task to send an email in the next five minutes:
    ```sql
      INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 minutes');
      INSERT INTO tasks VALUES ('time_to_eat', now(), INTERVAL '1 hour');
      INSERT INTO tasks VALUES ('security_block', now(), INTERVAL '1 day');
    ```
1. Create a view using a temporal filter **over the expiring time**. For our example, the expiring time represents the sum between the task's `created_date` and its `ttl`.
    ```sql
      CREATE MATERIALIZED VIEW tasks_scheduling AS
      SELECT *
      FROM tasks
      WHERE mz_logical_timestamp() < extract(epoch from (created_date + ttl)) * 1000;
    ```

    The filter clause will discard any row with an **expiring time** less or equal to `mz_logical_timestamp()`.
1. That's it! Use it in the way that best fits your use case.

### Usage examples

- Run a query to know the time to live for a row:
  ```sql
    SELECT
      extract(epoch from (created_date + ttl)) -
      (mz_logical_timestamp() / 1000) AS remaining_ttl
    FROM tasks_scheduling
    WHERE name = 'time_to_eat';
  ```

- Check if a particular row is still available:
  ```sql
  SELECT true
  FROM tasks_scheduling
  WHERE name = 'security_block';
  ```

- Trigger an external process when a row expires:
  ```sql
    INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 seconds');
    COPY( TAIL tasks_scheduling WITH (SNAPSHOT = false) ) TO STDOUT;

  ```
  ```nofmt
  mz_timestamp | mz_diff | event_name | event_timestamp | event_ttl |
  -------------|---------|------------|-----------------|-----------|
  ...          | -1      | send_email | ...             | ...       | <-- Time to send the email!
  ```
