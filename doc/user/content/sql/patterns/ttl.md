---
title: "Time-To-Live"
description: "Use the time-to-live pattern and assign an expiration time for a particular row."
menu:
  main:
    parent: 'sql-patterns'
---

The **time to live (TTL)** pattern helps to remove rows in views, queries, tails, or sinks using expiration times, keeping rows until they are no longer useful, and enabling new use cases.

The following examples are possible TTL use cases:

- Trigger schedule tasks, like emails or messages
- Store temporal offers for an e-commerce
- Maintain time-sensitive security blockers
- Saving-up memory

Before continuing, make sure to understand how [temporal filters](/guides/temporal-filters/) work.

## Pattern

The pattern uses a temporal filter over a row's creation timestamp plus a TTL. The sum represents the row's **expiring time**. After reaching the expiring time, the query will drop the row.

Pattern example:
```sql
  CREATE VIEW TTL_VIEW
  SELECT (created_ts + ttl) as expiring_time
  FROM events
  WHERE mz_logical_timestamp() < (created_ts + ttl);
```

To know the remaining time to live:

```sql
  SELECT (expiring_time - mz_logical_timestamp()) AS remaining_ttl
  FROM TTL_VIEW;
```

## Example

To have a crystal clear understanding of the pattern, let's build a task tracking system. It will consist on a view filtering rows from a table. Each row in the table contains the name, creation timestamp, and TTL of a task.

1.  First, we need to set up the table:
    ```sql
      CREATE TABLE tasks (name TEXT, created_ts TIMESTAMP, ttl INTERVAL);
    ```
1.  Add some tasks to track:
    ```sql
      INSERT INTO tasks VALUES ('send_email', now(), INTERVAL '5 minutes');
      INSERT INTO tasks VALUES ('time_to_eat', now(), INTERVAL '1 hour');
      INSERT INTO tasks VALUES ('security_block', now(), INTERVAL '1 day');
    ```
1. Create a view using a temporal filter **over the expiring time**. For our example, the expiring time represents the sum between the task's `created_ts` and its `ttl`.
    ```sql
      CREATE MATERIALIZED VIEW tracking_tasks AS
      SELECT
        name,
        extract(epoch from (created_ts + ttl)) * 1000 as expiring_time
      FROM tasks
      WHERE mz_logical_timestamp() < extract(epoch from (created_ts + ttl)) * 1000;
    ```

    The filter clause will discard any row with an **expiring time** less or equal to `mz_logical_timestamp()`.
1. That's it! Use it in the way that best fits your use case.

### Usage examples

- Run a query to know the time to live for a row:
  ```sql
    SELECT expiring_time - mz_logical_timestamp() AS remaining_ttl
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
    COPY( TAIL tracking_tasks WITH (SNAPSHOT = false) ) TO STDOUT;

  ```
  ```nofmt
  mz_timestamp | mz_diff | name       | expiring_time   |
  -------------|---------|------------|-----------------|
  ...          | -1      | send_email | ...             | <-- Time to send the email!
  ```
