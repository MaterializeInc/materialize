---
title: "Time-To-Live"
description: "Use the time-to-live pattern and assign an expiration time for a particular row."
aliases:
  - /sql/patterns/ttl/
menu:
  main:
    parent: 'sql-patterns'
---

The **time to live (TTL)** pattern helps to filter rows using expiration times, keeping rows until they are no longer useful and enabling new use cases.

The following examples are possible TTL use cases:

- Trigger schedule tasks, like emails or messages
- Store temporal offers for an e-commerce
- Maintain time-sensitive security blockers
- Saving-up memory

Before continuing, make sure to understand how [mz_now()](/sql/functions/now_and_mz_now/) and [temporal filters](/sql/patterns/temporal-filters/) work.

## Pattern

The pattern uses a temporal filter over a row's creation timestamp plus a TTL. The sum represents the row's **expiration time**. After reaching the expiration time, the query will drop the row.

Pattern example:
```sql
  CREATE VIEW TTL_VIEW AS
  SELECT (created_ts + ttl) AS expiration_time
  FROM events
  WHERE mz_now() < (created_ts + ttl);
```

To know the remaining time to live:

```sql
  SELECT (expiration_time - now()) AS remaining_ttl
  FROM TTL_VIEW;
```

## Example

For a real-world example of the pattern, let's build a task tracking system. It will consist on a view filtering rows from a table. Each row in the table contains a name, creation timestamp, and TTL of a task.

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
1. Create a view using a temporal filter **over the expiration time**. For our example, the expiration time represents the sum between the task's `created_ts` and its `ttl`.
    ```sql
      CREATE MATERIALIZED VIEW tracking_tasks AS
      SELECT
        name,
        created_ts + ttl as expiration_time
      FROM tasks
      WHERE mz_now() < created_ts + ttl;
    ```

    The filter clause will discard any row with an **expiration time** less than or equal to `mz_now()`.
1. That's it! Use it in the way that best fits your use case.

### Usage examples

- Query the time to live for a row:
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
  mz_timestamp | mz_diff | name       | expiration_time   |
  -------------|---------|------------|-----------------|
  ...          | -1      | send_email | ...             | <-- Time to send the email!
  ```
