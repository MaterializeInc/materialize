---
title: "now and mz_now functions"
description: "Details the differences between the `now()` and `mz_now()` functions."
aliases:
  - /sql/functions/now_and_mz_logical_timestamp/
menu:
  main:
    parent: 'sql-functions'
---

In Materialize, `now()` returns the value of the system clock with timezone when the transaction began. It cannot be used when creating materialized views.

By contrast, `mz_now()` returns the logical time at which the query was executed. This may be arbitrarily ahead of or behind the system clock.

For example, at 9pm, Materialize may choose to execute a query as of logical time 8:30pm, perhaps because data for 8:30–9pm has not yet arrived. In this scenario, `now()` would return 9pm, while `mz_now()` would return 8:30pm.

The typical uses of `now()` and `mz_now()` are:

* **Temporal filters**

  You can use `mz_now()` in a `WHERE` or `HAVING` clause to limit the working dataset.
  This is referred to as a **temporal filter**.
  See the [temporal filter](/sql/patterns/temporal-filters) pattern for more details.

* **Query timestamp introspection**

  An ad-hoc `SELECT` query with `now()` and `mz_now()` can be useful if you need to understand how up to date the data returned by a query is.
  The data returned by the query reflects the results as of the logical time returned by a call to `mz_now()` in that query.


{{< warning >}}
Queries that contain `now()` or `mz_now()` in the `SELECT` clause cannot be materialized.
In other words, you cannot create an index or a materialized view on a query with `now()` or `mz_now()` in the `SELECT` clause.
This is because `now()` and `mz_now()` change every millisecond, so if this materialization _were_ allowed, every record in the collection would be updated every millisecond, which would be resource prohibitive.
{{< /warning >}}

## Examples

### Temporal filters

<!-- This example also appears in temporal-filters -->
It is common for real-time applications to be concerned with only a recent period of time.
In this case, we will filter a table to only include records from the last 30 seconds.

```sql
-- Create a table of timestamped events.
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
COPY (SUBSCRIBE (SELECT event_ts, content FROM last_30_sec)) TO STDOUT;
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

### Query timestamp introspection

If you haven't already done so in the previous example, create a table called `events` and add a few records.

```sql
-- Create a table of timestamped events.
CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
);
-- Insert records
INSERT INTO events VALUES (
    'hello',
    now()
);
INSERT INTO events VALUES (
    'welcome',
    now()
);
INSERT INTO events VALUES (
    'goodbye',
    now()
);
```

Execute this ad-hoc query that adds the current system timestamp and current logical timestamp to the events in the `events` table.

```sql
SELECT now(), mz_now(), * FROM events
```

```nofmt
            now            |    mz_now     | content |       event_ts
---------------------------+---------------+---------+-------------------------
 2023-06-15 22:38:14.18+00 | 1686868693480 | hello   | 2023-06-15 22:29:50.711
 2023-06-15 22:38:14.18+00 | 1686868693480 | goodbye | 2023-06-15 22:29:51.233
 2023-06-15 22:38:14.18+00 | 1686868693480 | welcome | 2023-06-15 22:29:50.874
(3 rows)
```

Notice when you try to materialize this query, you get errors:

```sql
CREATE MATERIALIZED VIEW cant_materialize
    AS SELECT now(), mz_now(), * FROM events;
```

```nofmt
ERROR:  cannot materialize call to current_timestamp
ERROR:  cannot materialize call to mz_now
```
