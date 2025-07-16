---
title: "now and mz_now functions"
description: "Details the differences between the `now()` and `mz_now()` functions."
aliases:
  - /sql/functions/now_and_mz_logical_timestamp/
menu:
  main:
    parent: 'sql-functions'
---

In Materialize, `now()` returns the value of the system clock when the
transaction began as a [`timestamp with time zone`] value.

By contrast, `mz_now()` returns the logical time at which the query was executed
as a [`mz_timestamp`] value.

## Details

### `mz_now()` clause

{{< include-md file="shared-content/mz_now_clause_requirements.md" >}}

### Usage patterns

The typical uses of `now()` and `mz_now()` are:

* **Temporal filters**

  You can use `mz_now()` in a `WHERE` or `HAVING` clause to limit the working dataset.
  This is referred to as a **temporal filter**.
  See the [temporal filter](/sql/patterns/temporal-filters) pattern for more details.

* **Query timestamp introspection**

  An ad hoc `SELECT` query with `now()` and `mz_now()` can be useful if you need to understand how up to date the data returned by a query is.
  The data returned by the query reflects the results as of the logical time returned by a call to `mz_now()` in that query.

### Logical timestamp selection

When using the [serializable](/get-started/isolation-level#serializable)
isolation level, the logical timestamp may be arbitrarily ahead of or behind the
system clock. For example, at a wall clock time of 9pm, Materialize may choose
to execute a serializable query as of logical time 8:30pm, perhaps because data
for 8:30–9pm has not yet arrived. In this scenario, `now()` would return 9pm,
while `mz_now()` would return 8:30pm.

When using the [strict serializable](/get-started/isolation-level#strict-serializable)
isolation level, Materialize attempts to keep the logical timestamp reasonably
close to wall clock time. In most cases, the logical timestamp of a query will
be within a few seconds of the wall clock time. For example, when executing
a strict serializable query at a wall clock time of 9pm, Materialize will choose
a logical timestamp within a few seconds of 9pm, even if data for 8:30–9pm has
not yet arrived and the query will need to block until the data for 9pm arrives.
In this scenario, both `now()` and `mz_now()` would return 9pm.

### Limitations

#### Materialization

* Queries that use `now()` cannot be materialized. In other words, you cannot
  create an index or a materialized view on a query that calls `now()`.

* Queries that use `mz_now()` can only be materialized if the call to
  `mz_now()` is used in a [temporal filter](/sql/patterns/temporal-filters).

These limitations are in place because `now()` changes every microsecond and
`mz_now()` changes every millisecond. Allowing these functions to be
materialized would be resource prohibitive.

#### `mz_now()` restrictions

The [`mz_now()`](/sql/functions/now_and_mz_now) clause has the following
restrictions:

- {{< include-md file="shared-content/mz_now_clause_disjunction_restrictions.md" >}}

  For example:

  {{< yaml-table data="mz_now/mz_now_combination" >}}

  For alternatives, see [Disjunction (OR)
  alternatives](http://localhost:1313/docs/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or).

- If part of a  `WHERE` clause, the `WHERE` clause cannot be an [aggregate
 `FILTER` expression](/sql/functions/filters).

## Examples

### Temporal filters

<!-- This example also appears in temporal-filters -->
It is common for real-time applications to be concerned with only a recent period of time.
In this case, we will filter a table to only include records from the last 30 seconds.

```mzsql
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

```mzsql
COPY (SUBSCRIBE (SELECT event_ts, content FROM last_30_sec)) TO STDOUT;
```

In a separate session, insert a record.

```mzsql
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

```mzsql
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

Execute this ad hoc query that adds the current system timestamp and current logical timestamp to the events in the `events` table.

```mzsql
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

```mzsql
CREATE MATERIALIZED VIEW cant_materialize
    AS SELECT now(), mz_now(), * FROM events;
```

```nofmt
ERROR:  cannot materialize call to current_timestamp
ERROR:  cannot materialize call to mz_now
```

[`mz_timestamp`]: /sql/types/mz_timestamp
[`timestamp with time zone`]: /sql/types/timestamptz
