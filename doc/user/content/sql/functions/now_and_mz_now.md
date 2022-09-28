---
title: "now and mz_now functions"
description: "Details the differences between the `now()` and `mz_now()` functions."
menu:
  main:
    parent: 'sql-functions'
---

In Materialize, `now()` returns the value of the system clock with timezone when the transaction began. It cannot be used when creating views.

By contrast, `mz_now()` returns the logical time at which the query was executed. This may be arbitrarily ahead of or behind the system clock.

For example, at 9pm, Materialize may choose to execute a query as of logical time 8:30pm, perhaps because data for 8:30–9pm has not yet arrived. In this scenario, `now()` would return 9pm, while `mz_now()` would return 8:30pm.

Its typical uses are:

* **Query timestamp introspection**

  `mz_now()` can be useful if you need to understand how up to date the data returned by a query is. The data returned by a query reflects the results as of the logical time returned by a call to `mz_now()` in that query.

* **Temporal filters**

  Various windowing idioms involve using `mz_now()` in a [temporal filter](/sql/patterns/temporal-filters).

## Example

### Temporal filter using mz_now()

<!-- This example also appears in temporal-filters -->
For this example, you'll need to create a sample data source and create an indexed view from it for later reference.

```sql
--Create a table of timestamped events.
CREATE TABLE events (
    content text,
    insert_t timestamp,
    delete_t timestamp
);

--Create an indexed view of events valid at a given logical time.
CREATE VIEW valid AS
SELECT content, insert_t, delete_t
FROM events
WHERE mz_now() >= insert_t
  AND mz_now() < delete_t;
CREATE DEFAULT INDEX ON valid;
```

Next, you'll populate the table with timestamp data.

```sql
INSERT INTO events VALUES (
    'hello',
    now(),
    now() + '100s'
);
INSERT INTO events VALUES (
    'welcome',
    now(),
    now() + '150s'
);
INSERT INTO events VALUES (
    'goodbye',
    now(),
    now() + '200s'
);
```

Then, before 100 seconds elapse, run the following query to see all the records:

```sql
SELECT *, mz_now() FROM valid;
```
```nofmt
 content |        insert_t         |        delete_t         |    mz_now
---------+-------------------------+-------------------------+---------------
 hello   | 2022-09-27 23:52:34.831 | 2022-09-27 23:54:14.831 | 1664322794280
 goodbye | 2022-09-27 23:53:04.262 | 2022-09-27 23:56:24.262 | 1664322794280
 welcome | 2022-09-27 23:53:03.142 | 2022-09-27 23:55:33.142 | 1664322794280
(3 rows)
```

If you run this query again after 100 seconds from the first insertion, you'll see only two results, because the first result no longer satisfies the predicate.

### Query using now()


```sql
SELECT * FROM valid
  WHERE insert_t <= now();
```
```nofmt
 content |        insert_t         |        delete_t
---------+-------------------------+-------------------------
 goodbye | 2022-09-27 23:53:04.262 | 2022-09-27 23:56:24.262
 welcome | 2022-09-27 23:53:03.142 | 2022-09-27 23:55:33.142
```
