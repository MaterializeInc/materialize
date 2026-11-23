---
title: "Build SCD Type 2 history from an Iceberg sink"
description: "Derive a Slowly Changing Dimension (SCD) Type 2 history, with a validity interval for every version of every row, from an Iceberg sink in append mode."
menu:
  main:
    parent: 'sink-patterns'
    name: "Build SCD Type 2 history"
---

An [Iceberg sink](/export-data/iceberg/) created with `MODE APPEND` writes
every change to the sinked relation as a new row, so the Iceberg table holds
the relation's full history. A query engine reading the table can turn that
changelog into a [Slowly Changing Dimension (SCD) Type
2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row)
table: one row per version of each key, with the interval during which that
version was valid.

Use this pattern when consumers need the history of each key. The query reads
the full changelog, which grows with every change. If consumers need only
current state, create the sink with
[`MODE UPSERT`](/sql/create-sink/iceberg/#creating-an-upsert-sink) instead.

## Before you begin

- Set up an Iceberg catalog connection. See [Apache
  Iceberg](/export-data/iceberg/) for the guide matching your catalog.
- The relation you sink must have a unique key, such as a customer ID. The
  history is built per key. See [Relations without a unique
  key](#relations-without-a-unique-key).

## Step 1. Sink the relation in append mode

Create the sink with `MODE APPEND`. For example, to sink a `customers` table
from Materialize into an Iceberg table called `customers_log`, with the unique
key `customer_id`, run the following:

```mzsql
CREATE SINK customers_sink
  FROM customers
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog_connection (
    NAMESPACE = 'crm',
    TABLE = 'customers_log'
  )
  MODE APPEND
  WITH (COMMIT INTERVAL = '1m');
```

Each change in Materialize becomes one or more rows in `customers_log`, with
two extra columns:

- `_mz_diff`: `1` for an insertion and `-1` for a deletion. An update writes
  both rows: `-1` for the old version and `1` for the new one.
- `_mz_timestamp`: the time of the change, in milliseconds since the Unix
  epoch. Both rows of an update share the same `_mz_timestamp`.

For example, after the following changes:

```mzsql
INSERT INTO customers VALUES
  (1, 'Ada', 'London'), (2, 'Grace', 'New York'), (3, 'Alan', 'Manchester');
-- Create the sink.
UPDATE customers SET city = 'Paris' WHERE customer_id = 1;
DELETE FROM customers WHERE customer_id = 2;
UPDATE customers SET city = 'Berlin' WHERE customer_id = 1;
INSERT INTO customers VALUES (2, 'Grace', 'Boston');
UPDATE customers SET name = 'Alan T.', city = 'Cambridge' WHERE customer_id = 3;
```

`customers_log` contains:

```nofmt
 customer_id |  name   |    city    | _mz_diff | _mz_timestamp
-------------+---------+------------+----------+---------------
           1 | Ada     | London     |        1 | 1793125617107
           2 | Grace   | New York   |        1 | 1793125617107
           3 | Alan    | Manchester |        1 | 1793125617107
           1 | Ada     | London     |       -1 | 1793125627127
           1 | Ada     | Paris      |        1 | 1793125627127
           2 | Grace   | New York   |       -1 | 1793125631219
           1 | Ada     | Paris      |       -1 | 1793125635341
           1 | Ada     | Berlin     |        1 | 1793125635341
           2 | Grace   | Boston     |        1 | 1793125639525
           3 | Alan    | Manchester |       -1 | 1793125660337
           3 | Alan T. | Cambridge  |        1 | 1793125660337
```

## Step 2. Derive validity intervals

In the query engine that reads the Iceberg table, run the following query,
replacing `customer_id` with the unique key of your relation. Each version is
valid from its own `_mz_timestamp` until the next change to the same key:

```sql
WITH ordered AS (
  SELECT customer_id, name, city, _mz_diff, _mz_timestamp,
         LEAD(_mz_timestamp) OVER (
           PARTITION BY customer_id
           ORDER BY _mz_timestamp, _mz_diff
         ) AS next_ts
  FROM customers_log
)
SELECT customer_id, name, city,
       _mz_timestamp   AS valid_from,
       next_ts         AS valid_to,
       next_ts IS NULL AS is_current
FROM ordered
WHERE _mz_diff = 1
ORDER BY customer_id, valid_from;
```

```nofmt
 customer_id |  name   |    city    |  valid_from   |   valid_to    | is_current
-------------+---------+------------+---------------+---------------+------------
           1 | Ada     | London     | 1793125617107 | 1793125627127 | false
           1 | Ada     | Paris      | 1793125627127 | 1793125635341 | false
           1 | Ada     | Berlin     | 1793125635341 | NULL          | true
           2 | Grace   | New York   | 1793125617107 | 1793125631219 | false
           2 | Grace   | Boston     | 1793125639525 | NULL          | true
           3 | Alan    | Manchester | 1793125617107 | 1793125660337 | false
           3 | Alan T. | Cambridge  | 1793125660337 | NULL          | true
```

Two details make the query correct:

- Within a timestamp, ordering by `_mz_diff` sorts the `-1` row of an update
  before its `1` row. The old version's interval therefore closes at the
  update, and the new version's interval stays open.
- A deletion writes a `-1` row with no matching `1` row. The deleted version's
  interval closes at the deletion, and no version is valid until the key is
  inserted again. In the example, customer `2` has no valid version between
  `1793125631219` and `1793125639525`.

To find the version valid at a point in time `t`, filter on
`valid_from <= t AND (valid_to IS NULL OR valid_to > t)`. To use the history
in other queries, store the result as a table in your query engine and refresh
it on a schedule. Running the query directly, or through a view, reads the
full changelog every time.

## Considerations

### Initial snapshot

When you create the sink, Materialize writes the relation's current contents
with the timestamp of sink creation. The `valid_from` of those rows is when the
sink was created, not when the rows were originally inserted.

If you drop the sink and create a new one that writes to a different table,
the new table's history starts with a new initial snapshot. Its `valid_from`
values begin at the new sink's creation time, and it has none of the changes
in the earlier table.

### Converting timestamps

`_mz_timestamp` is a `long` holding milliseconds since the Unix epoch. Convert
it with your query engine's epoch function, for example
`epoch_ms(_mz_timestamp)` in DuckDB or `TO_TIMESTAMP(_mz_timestamp, 3)` in
Snowflake.

### Changes at the same timestamp

Changes that Materialize applies at the same timestamp appear in the
changelog only as their net effect, so the history never contains
intermediate states within one timestamp.

### Relations without a unique key

`_mz_diff` is always `1` or `-1`. A row that appears `n` times in the relation
is written as `n` identical rows. Without a unique key, the query above
produces incorrect intervals for duplicate rows. To reconstruct current state
for such a relation, see [Consume an Iceberg
sink](/export-data/iceberg/#consume-an-iceberg-sink).

## Related pages

- [Apache Iceberg](/export-data/iceberg/)
- [`CREATE SINK`: Iceberg](/sql/create-sink/iceberg/#append-mode)
