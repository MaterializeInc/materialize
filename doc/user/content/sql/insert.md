---
title: "INSERT"
description: "`INSERT` inserts values into a table."
menu:
  main:
    parent: 'sql'
---

`INSERT` inserts values into a table.

## Conceptual framework

`INSERT` statements inserting data into [tables](../create-table).

## Syntax

{{< diagram "insert.svg" >}}

Field | Use
------|-----
_table&lowbar;name_ | The name of the target table.
_col&lowbar;value_ | The value to be inserted into the column, or `NULL` if column is nullable.

## Details

### Restrictions

Any data inserted into a table will not be persisted. This means that restarting a
Materialize instance will lose any data that was previously inserted into a table.

The `INSERT` statement currently only supports a `VALUES` clause. Any other type
of `INSERT` statement, such as `INSERT INTO ... SELECT` or `INSERT INTO .. DEFAULT VALUES`,
is unsupported.

## Examples

### Inserting data into a table

To insert data into a table, execute an `INSERT` statement where the `VALUES` clause
is followed by a list of tuples. Each tuple in the `VALUES` clause should have a value
for each column in the table. If a column is nullable, a `NULL` value may be provided.

```sql
CREATE TABLE t (a int, b text NOT NULL);

INSERT INTO t VALUES (1, 'a'), (NULL, 'b');
```

In the above example, the second tuple provides a `NULL` value for column `a`, which
is nullable. `NULL` values may not be inserted into column `b`, which is not nullable.

## Related pages

- [`CREATE TABLE`](../create-table)
- [`DROP TABLE`](../drop-table)
