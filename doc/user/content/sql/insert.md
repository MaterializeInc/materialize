---
title: "INSERT"
description: "`INSERT` inserts values into a table."
menu:
  main:
    parent: 'sql'
---

`INSERT` inserts values into a table.

{{< warning >}}
**INSERT is currently only available in the [developer](https://mtrlz.dev/) (unstable) builds.**
{{< /warning >}}

{{< warning >}}
At the moment, tables do not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.
{{< /warning >}}


## Conceptual framework

`INSERT` statements insert data into [tables](../create-table). You may want to `INSERT` data
into a table when:

- Manually inserting rows into Materialize from a non-streaming data source.
- Testing Materialize's features without setting up a data stream.

## Syntax

{{< diagram "insert.svg" >}}

Field | Use
------|-----
_table&lowbar;name_ | The name of the target table.
_col&lowbar;value_ | The value to be inserted into the column. If a given column is nullable, a `NULL` value may be provided.

## Details

### Restrictions

Tables do not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.

`INSERT` currently only supports a `VALUES` clause. You cannot use other clauses,
such as `INSERT INTO ... SELECT` or `INSERT INTO .. DEFAULT VALUES` with `INSERT`.

## Examples

### Inserting data into a table

To insert data into a table, execute an `INSERT` statement where the `VALUES` clause
is followed by a list of tuples. Each tuple in the `VALUES` clause must have a value
for each column in the table. If a column is nullable, a `NULL` value may be provided.

```sql
CREATE TABLE t (a int, b text NOT NULL);

INSERT INTO t VALUES (1, 'a'), (NULL, 'b');

SELECT * FROM t;
 a | b
---+---
   | b
 1 | a
```

In the above example, the second tuple provides a `NULL` value for column `a`, which
is nullable. `NULL` values may not be inserted into column `b`, which is not nullable.

## Related pages

- [`CREATE TABLE`](../create-table)
- [`DROP TABLE`](../drop-table)
