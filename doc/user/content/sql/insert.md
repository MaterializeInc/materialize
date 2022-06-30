---
title: "INSERT"
description: "`INSERT` inserts values into a table."
menu:
  main:
    parent: commands
---

`INSERT` writes values to [user-defined tables](../create-table).

{{< warning >}}
At the moment, tables do not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.
{{< /warning >}}

## Conceptual framework

You might want to `INSERT` data into tables when:

- Manually inserting rows into Materialize from a non-streaming data source.
- Testing Materialize's features without setting up a data stream.

## Syntax

{{< diagram "insert.svg" >}}

Field | Use
------|-----
**INSERT INTO** _table_name_ | The table to write values to.
_alias_ | Only permit references to _table_name_ as _alias_.
_column_name_... | Correlates the inserted rows' columns to _table_name_'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. <br/><br/>If some but not all of _table_name_'s columns are provided, the unprovided columns receive their type's default value, or `NULL` if no default value was specified.
_expr_... | The expression or value to be inserted into the column. If a given column is nullable, a `NULL` value may be provided.
_query_ | A [`SELECT`](../select) statements whose returned rows you want to write to the table.

## Details

The optional `RETURNING` clause causes `INSERT` to return values based on each inserted row.

### Restrictions

Tables do not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.

## Examples

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

You may also insert data using a column specification.

```sql
CREATE TABLE t (a int, b text NOT NULL);

INSERT INTO t (b, a) VALUES ('a', 1), ('b', NULL);

SELECT * FROM t;
```
```
 a | b
---+---
   | b
 1 | a
```

You can also insert the values returned from `SELECT` statements:

```sql
CREATE TABLE s (a text);

INSERT INTO s VALUES ('c');

INSERT INTO t (b) SELECT * FROM s;

SELECT * FROM t;
```
```
 a | b
---+---
   | b
   | c
 1 | a
```

## Related pages

- [`CREATE TABLE`](../create-table)
- [`DROP TABLE`](../drop-table)
- [`SELECT`](../select)
