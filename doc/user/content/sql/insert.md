---
title: "INSERT"
description: "`INSERT` inserts values into a table."
menu:
  main:
    parent: commands
---

`INSERT` writes values to [user-defined tables](../create-table).

## Syntax

{{% include-syntax file="examples/insert" example="syntax" %}}

## Details

### Known limitations

* `INSERT ... SELECT` can reference [read-write tables](../create-table) but not
  [sources](../create-source) or read-only tables _(or views, materialized views, and indexes that
  depend on sources)_.
* **Low performance.** While processing an `INSERT ... SELECT` statement,
  Materialize cannot process other `INSERT`, `UPDATE`, or `DELETE` statements.

## Examples

To insert data into a table, execute an `INSERT` statement where the `VALUES` clause
is followed by a list of tuples. Each tuple in the `VALUES` clause must have a value
for each column in the table. If a column is nullable, a `NULL` value may be provided.

```mzsql
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

```mzsql
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

```mzsql
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

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/insert" %}}

## Related pages

- [`CREATE TABLE`](../create-table)
- [`DROP TABLE`](../drop-table)
- [`SELECT`](../select)
