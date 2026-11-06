---
title: "UPDATE"
description: "`UPDATE` changes values stored in tables."
menu:
  main:
    parent: commands
---

`UPDATE` changes values stored in [user-created tables](../create-table).

## Syntax

```mzsql
UPDATE <table_name> [ AS <table_alias> ]
   SET <column_name> = <expression> [, <column2_name> = <expression2>, ...]
[WHERE <condition(s)> ];
```

Syntax element                | Description
------------------------------|------------
**AS** <table_alias>          | If specified, you can only use the alias to refer to the table within that `UPDATE` statement.
**WHERE** <condition(s)>      | If specified, only update rows that meet the condition(s).

## Details

### Known limitations

* `UPDATE` cannot be used inside [transactions](../begin).
* `UPDATE` can reference [read-write tables](../create-table) but not
  [sources](../create-source) or read-only tables.
* **Low performance.** While processing an `UPDATE` statement, Materialize cannot
  process other `INSERT`, `UPDATE`, or `DELETE` statements.
* **Large updates.** A single `UPDATE` that matches a very large number of rows
  can fail with a timeout or replica-disconnect error. See
  [Large `UPDATE` statements](#large-update-statements).

### Large `UPDATE` statements

Materialize processes an `UPDATE` by reading every row the statement matches back
through the cluster, so the work scales with the number of rows matched, not just
the size of the table or the complexity of the `WHERE` clause.

A single `UPDATE` that matches a very large number of rows can fail (for example,
with a timeout or a `replica ... disconnected` error) when reading the matched
rows back takes longer than the cluster can complete in one statement. There is
no fixed row or byte limit: the threshold depends on how many rows the statement
matches, how wide those rows are, and the size of the cluster.

If a large `UPDATE` fails this way, split it into smaller statements that each
match fewer rows, using a predicate on an [indexed](../create-index) column (for
example, by key range). Increasing the size of the cluster can raise the
threshold, but splitting the statement is the reliable approach. The same
guidance applies to large [`DELETE`](../delete) statements.

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `UPDATE` privileges on the table being updated.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
    execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Examples

All examples below will use the `example_table` table:

```mzsql
CREATE TABLE example_table (a int, b text);
INSERT INTO example_table VALUES (1, 'hello'), (2, 'goodbye');
```

To verify the initial state of the table, run the following `SELECT` statement:

```mzsql
SELECT * FROM example_table;
```

The `SELECT` statement above should return two rows:

```
 a |    b
---+---------
 1 | hello
 2 | goodbye
```

### Update based on a condition

The following `UPDATE` example includes a `WHERE` clause to specify which rows
to update:

```mzsql
UPDATE example_table
SET a = a + 2
WHERE b = 'hello';
```

Only one row should be updated, namely the row with `b = 'hello'`. To verify
that the operation updated the `a` column only for that row, run the following
`SELECT` statement:

```mzsql
SELECT * FROM example_table;
```

The returned results show that column `a` was updated only for the row with `b
= 'hello'`:

```
 a |    b
---+---------
 3 | hello         -- Previous value: 1
 2 | goodbye
```

### Update all rows

The following `UPDATE` example updates all rows in the table to set `a` to 0 and
`b` to `aloha`:

```mzsql
UPDATE example_table
SET a = 0, b = 'aloha';
```

To verify the results, run the following `SELECT` statement:

```mzsql
SELECT * FROM example_table;
```

The returned results show that all rows were updated:

```
 a |   b
---+-------
 0 | aloha
 0 | aloha
```

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)
