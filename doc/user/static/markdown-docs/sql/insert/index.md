# INSERT
`INSERT` inserts values into a table.
`INSERT` writes values to [user-defined tables](../create-table).

## Syntax



```mzsql
INSERT INTO <table_name> [[AS] <alias>] [ ( <col1> [, ...] ) ]
VALUES ( <expr1> [, ...] ) [, ...] | DEFAULT VALUES | <query>
[RETURNING <output_expr | *> [, ...] ]

```

| Syntax element | Description |
| --- | --- |
| `<table_name>` | The table to write values to.  |
| `<col1> [, ...]` | Correlates the inserted rows' columns to `<table_name>`'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column.  If some but not all of `<table_name>`'s columns are provided, the unprovided columns receive their type's default value, or `NULL` if no default value was specified.  |
| `VALUES ( <expr1> [, ...] ) [, ...]` | A list of tuples `( <expr1> [, ...] ) [, ...]` to insert. Each tuple contains expressions or values to be inserted into the columns. If a given column is nullable, a `NULL` value may be provided.  |
| `DEFAULT VALUES` | Insert a single row using the default value for all columns.  |
| `<query>` | A [`SELECT`](/sql/select) statement whose returned rows you want to write to the table.  |
| `RETURNING <output_expr \| *> [, ...]` | Causes `INSERT` to return values based on each inserted row: - `*` to return all columns - `<output_expr> [[AS] <alias>]`. [Aggregate functions](/sql/functions/#aggregate-functions) are not allowed in the `RETURNING` clause.  |


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

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `INSERT` privileges on `table_name`.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
    execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Related pages

- [`CREATE TABLE`](../create-table)
- [`DROP TABLE`](../drop-table)
- [`SELECT`](../select)
