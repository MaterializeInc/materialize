---
audience: developer
canonical_url: https://materialize.com/docs/sql/delete/
complexity: intermediate
description: '`DELETE` removes values stored in tables.'
doc_type: reference
keywords:
- FROM
- WHERE
- Low performance.
- DELETE
- CREATE TABLE
- USING
- AS
- DELETE FROM
- INSERT INTO
product_area: Indexes
status: stable
title: DELETE
---

# DELETE

## Purpose
`DELETE` removes values stored in tables.

If you need to understand the syntax and options for this command, you're in the right place.


`DELETE` removes values stored in tables.



`DELETE` removes values stored in [user-created tables](../create-table).

## Syntax

This section covers syntax.

```mzsql
DELETE FROM <table_name> [AS <alias>]
[USING <from_item> [, ...]]
[WHERE <condition>]
;
```text

Syntax element | Description
---------------|------------
`<table_name>` | The table whose values you want to remove.
**AS** `<alias>` | Optional. The alias for the table. If specified, only permit references to `<table_name>` as `<alias>`.
**USING** _from_item_ | Optional. Table expressions whose columns you want to reference in the `WHERE` clause. This supports the same syntax as the **FROM** clause in [`SELECT`](../select) statements, e.g. supporting aliases.
**WHERE** `<condition>` | Optional. Only remove rows which evaluate to `true` for _condition_.

## Details

This section covers details.

### Known limitations

* `DELETE` cannot be used inside [transactions](../begin).
* `DELETE` can reference [read-write tables](../create-table) but not
  [sources](../create-source) or read-only tables.
* **Low performance.** While processing a `DELETE` statement, Materialize cannot
  process other `INSERT`, `UPDATE`, or `DELETE` statements.

## Examples

This section covers examples.

```mzsql
CREATE TABLE delete_me (a int, b text);
INSERT INTO delete_me
    VALUES
    (1, 'hello'),
    (2, 'goodbye'),
    (3, 'ok');
DELETE FROM delete_me WHERE b = 'hello';
SELECT * FROM delete_me ORDER BY a;
```text
```
 a |    b
---+---------
 2 | goodbye
 3 | ok
```text
```mzsql
CREATE TABLE delete_using (b text);
INSERT INTO delete_using VALUES ('goodbye'), ('ciao');
DELETE FROM delete_me
    USING delete_using
    WHERE delete_me.b = delete_using.b;
SELECT * FROM delete_me;
```text
```
 a | b
---+----
 3 | ok
```text
```mzsql
DELETE FROM delete_me;
SELECT * FROM delete_me;
```text
```
 a | b
---+---
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `DELETE` privileges on `table_name`.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
    execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.


## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)

