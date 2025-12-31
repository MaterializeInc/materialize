---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-table/
complexity: intermediate
description: '`DROP TABLE` removes a table from Materialize.'
doc_type: reference
keywords:
- CREATE A
- DROP TABLE
- IF EXISTS
- RESTRICT
- CASCADE
product_area: Indexes
status: stable
title: DROP TABLE
---

# DROP TABLE

## Purpose
`DROP TABLE` removes a table from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP TABLE` removes a table from Materialize.



`DROP TABLE` removes a table from Materialize.

## Syntax

This section covers syntax.

```mzsql
DROP TABLE [IF EXISTS] <table_name> [RESTRICT|CASCADE];
```text

Syntax element | Description
---------------|------------
**IF EXISTS**  | Optional. If specified, do not return an error if the named table doesn't exist.
`<table_name>` | The name of the table to remove.
**CASCADE** | Optional. If specified, remove the table and its dependent objects.
**RESTRICT**  | Optional. Don't remove the table if any non-index objects depend on it. _(Default.)_

## Examples

This section covers examples.

### Remove a table with no dependent objects
Create a table *t* and verify that it was created:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
SHOW TABLES;
```text
```
TABLES
------
t
```text

Remove the table:

```mzsql
DROP TABLE t;
```bash
### Remove a table with dependent objects

Create a table *t*:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
INSERT INTO t VALUES (1, 'yes'), (2, 'no'), (3, 'maybe');
SELECT * FROM t;
```text
```
a |   b
---+-------
2 | no
1 | yes
3 | maybe
(3 rows)
```text

Create a materialized view from *t*:

```mzsql
CREATE MATERIALIZED VIEW t_view AS SELECT sum(a) AS sum FROM t;
SHOW MATERIALIZED VIEWS;
```text
```
name    | cluster
--------+---------
t_view  | default
(1 row)
```text

Remove table *t*:

```mzsql
DROP TABLE t CASCADE;
```bash

### Remove a table only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP TABLE t;
  ```text
- ```mzsql
  DROP TABLE t RESTRICT;
  ```bash

### Do not issue an error if attempting to remove a nonexistent table

```mzsql
DROP TABLE IF EXISTS t;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped table.
- `USAGE` privileges on the containing schema.


## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`DROP OWNED`](../drop-owned)

