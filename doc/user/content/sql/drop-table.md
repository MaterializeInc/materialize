---
title: "DROP TABLE"
description: "`DROP TABLE` removes a table from Materialize."
menu:
  main:
    parent: commands
---

`DROP TABLE` removes a table from Materialize.

## Conceptual framework

[Tables](../create-table) store non-streaming data that is inserted via [INSERT](../insert)
statements. `DROP TABLE` removes a table from Materialize.

## Syntax

{{< diagram "drop-table.svg" >}}

Field | Use
------|-----
**IF EXISTS**  | Do not return an error if the named table doesn't exist.
_table_name_ | The name of the table to remove.
**CASCADE** | Remove the table and its dependent objects.
**RESTRICT**  | Don't remove the table if any non-index objects depend on it. _(Default.)_

## Examples

### Remove a table with no dependent objects
Create a table *t* and verify that it was created:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
SHOW TABLES;
```
```
TABLES
------
t
```

Remove the table:

```mzsql
DROP TABLE t;
```
### Remove a table with dependent objects

Create a table *t*:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
INSERT INTO t VALUES (1, 'yes'), (2, 'no'), (3, 'maybe');
SELECT * FROM t;
```
```
a |   b
---+-------
2 | no
1 | yes
3 | maybe
(3 rows)
```

Create a materialized view from *t*:

```mzsql
CREATE MATERIALIZED VIEW t_view AS SELECT sum(a) AS sum FROM t;
SHOW MATERIALIZED VIEWS;
```
```
name    | cluster
--------+---------
t_view  | default
(1 row)
```

Remove table *t*:

```mzsql
DROP TABLE t CASCADE;
```

### Remove a table only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP TABLE t;
  ```
- ```mzsql
  DROP TABLE t RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent table

```mzsql
DROP TABLE IF EXISTS t;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-table.md" >}}

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`DROP OWNED`](../drop-owned)
