---
title: "DROP TABLE"
description: "`DROP TABLE` removes a table from your Materialize instance."
menu:
  main:
    parent: 'sql'
---

`DROP TABLE` removes a table from your Materialize instance.

## Conceptual framework

[Tables](../create-table) store non-streaming data that is inserted via [INSERT](../insert)
statements. `DROP TABLE` removes tables from your Materialize instance.

## Syntax

{{< diagram "drop-table.svg" >}}

Field | Use
------|-----
_data_type_name_ | The name of the table to remove.
`CASCADE` | Automatically removes any objects that depend on the table, as well as the table.
`IF EXISTS`  | Do not issue an error if the named table doesn't exist.
`RESTRICT`  | Don't remove the table if any objects depend on it. _(Default.)_
  |
## Examples

### Drop a table (no dependencies)
Create a table *t* and verify that it was created:

```sql
CREATE TABLE t (a int, b text NOT NULL);

SHOW TABLES;
```
```
TABLES
------
t
```

Remove the table:

```sql
DROP TABLE t;
```
### Drop a table (with dependencies)

Create a table *t*:

```sql
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

```sql
CREATE MATERIALIZED VIEW t_view AS SELECT sum(a) AS sum FROM t;
SHOW MATERIALIZED VIEWS;
```
```
    name
---------------
 t_view
(1 row)
```

Remove table *t*:

```sql
DROP TABLE t CASCADE;
```

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
