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
**IF EXISTS** | Do not return an error if the named table does not exist.
_table&lowbar;name_ | The name of the table to drop.

## Examples

### Dropping a table

We can create a table `t` and verify that it was created with the following
statements:

```sql
CREATE TABLE t (a int, b text NOT NULL);

SHOW TABLES;
TABLES
------
t
```

We can then remove the table from the Materialize instance with a `DROP` statement.

```sql
DROP TABLE t;
```

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
