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

## Details

### Restrictions

Tables will not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.

Additionally, tables do not currently support:
    - Primary keys
    - Unique constraints
    - Check constraints
    - Default column values
    - Insert statements that use anything other than a `VALUES` clause, such as
      `INSERT INTO ... SELECT`
    - `UPDATE ...` and `DELETE` statements

## Examples

### Creating a table

We can create a table `t` with the following statement:

```sql
CREATE TABLE t (a int, b text NOT NULL);
```

Once a table is created, we can inspect it with various `SHOW` commands.

```sql
SHOW TABLES;
TABLES
------
t

SHOW COLUMNS IN t;
Field      Nullable  Type
-------------------------
a          YES       int4
b          NO        text
```

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
