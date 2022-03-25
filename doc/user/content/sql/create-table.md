---
title: "CREATE TABLE"
description: "`CREATE TABLE` creates an in-memory table."
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.5.0 />}}

`CREATE TABLE` creates an in-memory table.

## Conceptual framework

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created but rows can be added to at will via [INSERT](../insert) statements.

You can seamlessly join tables with other tables, views, and sources in the
system.

{{< warning >}}
At the moment, tables serve a niche use case. They lack many features that are
standard in relational databases. In most situations you should use
[sources](/sql/create-source) instead.
{{< /warning >}}

### When to use a table

A table can be more convenient than a source, but only if all of the following
statements are true:

1. Your dataset is either static or append only.
2. You do not need the dataset to survive reboots of Materialize.
3. Your dataset is several times smaller than the available memory on your
   machine. See the [memory usage](#memory-usage) section below for details.

If any of those statements do not describe your situation, we recommend that you
use a [source](/sql/create-source) instead.

## Syntax

{{< diagram "create-table.svg" >}}

### `col_option`

{{< diagram "col-option.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the table as [temporary](#temporary-tables).
_table&lowbar;name_ | A name for the table.
_col&lowbar;name_ | The name of the column to be created in the table.
_col&lowbar;type_ | The data type of the column indicated by _col&lowbar;name_.
**NOT NULL** | Do not allow the column to contain _NULL_ values. Columns without this constraint can contain _NULL_ values.
*default_expr* | A default value to use for the column in an [`INSERT`](/sql/insert) statement if an explicit value is not provided. If not specified, `NULL` is assumed.

## Details

### Restrictions

{{< warning >}}
Tables do not persist any data that is inserted. This means that restarting a
Materialize instance will lose any data that was previously stored in a table.
{{< /warning >}}

Additionally, tables do not currently support:
- Primary keys
- Unique constraints
- Check constraints
- Insert statements that refer to data in other relations, e.g.:
  ```sql
  INSERT INTO t1 SELECT * FROM t2
  ```
- `UPDATE ...` and `DELETE` statements

### Temporary tables

The `TEMP`/`TEMPORARY` keyword creates a temporary table. Temporary tables are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary tables may depend upon other temporary database objects, but non-temporary
tables may not depend on temporary objects.

### Memory usage

Tables presently store their data in memory. Therefore you must ensure that the
data you store in tables fits in the amount of memory you have available on your
system. Remember that any additional indexes or derived materialized views will
count against your memory budget.

If your dataset is too large to fit in memory, consider using an unmaterialized
[source](/sql/create-source) instead. This lets you defer materialization to
views derived from this source, which can aggregate or filter the data down to a
manageable size.

{{< version-changed v0.23.0 >}}
Tables no longer have a mandatory default [index](/overview/api-components/#indexes).
{{< /version-changed >}}

## Examples

### Creating a table

You can create a table `t` with the following statement:

```sql
CREATE TABLE t (a int, b text NOT NULL);
```

Once a table is created, you can inspect the table with various `SHOW` commands.

```sql
SHOW TABLES;
TABLES
------
t

SHOW COLUMNS IN t;
name       nullable  type
-------------------------
a          true      int4
b          false     text
```

## Related pages

- [`INSERT`](../insert)
- [`DROP TABLE`](../drop-table)
