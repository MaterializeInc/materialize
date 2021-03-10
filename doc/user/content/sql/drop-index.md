---
title: "DROP INDEX"
description: "DROP INDEX removes an index"
menu:
  main:
    parent: 'sql'
---

`DROP INDEX` removes an index from a materialized view. (Non-materialized views
do not have any indexes.)

## Syntax

{{< diagram "drop-index.svg" >}}

Field | Use
------|-----
`IF EXISTS` | Do not return an error if the named index doesn't exist.
_index&lowbar;name_ | The name of the index you want to remove.
`CASCADE` | Remove the index and its dependent objects.
`RESTRICT` |  Remove the index, converting the materialized view to a non-materialized view. _(Default.)_

## Details

### Indexes and materialized views

If you drop the only index on a materialized view, it becomes a non-materialized
view. For more details on non-materialized views, see [`CREATE
VIEW`](../create-view).

### Primary indexes

By default, materialized views only have one index, which we call the "primary
index," and which stores the result set of the materialized view's embedded `SELECT`
statement. You can identify the primary index by its name, which follows the
format `<view name>_primary_idx`.

This primary index doesn't follow the constraints for typical for primary indexes in traditional databases; it is simply the first index created on a view. However, if you remove the primary index, you will not be able to query any columns not included in another index for the materialized view.

If you remove the primary index and want to recreate it:

1. Retrieve the view's embedded `SELECT` statement with [`SHOW CREATE
   VIEW`](../show-create-view).
1. Re-create the view as a materialized view with [`CREATE OR REPLACE
   MATERIALIZED VIEW`](../create-materialized-view).

Alternatively, you can also [`CREATE INDEX`](../create-index) to materialize any
view.

Removing the only index on a materialized view does not remove the view entirely; it merely transforms it to a non-materialized view.

## Examples

### Remove an index with no dependent objects

```sql
SHOW VIEWS;
```
```nofmt
+-----------------------------------+
| VIEWS                             |
|-----------------------------------|
| ...                               |
| q01                               |
+-----------------------------------+
```
```sql
SHOW INDEXES FROM q01;
```
```nofmt
+------------------------+--------------------------------+-----
| View                   | Key_name                       | ...
|------------------------+--------------------------------+----
| materialize.public.q01 | materialize.public.q01_geo_idx | ...
+------------------------+--------------------------------+-----
```

You can use the unqualified index name (`q01_geo_idx`) rather the fully qualified name (`materialize.public.q01_geo_idx`).

```sql
DROP INDEX q01_geo_idx;
```

### Remove an index with dependent objects

```sql
SHOW VIEWS;
```
```nofmt
+-----------------------------------+
| VIEWS                             |
|-----------------------------------|
| ...                               |
| q01                               |
+-----------------------------------+
```
```sql
SHOW INDEXES FROM q01;
```
```nofmt
+------------------------+--------------------------------+-----
| View                   | Key_name                       | ...
|------------------------+--------------------------------+----
| materialize.public.q01 | materialize.public.q01_geo_idx | ...
+------------------------+--------------------------------+-----
```
You can use the unqualified index name (`q01_geo_idx`) rather the fully qualified name (`materialize.public.q01_geo_idx`).

```sql
DROP INDEX q01_geo_idx CASCADE;
```

### Remove an index only if it has no dependent objects

```sql
DROP INDEX q01_geo_idx;
```

```sql
DROP INDEX q01_geo_idx RESTRICT;
```

### Do not issue an error if attempting to remove a nonexistent index

```sql
DROP INDEX IF EXISTS q01_geo_idx;
```

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
