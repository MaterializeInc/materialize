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

{{< diagram "drop-index.html" >}}

Field | Use
------|-----
_view&lowbar;name_ | The name of the index you want to remove.

## Details

### Indexes and materialized views

If you drop the only index on a materialized view, it becomes a non-materialized
view. For more details on non-materialized views, see [`CREATE
VIEW`](../create-view).

### Primary indexes

By default, materialized views only have one index, which we call the "primary
index," and stores the result set of the materialized view's embedded `SELECT`
statement. You can identify the primary index by its name, which follows the
format `<view name>_primary_idx`.

If you drop the primary index and want to recreate it:

1. Retrieve the view's embedded `SELECT` statement with [`SHOW CREATE
   VIEW`](../show-create-view).
1. Re-create the view as a materialized view with [`CREATE OR REPLACE
   MATERIALIZED VIEW`](../create-materialized-view).

Alternatively, you can also [`CREATE INDEX`](../create-index) to materialize any
view.

## Examples

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
```sql
DROP INDEX materialize.public.q01_geo_idx;
```

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
