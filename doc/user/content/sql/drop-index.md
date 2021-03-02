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
_view&lowbar;name_ | The name of the index you want to remove.
`CASCADE` | Automatically removes any objects that depend on the index, as well as the index.
`IF EXISTS`  |  Do not issue an error if the named index doesn't exist.
`RESTRICT`  |  Remove the index, converting the materialized view to a non-materialized view. _(Default)._

## Details

### Indexes and materialized views

If you drop the only index on a materialized view, it becomes a non-materialized
view. For more details on non-materialized views, see [`CREATE
VIEW`](../create-view).

### Primary indexes

By default, materialized views only have one index, called the "primary
index," which stores the result set of the materialized view's embedded `SELECT`
statement. You can identify the primary index by its name, which follows the
format `<view name>_primary_idx`.

If you drop the primary index and want to recreate it:

1. Retrieve the view's embedded `SELECT` statement with [`SHOW CREATE
   VIEW`](../show-create-view).
1. Re-create the view as a materialized view with [`CREATE OR REPLACE
   MATERIALIZED VIEW`](../create-materialized-view).

Alternatively, you can also [`CREATE INDEX`](../create-index) to materialize any
view.

Dropping the only index on a materialized view does not remove the view entirely; it merely transforms it to a non-materialized view.

## Examples

### Drop indexes (no dependencies)

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

### Drop indexes and dependent objects

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
DROP INDEX materialize.public.q01_geo_idx CASCADE;
```

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
