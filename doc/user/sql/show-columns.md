---
title: "SHOW COLUMNS"
description: "`SHOW COLUMNS` lists the columns available from an item."
menu:
  main:
    parent: 'sql'
aliases:
    - /docs/sql/show-column
---

`SHOW COLUMNS` lists the columns available from an item&mdash;either sources, materialized views, or non-materialized views.

## Syntax

{{< diagram "show-columns.html" >}}

Field | Use
------|-----
_item&lowbar;ref_ | The name of the item whose columns you want to view. These can be [sources](../create-sources) or views (either [materialized](../create-materialized-view) or [non-materialized](../create-view)).

## Details

### Output format

`SHOW COLUMN`'s output is a table, with this structure:

```nofmt
+---------+------------+--------+
| Field   | Nullable   | Type   |
|---------+------------+--------|
| ...     | ...        | ...    |
+---------+------------+--------+
```

Field | Meaning
------|--------
**Field** | The name of the column
**Nullable** | Does the column accept `null` values?
**Type** | The column's [type](../types)

## Examples

```sql
SHOW SOURCES;
```
```nofmt
 SOURCES
----------
my_sources
```
```sql
SHOW COLUMNS FROM my_source;
```
```nofmt
  Field  | Nullable | Type
---------+----------+------
 column1 | NO       | int4
 column2 | YES      | text
```

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
