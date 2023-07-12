---
title: "SHOW COLUMNS"
description: "`SHOW COLUMNS` lists the columns available for an object."
menu:
  main:
    parent: commands
aliases:
    - /sql/show-column
---

`SHOW COLUMNS` lists the columns available for an object. This can be a source,
subsource, sink, materialized view, view, or table.

## Syntax

{{< diagram "show-columns.svg" >}}

Field | Use
------|-----
_item&lowbar;ref_ | The name of the object whose columns you want to view. This can be a source, subsource, sink, materialized view, view, or table.

## Details

### Output format

`SHOW COLUMNS`'s output is a table, with this structure:

```nofmt
+---------+------------+--------+
| name    | nullable   | type   |
|---------+------------+--------|
| ...     | ...        | ...    |
+---------+------------+--------+
```

Field | Meaning
------|--------
**name** | The name of the column
**nullable** | Does the column accept `null` values?
**type** | The column's [type](../types)

Rows are sorted by the order in which the fields are defined in the targeted
object.

## Examples

```sql
SHOW SOURCES;
```
```nofmt
   name
----------
my_sources
```
```sql
SHOW COLUMNS FROM my_source;
```
```nofmt
  name  | nullable | type
---------+----------+------
 column1 | f       | int4
 column2 | f       | text
```

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing `item_ref`.

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
