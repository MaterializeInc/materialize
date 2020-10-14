---
title: "SHOW COLUMNS"
description: "`SHOW COLUMNS` lists the columns available from an item."
menu:
  main:
    parent: 'sql'
aliases:
    - /sql/show-column
---

`SHOW COLUMNS` lists the columns available from an item&mdash;either sources, materialized views, or non-materialized views.

## Syntax

{{< diagram "show-columns.svg" >}}

Field | Use
------|-----
_item&lowbar;ref_ | The name of the item whose columns you want to view. These can be [sources](../create-source) or views (either [materialized](../create-materialized-view) or [non-materialized](../create-view)).

## Details

### Output format

`SHOW COLUMNS`'s output is a table, with this structure:

```nofmt
+---------+------------+--------+
| field   | nullable   | type   |
|---------+------------+--------|
| ...     | ...        | ...    |
+---------+------------+--------+
```

Field | Meaning
------|--------
**field** | The name of the column
**nullable** | Does the column accept `null` values?
**type** | The column's [type](../types)


{{< version-changed v0.4.2 >}}
Rows are sorted by the order in which the fields are defined in the targeted
source, view, or table. Prior versions did not guarantee any particular ordering.
{{< /version-changed >}}

{{< version-changed v0.5.0 >}}
The `Field`, `Nullable`, and `Type` columns are renamed to lowercase,
i.e., `field`, `nullable`, and `type`, respectively.
{{< /version-changed >}}

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
  field  | nullable | type
---------+----------+------
 column1 | NO       | int4
 column2 | YES      | text
```

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
