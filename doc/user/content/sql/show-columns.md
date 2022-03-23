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

```sql
SHOW COLUMNS FROM item_ref [ LIKE 'pattern' | WHERE expr]
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "show-columns.svg" >}}

</details>
<br/>

Field | Use
------|-----
_item&lowbar;ref_ | The name of the item whose columns you want to view. These can be [sources](../create-source) or views (either [materialized](../create-materialized-view) or [non-materialized](../create-view)).

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


{{< version-changed v0.4.2 >}}
Rows are sorted by the order in which the fields are defined in the targeted
source, view, or table. Prior versions did not guarantee any particular ordering.
{{< /version-changed >}}

{{< version-changed v0.5.0 >}}
The `name`, `nullable`, and `type` columns are renamed to `name`, `nullable`,
and `type`, respectively.
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
  name  | nullable | type
---------+----------+------
 column1 | NO       | int4
 column2 | YES      | text
```

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
