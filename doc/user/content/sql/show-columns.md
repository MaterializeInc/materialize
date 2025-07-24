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
subsource, materialized view, view, or table.

## Syntax

```sql
SHOW COLUMNS FROM <object_name>
[LIKE <pattern> | WHERE <condition(s)>]
```

Option                        | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show columns that match the pattern.
**WHERE** <condition(s)>      | If specified, only show columns that match the condition(s).

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

```mzsql
SHOW SOURCES;
```
```nofmt
   name
----------
my_sources
```
```mzsql
SHOW COLUMNS FROM my_source;
```
```nofmt
  name  | nullable | type
---------+----------+------
 column1 | f       | int4
 column2 | f       | text
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/show-columns.md" >}}

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
