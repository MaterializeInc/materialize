---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-columns/
complexity: intermediate
description: '`SHOW COLUMNS` lists the columns available for an object.'
doc_type: reference
keywords:
- LIKE
- WHERE
- name
- type
- SHOW COLUMNS
- nullable
product_area: Indexes
status: stable
title: SHOW COLUMNS
---

# SHOW COLUMNS

## Purpose
`SHOW COLUMNS` lists the columns available for an object.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW COLUMNS` lists the columns available for an object.



`SHOW COLUMNS` lists the columns available for an object. This can be a source,
subsource, materialized view, view, or table.

## Syntax

This section covers syntax.

```sql
SHOW COLUMNS FROM <object_name>
[LIKE <pattern> | WHERE <condition(s)>]
;
```text

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show columns that match the pattern.
**WHERE** <condition(s)>      | If specified, only show columns that match the condition(s).

## Details

This section covers details.

### Output format

`SHOW COLUMNS`'s output is a table, with this structure:

```nofmt
+---------+------------+--------+
| name    | nullable   | type   |
|---------+------------+--------|
| ...     | ...        | ...    |
+---------+------------+--------+
```text

Field | Meaning
------|--------
**name** | The name of the column
**nullable** | Does the column accept `null` values?
**type** | The column's [type](../types)

Rows are sorted by the order in which the fields are defined in the targeted
object.

## Examples

This section covers examples.

```mzsql
SHOW SOURCES;
```text
```nofmt
   name
----------
my_sources
```text
```mzsql
SHOW COLUMNS FROM my_source;
```text
```nofmt
  name  | nullable | type
---------+----------+------
 column1 | f       | int4
 column2 | f       | text
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing `item_ref`.


## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)

