---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-indexes/
complexity: intermediate
description: SHOW INDEXES provides details about indexes built on a source, view,
  or materialized view
doc_type: reference
keywords:
- 'ON'
- FROM
- WHERE
- SHOW INDEXES
- IN CLUSTER
- LIKE
product_area: Indexes
status: stable
title: SHOW INDEXES
---

# SHOW INDEXES

## Purpose
SHOW INDEXES provides details about indexes built on a source, view, or materialized view

If you need to understand the syntax and options for this command, you're in the right place.


SHOW INDEXES provides details about indexes built on a source, view, or materialized view



`SHOW INDEXES` provides details about indexes built on a source, view, or materialized view.

## Syntax

This section covers syntax.

```mzsql
SHOW INDEXES [ FROM <schema_name> | ON <object_name> ]
[ IN CLUSTER <cluster_name> ]
[ LIKE <pattern> | WHERE <condition(s)> ]
;
```text

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show indexes from the specified schema. Defaults to first resolvable schema in the search path if neither `ON <object_name>` nor `IN CLUSTER <cluster_name>` are specified. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**ON** <object_name>          | If specified, only show indexes for the specified object.
**IN CLUSTER** <cluster_name> | If specified, only show indexes from the specified cluster.
**LIKE** \<pattern\>          | If specified, only show indexes that match the pattern.
**WHERE** <condition(s)>      | If specified, only show indexes that match the condition(s).

## Details

This section covers details.

### Output format

`SHOW INDEX`'s output is a table with the following structure:

```nofmt
name | on  | cluster | key
-----+-----+---------+----
 ... | ... | ...     | ...
```text

Field | Meaning
------|--------
**name** | The name of the index.
**on** | The name of the table, source, or view the index belongs to.
**cluster** | The name of the [cluster](/concepts/clusters/) containing the index.
**key** | A text array describing the expressions in the index key.

## Examples

This section covers examples.

```mzsql
SHOW VIEWS;
```text
```nofmt
          name
-------------------------
 my_nonmaterialized_view
 my_materialized_view
```text

```mzsql
SHOW INDEXES ON my_materialized_view;
```text
```nofmt
 name | on  | cluster | key
------+-----+---------+----
 ...  | ... | ...     | ...
```

## Related pages

- [`SHOW CREATE INDEX`](../show-create-index)
- [`DROP INDEX`](../drop-index)

