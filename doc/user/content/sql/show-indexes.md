---
title: "SHOW INDEXES"
description: "SHOW INDEXES provides details about indexes built on a source, view, or materialized view"
menu:
  main:
    parent: commands
---

`SHOW INDEXES` provides details about indexes built on a source, view, or materialized view.

## Syntax

```mzsql
SHOW INDEXES [ FROM <schema_name> | ON <object_name> ]
[ IN CLUSTER <cluster_name> ]
[ LIKE <pattern> | WHERE <condition(s)> ]
;
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show indexes from the specified schema. Defaults to first resolvable schema in the search path if neither `ON <object_name>` nor `IN CLUSTER <cluster_name>` are specified. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**ON** <object_name>          | If specified, only show indexes for the specified object.
**IN CLUSTER** <cluster_name> | If specified, only show indexes from the specified cluster.
**LIKE** \<pattern\>          | If specified, only show indexes that match the pattern.
**WHERE** <condition(s)>      | If specified, only show indexes that match the condition(s).

## Details

### Output format

`SHOW INDEX`'s output is a table with the following structure:

```nofmt
name | on  | cluster | key
-----+-----+---------+----
 ... | ... | ...     | ...
```

Field | Meaning
------|--------
**name** | The name of the index.
**on** | The name of the table, source, or view the index belongs to.
**cluster** | The name of the [cluster](/concepts/clusters/) containing the index.
**key** | A text array describing the expressions in the index key.

## Examples

```mzsql
SHOW VIEWS;
```
```nofmt
          name
-------------------------
 my_nonmaterialized_view
 my_materialized_view
```

```mzsql
SHOW INDEXES ON my_materialized_view;
```
```nofmt
 name | on  | cluster | key
------+-----+---------+----
 ...  | ... | ...     | ...
```

## Related pages

- [`SHOW CREATE INDEX`](../show-create-index)
- [`DROP INDEX`](../drop-index)
