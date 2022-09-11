---
title: "SHOW INDEXES"
description: "SHOW INDEXES provides details about indexes built on a source, view, or materialized view"
menu:
  main:
    parent: commands
---

`SHOW INDEXES` provides details about indexes built on a source, view, or materialized view.

## Syntax

{{< diagram "show-indexes.svg" >}}

Field | Use
------|-----
_on&lowbar;name_ | The name of the object whose indexes you want to show. If omitted, all indexes in the cluster are shown.
_cluster&lowbar;name_ | The cluster to show indexes from. If omitted, indexes from all clusters are shown.

## Details

### Output format

`SHOW INDEX`'s output is a table with the following structure:

```nofmt
cluster | on_name | key_name | seq_in_index | column_name | expression | nullable | enabled
--------+---------+----------+--------------+-------------+------------+----------+--------
 ...    | ...     | ...      | ...          | ...         | ...        | ...      | ...
```

Field | Meaning
------|--------
**cluster** | The name of the [cluster](/overview/key-concepts/#clusters) containing the index.
**on_name** | The name of the table, source, or view the index belongs to.
**key_name** | The name of the index.
**seq_in_index** | The column's position in the index.
**column_name** | The indexed column.
**expression** | An expression used to generate the column in the index.
**null** | Is the column nullable?

## Examples

```sql
SHOW VIEWS;
```
```nofmt
          name           | type
-------------------------+------
 my_nonmaterialized_view | user
 my_materialized_view    | user
```

```sql
SHOW INDEXES FROM my_materialized_view;
```
```nofmt
 on_name | key_name | seq_in_index | column_name | expression | nullable | enabled
---------+----------+--------------+-------------+------------+----------+--------
 ...     | ...      | ...          | ...         | ...        | ...      | ...
```

## Related pages

- [`SHOW CREATE INDEX`](../show-create-index)
- [`DROP INDEX`](../drop-index)
