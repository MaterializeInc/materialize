---
title: "SHOW INDEX"
description: "SHOW INDEX provides details about a materialized view's indexes"
menu:
  main:
    parent: commands
aliases:
    - /sql/show-indexes
    - /sql/show-keys
---

`SHOW INDEX` provides details about a materialized view's indexes.

## Syntax

{{< diagram "show-index.svg" >}}

Field | Use
------|-----
_on&lowbar;name_ | The name of the object whose indexes you want to show. This can be the name of a table, source, or view.

## Details

### Output format

`SHOW INDEX`'s output is a table, with this structure:

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

### Determine which views have indexes

[`SHOW FULL VIEWS`](../show-views/#show-details-about-views) includes details about which views have indexes, i.e. are materialized.

## Examples

```sql
SHOW FULL VIEWS;
```
```nofmt
          name           | type | materialized
-------------------------+------+ -------------
 my_nonmaterialized_view | user | f
 my_materialized_view    | user | t
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

- [`SHOW VIEWS`](../show-views)
- [`DROP INDEX`](../drop-index)
