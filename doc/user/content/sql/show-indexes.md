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
name | on  | cluster | key
-----+-----+---------+----
 ... | ... | ...     | ...
```

Field | Meaning
------|--------
**name** | The name of the index.
**on** | The name of the table, source, or view the index belongs to.
**cluster** | The name of the [cluster](/overview/key-concepts/#clusters) containing the index.
**key** | A text array describing the expressions in the index key.

## Examples

```sql
SHOW VIEWS;
```
```nofmt
          name
-------------------------
 my_nonmaterialized_view
 my_materialized_view
```

```sql
SHOW INDEXES FROM my_materialized_view;
```
```nofmt
 name | on  | cluster | key
------+-----+---------+----
 ...  | ... | ...     | ...
```

## Related pages

- [`SHOW CREATE INDEX`](../show-create-index)
- [`DROP INDEX`](../drop-index)
