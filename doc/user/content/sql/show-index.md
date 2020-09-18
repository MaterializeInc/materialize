---
title: "SHOW INDEX"
description: "SHOW INDEX provides details about a materialized view's indexes"
menu:
  main:
    parent: 'sql'
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
 On_name | Key_name | Column_name | Expression | Null | Seq_in_index
---------+----------+-------------+------------+------+--------------
 ...     | ...      | ...         | ...        | ...  | ...
```

Field | Meaning
------|--------
**On_name** | The name of the table, source, or view the index belongs to.
**Key_name** | The name of the index.
**Column_name** | The indexed column.
**Expression** | An expression used to generate the column in the index.
**Null** | Is the column nullable?
**Seq_in_index** | The column's position in the index.

### Determine which views have indexes

[`SHOW FULL VIEWS`](../show-views/#show-details-about-views) includes details about which views have indexes, i.e. are materialized.

## Examples

```sql
SHOW FULL VIEWS;
```
```nofmt
          VIEWS          | TYPE | QUERYABLE | MATERIALIZED
-------------------------+------+-----------+--------------
 my_nonmaterialized_view | USER | t         | f
 my_materialized_view    | USER | t         | t
```
```sql
SHOW INDEXES FROM my_materialized_view;
```
```nofmt
 On_name | Key_name | Column_name | Expression | Null | Seq_in_index
---------+----------+-------------+------------+------+--------------
 ...     | ...      | ...         | ...        | ...  | ...
```

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`DROP INDEX`](../drop-index)
