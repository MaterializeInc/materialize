---
title: "SHOW INDEX"
description: "SHOW INDEX provides details about a materialized view's indexes"
menu:
  main:
    parent: 'sql'
aliases:
    - /docs/sql/show-indexes
    - /docs/sql/show-keys
---

`SHOW INDEX` provides details about a materialized view's indexes.

## Syntax

{{< diagram "show-index.html" >}}

Field | Use
------|-----
_view&lowbar;name_ | The name of the view whose indexes you want to show.

## Output format

`SHOW INDEX`'s output is a table, with this structure:

```nofmt
+------+----------+-------------+------------+------+--------------+
| View | Key_name | Column_name | Expression | Null | Seq_in_index |
+------+----------+-------------+------------+------+--------------+
| ... | ...       | ...         | ...        | ...  | ...          |
+------+----------+-------------+------------+------+--------------+
```

Field | Meaning
------|--------
**View** | The name of the view the index belongs to.
**Key_name** | The name of the index.
**Column_name** | The indexed column.
**Expression** | An expression used to generate the column in the index.
**Null** | Is the column nullable?
**Seq_in_index** | The column's position in the index.

## Examples

```sql
SHOW VIEWS;
```
```nofmt
+-----------------------------------+
| VIEWS                             |
|-----------------------------------|
| ...                               |
| q01                               |
+-----------------------------------+
```
```sql
SHOW INDEXES FROM q01;
```
```
+------+----------+-------------+------------+------+--------------+
| View | Key_name | Column_name | Expression | Null | Seq_in_index |
+------+----------+-------------+------------+------+--------------+
| ... | ...       | ...         | ...        | ...  | ..           |
+------+----------+-------------+------------+------+--------------+
```

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`DROP INDEX`](../drop-index)
