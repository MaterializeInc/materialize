---
title: "TAIL"
description: "`TAIL` streams updates that occur to a materialized view."
menu:
    main:
        parent: "sql"
---

`TAIL` streams updates that occur to a materialized view. This data only
represents updates that occur after running the `TAIL` command.

## Syntax

{{< diagram "tail.html" >}}

Field | Use
------|-----
_object&lowbar;name_ | The item you want to tail; the item must be materialized (i.e. [materialized view](../create-materialized-view) or [materialized source](../create-source))

## Details

### Output

`TAIL`'s output is:

```shell
[tab-separated column values] Diff: [diff value] at [logical timestamp]
```

Field | Represents
------|-----------
`tab-separated column values` | The row's columns' values separated by tab characters.
`diff value` | Whether the record is an insert (`1`), delete (`-1`), or update (delete for old value, followed by insert of new value).
`logical timestamp` | Materialize's internal logical timestamp.

## Example

In this example, we'll assume `some_materialized_view` has one `text` column.

```sql
TAIL some_materialized_view
```
```
insert_key   Diff: 1 at 1585752182327
will_delete  Diff: 1 at 1585752197827
will_delete  Diff: -1 at 1585752335626
will_update_old  Diff: 1 at 1585752351626
will_update_old  Diff: -1 at 1585752356422
will_update_new  Diff: 1 at 1585752356422
````

This represents:

- Inserting `insert_key`.
- Inserting and then deleting `will_delete`.
- Inserting `will_update_old`, and then updating it to `will_update_new`
