---
title: "TAIL"
description: "`TAIL` continually reports updates that occur to a source or view."
menu:
    main:
        parent: "sql"
---

`TAIL` continually reports updates that occur to a source or view.
For materialized sources or views this data only represents updates that occur after running the `TAIL` command.
For non-materialized sources or views, all updates are presented.

Tail will continue to run until cancelled, or until all updates the tailed item could undergo have been presented. The latter case may happen with static views (e.g. `SELECT true`), files without the `tail = true` modifier, or other settings in which a collection can cease to experience updates.

In order for a tail to be possible, all involved sources must be valid to read from at the chosen timestamp.
A tail at a given timestamp might not be possible if data it would report has already been compacted by Materialize.

## Syntax

{{< diagram "tail-stmt.svg" >}}

Field | Use
------|-----
_object&lowbar;name_ | The item you want to tail
_timestamp&lowbar;expression_ | The logical time to tail from onwards (either a number of milliseconds since the Unix epoch, or a `TIMESTAMP` or `TIMESTAMPTZ`).

## Details

### Output

`TAIL`'s output is the item's columns prepended with `timestamp` and `diff` columns.

Field | Represents
------|-----------
`timestamp` | Materialize's internal logical timestamp.
`diff` | Whether the record is an insert (`1`), delete (`-1`), or update (delete for old value, followed by insert of new value).
column values | The row's columns' values, each as its own column.

### AS OF

`AS OF` is the specific point in time to start reporting all events for a given `TAIL`. If you don't
use `AS OF`, Materialize will pick a timestamp itself.

### WITH SNAPSHOT or WITHOUT SNAPSHOT

By default, each TAIL is created with a `SNAPSHOT` which contains the results of the query at its `AS OF` timestamp.
Any further updates to these results are produced at the time when they occur. To only see results after the
`AS OF` timestamp, specify `WITHOUT SNAPSHOT`.

## Example

### Tailing to your terminal

In this example, we'll assume `some_materialized_view` has one `text` column.

```sql
COPY (TAIL some_materialized_view) TO STDOUT
```
```
1580000000000  1 insert_key
1580000000001  1 will_delete
1580000000003 -1 will_delete
1580000000005  1 will_update_old
1580000000007 -1 will_update_old
1580000000007  1 will_update_new
````

This represents:

- Inserting `insert_key`.
- Inserting and then deleting `will_delete`.
- Inserting `will_update_old`, and then updating it to `will_update_new`

If we wanted to see the updates that had occurred in the last 30 seconds, we could run:

```sql
TAIL some_materialized_view AS OF now() - '30s'::INTERVAL
```

### Tailing through a driver

`TAIL` produces rows similar to a `SELECT` statement, except that `TAIL` may never complete.
Many drivers buffer all results until a query is complete, and so will never return.
Instead, use [`COPY TO`](/sql/copy-to) which is unbuffered by drivers and so is suitable for streaming.
As long as your driver lets you send your own `COPY` statement to the running Materialize node, you can `TAIL` updates from Materialize anywhere you'd like.

```python
#!/usr/bin/env python3

import sys
import psycopg2

def main():

    dsn = 'postgresql://localhost:6875/materialize?sslmode=disable'
    conn = psycopg2.connect(dsn)

    with conn.cursor() as cursor:
        cursor.copy_expert("COPY (TAIL some_materialized_view) TO STDOUT", sys.stdout)

if __name__ == '__main__':
    main()
```

This will then stream the same output we saw above to `stdout`, though you could
obviously do whatever you like with the output from this point.

If your driver does support unbuffered result streaming, then there is no need to use `COPY TO`.
