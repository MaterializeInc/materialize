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

`TAIL`'s output is:

```shell
[tab-separated column values] Diff: [diff value] at [logical timestamp]
```

Field | Represents
------|-----------
`tab-separated column values` | The row's columns' values separated by tab characters.
`diff value` | Whether the record is an insert (`1`), delete (`-1`), or update (delete for old value, followed by insert of new value).
`logical timestamp` | Materialize's internal logical timestamp.

### AS OF

`AS OF` is the specific point in time to start reporting all events for a given `TAIL`. If you don't
use `AS OF`, Materialize will pick a timestamp itself.

### WITHOUT SNAPSHOT

By default, each `TAIL` is created with a `SNAPSHOT`: a compacted version of updates up until the moment of the
`TAIL`'s creation. To only see events after the `TAIL` was created, specify `WITHOUT SNAPSHOT` instead.

## Example

### Tailing to your terminal

In this example, we'll assume `some_materialized_view` has one `text` column.

```sql
TAIL some_materialized_view
```
```
insert_key   Diff: 1 at 1580000000000
will_delete  Diff: 1 at 1580000000001
will_delete  Diff: -1 at 1580000000003
will_update_old  Diff: 1 at 1580000000005
will_update_old  Diff: -1 at 1580000000007
will_update_new  Diff: 1 at 1580000000007
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

In terms of the `pgwire` protocol, `TAIL` is handled as a non-standard `COPY TO`
statement. As long as your driver lets you send your own `COPY` statement to the
running Materialize node, you can `TAIL` updates from Materialize anywhere you'd
like.

```python
#!/usr/bin/env python3

import sys
import psycopg2

def main():

    dsn = 'postgresql://localhost:6875/materialize?sslmode=disable'
    conn = psycopg2.connect(dsn)

    with conn.cursor() as cursor:
        cursor.copy_expert("TAIL some_materialized_view", sys.stdout)

if __name__ == '__main__':
    main()
```

This will then stream the same output we saw above to `stdout`, though you could
obviously do whatever you like with the output from this point.
