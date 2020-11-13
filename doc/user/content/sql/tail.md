---
title: "TAIL"
description: "`TAIL` streams updates from a relation as they occur."
menu:
  main:
    parent: "sql"
---

`TAIL` streams updates from a source, table, or view as they occur.

## Conceptual framework

The `TAIL` statement is a more general form of a [`SELECT`](/sql/select)
statement. While a `SELECT` statement computes a relation at a moment in time, a
tail operation computes how a relation *changes* over time.

Fundamentally, `TAIL` produces a sequence of updates. An update describes either
the insertion or deletion of a row to the relation at a specific time. Taken
together, the updates describes the complete set of changes to a relation, in
order, while the `TAIL` is active.

Clients can use `TAIL` to:

  - Power event processors that react to every change to a relation.
  - Replicate the complete history of a relation while the `TAIL` is active.

## Syntax

{{< diagram "tail-stmt.svg" >}}

Field | Use
------|-----
_object&lowbar;name_ | The name of the source, table, or view that you want to tail.
_timestamp&lowbar;expression_ | The logical time to tail from onwards, either as a [`bigint`] representing milliseconds since the Unix epoch, or as a [`timestamp`] or [`timestamptz`].

Supported `WITH` option values:

Option name | Value type | Default | Describes
------------|------------|---------|----------
`SNAPSHOT`  | `bool`     | `true`  | Whether to emit a snapshot of the current state of the relation at the start of the operation. See [`SNAPSHOT`](#snapshot) below.
`PROGRESS`  | `bool`     | `false` | Whether to include detailed progress information. See [`PROGRESS`](#progress) below.

## Details

### Output

`TAIL` emits a sequence of updates. Each row contains all of the columns of
the tailed relation, prepended with several additional columns that describe
the nature of the update:

<table>
<thead>
  <tr>
    <th>Field</th>
    <th>Type</th>
    <th>Represents</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td><code>timestamp</code></td>
    <td><code>numeric</code></td>
    <td>
      Materialize's internal logical timestamp. This will never be less than any
      timestamp previously emitted by the same <code>TAIL</code> operation.
    </td>
  </tr>
  <tr>
    <td><code>progressed</code></td>
    <td><code>boolean</code></td>
    <td>
      <p>
        <em>
          This column is only present if the
          <a href="#progress"><code>PROGRESS</code></a> option is specified.
        </em>
      </p>
      If <code>true</code>, indicates that the <code>TAIL</code> will not emit
      additional records at times strictly less than <code>timestamp</code>. See
      <a href="#progress"><code>PROGRESS</code></a> below.
    </td>
  </tr>
  <tr>
    <td><code>diff</code></td>
    <td><code>bigint</code></td>
    <td>
      The change in frequency of the row. A positive number indicates that
      <code>diff</code> copies of the row were inserted, while a negative
      number indicates that <code>|diff|</code> copies of the row were deleted.
    </td>
  </tr>
  <tr>
    <td>Column 1</td>
    <td>Varies</td>
    <td rowspan="3" style="vertical-align: middle; border-left: 1px solid #ccc">
        The columns from the tailed relation, each as its own column,
        representing the data that was inserted into or deleted from the
        relation.
    </td>
  </tr>
  <tr>
    <td colspan="2" style="text-align: center">...</td>
  </tr>
  <tr>
    <td>Column <em>N</em></td>
    <td>Varies</td>
  </tr>
</tbody>
</table>

`TAIL` will continue to run until cancelled, or until all updates the tailed
item could undergo have been presented. The latter case typically occurs when
tailing constant views (e.g. `CREATE VIEW v AS SELECT 1`) or
[file sources](/sql/create-source/text-file) that were created in non-tailing
mode (`tail = false`).

{{< version-changed v0.5.1 >}}
The timestamp and diff information moved to leading, well-typed columns.
Previously the timestamp and diff information was encoded in a human-readable
string in a trailing [`text`](/sql/types/text) column.
{{</ version-changed >}}

{{< version-changed v0.5.1 >}}
`TAIL` sends rows to the client normally, i.e., as if they were sent by a
[`SELECT`](/sql/select) statement. Previously `TAIL` was implicitly wrapped in
a [`COPY TO`](/sql/copy-to) statement.
{{</ version-changed >}}

{{< version-changed v0.5.2 >}}
`TAIL` is now guaranteed to send timestamps in non-decreasing order.
{{</ version-changed >}}

{{< version-changed v0.5.2 >}}
Syntax has changed. `WITH SNAPSHOT` is now `WITH (SNAPSHOT)`.
`WITHOUT SNAPSHOT` is now `WITH (SNAPSHOT = false)`.
{{</ version-changed >}}

{{< version-changed v0.5.2 >}}
The [`PROGRESS`](#progress) option has been added.
{{</ version-changed >}}

### `AS OF`

The `AS OF` clauses specifies a point in time to start reporting all events for
a given `TAIL` operation. If you don't use `AS OF`, Materialize will pick a
timestamp automatically according to the following rules:

  - If the tailed relation is materialized, pick the latest time for which
    results are computed.
  - If the tailed relation is unmaterialized, pick time `0`.

All involved sources must be valid to read from at the chosen timestamp. A given
timestamp will be rejected if data it would report has already been compacted by
Materialize. See the [`--logical-compaction-window`](/cli/#compaction-window)
command-line option for details on Materialize's compaction policy.

### `SNAPSHOT`

By default, each `TAIL` begins by emitting a snapshot of the tailed relation,
which contains the contents of the relation at its `AS OF` timestamp. Any further
updates to these results are produced at the time when they occur.

To only see results after the [`AS OF`](#as-of) timestamp, specify `WITH
(SNAPSHOT = false)`.

### `PROGRESS`

If the `PROGRESS` option is specified via `WITH (PROGRESS)`, an additional
`progressed` column appears in the output.
It is `false` if there may be more rows with the same timestamp.
It is `true` if no more timestamps will appear that are strictly less than the
timestamp.
All further columns after `progressed` will be `NULL` in the `true` case.

Intuitively, progress messages are used to explicitly communicate that no
updates have occurred in a given time window. Without explicit progress
messages, it is impossible to distinguish between a stall in Materialize and
a legimate period of no updates.

Not all timestamps that appear will have a corresponding `progressed` row.
For example, the following is a valid sequence of updates:

`timestamp` | `progressed` | `diff` | `column1`
------------|--------------|--------|----------------
1           | `false`      | 1      | data
1           | `false`      | 1      | more data
2           | `false`      | 1      | even more data
4           | `true`       | `NULL` | `NULL`

Notice how Materialize did not emit explicit progress messages for timestamps
`1`, `2`, or `3`. The receipt of the update at timestamp `2` implies that there
are no more updates for timestamp `1`, because timestamps are always presented
in non-decreasing order. The receipt of the explicit progress message at
timestamp `4` implies that there are no more updates for either timestamp
`2` or `3`.

## Example

### Tailing via the `psql` command-line client

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

If you want to see the updates that had occurred in the last 30 seconds, you could run:

```sql
TAIL some_materialized_view AS OF now() - '30s'::INTERVAL
```

If you want timestamp completion messages:

```sql
COPY (TAIL some_materialized_view WITH (PROGRESS)) TO STDOUT
```
```
1580000000000 f  1 insert_key
1580000000001 t \N \N
1580000000001 f  1 will_delete
1580000000003 f -1 will_delete
1580000000005 t \N \N
1580000000005 f  1 will_update_old
1580000000006 t \N \N
1580000000007 t \N \N
1580000000007 f -1 will_update_old
1580000000007 f  1 will_update_new
1580000000009 t \N \N
````

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

[`bigint`]: /sql/types/bigint
[`numeric`]: /sql/types/numeric
