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
_timestamp&lowbar;expression_ | The logical time at which the `TAIL` begins as a [`bigint`] representing milliseconds since the Unix epoch. See [`AS OF`](#as-of) below.

Supported `WITH` option values:

Option name | Value type | Default | Describes
------------|------------|---------|----------
`SNAPSHOT`  | `boolean`     | `true`  | Whether to emit a snapshot of the current state of the relation at the start of the operation. See [`SNAPSHOT`](#snapshot) below.
`PROGRESS`  | `boolean`     | `false` | Whether to include detailed progress information. See [`PROGRESS`](#progress) below.

## Details

### Output

`TAIL` emits a sequence of updates. Each row contains all of the columns of
the tailed relation, prepended with several additional columns that describe
the nature of the update:

<table>
<thead>
  <tr>
    <th>Column</th>
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

{{< warning >}}

Many PostgreSQL drivers wait for a query to complete before returning its
results. Since `TAIL` can run forever, naively executing a `TAIL` using your
driver's standard query API may never return.

Either use an API in your driver that does not buffer rows or use the
[`FETCH`](/sql/fetch) statement to fetch rows from a `TAIL` in batches.
See the [examples](#examples) for details.

{{< /warning >}}

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

The `AS OF` clause specifies the time at which a `TAIL` operation begins.
See [`SNAPSHOT`](#snapshot) below for details on what this means.

If you don't specify `AS OF` explicitly, Materialize will pick a timestamp
automatically:

  - If the tailed relation is [materialized](/overview/api-components/#indexes),
    Materialize picks the latest time for which results are computed.
  - If the tailed relation is not materialized, Materialize picks time `0`.

A given timestamp will be rejected if data it would report has already been
compacted by Materialize. See the
[`--logical-compaction-window`](/cli/#compaction-window) command-line option for
details on Materialize's compaction policy.

### `SNAPSHOT`

By default, a `TAIL` begins by emitting a snapshot of the tailed relation, which
consists of a series of updates describing the contents of the relation at its
[`AS OF`](#as-of) timestamp. After the snapshot, `TAIL` emits further updates as
they occur.

For updates in the snapshot, the `timestamp` field will be fast-forwarded to the
`AS OF` timestamp. For example, `TAIL ... AS OF 21` would present an insert that
occured at time 15 as if it occurred at time 21.

To see only updates after the `AS OF` timestamp, specify `WITH (SNAPSHOT =
false)`.

### `PROGRESS`

If the `PROGRESS` option is specified via `WITH (PROGRESS)`, an additional
`progressed` column appears in the output.
It is `false` if there may be more rows with the same timestamp.
It is `true` if no more timestamps will appear that are strictly less than the
timestamp.
All further columns after `progressed` will be `NULL` in the `true` case.

Intuitively, progress messages communicate that no updates have occurred in a
given time window. Without explicit progress messages, it is impossible to
distinguish between a stall in Materialize and a legimate period of no updates.

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
`2` or `3`—but that there may be more data arriving at timestamp `4`.

## Examples

`TAIL` produces rows similar to a `SELECT` statement, except that `TAIL` may never complete.
Many drivers buffer all results until a query is complete, and so will never return.
Below are the recommended ways to work around this.

### Tailing with `FETCH`

The recommended way to use `TAIL` is with [`DECLARE`](/sql/declare) and [`FETCH`](/sql/fetch).
These must be used within a transaction.
This allows you to limit the number of rows and the time window of your requests. First, declare a `TAIL` cursor:

```sql
BEGIN;
DECLARE c CURSOR FOR TAIL t;
````

Now use [`FETCH`](/sql/fetch) in a loop to retrieve some number of rows within a time window:

```sql
FETCH ALL c;
```

That will retrieve all of the rows that are currently available.
If there are no rows available, it will wait until there are some ready and return those.
A `timeout` can be used to specify a window in which to wait for rows. This will return up to the specified count (or `ALL`) of rows that are ready within the timeout. To retrieve up to 100 rows that are available in at most the next `1s`:

```sql
FETCH 100 c WITH (timeout='1s');
```

To retrieve all available rows available over the next `1s`:

```sql
FETCH ALL c WITH (timeout='1s');
```

A `0s` timeout can be used to return rows that are available now without waiting:

```sql
FETCH ALL c WITH (timeout='0s');
```

#### `FETCH` with Python and psycopg2

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("DECLARE c CURSOR FOR TAIL v")
    while True:
        cur.execute("FETCH ALL c")
        for row in cur:
            print(row)
```

#### Streaming with Python and psycopg3

{{< warning >}}
psycopg3 is not yet stable.
The example here could break if their API changes.
{{< /warning >}}

Although psycopg3 can function identically as the psycopg2 example above,
it also has a `stream` feature where rows are not buffered and we can thus use `TAIL` directly.

```python
#!/usr/bin/env python3

import psycopg3
import sys

dsn = "postgresql://materialize@localhost:6875/materialize?sslmode=disable"
conn = psycopg3.connect(dsn)

conn = psycopg3.connect(dsn)
with conn.cursor() as cur:
    for row in cur.stream("TAIL t"):
        print(row)
```

#### `FETCH` with C# and Npgsql

```csharp
var txn = conn.BeginTransaction();
new NpgsqlCommand("DECLARE c CURSOR FOR TAIL t", conn, txn).ExecuteNonQuery();
while (true)
{
    using (var cmd = new NpgsqlCommand("FETCH ALL c", conn, txn))
    using (var reader  = cmd.ExecuteReader())
    {
        while (reader.Read())
        {
            // More columns available with reader[3], etc.
            Console.WriteLine("ts: " + reader[0] + ", diff: " + reader[1] + ", column 1: " + reader[2]);
        }
    }
}
```

### Interactive `TAIL`

If you want to use `TAIL` from an interactive SQL session (for example in `psql`), wrap the query in `COPY`.

```sql
COPY (TAIL t) TO STDOUT
```

[`bigint`]: /sql/types/bigint
[`numeric`]: /sql/types/numeric
