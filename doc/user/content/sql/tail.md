---
title: "TAIL"
description: "`TAIL` streams updates from an arbitrary `SELECT` statement as they occur."
menu:
    main:
        parent: "sql"
---

`TAIL` queries and streams updates as they occur from a source, table, view, or an arbitrary `SELECT` statement.

## Conceptual framework

While a `SELECT` computes rows at a given time, a
`TAIL` can compute the same rows and stream their _updates_ over time.

Fundamentally, `TAIL` produces a sequence of updates as rows. Each row describes either
the insertion or deletion of a row at a specific time. Update rows, taken together, describe the complete set of changes in order since the TAIL is active.

You can use `TAIL` to:

-   Power event processors that react to every change to a relation or an arbitrary `SELECT` statement.
-   Replicate the complete history of a relation while the `TAIL` is active.

## Syntax

{{< diagram "tail-stmt.svg" >}}

| Field                  | Use                                                                                                                                      |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| _object_name_          | The name of the source, table, or view that you want to tail.                                                                            |
| _select_stmt_          | The [`SELECT` statement](../select) whose output you want to tail.                                                                       |
| _timestamp_expression_ | The logical time at which the `TAIL` begins as a [`bigint`] representing milliseconds since the Unix epoch. See [`AS OF`](#as-of) below. |

### `WITH` options

The following options are valid within the `WITH` clause.

| Option name | Value type | Default | Describes                                                                                                                         |
| ----------- | ---------- | ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `SNAPSHOT`  | `boolean`  | `true`  | Whether to emit a snapshot of the current state of the relation at the start of the operation. See [`SNAPSHOT`](#snapshot) below. |
| `PROGRESS`  | `boolean`  | `false` | Whether to include detailed progress information. See [`PROGRESS`](#progress) below.                                              |

## Details

### Output

`TAIL` emits a sequence of updates as rows. Each row contains all of the columns of
the tailed relation or derived from the `SELECT` statement, prepended with several additional columns that describe
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
    <td><code>mz_timestamp</code></td>
    <td><code>numeric</code></td>
    <td>
      Materialize's internal logical timestamp. This will never be less than any
      timestamp previously emitted by the same <code>TAIL</code> operation.
    </td>
  </tr>
  <tr>
    <td><code>mz_progressed</code></td>
    <td><code>boolean</code></td>
    <td>
      <p>
        <em>
          This column is only present if the
          <a href="#progress"><code>PROGRESS</code></a> option is specified.
        </em>
      </p>
      If <code>true</code>, indicates that the <code>TAIL</code> will not emit
      additional records at times strictly less than <code>mz_timestamp</code>. See
      <a href="#progress"><code>PROGRESS</code></a> below.
    </td>
  </tr>
  <tr>
    <td><code>mz_diff</code></td>
    <td><code>bigint</code></td>
    <td>
      The change in frequency of the row. A positive number indicates that
      <code>mz_diff</code> copies of the row were inserted, while a negative
      number indicates that <code>|mz_diff|</code> copies of the row were deleted.
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

{{< version-changed v0.20.0 >}}
Support arbitrary SELECT statements in `TAIL`.
{{</ version-changed >}}

{{< version-changed v0.8.1 >}}
Columns names added by `TAIL` now prepended by `mz_`.
{{</ version-changed >}}

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

### Life of a tail

`TAIL` will continue to run until canceled, session ends, or until all updates the tailed
item could undergo have been presented. The latter case typically occurs when
tailing constant views (e.g. `CREATE VIEW v AS SELECT 1`) or
[file sources](/sql/create-source/text-file) that were created in non-tailing
mode (`tail = false`).

### `AS OF`

The `AS OF` clause specifies the time at which a `TAIL` operation begins.
See [`SNAPSHOT`](#snapshot) below for details on what this means.

If you don't specify `AS OF` explicitly, Materialize will pick a timestamp
automatically:

-   If the tailed relation is [materialized](/overview/api-components/#indexes),
    Materialize picks the latest time for which results are computed.
-   If the tailed relation is not materialized, Materialize picks time `0`.

A given timestamp will be rejected if data it would report has already been
compacted by Materialize. See the
[`--logical-compaction-window`](/cli/#compaction-window) command-line option for
details on Materialize's compaction policy.

### `SNAPSHOT`

By default, a `TAIL` begins by emitting a snapshot of the tailed relation, which
consists of a series of updates describing the contents of the relation at its
[`AS OF`](#as-of) timestamp. After the snapshot, `TAIL` emits further updates as
they occur.

For updates in the snapshot, the `mz_timestamp` field will be fast-forwarded to the
`AS OF` timestamp. For example, `TAIL ... AS OF 21` would present an insert that
occured at time 15 as if it occurred at time 21.

To see only updates after the `AS OF` timestamp, specify `WITH (SNAPSHOT = false)`.

### `PROGRESS`

Intuitively, progress messages communicate that no updates have occurred in a
given time window. Without explicit progress messages, it is impossible to
distinguish between a stall in Materialize and a legitimate period of no
updates.

If the `PROGRESS` option is specified via `WITH (PROGRESS)`, an additional
`mz_progressed` column appears in the output.
It is `false` if there may be more rows with the same timestamp.
It is `true` if no more timestamps will appear that are strictly less than the
timestamp.
All further columns after `mz_progressed` will be `NULL` in the `true` case.

Not all timestamps that appear will have a corresponding `mz_progressed` row.
For example, the following is a valid sequence of updates:

```nofmt
mz_timestamp | mz_progressed | mz_diff | column1
-------------|---------------|---------|--------------
1            | false         | 1       | data
2            | false         | 1       | more data
3            | false         | 1       | even more data
4            | true          | NULL    | NULL
```

Notice how Materialize did not emit explicit progress messages for timestamps
`1` or `2`. The receipt of the update at timestamp `2` implies that there
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
These must be used within a transaction, with [only one `DECLARE`](/sql/begin/#read-only-transactions) per transaction.
This allows you to limit the number of rows and the time window of your requests. First, declare a `TAIL` cursor:

```sql
BEGIN;
DECLARE c CURSOR FOR TAIL t;
```

Now use [`FETCH`](/sql/fetch) in a loop to retrieve each batch of results as soon as it is ready:

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

### Using clients

{{< tabs tabID="Clients" >}}
{{< tab "Shell">}}

If you want to use `TAIL` from an interactive SQL session (for example in `psql`), wrap the query in `COPY`.

```sql
COPY (TAIL (SELECT * FROM mz_catalog.mz_tables)) TO STDOUT;
```

[`bigint`]: /sql/types/bigint
[`numeric`]: /sql/types/numeric

{{< /tab >}}

{{< tab "Python">}}

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

#### `Streaming` with Python and psycopg3

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

{{< /tab >}}
{{< tab "C#">}}

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

{{< /tab >}}
{{< tab "Node.js">}}

#### `FETCH` with Node.js and pg

```js
import pg from "pg";

async function main() {
    const client = new pg.Client(
        "postgres://materialize@localhost:6875/materialize"
    );
    await client.connect();

    await client.query("BEGIN");
    await client.query("DECLARE c CURSOR FOR TAIL t");
    while (true) {
        const res = await client.query("FETCH ALL c");
        console.log(res.rows);
    }
}

main();
```

{{< /tab >}}

{{< tab "PHP">}}

#### `FETCH` with PHP

```php
 <?php
 // Include the Postgres connection details
 require 'connect.php';
 // Begin a transaction
 $connection->beginTransaction();
 // Declare a cursor
 $statement = $connection->prepare('DECLARE c CURSOR FOR TAIL (SELECT * FROM mz_catalog.mz_tables)');
 // Execute the statement
 $statement->execute();
 /* Fetch all of the remaining rows in the result set */
 while (true) {
     $tail = $connection->prepare('FETCH ALL c');
     $tail->execute();
     $result = $tail->fetchAll(PDO::FETCH_ASSOC);
     print_r($result);
 }
```

{{< /tab >}}

{{< tab "Java">}}

#### `FETCH` with Java

```java
  import java.sql.*;

  /* ... */

  String url = "jdbc:postgresql://localhost:6875/materialize";
  Connection conn = DriverManager.getConnection(url, "materialize", "materialize");
  Statement stmt = conn.createStatement();

  conn.setAutoCommit(false);

  stmt.executeUpdate("DECLARE c CURSOR FOR TAIL (SELECT * FROM antennas_performance);");

  while (true) {
      ResultSet fetchResultSet = stmt.executeQuery("FETCH 100 c WITH (timeout='1s');");

      while (fetchResultSet.next()) {
          System.out.println(fetchResultSet.getString(3));
      }
  }
```

{{< /tab >}}
{{< /tabs >}}

{{< warning >}}

Many PostgreSQL drivers wait for a query to complete before returning its
results. Since `TAIL` can run forever, naively executing a `TAIL` using your
driver's standard query API may never return.

Either use an API in your driver that does not buffer rows or use the
[`FETCH`](/sql/fetch) statement to fetch rows from a `TAIL` in batches.
See the [examples](#examples) for details.

{{< /warning >}}

### Using `AS OF`

Begin creating a view:

```sql
CREATE VIEW most_tired_worker AS
  SELECT worker, max(elapsed_ns)
  FROM mz_catalog.mz_scheduling_elapsed
  WHERE worker = 0
  GROUP BY worker;
```

Set the compaction in the index creation:

```sql
CREATE INDEX most_tired_worker_idx
  ON most_tired_worker (worker, max)
  WITH (logical_compaction_window = '10 seconds');
```

Stream out changes from ten seconds before the statement is executed:

```sql
COPY (TAIL most_tired_worker AS OF NOW() - INTERVAL '10 seconds') TO STDOUT;
```

Take into account, for this example, that ten logical seconds need to pass by inside Materialize to recover changes from the last ten seconds.
