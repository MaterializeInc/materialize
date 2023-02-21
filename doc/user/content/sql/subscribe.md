---
title: "SUBSCRIBE"
description: "`SUBSCRIBE` streams updates from a source, table, or view as they occur."
menu:
  main:
    parent: commands
aliases:
  - /sql/tail
---

`SUBSCRIBE` streams updates from a source, table, or view as they occur.

## Conceptual framework

The `SUBSCRIBE` statement is a more general form of a [`SELECT`](/sql/select)
statement. While a `SELECT` statement computes a relation at a moment in time, a
subscribe operation computes how a relation *changes* over time.

Fundamentally, `SUBSCRIBE` produces a sequence of updates. An update describes either
the insertion or deletion of a row to the relation at a specific time. Taken
together, the updates describe the complete set of changes to a relation, in
order, while `SUBSCRIBE` is active.

You can use `SUBSCRIBE` to:

-   Power event processors that react to every change to a relation or an arbitrary `SELECT` statement.
-   Replicate the complete history of a relation while `SUBSCRIBE` is active.

## Syntax

{{< diagram "subscribe-stmt.svg" >}}

| Field                  | Use                                                                                                                                      |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| _object_name_          | The name of the source, table, or view that you want to subscribe to.                                                                            |
| _select_stmt_          | The [`SELECT` statement](../select) whose output you want to subscribe to.                                                                       |

### `WITH` options

The following options are valid within the `WITH` clause.

| Option name | Value type | Default | Describes                                                                                                                         |
| ----------- | ---------- | ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `SNAPSHOT`  | `boolean`  | `true`  | Whether to emit a snapshot of the current state of the relation at the start of the operation. See [`SNAPSHOT`](#snapshot) below. |
| `PROGRESS`  | `boolean`  | `false` | Whether to include detailed progress information. See [`PROGRESS`](#progress) below.                                              |

## Details

### Output

`SUBSCRIBE` emits a sequence of updates as rows. Each row contains all of the
columns of the subscribed relation or derived from the `SELECT` statement, prepended
with several additional columns that describe the nature of the update:

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
      timestamp previously emitted by the same <code>SUBSCRIBE</code> operation.
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
      If <code>true</code>, indicates that the <code>SUBSCRIBE</code> will not emit
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
        The columns from the subscribed relation, each as its own column,
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

### `AS OF`

The `AS OF` clause allows specifying a timestamp at which the `SUBSCRIBE` should begin returning results, in order to inspect the historical state of a relation. If `AS OF` is specified, no rows whose timestamp is less than the specified timestamp will be returned. If the timestamp specified is earlier than the earliest historical state retained by the source relations, an error will be signaled.

Currently, all user-defined sources and tables have a retention window of one second, so `AS OF` is of limited usefulness except when subscribing to queries over certain internal relations.

### `UP TO`

The `UP TO` clause allows specifying a timestamp at which the `SUBSCRIBE` will cease running. If `UP TO` is specified, no rows whose timestamp is greater than or equal to the specified timestamp will be returned.

### Interaction of `AS OF` and `UP TO`

The lower timestamp bound specified by `AS OF` is inclusive, whereas the upper bound specified by `UP TO` is exclusive. Thus, a `SUBSCRIBE` query whose `AS OF` is equal to its `UP TO` will terminate after returning zero rows.

A `SUBSCRIBE` whose `UP TO` is less than its "as of" timestamp (whether that timestamp was specified in an `AS OF` clause or chosen by the system) will signal an error.

### Duration

`SUBSCRIBE` will continue to run until canceled, the session ends, the `UP TO` timestamp is reached, or all updates have been presented. The latter case typically occurs when
tailing constant views (e.g. `CREATE VIEW v AS SELECT 1`).

{{< warning >}}

Many PostgreSQL drivers wait for a query to complete before returning its
results. Since `SUBSCRIBE` can run forever, naively executing a `SUBSCRIBE` using your
driver's standard query API may never return.

Either use an API in your driver that does not buffer rows or use the
[`FETCH`](/sql/fetch) statement or `AS OF` and `UP TO` bounds
to fetch rows from `SUBSCRIBE` in batches.
See the [examples](#examples) for details.

{{< /warning >}}

### `SNAPSHOT`

By default, `SUBSCRIBE` begins by emitting a snapshot of the subscribed relation, which
consists of a series of updates describing the contents of the relation at its
initial timestamp. After the snapshot, `SUBSCRIBE` emits further updates as
they occur.

For updates in the snapshot, the `mz_timestamp` field will be fast-forwarded to the initial timestamp.
For example, an insert that occurred before the `SUBSCRIBE` began would appear in the snapshot.

To see only updates after the initial timestamp, specify `WITH (SNAPSHOT = false)`.

### `PROGRESS`

Intuitively, progress messages communicate that no updates have occurred in a
given time window. Without explicit progress messages, it is impossible to
distinguish between a stall in Materialize and a legitimate period of no
updates.

If the `PROGRESS` option is specified via `WITH (PROGRESS)`, an additional `mz_progressed` column appears in the output.
When the column is `false`, the rest of the row is a valid update.
When the column is `true` the rest of the row is not a valid update and its content should be ignored;
the row exists only to communicate that timestamps have advanced.

Not all timestamps that appear will have a corresponding row with `mz_progressed` set to `true`.
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

`SUBSCRIBE` produces rows similar to a `SELECT` statement, except that `SUBSCRIBE` may never complete.
Many drivers buffer all results until a query is complete, and so will never return.
Below are the recommended ways to work around this.

### Creating a counter load generator

As an example, we'll create a [counter load generator](https://materialize.com/docs/sql/create-source/load-generator/#creating-a-counter-load-generator) that emits a row every second:

```sql
CREATE SOURCE counter
  FROM LOAD GENERATOR COUNTER
  WITH (SIZE = '3xsmall');
```

### Subscribing with `FETCH`

The recommended way to use `SUBSCRIBE` is with [`DECLARE`](/sql/declare) and [`FETCH`](/sql/fetch).
These must be used within a transaction, with [a single `DECLARE`](/sql/begin/#read-only-transactions) per transaction.
This allows you to limit the number of rows and the time window of your requests.
Next, let's subscribe to the `counter` load generator source that we've created above.

First, declare a `SUBSCRIBE` cursor:

```sql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM counter);
```

Then, use [`FETCH`](/sql/fetch) in a loop to retrieve each batch of results as soon as it's ready:

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

If you want to use `SUBSCRIBE` from an interactive SQL session (e.g.`psql`), wrap the query in `COPY`:

```sql
COPY (SUBSCRIBE (SELECT * FROM counter)) TO STDOUT;
```

| Additional guides |
| ---------------------- |
| [Go](/integrations/golang/#stream)|
| [Java](/integrations/java-jdbc/#stream)|
| [Node.js](/integrations/node-js/#stream)|
| [PHP](/integrations/php/#stream)|
| [Python](/integrations/python/#stream)|
| [Ruby](/integrations/ruby/#stream)|

### Mapping rows to their updates

After all the rows from the [`SNAPSHOT`](#snapshot) have been transmitted, the updates will be emitted as they occur. How can you map each row to its corresponding update?

| mz_timestamp | mz_progressed | mz_diff | Column 1 | .... | Column N |
| ------------ | ------------- | ------- | -------- | ---- | -------- | --------------------------- |
| 1            | false         | 1       | id1      |      | value1   |
| 1            | false         | 1       | id2      |      | value2   |
| 1            | false         | 1       | id3      |      | value3   | <- Last row from `SNAPSHOT` |
| 2            | false         | -1      | id1      |      | value1   |
| 2            | false         | 1       | id1      |      | value4   |

If your row has a unique column key, it is possible to map the update to its corresponding origin row; if the key is unknown, you can use the output of `hash(columns_values)` instead.

In the example above, `Column 1` acts as the column key that uniquely identifies the origin row the update refers to; in case this was unknown, hashing the values from `Column 1` to `Column N` would identify the origin row.

### Dropping the `counter` load generator source

When you're done, you can drop the `counter` load generator source:

```sql
DROP SOURCE counter;
```
