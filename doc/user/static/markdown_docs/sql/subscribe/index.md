<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# SUBSCRIBE

`SUBSCRIBE` streams updates from a source, table, view, or materialized
view as they occur.

## Conceptual framework

The `SUBSCRIBE` statement is a more general form of a
[`SELECT`](/docs/self-managed/v25.2/sql/select) statement. While a
`SELECT` statement computes a relation at a moment in time, a subscribe
operation computes how a relation *changes* over time.

Fundamentally, `SUBSCRIBE` produces a sequence of updates. An update
describes either the insertion or deletion of a row to the relation at a
specific time. Taken together, the updates describe the complete set of
changes to a relation, in order, while `SUBSCRIBE` is active.

You can use `SUBSCRIBE` to:

- Power event processors that react to every change to a relation or an
  arbitrary `SELECT` statement.
- Replicate the complete history of a relation while `SUBSCRIBE` is
  active.

## Syntax

<div class="highlight">

``` chroma
SUBSCRIBE [TO] <object_name | (SELECT ...)>
[ENVELOPE UPSERT (KEY (<key1>, ...)) | ENVELOPE DEBEZIUM (KEY (<key1>, ...))]
[WITHIN TIMESTAMP ORDER BY <column1> [ASC | DESC] [NULLS LAST | NULLS FIRST], ...]
[WITH (<option_name> [= <option_value>], ...)]
[AS OF [AT LEAST] <timestamp_expression>]
[UP TO <timestamp_expression>]
```

</div>

where:

- `<object_name>` is the name of the source, table, view, or
  materialized view that you want to subscribe to.
- `<select_stmt>` is the [`SELECT`
  statement](/docs/self-managed/v25.2/sql/select) whose output you want
  to subscribe to.

The generated schemas have a Debezium-style diff envelope to capture
changes in the input view or source.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>ENVELOPE UPSERT (KEY (</strong>&lt;key1&gt;,
…<strong>))</strong></td>
<td>If specified, use the upsert envelope, which takes a list of
<code>KEY</code> columns. The upsert envelope supports inserts, updates
and deletes in the subscription output. For more information, see <a
href="#modifying-the-output-format">Modifying the output
format</a>.</td>
</tr>
<tr>
<td><strong>ENVELOPE DEBEZIUM (KEY (</strong>&lt;key1&gt;,
…<strong>))</strong></td>
<td>If specified, use a <a
href="/docs/self-managed/v25.2/sql/create-sink/kafka/#debezium">Debezium-style
diff envelope</a>, which takes a list of <code>KEY</code> columns. The
Debezium envelope supports inserts, updates and deletes in the
subscription output along with the previous state of the key. For more
information, see <a href="#modifying-the-output-format">Modifying the
output format</a>.</td>
</tr>
<tr>
<td><strong>WITHIN TIMESTAMP ORDER BY</strong> &lt;column1&gt;, …</td>
<td>If specified, use an <code>ORDER BY</code> clause to sort the
subscription output within a timestamp. For each <code>ORDER BY</code>
column, you can optionally specify:
<ul>
<li><code>ASC</code> or <code>DESC</code></li>
<li><code>NULLS FIRST</code> or <code>NULLS LAST</code></li>
</ul>
For more information, see <a
href="#modifying-the-output-format">Modifying the output
format</a>.</td>
</tr>
<tr>
<td><strong>WITH</strong> &lt;option_name&gt; [=
&lt;option_value&gt;]</td>
<td>If specified, use the specified option. For more information, see <a
href="#with-options"><code>WITH</code> options</a>.</td>
</tr>
<tr>
<td><strong>AS OF</strong> &lt;timestamp_expression&gt;</td>
<td>If specified, no rows whose timestamp is earlier than the specified
timestamp will be returned. For more information, see <a
href="#as-of"><code>AS OF</code></a>.</td>
</tr>
<tr>
<td><strong>UP TO</strong> &lt;timestamp_expression&gt;</td>
<td>If specified, no rows whose timestamp is greater than or equal to
the specified timestamp will be returned. For more information, see <a
href="#up-to"><code>UP TO</code></a>.</td>
</tr>
</tbody>
</table>

#### `WITH` options

The following options are valid within the `WITH` clause.

| Option name | Value type | Default | Describes |
|----|----|----|----|
| `SNAPSHOT` | `boolean` | `true` | Whether to emit a snapshot of the current state of the relation at the start of the operation. See [`SNAPSHOT`](#snapshot). |
| `PROGRESS` | `boolean` | `false` | Whether to include detailed progress information. See [`PROGRESS`](#progress). |

## Details

### Output

`SUBSCRIBE` emits a sequence of updates as rows. Each row contains all
of the columns of the subscribed relation or derived from the `SELECT`
statement, prepended with several additional columns that describe the
nature of the update:

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
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
<td>Materialize's internal logical timestamp. This will never be less
than any timestamp previously emitted by the same <code>SUBSCRIBE</code>
operation.</td>
</tr>
<tr>
<td><code>mz_progressed</code></td>
<td><code>boolean</code></td>
<td><p><em>This column is only present if the <a
href="#progress"><code>PROGRESS</code></a> option is specified.</em></p>
If <code>true</code>, indicates that the <code>SUBSCRIBE</code> will not
emit additional records at times strictly less than
<code>mz_timestamp</code>. See <a
href="#progress"><code>PROGRESS</code></a> below.</td>
</tr>
<tr>
<td><code>mz_diff</code></td>
<td><code>bigint</code></td>
<td>The change in frequency of the row. A positive number indicates that
<code>mz_diff</code> copies of the row were inserted, while a negative
number indicates that <code>|mz_diff|</code> copies of the row were
deleted.</td>
</tr>
<tr>
<td>Column 1</td>
<td>Varies</td>
<td rowspan="3"
style="vertical-align: middle; border-left: 1px solid #ccc">The columns
from the subscribed relation, each as its own column, representing the
data that was inserted into or deleted from the relation.</td>
</tr>
<tr>
<td colspan="2" style="text-align: center;">...</td>
</tr>
<tr>
<td>Column <em>N</em></td>
<td>Varies</td>
</tr>
</tbody>
</table>

### `AS OF`

When a [history rentention
period](/docs/self-managed/v25.2/transform-data/patterns/durable-subscriptions/#history-retention-period)
is configured for the object(s) powering the subscription, the `AS OF`
clause allows specifying a timestamp at which the `SUBSCRIBE` command
should begin returning results. If `AS OF` is specified, no rows whose
timestamp is earlier than the specified timestamp will be returned. If
the timestamp specified is earlier than the earliest historical state
retained by the underlying objects, an error is thrown.

To configure the history retention period for objects used in a
subscription, see [Durable
subscriptions](/docs/self-managed/v25.2/transform-data/patterns/durable-subscriptions/#history-retention-period).
If `AS OF` is unspecified, the system automatically chooses an `AS OF`
timestamp.

### `UP TO`

The `UP TO` clause allows specifying a timestamp at which the
`SUBSCRIBE` will cease running. If `UP TO` is specified, no rows whose
timestamp is greater than or equal to the specified timestamp will be
returned.

### Interaction of `AS OF` and `UP TO`

The lower timestamp bound specified by `AS OF` is inclusive, whereas the
upper bound specified by `UP TO` is exclusive. Thus, a `SUBSCRIBE` query
whose `AS OF` is equal to its `UP TO` will terminate after returning
zero rows.

A `SUBSCRIBE` whose `UP TO` is less than its `AS OF` timestamp (whether
that timestamp was specified in an `AS OF` clause or chosen by the
system) will signal an error.

### Duration

`SUBSCRIBE` will continue to run until canceled, the session ends, the
`UP TO` timestamp is reached, or all updates have been presented. The
latter case typically occurs when tailing constant views (e.g.
`CREATE VIEW v AS SELECT 1`).

<div class="warning">

**WARNING!**

Many PostgreSQL drivers wait for a query to complete before returning
its results. Since `SUBSCRIBE` can run forever, naively executing a
`SUBSCRIBE` using your driver’s standard query API may never return.

Either use an API in your driver that does not buffer rows or use the
[`FETCH`](/docs/self-managed/v25.2/sql/fetch) statement or `AS OF` and
`UP TO` bounds to fetch rows from `SUBSCRIBE` in batches. See the
[examples](#examples) for details.

</div>

### `SNAPSHOT`

By default, `SUBSCRIBE` begins by emitting a snapshot of the subscribed
relation, which consists of a series of updates at its [`AS OF`](#as-of)
timestamp describing the contents of the relation. After the snapshot,
`SUBSCRIBE` emits further updates as they occur.

For updates in the snapshot, the `mz_timestamp` field will be
fast-forwarded to the `AS OF` timestamp. For example, an insert that
occurred before the `SUBSCRIBE` began would appear in the snapshot.

To see only updates after the initial timestamp, specify
`WITH (SNAPSHOT = false)`.

### `PROGRESS`

If the `PROGRESS` option is specified via `WITH (PROGRESS)`:

- An additional `mz_progressed` column appears in the output. When the
  column is `false`, the rest of the row is a valid update. When the
  column is `true` everything in the row except for `mz_timestamp` is
  not a valid update and its content should be ignored; the row exists
  only to communicate that timestamps have advanced.

- The first update emitted by the `SUBSCRIBE` is guaranteed to be a
  progress message indicating the subscribe’s [`AS OF`](#as-of)
  timestamp.

Intuitively, progress messages communicate that no updates have occurred
in a given time window. Without explicit progress messages, it is
impossible to distinguish between a stall in Materialize and a
legitimate period of no updates.

Not all timestamps that appear will have a corresponding row with
`mz_progressed` set to `true`. For example, the following is a valid
sequence of updates:

```
mz_timestamp | mz_progressed | mz_diff | column1
-------------|---------------|---------|--------------
1            | false         | 1       | data
2            | false         | 1       | more data
3            | false         | 1       | even more data
4            | true          | NULL    | NULL
```

Notice how Materialize did not emit explicit progress messages for
timestamps `1` or `2`. The receipt of the update at timestamp `2`
implies that there are no more updates for timestamp `1`, because
timestamps are always presented in non-decreasing order. The receipt of
the explicit progress message at timestamp `4` implies that there are no
more updates for either timestamp `2` or `3`—but that there may be more
data arriving at timestamp `4`.

## Examples

`SUBSCRIBE` produces rows similar to a `SELECT` statement, except that
`SUBSCRIBE` may never complete. Many drivers buffer all results until a
query is complete, and so will never return. Below are the recommended
ways to work around this.

### Creating a counter load generator

As an example, we’ll create a [counter load
generator](/docs/self-managed/v25.2/sql/create-source/load-generator/#creating-a-counter-load-generator)
that emits a row every second:

<div class="highlight">

``` chroma
CREATE SOURCE counter FROM LOAD GENERATOR COUNTER;
```

</div>

### Subscribing with `FETCH`

The recommended way to use `SUBSCRIBE` is with
[`DECLARE`](/docs/self-managed/v25.2/sql/declare) and
[`FETCH`](/docs/self-managed/v25.2/sql/fetch). These must be used within
a transaction, with [a single
`DECLARE`](/docs/self-managed/v25.2/sql/begin/#read-only-transactions)
per transaction. This allows you to limit the number of rows and the
time window of your requests. Next, let’s subscribe to the `counter`
load generator source that we’ve created above.

First, declare a `SUBSCRIBE` cursor:

<div class="highlight">

``` chroma
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM counter);
```

</div>

Then, use [`FETCH`](/docs/self-managed/v25.2/sql/fetch) in a loop to
retrieve each batch of results as soon as it’s ready:

<div class="highlight">

``` chroma
FETCH ALL c;
```

</div>

That will retrieve all of the rows that are currently available. If
there are no rows available, it will wait until there are some ready and
return those. A `timeout` can be used to specify a window in which to
wait for rows. This will return up to the specified count (or `ALL`) of
rows that are ready within the timeout. To retrieve up to 100 rows that
are available in at most the next `1s`:

<div class="highlight">

``` chroma
FETCH 100 c WITH (timeout='1s');
```

</div>

To retrieve all available rows available over the next `1s`:

<div class="highlight">

``` chroma
FETCH ALL c WITH (timeout='1s');
```

</div>

A `0s` timeout can be used to return rows that are available now without
waiting:

<div class="highlight">

``` chroma
FETCH ALL c WITH (timeout='0s');
```

</div>

### Using clients

If you want to use `SUBSCRIBE` from an interactive SQL session
(e.g.`psql`), wrap the query in `COPY`:

<div class="highlight">

``` chroma
COPY (SUBSCRIBE (SELECT * FROM counter)) TO STDOUT;
```

</div>

| Additional guides |
|----|
| [Go](/docs/self-managed/v25.2/integrations/client-libraries/golang/#stream) |
| [Java](/docs/self-managed/v25.2/integrations/client-libraries/java-jdbc/#stream) |
| [Node.js](/docs/self-managed/v25.2/integrations/client-libraries/node-js/#stream) |
| [PHP](/docs/self-managed/v25.2/integrations/client-libraries/php/#stream) |
| [Python](/docs/self-managed/v25.2/integrations/client-libraries/python/#stream) |
| [Ruby](/docs/self-managed/v25.2/integrations/client-libraries/ruby/#stream) |
| [Rust](/docs/self-managed/v25.2/integrations/client-libraries/rust/#stream) |

### Mapping rows to their updates

After all the rows from the [`SNAPSHOT`](#snapshot) have been
transmitted, the updates will be emitted as they occur. How can you map
each row to its corresponding update?

| mz_timestamp | mz_progressed | mz_diff | Column 1 | …. | Column N |  |
|----|----|----|----|----|----|----|
| 1 | false | 1 | id1 |  | value1 |  |
| 1 | false | 1 | id2 |  | value2 |  |
| 1 | false | 1 | id3 |  | value3 | \<- Last row from `SNAPSHOT` |
| 2 | false | -1 | id1 |  | value1 |  |
| 2 | false | 1 | id1 |  | value4 |  |

If your row has a unique column key, it is possible to map the update to
its corresponding origin row; if the key is unknown, you can use the
output of `hash(columns_values)` instead.

In the example above, `Column 1` acts as the column key that uniquely
identifies the origin row the update refers to; in case this was
unknown, hashing the values from `Column 1` to `Column N` would identify
the origin row.

### Modifying the output format

#### `ENVELOPE UPSERT`

To modify the output of `SUBSCRIBE` to support upserts, use
`ENVELOPE UPSERT`. This clause allows you to specify a `KEY` that
Materialize uses to interpret the rows as a series of inserts, updates
and deletes within each distinct timestamp.

The output columns are reordered so that all the key columns come before
the value columns.

- Using this modifier, the output rows will have the following
  structure:

  <div class="highlight">

  ``` chroma
  SUBSCRIBE mview ENVELOPE UPSERT (KEY (key));
  ```

  </div>

  <div class="highlight">

  ``` chroma
  mz_timestamp | mz_state | key  | value
  -------------|----------|------|--------
  100          | upsert   | 1    | 2
  100          | upsert   | 2    | 4
  ```

  </div>

- For inserts and updates, the value columns for each key are set to the
  resulting value of the series of operations, and `mz_state` is set to
  `upsert`.

  *Insert*

  <div class="highlight">

  ``` chroma
   -- at time 200, add a new row with key=3, value=6
   mz_timestamp | mz_state | key  | value
   -------------|----------|------|--------
   ...
   200          | upsert   | 3    | 6
   ...
  ```

  </div>

  *Update*

  <div class="highlight">

  ``` chroma
   -- at time 300, update key=1's value to 10
   mz_timestamp | mz_state | key  | value
   -------------|----------|------|--------
   ...
   300          | upsert   | 1    | 10
   ...
  ```

  </div>

- If only deletes occur within a timestamp, the value columns for each
  key are set to `NULL`, and `mz_state` is set to `delete`.

  *Delete*

  <div class="highlight">

  ``` chroma
   -- at time 400, delete all rows
   mz_timestamp | mz_state | key  | value
   -------------|----------|------|--------
   ...
   400          | delete   | 1    | NULL
   400          | delete   | 2    | NULL
   400          | delete   | 3    | NULL
   ...
  ```

  </div>

- Only use `ENVELOPE UPSERT` when there is at most one live value per
  key. If materialize detects that a given key has multiple values, it
  will generate an update with `mz_state` set to `"key_violation"`, the
  problematic key, and all the values nulled out. Materialize is not
  guaranteed to detect this case, please don’t rely on it.

  *Key violation*

  <div class="highlight">

  ``` chroma
   -- at time 500, introduce a key_violation
   mz_timestamp | mz_state        | key  | value
   -------------|-----------------|------|--------
   ...
   500          | key_violation   | 1    | NULL
   ...
  ```

  </div>

- If [`PROGRESS`](#progress) is set, Materialize also returns the
  `mz_progressed` column. Each progress row will have a `NULL` key and a
  `NULL` value.

#### `ENVELOPE DEBEZIUM`

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

To modify the output of `SUBSCRIBE` to support upserts using a
[Debezium-style diff
envelope](/docs/self-managed/v25.2/sql/create-sink/kafka/#debezium), use
`ENVELOPE DEBEZIUM`. This clause allows you to specify a `KEY` that
Materialize uses to interpret the rows as a series of inserts, updates
and deletes within each distinct timestamp. Unlike `ENVELOPE UPSERT`,
the output includes the state of the row before and after the upsert
operation.

The output columns are reordered so that all the key columns come before
the value columns. There are two copies of the value columns: one
prefixed with `before_`, which represents the value of the columns
before the upsert operation; and another prefixed with `after_`, which
represents the current value of the columns.

- Using this modifier, the output rows will have the following
  structure:

  <div class="highlight">

  ``` chroma
  SUBSCRIBE mview ENVELOPE DEBEZIUM (KEY (key));
  ```

  </div>

  <div class="highlight">

  ``` chroma
  mz_timestamp | mz_state | key  | before_value | after_value
  -------------|----------|------|--------------|-------
  100          | upsert   | 1    | NULL         | 2
  100          | upsert   | 2    | NULL         | 4
  ```

  </div>

- For inserts: the before values are `NULL`, the current value is the
  newly inserted value and `mz_state` is set to `insert`.

  *Insert*

  <div class="highlight">

  ``` chroma
   -- at time 200, add a new row with key=3, value=6
   mz_timestamp | mz_state | key  | before_value | after_value
   -------------|----------|------|--------------|-------
   ...
   200          | insert   | 3    | NULL         | 6
   ...
  ```

  </div>

- For updates: the before values are the old values, the value columns
  are the resulting values of the update, and `mz_state` is set
  to`upsert`.

  *Update*

  <div class="highlight">

  ``` chroma
   -- at time 300, update key=1's value to 10
   mz_timestamp | mz_state | key  | before_value | after_value
   -------------|----------|------|--------------|-------
   ...
   300          | upsert   | 1    | 2            | 10
   ...
  ```

  </div>

- If only deletes occur within a timestamp, the value columns for each
  key are set to `NULL`, the before values are set to the old value and
  `mz_state` is set to `delete`.

  *Delete*

  <div class="highlight">

  ``` chroma
   -- at time 400, delete all rows
   mz_timestamp | mz_state | key  | before_value | after_value
   -------------|----------|------|--------------|-------
   ...
   400          | delete   | 1    | 10           | NULL
   400          | delete   | 2    | 4            | NULL
   400          | delete   | 3    | 6            | NULL
   ...
  ```

  </div>

- Like `ENVELOPE UPSERT`, using `ENVELOPE DEBEZIUM` requires that there
  is at most one live value per key. If Materialize detects that a given
  key has multiple values, it will generate an update with `mz_state`
  set to `"key_violation"`, the problematic key, and all the values
  nulled out. Materialize identifies key violations on a best-effort
  basis.

  *Key violation*

  <div class="highlight">

  ``` chroma
   -- at time 500, introduce a key_violation
   mz_timestamp | mz_state        | key  | before_value | after_value
   -------------|-----------------|------|--------------|-------
   ...
   500          | key_violation   | 1    | NULL         | NULL
   ...
  ```

  </div>

- If [`PROGRESS`](#progress) is set, Materialize also returns the
  `mz_progressed` column. Each progress row will have a `NULL` key and a
  `NULL` before and after value.

#### `WITHIN TIMESTAMP ORDER BY`

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

To modify the ordering of the output of `SUBSCRIBE`, use
`WITHIN TIMESTAMP ORDER BY`. This clause allows you to specify an
`ORDER BY` expression which is used to sort the rows within each
distinct timestamp.

- The `ORDER BY` expression can take any column in the underlying object
  or query, including `mz_diff`.

  <div class="highlight">

  ``` chroma
  SUBSCRIBE mview WITHIN TIMESTAMP ORDER BY c1, c2 DESC NULLS LAST, mz_diff;

  mz_timestamp | mz_diff | c1            | c2   | c3
  -------------|---------|---------------|------|-----
  100          | +1      | 1             | 20   | foo
  100          | -1      | 1             | 2    | bar
  100          | +1      | 1             | 2    | boo
  100          | +1      | 1             | 0    | data
  100          | -1      | 2             | 0    | old
  100          | +1      | 2             | 0    | new
  ```

  </div>

- If [`PROGRESS`](#progress) is set, progress messages are unaffected.

### Dropping the `counter` load generator source

When you’re done, you can drop the `counter` load generator source:

<div class="highlight">

``` chroma
DROP SOURCE counter;
```

</div>

### Durable subscriptions

Because `SUBSCRIBE` requests happen over the network, these connections
might get disrupted for both expected and unexpected reasons. You can
adjust the [history retention
period](/docs/self-managed/v25.2/transform-data/patterns/durable-subscriptions/#history-retention-period)
for the objects a subscription depends on, and then use
[`AS OF`](#as-of) to pick up where you left off on connection drops—this
ensures that no data is lost in the subscription process, and avoids the
need for re-snapshotting the data.

For more information, see [durable
subscriptions](/docs/self-managed/v25.2/transform-data/patterns/durable-subscriptions/).

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the
  query are contained in.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the
    necessary privileges to execute the view definition. Even if the
    view owner is a *superuser*, they still must explicitly be granted
    the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/subscribe.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
