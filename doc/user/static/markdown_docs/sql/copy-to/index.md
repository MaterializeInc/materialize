<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# COPY TO

`COPY TO` outputs results from Materialize to standard output or object
storage. This command is useful to output
[`SUBSCRIBE`](/docs/sql/subscribe/) results [to
`stdout`](#copy-to-stdout), or perform [bulk exports to Amazon
S3](#copy-to-s3).

## Syntax

<div class="code-tabs">

<div class="tab-content">

<div id="tab-copy-to-stdout" class="tab-pane" title="Copy to stdout">

### Copy to `stdout`

Copying results to `stdout` is useful to output the stream of updates
from a [`SUBSCRIBE`](/docs/sql/subscribe/) command in interactive SQL
clients like `psql`.

<div class="highlight">

``` chroma
COPY ( <query> ) TO STDOUT [WITH ( <option> = <val> )];
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;query&gt;</code></td>
<td>The <a href="/docs/sql/select"><code>SELECT</code></a> or <a
href="/docs/sql/subscribe"><code>SUBSCRIBE</code></a> query whose
results are copied.</td>
</tr>
<tr>
<td><code>WITH ( &lt;option&gt; = &lt;val&gt; )</code></td>
<td><p>Optional. The following <code>&lt;option&gt;</code> are
supported:</p>
<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>FORMAT</code></td>
<td>Sets the output format. Valid output formats are:
<code>TEXT</code>,<code>BINARY</code>, <code>CSV</code>.<br />
<br />
Default: <code>TEXT</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

<div id="tab-copy-to-amazon-s3-and-s3-compatible-services"
class="tab-pane" title="Copy to Amazon S3 and S3 compatible services">

### Copy to Amazon S3 and S3 compatible services

Copying results to Amazon S3 (or S3-compatible services) is useful to
perform tasks like periodic backups for auditing, or downstream
processing in analytical data warehouses like Snowflake, Databricks or
BigQuery. For step-by-step instructions, see the integration guide for
[Amazon S3](/docs/serve-results/s3/).

The `COPY TO` command is *one-shot*: every time you want to export
results, you must run the command. To automate exporting results on a
regular basis, you can set up scheduling, for example using a simple
`cron`-like service or an orchestration platform like Airflow or
Dagster.

<div class="highlight">

``` chroma
COPY <query> TO '<s3_uri>'
WITH (
  AWS CONNECTION = <connection_name>,
  FORMAT = <format>
  [, MAX FILE SIZE = <size> ]
);
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;query&gt;</code></td>
<td>The <a href="/docs/sql/select"><code>SELECT</code></a> query whose
results are copied.</td>
</tr>
<tr>
<td><code>&lt;s3_uri&gt;</code></td>
<td>The unique resource identifier (URI) of the Amazon S3 bucket (and
prefix) to store the output results in.</td>
</tr>
<tr>
<td><code>AWS CONNECTION = &lt;connection_name&gt;</code></td>
<td>The name of the AWS connection to use in the <code>COPY TO</code>
command. For details on creating connections, check the <a
href="/docs/sql/create-connection/#aws"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><code>FORMAT = '&lt;format&gt;'</code></td>
<td><p>The file format to write. Valid formats are <code>'csv'</code>
and <code>'parquet'</code>.</p>
<ul>
<li><p>For <code>'csv'</code> format, Materialize writes CSV files using
the following writer settings:</p>
<table>
<thead>
<tr>
<th>Setting</th>
<th>Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>delimiter</td>
<td><code>,</code></td>
</tr>
<tr>
<td>quote</td>
<td><code>"</code></td>
</tr>
<tr>
<td>escape</td>
<td><code>"</code></td>
</tr>
<tr>
<td>header</td>
<td><code>false</code></td>
</tr>
</tbody>
</table></li>
<li><p>For <code>'parquet'</code> format, Materialize writes Parquet
files that aim for maximum compatibility with downstream systems. The
following Parquet writer settings are used:</p>
<table>
<thead>
<tr>
<th>Setting</th>
<th>Value</th>
</tr>
</thead>
<tbody>
<tr>
<td>Writer version</td>
<td>1.0</td>
</tr>
<tr>
<td>Compression</td>
<td><code>snappy</code></td>
</tr>
<tr>
<td>Default column encoding</td>
<td>Dictionary</td>
</tr>
<tr>
<td>Fallback column encoding</td>
<td>Plain</td>
</tr>
<tr>
<td>Dictionary page encoding</td>
<td>Plain</td>
</tr>
<tr>
<td>Dictionary data page encoding</td>
<td><code>RLE_DICTIONARY</code></td>
</tr>
</tbody>
</table>
<p>If you encounter issues trying to ingest Parquet files produced by
Materialize into your downstream systems, please <a
href="/docs/support/">contact our team</a>.</p></li>
</ul></td>
</tr>
<tr>
<td>[<code>MAX FILE SIZE = &lt;size&gt;</code>]</td>
<td>Optional. Sets the approximate maximum file size (in bytes) of each
file uploaded to the S3 bucket.</td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

## Details

### Copy to S3: CSV

#### Writer settings

For `'csv'` format, Materialize writes CSV files using the following
writer settings:

| Setting   | Value   |
|-----------|---------|
| delimiter | `,`     |
| quote     | `"`     |
| escape    | `"`     |
| header    | `false` |

### Copy to S3: Parquet

#### Writer settings

For `'parquet'` format, Materialize writes Parquet files that aim for
maximum compatibility with downstream systems. The following Parquet
writer settings are used:

| Setting                       | Value            |
|-------------------------------|------------------|
| Writer version                | 1.0              |
| Compression                   | `snappy`         |
| Default column encoding       | Dictionary       |
| Fallback column encoding      | Plain            |
| Dictionary page encoding      | Plain            |
| Dictionary data page encoding | `RLE_DICTIONARY` |

If you encounter issues trying to ingest Parquet files produced by
Materialize into your downstream systems, please [contact our
team](/docs/support/).

#### Parquet data types

When using the `parquet` format, Materialize converts the values in the
result set to [Apache Arrow](https://arrow.apache.org/docs/index.html),
and then serializes this Arrow representation to Parquet. The Arrow
schema is embedded in the Parquet file metadata and allows
reconstructing the Arrow representation using a compatible reader.

Materialize also includes [Parquet `LogicalType`
annotations](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#metadata)
where possible. However, many newer `LogicalType` annotations are not
supported in the 1.0 writer version.

Materialize also embeds its own type information into the Apache Arrow
schema. The field metadata in the schema contains an
`ARROW:extension:name` annotation to indicate the Materialize native
type the field originated from.

| Materialize type | Arrow extension name | [Arrow type](https://github.com/apache/arrow/blob/main/format/Schema.fbs) | [Parquet primitive type](https://parquet.apache.org/docs/file-format/types/) | [Parquet logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) |
|----|----|----|----|----|
| [`bigint`](/docs/sql/types/integer/#bigint-info) | `materialize.v1.bigint` | `int64` | `INT64` |  |
| [`boolean`](/docs/sql/types/boolean/) | `materialize.v1.boolean` | `bool` | `BOOLEAN` |  |
| [`bytea`](/docs/sql/types/bytea/) | `materialize.v1.bytea` | `large_binary` | `BYTE_ARRAY` |  |
| [`date`](/docs/sql/types/date/) | `materialize.v1.date` | `date32` | `INT32` | `DATE` |
| [`double precision`](/docs/sql/types/float/#double-precision-info) | `materialize.v1.double` | `float64` | `DOUBLE` |  |
| [`integer`](/docs/sql/types/integer/#integer-info) | `materialize.v1.integer` | `int32` | `INT32` |  |
| [`jsonb`](/docs/sql/types/jsonb/) | `materialize.v1.jsonb` | `large_utf8` | `BYTE_ARRAY` |  |
| [`map`](/docs/sql/types/map/) | `materialize.v1.map` | `map` (`struct` with fields `keys` and `values`) | Nested | `MAP` |
| [`list`](/docs/sql/types/list/) | `materialize.v1.list` | `list` | Nested |  |
| [`numeric`](/docs/sql/types/numeric/) | `materialize.v1.numeric` | `decimal128[38, 10 or max-scale]` | `FIXED_LEN_BYTE_ARRAY` | `DECIMAL` |
| [`real`](/docs/sql/types/float/#real-info) | `materialize.v1.real` | `float32` | `FLOAT` |  |
| [`smallint`](/docs/sql/types/integer/#smallint-info) | `materialize.v1.smallint` | `int16` | `INT32` | `INT(16, true)` |
| [`text`](/docs/sql/types/text/) | `materialize.v1.text` | `utf8` or `large_utf8` | `BYTE_ARRAY` | `STRING` |
| [`time`](/docs/sql/types/time/) | `materialize.v1.time` | `time64[nanosecond]` | `INT64` | `TIME[isAdjustedToUTC = false, unit = NANOS]` |
| [`uint2`](/docs/sql/types/uint/#uint2-info) | `materialize.v1.uint2` | `uint16` | `INT32` | `INT(16, false)` |
| [`uint4`](/docs/sql/types/uint/#uint4-info) | `materialize.v1.uint4` | `uint32` | `INT32` | `INT(32, false)` |
| [`uint8`](/docs/sql/types/uint/#uint8-info) | `materialize.v1.uint8` | `uint64` | `INT64` | `INT(64, false)` |
| [`timestamp`](/docs/sql/types/timestamp/#timestamp-info) | `materialize.v1.timestamp` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]` |
| [`timestamp with time zone`](/docs/sql/types/timestamp/#timestamp-with-time-zone-info) | `materialize.v1.timestampz` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]` |
| [Arrays](/docs/sql/types/array/) (`[]`) | `materialize.v1.array` | `struct` with `list` field `items` and `uint8` field `dimensions` | Nested |  |
| [`uuid`](/docs/sql/types/uuid/) | `materialize.v1.uuid` | `fixed_size_binary(16)` | `FIXED_LEN_BYTE_ARRAY` |  |
| [`oid`](/docs/sql/types/oid/) | Unsupported |  |  |  |
| [`interval`](/docs/sql/types/interval/) | Unsupported |  |  |  |
| [`record`](/docs/sql/types/record/) | Unsupported |  |  |  |

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

## Examples

### Copy to stdout

<div class="highlight">

``` chroma
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

</div>

### Copy to S3

#### File format Parquet

<div class="highlight">

``` chroma
COPY some_view TO 's3://mz-to-snow/parquet/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'parquet'
  );
```

</div>

For `'parquet'` format, Materialize writes Parquet files that aim for
maximum compatibility with downstream systems. The following Parquet
writer settings are used:

| Setting                       | Value            |
|-------------------------------|------------------|
| Writer version                | 1.0              |
| Compression                   | `snappy`         |
| Default column encoding       | Dictionary       |
| Fallback column encoding      | Plain            |
| Dictionary page encoding      | Plain            |
| Dictionary data page encoding | `RLE_DICTIONARY` |

If you encounter issues trying to ingest Parquet files produced by
Materialize into your downstream systems, please [contact our
team](/docs/support/).

See also [Copy to S3: Parquet Data Types](#parquet-data-types).

#### File format CSV

<div class="highlight">

``` chroma
COPY some_view TO 's3://mz-to-snow/csv/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'csv'
  );
```

</div>

For `'csv'` format, Materialize writes CSV files using the following
writer settings:

| Setting   | Value   |
|-----------|---------|
| delimiter | `,`     |
| quote     | `"`     |
| escape    | `"`     |
| header    | `false` |

## Related pages

- [`CREATE CONNECTION`](/docs/sql/create-connection)
- Integration guides:
  - [Amazon S3](/docs/serve-results/s3/)
  - [Snowflake (via S3)](/docs/serve-results/snowflake/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/copy-to.md"
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

© 2026 Materialize Inc.

</div>
