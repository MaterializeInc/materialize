# COPY TO
`COPY TO` outputs results from Materialize to standard output or object storage.
`COPY TO` outputs results from Materialize to standard output or object storage.
This command is useful to output [`SUBSCRIBE`](/sql/subscribe/) results
[to `stdout`](#copy-to-stdout), or perform [bulk exports to Amazon S3](#copy-to-s3).

## Syntax

**Copy to stdout:**
### Copy to `stdout`  {#copy-to-stdout}

Copying results to `stdout` is useful to output the stream of updates from a
[`SUBSCRIBE`](/sql/subscribe/) command in interactive SQL clients like `psql`.



```mzsql
COPY ( <query> ) TO STDOUT [WITH ( <option> = <val> )];

```

| Syntax element | Description |
| --- | --- |
| `<query>` | The [`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe) query whose results are copied.  |
| `WITH ( <option> = <val> )` | Optional. The following `<option>` are supported: \| Name \|  Description \| \|------\|---------------\| `FORMAT` \| Sets the output format. Valid output formats are: `TEXT`,`BINARY`, `CSV`.<br><br> Default: `TEXT`.  |



**Copy to Amazon S3 and S3 compatible services:**
### Copy to Amazon S3 and S3 compatible services {#copy-to-s3}

Copying results to Amazon S3 (or S3-compatible services) is useful to perform
tasks like periodic backups for auditing, or downstream processing in
analytical data warehouses like Snowflake, Databricks or BigQuery. For
step-by-step instructions, see the integration guide for [Amazon S3](/serve-results/s3/).

The `COPY TO` command is _one-shot_: every time you want to export results, you
must run the command. To automate exporting results on a regular basis, you can
set up scheduling, for example using a simple `cron`-like service or an
orchestration platform like Airflow or Dagster.



```mzsql
COPY <query> TO '<s3_uri>'
WITH (
  AWS CONNECTION = <connection_name>,
  FORMAT = <format>
  [, MAX FILE SIZE = <size> ]
);

```

| Syntax element | Description |
| --- | --- |
| `<query>` | The [`SELECT`](/sql/select) query whose results are copied.  |
| `<s3_uri>` | The unique resource identifier (URI) of the Amazon S3 bucket (and prefix) to store the output results in.  |
| `AWS CONNECTION = <connection_name>` | The name of the AWS connection to use in the `COPY TO` command. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#aws) documentation page.  |
| `FORMAT = '<format>'` | The file format to write. Valid formats are `'csv'` and `'parquet'`.  - {{< include-from-yaml data="examples/copy_to" name="csv-writer-settings" >}}  - {{< include-from-yaml data="examples/copy_to" name="parquet-writer-settings" >}}  |
| [`MAX FILE SIZE = <size>`] | Optional. Sets the approximate maximum file size (in bytes) of each file uploaded to the S3 bucket.  |






## Details

### Copy to S3: CSV {#copy-to-s3-csv}

#### Writer settings

<p>For <code>'csv'</code> format, Materialize writes CSV files using the following
writer settings:</p>
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
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>escape</td>
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>header</td>
          <td><code>false</code></td>
      </tr>
  </tbody>
</table>

### Copy to S3: Parquet {#copy-to-s3-parquet}

#### Writer settings

<p>For <code>'parquet'</code> format, Materialize writes Parquet files that aim for
maximum compatibility with downstream systems. The following Parquet
writer settings are used:</p>
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
Materialize into your downstream systems, please <a href="/support/" >contact our
team</a>.</p>


#### Parquet data types

<p>When using the <code>parquet</code> format, Materialize converts the values in the
result set to <a href="https://arrow.apache.org/docs/index.html" >Apache Arrow</a>,
and then serializes this Arrow representation to Parquet. The Arrow schema is
embedded in the Parquet file metadata and allows reconstructing the Arrow
representation using a compatible reader.</p>
<p>Materialize also includes <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#metadata" >Parquet <code>LogicalType</code> annotations</a>
where possible. However, many newer <code>LogicalType</code> annotations are not supported
in the 1.0 writer version.</p>
<p>Materialize also embeds its own type information into the Apache Arrow schema.
The field metadata in the schema contains an <code>ARROW:extension:name</code> annotation
to indicate the Materialize native type the field originated from.</p>
<table>
  <thead>
      <tr>
          <th>Materialize type</th>
          <th>Arrow extension name</th>
          <th><a href="https://github.com/apache/arrow/blob/main/format/Schema.fbs" >Arrow type</a></th>
          <th><a href="https://parquet.apache.org/docs/file-format/types/" >Parquet primitive type</a></th>
          <th><a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md" >Parquet logical type</a></th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/sql/types/integer/#bigint-info" ><code>bigint</code></a></td>
          <td><code>materialize.v1.bigint</code></td>
          <td><code>int64</code></td>
          <td><code>INT64</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/boolean/" ><code>boolean</code></a></td>
          <td><code>materialize.v1.boolean</code></td>
          <td><code>bool</code></td>
          <td><code>BOOLEAN</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/bytea/" ><code>bytea</code></a></td>
          <td><code>materialize.v1.bytea</code></td>
          <td><code>large_binary</code></td>
          <td><code>BYTE_ARRAY</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/date/" ><code>date</code></a></td>
          <td><code>materialize.v1.date</code></td>
          <td><code>date32</code></td>
          <td><code>INT32</code></td>
          <td><code>DATE</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/float/#double-precision-info" ><code>double precision</code></a></td>
          <td><code>materialize.v1.double</code></td>
          <td><code>float64</code></td>
          <td><code>DOUBLE</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/integer/#integer-info" ><code>integer</code></a></td>
          <td><code>materialize.v1.integer</code></td>
          <td><code>int32</code></td>
          <td><code>INT32</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/jsonb/" ><code>jsonb</code></a></td>
          <td><code>materialize.v1.jsonb</code></td>
          <td><code>large_utf8</code></td>
          <td><code>BYTE_ARRAY</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/map/" ><code>map</code></a></td>
          <td><code>materialize.v1.map</code></td>
          <td><code>map</code> (<code>struct</code> with fields <code>keys</code> and <code>values</code>)</td>
          <td>Nested</td>
          <td><code>MAP</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/list/" ><code>list</code></a></td>
          <td><code>materialize.v1.list</code></td>
          <td><code>list</code></td>
          <td>Nested</td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/numeric/" ><code>numeric</code></a></td>
          <td><code>materialize.v1.numeric</code></td>
          <td><code>decimal128[38, 10 or max-scale]</code></td>
          <td><code>FIXED_LEN_BYTE_ARRAY</code></td>
          <td><code>DECIMAL</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/float/#real-info" ><code>real</code></a></td>
          <td><code>materialize.v1.real</code></td>
          <td><code>float32</code></td>
          <td><code>FLOAT</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/integer/#smallint-info" ><code>smallint</code></a></td>
          <td><code>materialize.v1.smallint</code></td>
          <td><code>int16</code></td>
          <td><code>INT32</code></td>
          <td><code>INT(16, true)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/text/" ><code>text</code></a></td>
          <td><code>materialize.v1.text</code></td>
          <td><code>utf8</code> or <code>large_utf8</code></td>
          <td><code>BYTE_ARRAY</code></td>
          <td><code>STRING</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/time/" ><code>time</code></a></td>
          <td><code>materialize.v1.time</code></td>
          <td><code>time64[nanosecond]</code></td>
          <td><code>INT64</code></td>
          <td><code>TIME[isAdjustedToUTC = false, unit = NANOS]</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uint/#uint2-info" ><code>uint2</code></a></td>
          <td><code>materialize.v1.uint2</code></td>
          <td><code>uint16</code></td>
          <td><code>INT32</code></td>
          <td><code>INT(16, false)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uint/#uint4-info" ><code>uint4</code></a></td>
          <td><code>materialize.v1.uint4</code></td>
          <td><code>uint32</code></td>
          <td><code>INT32</code></td>
          <td><code>INT(32, false)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uint/#uint8-info" ><code>uint8</code></a></td>
          <td><code>materialize.v1.uint8</code></td>
          <td><code>uint64</code></td>
          <td><code>INT64</code></td>
          <td><code>INT(64, false)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/timestamp/#timestamp-info" ><code>timestamp</code></a></td>
          <td><code>materialize.v1.timestamp</code></td>
          <td><code>time64[microsecond]</code></td>
          <td><code>INT64</code></td>
          <td><code>TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/timestamp/#timestamp-with-time-zone-info" ><code>timestamp with time zone</code></a></td>
          <td><code>materialize.v1.timestampz</code></td>
          <td><code>time64[microsecond]</code></td>
          <td><code>INT64</code></td>
          <td><code>TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/array/" >Arrays</a> (<code>[]</code>)</td>
          <td><code>materialize.v1.array</code></td>
          <td><code>struct</code> with <code>list</code> field <code>items</code> and <code>uint8</code> field <code>dimensions</code></td>
          <td>Nested</td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uuid/" ><code>uuid</code></a></td>
          <td><code>materialize.v1.uuid</code></td>
          <td><code>fixed_size_binary(16)</code></td>
          <td><code>FIXED_LEN_BYTE_ARRAY</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/oid/" ><code>oid</code></a></td>
          <td>Unsupported</td>
          <td></td>
          <td></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/interval/" ><code>interval</code></a></td>
          <td>Unsupported</td>
          <td></td>
          <td></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/record/" ><code>record</code></a></td>
          <td>Unsupported</td>
          <td></td>
          <td></td>
          <td></td>
      </tr>
  </tbody>
</table>


## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `SELECT` privileges on all relations in the query.
    - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
      execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
      granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Examples

### Copy to stdout {#copy-to-stdout-examples}

```mzsql
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

### Copy to S3 {#copy-to-s3-examples}

#### File format Parquet

```mzsql
COPY some_view TO 's3://mz-to-snow/parquet/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'parquet'
  );
```

<p>For <code>'parquet'</code> format, Materialize writes Parquet files that aim for
maximum compatibility with downstream systems. The following Parquet
writer settings are used:</p>
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
Materialize into your downstream systems, please <a href="/support/" >contact our
team</a>.</p>


See also [Copy to S3: Parquet Data Types](#parquet-data-types).

#### File format CSV

```mzsql
COPY some_view TO 's3://mz-to-snow/csv/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'csv'
  );
```

<p>For <code>'csv'</code> format, Materialize writes CSV files using the following
writer settings:</p>
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
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>escape</td>
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>header</td>
          <td><code>false</code></td>
      </tr>
  </tbody>
</table>


## Related pages

- [`CREATE CONNECTION`](/sql/create-connection)
- Integration guides:
  - [Amazon S3](/serve-results/s3/)
  - [Snowflake (via S3)](/serve-results/snowflake/)
