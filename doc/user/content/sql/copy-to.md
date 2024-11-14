---
title: "COPY TO"
description: "`COPY TO` outputs results from Materialize to standard output or object storage."
menu:
    main:
        parent: "commands"
---

`COPY TO` outputs results from Materialize to standard output or object storage.
This command is useful to output [`SUBSCRIBE`](/sql/subscribe/) results
[to `stdout`](#copy-to-stdout), or perform [bulk exports to Amazon S3](#copy-to-s3).

## Copy to `stdout`

Copying results to `stdout` is useful to output the stream of updates from a
[`SUBSCRIBE`](/sql/subscribe/) command in interactive SQL clients like `psql`.

### Syntax {#copy-to-stdout-syntax}

{{< diagram "copy-to-stdout.svg" >}}

Field         | Use
--------------|-----
_query_       | The [`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe) query to output results for.
_field_       | The name of the option you want to set.
_val_         | The value for the option.

### `WITH` options {#copy-to-stdout-with-options}

Name     | Values                 | Default value | Description
---------|------------------------|---------------|-----------------------------------
`FORMAT` | `TEXT`,`BINARY`, `CSV` | `TEXT`        | Sets the output formatting method.

### Examples {#copy-to-stdout-examples}

#### Subscribing to a view with binary output

```mzsql
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

## Copy to Amazon S3 {#copy-to-s3}

{{< public-preview />}}

Copying results to Amazon S3 (or S3-compatible services) is useful to perform
tasks like periodic backups for auditing, or downstream processing in
analytical data warehouses like Snowflake, Databricks or BigQuery. For
step-by-step instructions, see the integration guide for [Amazon S3](/serve-results/s3/).

The `COPY TO` command is _one-shot_: every time you want to export results, you
must run the command. To automate exporting results on a regular basis, you can
set up scheduling, for example using a simple `cron`-like service or an
orchestration platform like Airflow or Dagster.

### Syntax {#copy-to-s3-syntax}

{{< diagram "copy-to-s3.svg" >}}

Field         | Use
--------------|-----
_query_       | The [`SELECT`](/sql/select) query to copy results out for.
_object_name_ | The name of the object to copy results out for.
**AWS CONNECTION** _connection_name_ | The name of the AWS connection to use in the `COPY TO` command. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#aws) documentation page.
_s3_uri_      | The unique resource identifier (URI) of the Amazon S3 bucket (and prefix) to store the output results in.
**FORMAT**    | The file format to write.
_field_       | The name of the option you want to set.
_val_         | The value for the option.

### `WITH` options {#copy-to-s3-with-options}

Name             | Values          | Default value | Description                       |
-----------------|-----------------|---------------|-----------------------------------|
`MAX FILE SIZE`  | `integer`       |               | Sets the approximate maximum file size (in bytes) of each file uploaded to the S3 bucket. |

### Supported formats {#copy-to-s3-supported-formats}

#### CSV {#copy-to-s3-csv}

**Syntax:** `FORMAT = 'csv'`

By default, Materialize writes CSV files using the following writer settings:

Setting     | Value
------------|---------------
delimiter   | `,`
quote       | `"`
escape      | `"`
header      | `false`

#### Parquet {#copy-to-s3-parquet}

**Syntax:** `FORMAT = 'parquet'`

Materialize writes Parquet files that aim for maximum compatibility with
downstream systems. By default, the following Parquet writer settings are
used:

Setting                       | Value
------------------------------|---------------
Writer version                | 1.0
Compression                   | `snappy`
Default column encoding       | Dictionary
Fallback column encoding      | Plain
Dictionary page encoding      | Plain
Dictionary data page encoding | `RLE_DICTIONARY`

If you run into a snag trying to ingest Parquet files produced by Materialize
into your downstream systems, please [contact our team](https://materialize.com/docs/support/)
or [open a bug report](https://github.com/MaterializeInc/materialize/discussions/new?category=bug-reports)!

##### Data types {#copy-to-s3-parquet-data-types}

Materialize converts the values in the result set to [Apache Arrow](https://arrow.apache.org/docs/index.html),
and then serializes this Arrow representation to Parquet. The Arrow schema is
embedded in the Parquet file metadata and allows reconstructing the Arrow
representation using a compatible reader.

Materialize also includes [Parquet `LogicalType` annotations](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#metadata)
where possible. However, many newer `LogicalType` annotations are not supported
in the 1.0 writer version.

Materialize also embeds its own type information into the Apache Arrow schema.
The field metadata in the schema contains an `ARROW:extension:name` annotation
to indicate the Materialize native type the field originated from.

Materialize type | Arrow extension name | [Arrow type](https://github.com/apache/arrow/blob/main/format/Schema.fbs) | [Parquet primitive type](https://parquet.apache.org/docs/file-format/types/) | [Parquet logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)
----------------------------------|----------------------------|------------|-------------------|--------------
[`bigint`](/sql/types/integer/#bigint-info)         | `materialize.v1.bigint`    | `int64` | `INT64`
[`boolean`](/sql/types/boolean/)        | `materialize.v1.boolean`   | `bool` | `BOOLEAN`
[`bytea`](/sql/types/bytea/)            | `materialize.v1.bytea`     | `large_binary` | `BYTE_ARRAY`
[`date`](/sql/types/date/)              | `materialize.v1.date`      | `date32` | `INT32` | `DATE`
[`double precision`](/sql/types/float/#double-precision-info) | `materialize.v1.double`    | `float64` | `DOUBLE`
[`integer`](/sql/types/integer/#integer-info)        | `materialize.v1.integer`   | `int32` | `INT32`
[`jsonb`](/sql/types/jsonb/)            | `materialize.v1.jsonb`     | `large_utf8` | `BYTE_ARRAY`
[`map`](/sql/types/map/)                | `materialize.v1.map`       | `map` (`struct` with fields `keys` and `values`) | Nested | `MAP`
[`list`](/sql/types/list/)              | `materialize.v1.list`      | `list` | Nested
[`numeric`](/sql/types/numeric/)        | `materialize.v1.numeric`   | `decimal128[38, 10 or max-scale]` | `FIXED_LEN_BYTE_ARRAY`             | `DECIMAL`
[`real`](/sql/types/float/#real-info)             | `materialize.v1.real`      | `float32` | `FLOAT`
[`smallint`](/sql/types/integer/#smallint-info)       | `materialize.v1.smallint`  | `int16` | `INT32` | `INT(16, true)`
[`text`](/sql/types/text/)              | `materialize.v1.text`      | `utf8` or `large_utf8` | `BYTE_ARRAY` | `STRING`
[`time`](/sql/types/time/)              | `materialize.v1.time`      | `time64[nanosecond]` | `INT64` | `TIME[isAdjustedToUTC = false, unit = NANOS]`
[`uint2`](/sql/types/uint/#uint2-info)             | `materialize.v1.uint2`     | `uint16` | `INT32` | `INT(16, false)`
[`uint4`](/sql/types/uint/#uint4-info)             | `materialize.v1.uint4`     | `uint32` | `INT32` | `INT(32, false)`
[`uint8`](/sql/types/uint/#uint8-info)             | `materialize.v1.uint8`     | `uint64` | `INT64` | `INT(64, false)`
[`timestamp`](/sql/types/timestamp/#timestamp-info)    | `materialize.v1.timestamp` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]`
[`timestamp with time zone`](/sql/types/timestamp/#timestamp-with-time-zone-info) | `materialize.v1.timestampz` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]`
[Arrays](/sql/types/array/) (`[]`)      | `materialize.v1.array`     | `struct` with `list` field `items` and `uint8` field `dimensions` | Nested
[`uuid`](/sql/types/uuid/)              | `materialize.v1.uuid`      | `fixed_size_binary(16)` | `FIXED_LEN_BYTE_ARRAY`
[`oid`](/sql/types/oid/)                      | Unsupported
[`interval`](/sql/types/interval/)            | Unsupported
[`record`](/sql/types/record/)                | Unsupported

### Examples {#copy-to-s3-examples}

{{< tabs >}}
{{< tab "Parquet">}}

```mzsql
COPY some_view TO 's3://mz-to-snow/parquet/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'parquet'
  );
```

{{< /tab >}}

{{< tab "CSV">}}

```mzsql
COPY some_view TO 's3://mz-to-snow/csv/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'csv'
  );
```

{{< /tab >}}
{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `SELECT` privileges on all relations in the query.
    - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
      execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
      granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Related pages

- [`CREATE CONNECTION`](/sql/create-connection)
- Integration guides:
  - [Amazon S3](/serve-results/s3/)
  - [Snowflake (via S3)](/serve-results/snowflake/)
