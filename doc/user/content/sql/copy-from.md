---
title: "COPY FROM"
description: "`COPY FROM` copies data into a table using the COPY protocol."
menu:
    main:
        parent: "commands"
---

`COPY FROM` copies data into a table using the [Postgres `COPY` protocol][pg-copy-from].

## Syntax

{{< tabs >}}

{{< tab "Copy from STDIN" >}}
{{% include-syntax file="examples/copy_from" example="syntax" %}}
{{< /tab >}}

{{< tab "Copy from S3 and S3 compatible services" >}}
{{% include-syntax file="examples/copy_from_s3" example="syntax" %}}
{{< /tab >}}

{{< /tabs >}}

## Details

### S3 Bucket IAM Policies

To use `COPY FROM` with S3, you need to allow the following actions in your IAM policy:

| Action type | Action name                                                                               | Action description                                                |
| ----------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| Read        | [`s3:GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)      | Grants permission to retrieve an object from a bucket.            |
| List        | [`s3:ListBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html) | Grants permission to list some or all of the objects in a bucket. |

{{< note >}}
For S3-compatible object storage services (e.g., Google Cloud Storage, Cloudflare R2, MinIO),
you need to enable equivalent permissions on the service you are using. The specific
configuration steps will vary by provider, but the access credentials must allow the same
read and list operations on the target bucket.
{{< /note >}}

### Text formatting

As described in the **Text Format** section of [PostgreSQL's documentation][pg-copy-from].

### CSV formatting

As described in the **CSV Format** section of [PostgreSQL's documentation][pg-copy-from]
except that:

- More than one layer of escaped quote characters returns the wrong result.

- Quote characters must immediately follow a delimiter to be treated as
  expected.

- Single-column rows containing quoted end-of-data markers (e.g. `"\."`) will be
  treated as end-of-data markers despite being quoted. In PostgreSQL, this data
  would be escaped and would not terminate the data processing.

- Quoted null strings will be parsed as nulls, despite being quoted. In
  PostgreSQL, this data would be escaped.

    To ensure proper null handling, we recommend specifying a unique string for
    null values, and ensuring it is never quoted.

- Unterminated quotes are allowed, i.e. they do not generate errors. In
  PostgreSQL, all open unescaped quotation punctuation must have a matching
  piece of unescaped quotation punctuation or it generates an error.

### PARQUET formatting

Supported PARQUET compression formats

- snappy
- gzip
- brotli
- zstd
- lz4

{{< comment >}}
TODO:

- Text can be imported as text or JSON/JSONB or a map.. do we document casting rules/make a whole section for casting?
- Parquet has an interval type - can materialize import it?
  {{< /comment >}}

| [Arrow type](https://github.com/apache/arrow/blob/main/format/Schema.fbs) | [Parquet primitive type](https://parquet.apache.org/docs/file-format/types/) | [Parquet logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) | Materialize type                                                                  |
| ------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `bool`                                                                    | `BOOLEAN`                                                                    |                                                                                              | [`boolean`](/sql/types/boolean/)                                                  |
| `date32`                                                                  | `INT32`                                                                      | `DATE`                                                                                       | [`date`](/sql/types/date/)                                                        |
| `decimal128[38, 10 or max-scale]`                                         | `FIXED_LEN_BYTE_ARRAY`                                                       | `DECIMAL`                                                                                    | [`numeric`](/sql/types/numeric/)                                                  |
| `fixed_size_binary(16)`                                                   | `FIXED_LEN_BYTE_ARRAY`                                                       |                                                                                              | [`bytea`](/sql/types/bytea/)                                                      |
| `float32`                                                                 | `FLOAT`                                                                      |                                                                                              | [`real`](/sql/types/float/#real-info)                                             |
| `float64`                                                                 | `DOUBLE`                                                                     |                                                                                              | [`double precision`](/sql/types/float/#double-precision-info)                     |
| `int16`                                                                   | `INT32`                                                                      | `INT(16, true)`                                                                              | [`smallint`](/sql/types/integer/#smallint-info)                                   |
| `int32`                                                                   | `INT32`                                                                      |                                                                                              | [`integer`](/sql/types/integer/#integer-info)                                     |
| `int64`                                                                   | `INT64`                                                                      |                                                                                              | [`bigint`](/sql/types/integer/#bigint-info)                                       |
| `large_binary`                                                            | `BYTE_ARRAY`                                                                 |                                                                                              | [`bytea`](/sql/types/bytea/)                                                      |
| `large_utf8`                                                              | `BYTE_ARRAY`                                                                 |                                                                                              | [`jsonb`](/sql/types/jsonb/)                                                      |
| `list`                                                                    | Nested                                                                       |                                                                                              | [`list`](/sql/types/list/)                                                        |
| `struct`                                                                  | Nested                                                                       |                                                                                              | [Arrays](/sql/types/array/) (`[]`)                                                |
| `time64[microsecond]`                                                     | `INT64`                                                                      | `TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]`                                          | [`timestamp`](/sql/types/timestamp/#timestamp-info)                               |
| `time64[microsecond]`                                                     | `INT64`                                                                      | `TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]`                                           | [`timestamp with time zone`](/sql/types/timestamp/#timestamp-with-time-zone-info) |
| `time64[nanosecond]`                                                      | `INT64`                                                                      | `TIME[isAdjustedToUTC = false, unit = NANOS]`                                                | [`time`](/sql/types/time/)                                                        |
| `uint16`                                                                  | `INT32`                                                                      | `INT(16, false)`                                                                             | [`uint2`](/sql/types/uint/#uint2-info)                                            |
| `uint32`                                                                  | `INT32`                                                                      | `INT(32, false)`                                                                             | [`uint4`](/sql/types/uint/#uint4-info)                                            |
| `uint64`                                                                  | `INT64`                                                                      | `INT(64, false)`                                                                             | [`uint8`](/sql/types/uint/#uint8-info)                                            |
| `utf8` or `large_utf8`                                                    | `BYTE_ARRAY`                                                                 | `STRING`                                                                                     | [`text`](/sql/types/text/)                                                        |
|                                                                           |                                                                              | `map`                                                                                        | unsupported                                                                       |
|                                                                           |                                                                              | `interval`                                                                                   | unsupported                                                                       |

### Limits

You can copy up to 10 GiB of data at a time. If you need to copy more than that, please [contact support](/support/).

When importing parquet files, entire row groups are held in memory at once, so ensure that your
Materialize instance has enough available memory to accomodate your parquet files. If you are
encountering memory issues, and are unable to reduce the sizes of your row groups, please [contact support](/support/).

## Examples

### From STDIN

```mzsql
COPY t FROM STDIN WITH (DELIMITER '|');
```

```mzsql
COPY t FROM STDIN (FORMAT CSV);
```

```mzsql
COPY t FROM STDIN (DELIMITER '|');
```

### From AWS S3

#### Using AWS connection

Perform bulk import:

Using `FILES` option:

```mzsql
COPY INTO csv_table FROM 's3://example_bucket' (FORMAT CSV, AWS CONNECTION = example_aws_conn, FILES = ['example_data.csv']);
```

Using the full s3 URI:

```mzsql
COPY INTO csv_table FROM 's3://example_bucket/example_data.csv' (FORMAT CSV, AWS CONNECTION = example_aws_conn);
```

Using `PATTERN` option:

```mzsql
COPY INTO parquet_table FROM 's3://example_bucket' (FORMAT PARQUET, AWS CONNECTION = example_aws_conn, PATTERN = '*parquet*');
```

#### Using S3-compatible object storage

You can use `COPY FROM` with any S3-compatible object storage service, such as
Google Cloud Storage, Cloudflare R2, or MinIO. First,
[create an AWS connection for S3-compatible storage](/sql/create-connection/#s3-compatible-object-storage),
then use it in the `COPY` command. Make sure your credentials have the necessary
permissions as described in [S3 Bucket IAM Policies](#s3-bucket-iam-policies).

```mzsql
COPY INTO csv_table FROM 's3://my_bucket/my_data.csv' (FORMAT CSV, AWS CONNECTION = gcs_connection);
```

#### Using presigned URL

```mzsql
COPY INTO csv_table FROM '<s3 presigned URL>' (FORMAT CSV);
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/copy-from" %}}

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html
