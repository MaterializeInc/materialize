---
title: "COPY FROM"
description: "`COPY FROM` copies data into a table using the COPY protocol."
menu:
    main:
        parent: "commands"
---

`COPY FROM` copies data into a table using the [Postgres `COPY` protocol][pg-copy-from].

## Syntax

{{< diagram "copy-from.svg" >}}

Field | Use
------|-----
_table_name_ | Copy values to this table.
**(**_column_...**)** | Correlate the inserted rows' columns to _table_name_'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. <br/><br/>Without a column list, all columns must have data provided, and will be referenced using their order in the table. With a partial column list, all unreferenced columns will receive their default value.
_s3_uri_ | The unique resource identifier (URI) of the Amazon S3 bucket (and prefix) to store the output results in.
_http_url_ | The unique resource identifier (URI)  (and prefix) to store the output results in.
_field_ | The name of the option you want to set.
_val_ | The value for the option.

### `WITH` options

The following options are valid within the `WITH` clause.

Name | Value type | Default value | Description
-----|-----------------|---------------|------------
`FORMAT` | `TEXT`, `CSV`, `PARQUET` | `TEXT` | Sets the input formatting method. For more information see [Text formatting](#text-formatting), [CSV formatting](#csv-formatting).
`DELIMITER` | Single-quoted one-byte character | Format-dependent | Overrides the format's default column delimiter.
`NULL` | Single-quoted strings | Format-dependent | Specifies the string that represents a _NULL_ value.
`QUOTE` | Single-quoted one-byte character | `"` | Specifies the character to signal a quoted string, which may contain the `DELIMITER` value (without beginning new columns). To include the `QUOTE` character itself in column, wrap the column's value in the `QUOTE` character and prefix all instance of the value you want to literally interpret with the `ESCAPE` value. _`FORMAT CSV` only_
`ESCAPE` | Single-quoted strings | `QUOTE`'s value | Specifies the character to allow instances of the `QUOTE` character to be parsed literally as part of a column's value. _`FORMAT CSV` only_
`HEADER`  | `boolean`   | `boolean`  | Specifies that the file contains a header line with the names of each column in the file. The first line is ignored on input.  _`FORMAT CSV` only._
`AWS CONNECTION` | _connection_name_ |  |  The name of the AWS connection to use in the `COPY FROM` command. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#aws) documentation page. _Only valid with an S3._
`FILES`   | array | A list of files to be appended to the URI. Example: `[ "top.csv", "files/a.csv", "files/b.csv" ]`. _Only valid with S3 OR HTTP._
`PATTERN` | string | A glob used to identify files at at the URI. Example: `"files/**"`. _Only valid with S3 OR HTTP._

Note that `DELIMITER` and `QUOTE` must use distinct values.

## Details

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

Supported PARQUET compression
- snappy
- gzip
- brotli
- zstd
- lz4

TODO:
- Text can be imported as text or JSON/JSONB or a map.. do we document casting rules?
- Parquet has an interval type - can materialize import it?

[Arrow type](https://github.com/apache/arrow/blob/main/format/Schema.fbs) | [Parquet primitive type](https://parquet.apache.org/docs/file-format/types/) | [Parquet logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)    Materialize type
--------------------------------------------------------------------------|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|----------------------------------
`bool`                                                                    | `BOOLEAN`                                                                    |                                                                                              | [`boolean`](/sql/types/boolean/) 
`date32`                                                                  | `INT32`                                                                      | `DATE`                                                                                       | [`date`](/sql/types/date/)
`decimal128[38, 10 or max-scale]`                                         | `FIXED_LEN_BYTE_ARRAY`                                                       | `DECIMAL`                                                                                    | [`numeric`](/sql/types/numeric/)
`fixed_size_binary(16)`                                                   | `FIXED_LEN_BYTE_ARRAY`                                                       |                                                                                              | [`bytea`](/sql/types/bytea/) <--- what about using the UUID logical type?
`float32`                                                                 | `FLOAT`                                                                      |                                                                                              | [`real`](/sql/types/float/#real-info)
`float64`                                                                 | `DOUBLE`                                                                     |                                                                                              | [`double precision`](/sql/types/float/#double-precision-info)
`int16`                                                                   | `INT32`                                                                      | `INT(16, true)`                                                                              | [`smallint`](/sql/types/integer/#smallint-info)
`int32`                                                                   | `INT32`                                                                      |                                                                                              | [`integer`](/sql/types/integer/#integer-info)
`int64`                                                                   | `INT64`                                                                      |                                                                                              | [`bigint`](/sql/types/integer/#bigint-info) 
`large_binary`                                                            | `BYTE_ARRAY`                                                                 |                                                                                              | [`bytea`](/sql/types/bytea/) 
`large_utf8`                                                              | `BYTE_ARRAY`                                                                 |                                                                                              | [`jsonb`](/sql/types/jsonb/)
`list`                                                                    | Nested                                                                       |                                                                                              | [`list`](/sql/types/list/)
TBD `map` (`struct` with fields `keys` and `values`)                          | Nested                                                                       | `MAP`                                                                                        | [`map`](/sql/types/map/)
CUSTOM TYPE `struct`                                                                  | Nested                                                                       |                                                                                              | [Arrays](/sql/types/array/) (`[]`)
`time64[microsecond]`                                                     | `INT64`                                                                      | `TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]`                                          | [`timestamp`](/sql/types/timestamp/#timestamp-info)
`time64[microsecond]`                                                     | `INT64`                                                                      | `TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]`                                           | [`timestamp with time zone`](/sql/types/timestamp/#timestamp-with-time-zone-info)
`time64[nanosecond]`                                                      | `INT64`                                                                      | `TIME[isAdjustedToUTC = false, unit = NANOS]`                                                | [`time`](/sql/types/time/)
`uint16`                                                                  | `INT32`                                                                      | `INT(16, false)`                                                                             | [`uint2`](/sql/types/uint/#uint2-info)
`uint32`                                                                  | `INT32`                                                                      | `INT(32, false)`                                                                             | [`uint4`](/sql/types/uint/#uint4-info)
`uint64`                                                                  | `INT64`                                                                      | `INT(64, false)`                                                                             | [`uint8`](/sql/types/uint/#uint8-info)
`utf8` or `large_utf8`                                                    | `BYTE_ARRAY`                                                                 | `STRING`                                                                                     | [`text`](/sql/types/text/)




We have to add a section about casting!




## Example

```mzsql
COPY t FROM STDIN WITH (DELIMITER '|');
```

```mzsql
COPY t FROM STDIN (FORMAT CSV);
```

```mzsql
COPY t FROM STDIN (DELIMITER '|');
```


## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/copy-from" %}}

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html

## Limits

You can only copy up to 1 GiB of data at a time. If you need this limit increased, please [chat with our team](http://materialize.com/convert-account/).
