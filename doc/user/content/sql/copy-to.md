---
title: "COPY TO"
description: "`COPY TO` outputs a query via the COPY protocol."
menu:
    main:
        parent: "commands"
---

`COPY TO` sends rows using the [Postgres COPY protocol](https://www.postgresql.org/docs/current/sql-copy.html).

## Syntax

{{< diagram "copy-to.svg" >}}

Field | Use
------|-----
_query_ | The [`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe) query to send
_field_ | The name of the option you want to set.
_val_ | The value for the option.

### `WITH` options

Name | Value type | Default value | Description
----------------------------|--------|--------|--------
`FORMAT` | `TEXT`,`BINARY` | `TEXT` | Sets the output formatting method.

## Example

### Copying a view

```sql
COPY (SELECT * FROM some_view) TO STDOUT;
```

### Subscribing to a view with binary output

```sql
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `SELECT` privileges on all relations in the query.
    - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
      execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
      granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.


<!--

## Parquet Format

### Parquet Compatibility

Materialize writes Parquet files that aim for maximum compatibility with downstream systems.

By default the following Parquet writer settings are used:

Option | Setting
-------|--------
Writer Version | 1.0
Compression | snappy
Default Column Encoding | Dictionary
Fallback Column Encoding | Plain
Dictionary Page Encoding | Plain
Dictionary Data Page Encoding | RLE_DICTIONARY

### Data Types

Materialize converts the values in the result set to [Apache Arrow](https://arrow.apache.org/docs/index.html), and then serializes
this Arrow representation to Parquet.
The Arrow schema is embedded in the Parquet file metadata and allows reconstruction of the Arrow representation using a compatible reader.

Materialize will also include [Parquet Logical Type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) annotations where possible, however
many newer Logical Type annotations are not supported in Parquet Writer Version 1.0.

Materialize also embeds its own type information into the Apache Arrow schema. The field metadata in the schema contains an `ARROW:extension:name` annotation to indicate the Materialize native type the field came from.

Materialize Type | Arrow Extension Name | [Arrow Type](https://github.com/apache/arrow/blob/main/format/Schema.fbs) | [Parquet Primitive Type](https://parquet.apache.org/docs/file-format/types/) | [Parquet Logical Type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)
-----------------|----------------|------------|-------------------|--------------
[`bigint`](types/integer) | `materialize.v1.bigint` | `int64` | `INT64`
[`boolean`](types/boolean) | `materialize.v1.boolean` | `bool` | `BOOLEAN`
[`bytea`](types/bytea) | `materialize.v1.bytea` | `large_binary` | `BYTE_ARRAY`
[`date`](types/date) | `materialize.v1.date` | `date32` | `INT32` | `DATE`
[`double precision`](types/float) | `materialize.v1.double` | `float64` | `DOUBLE`
[`integer`](types/integer) | `materialize.v1.integer` | `int32` | `INT32`
[`jsonb`](types/jsonb) | `materialize.v1.jsonb` | `large_utf8` | `BYTE_ARRAY`
[`map`](types/map) | `materialize.v1.map` | `map` (`struct` with fields `keys` and `values`) | Nested | `MAP`
[`list`](types/list) | `materialize.v1.list` | `list` | Nested
[`numeric`](types/numeric) | `materialize.v1.numeric` | `decimal128[38, 10 or max-scale]` | `FIXED_LEN_BYTE_ARRAY` | `DECIMAL`
[`real`](types/float) | `materialize.v1.real` | `float32` | `FLOAT`
[`smallint`](types/integer) | `materialize.v1.smallint` | `int16` | `INT32` | `INT(16, true)`
[`text`](types/text) | `materialize.v1.text` | `utf8` or `large_utf8` | `BYTE_ARRAY` | `STRING`
[`time`](types/time) | `materialize.v1.time` | `time64[nanosecond]` | `INT64` | `TIME[isAdjustedToUTC = false, unit = NANOS]`
[`uint2`](types/uint) | `materialize.v1.uint2` | `uint16` | `INT32` | `INT(16, false)`
[`uint4`](types/uint) | `materialize.v1.uint4` | `uint32` | `INT32` | `INT(32, false)`
[`uint8`](types/uint) | `materialize.v1.uint8` | `uint64` | `INT64` | `INT(64, false)`
[`timestamp`](types/timestamp) | `materialize.v1.timestamp` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]`
[`timestamp with time zone`](types/timestamp) | `materialize.v1.timestampz` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]`
[Arrays](types/array) (`[]`) | `materialize.v1.array` | `struct` with `list` field `items` and `uint8` field `dimensions` | Nested
[`uuid`](types/uuid) | `materialize.v1.uuid` | `fixed_size_binary(16)` | `FIXED_LEN_BYTE_ARRAY`
[`oid`](oid) | Unsupported
[`interval`](interval) | Unsupported
[`record`](record) | Unsupported

-->
