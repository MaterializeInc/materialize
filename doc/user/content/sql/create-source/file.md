---
title: "CREATE SOURCE: Local files"
description: "Using Materialize to read from local files"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/files
    - /sql/create-source/text-file
    - /sql/create-source/csv-file
    - /sql/create-source/json-file
    - /sql/create-source/text-file
---

{{% create-source/intro %}}
This page details how to use Materialize to read from local files.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-text.svg" >}}

### key_constraint

{{< diagram "key-constraint.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-connector-details connector="file" envelopes="append-only" %}}

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`tail` | `boolean` | Continually check the file for new content.
`timestamp_frequency_ms`| `int` | Default: `1000`. Sets the timestamping frequency in `ms`. Reflects how frequently the source advances its timestamp. This measure reflects how stale data in views will be. Lower values result in more-up-to-date views but may reduce throughput.

## Supported formats

|<div style="width:290px">Format</div> | Append-only envelope | Upsert envelope | Debezium envelope |
---------------------------------------|:--------------------:|:---------------:|:-----------------:|
| Avro                                 | ✓                    |                 | ✓                 |
| JSON                                 | ✓                    |                 |                   |
| Protobuf                             |                      |                 |                   |
| Text/bytes                           | ✓                    |                 |                   |
| CSV                                  | ✓                    |                 |                   |

## Features

### Tailing files

In addition to reading static files and making the data available for processing downstream, Materialize can continually check a file for new lines and ingest them as they are appended. For a source to implement this behavior, you must configure it to use `WITH (tail = true)`:

```sql
CREATE SOURCE csv_source
  FROM FILE '[path to .csv]'
  WITH (tail = true)
  FORMAT CSV WITH HEADER;
```

### Using regex

If you're dealing with unstructured files, such as server logs, you can structure the content by providing a regular expression using the `REGEX` format specifier. This lets you generate multiple columns from arbitrary file content,
given it has some consistent formatting:

```sql
CREATE SOURCE hex
  FROM FILE '/xxd.log'
  WITH (tail = true)
  FORMAT REGEX '(?P<offset>[0-9a-f]{8}): (?:[0-9a-f]{4} ){8} (?P<decoded>.*)$';
```

It's important to note that Materialize uses the [Rust regex dialect](https://github.com/rust-lang/regex) to parse regex strings, which is similar but not _identical_ to the PostgreSQL regex dialect. For details on the supported syntax, refer to the [regex crate documentation](https://docs.rs/regex/latest/regex/#syntax).

## Examples

### Creating a source

{{< tabs tabID="1" >}}
{{< tab "Avro">}}

To read from a local [Avro Object Container
File](https://avro.apache.org/docs/current/spec.html#Object+Container+Files) (OCF):

```sql
CREATE SOURCE avro_source
  FROM AVRO OCF '[path to .ocf]'
  WITH (tail = true)
  ENVELOPE NONE;
```

This creates a source that...

- Automatically determines its schema from the OCF file's embedded schema.
- Dynamically checks for new entries.
- Is append-only.

{{< /tab >}}
{{< tab "JSON">}}

To read from a local JSON-formatted file:

```sql
CREATE SOURCE json_source
  FROM FILE '/local/path/file.json'
  FORMAT BYTES;
```

```sql
CREATE MATERIALIZED VIEW jsonified_file_source AS
  SELECT
    data->>'field1' AS field_1,
    data->>'field2' AS field_2,
    data->>'field3' AS field_3
  FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM json_source);
```
{{< /tab >}}
{{< tab "Text/bytes">}}

To read from a local text- or byte-formatted file:

```sql
CREATE SOURCE text_source
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  FORMAT TEXT
  USING SCHEMA FILE '/scratch/data.json'
  ENVELOPE UPSERT;
```

As an example, assume we have [`xxd`](https://linux.die.net/man/1/xxd)
creating hex dumps for some incoming files. Its output might look like this:

```nofmt
00000000: 7f45 4c46 0201 0100 0000 0000 0000 0000  .ELF............
00000010: 0300 3e00 0100 0000 105b 0000 0000 0000  ..>......[......
00000020: 4000 0000 0000 0000 7013 0200 0000 0000  @.......p.......
```

To create a source that takes in these entire lines and extracts the file
offset, as well as the decoded value:

```sql
CREATE SOURCE hex
  FROM FILE '/xxd.log'
  WITH (tail = true)
  FORMAT REGEX '(?P<offset>[0-9a-f]{8}): (?:[0-9a-f]{4} ){8} (?P<decoded>.*)$';
```

This creates a source that...

- Is append-only.
- Has two columns: `offset` and `decoded`.
- Discards the second group (i.e., `(?:[0-9a-f]{4} ){8}`).
- Dynamically checks for new entries.

Using the above example, the source would generate data that looks similar to
this:

```nofmt
 offset  |     decoded
---------+------------------
00000000 | .ELF............
00000010 | ..>......[......
00000020 | @.......p.......
```
{{< /tab >}}
{{< tab "CSV">}}

#### Static files

```sql
CREATE SOURCE static_w_header
  FROM FILE '[path to .csv]'
  FORMAT CSV WITH HEADER;
```

This creates a source that...

- Is append-only.
- Materialize reads once during source creation.
- Has as many columns as the first row of the CSV file, and uses that row to name the columns.

#### Dynamic files

```sql
CREATE SOURCE dynamic_wo_header (col_foo, col_bar, col_baz)
  FROM FILE '[path to .csv]'
  WITH (tail = true)
  FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.
- Materialize dynamically checks for new entries.

{{< /tab >}}
{{< /tabs >}}

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
