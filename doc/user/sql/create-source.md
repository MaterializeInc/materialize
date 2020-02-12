---
title: "CREATE SOURCE"
description: "`CREATE SOURCE` connects Materialize to an external data source."
menu:
  main:
    parent: 'sql'
aliases:
    - /docs/sql/create-source
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

## Conceptual framework

To provide data to Materialize, you must create "sources", which is a catchall
term for a resource Materialize can read data from. For more detail about how
sources work within the rest of Materialize, check out our [architecture
overview](/docs/overview/architecture/).

Sources consist of three distinct elements:

Element | Purpose | Example
--------|---------|--------
**Connector** | Provides actual bytes of data to Materialize | Kafka
**Format** | The structure of the external source's bytes | Avro
**Envelope** | How Materialize should handle the incoming data + additional formatting information | Append-only

### Connectors

Materialize can connect to the following types of sources:

- Streaming sources like Kafka
- File sources like `.csv` or unstructured log files

### Formats

Materialize can decode incoming bytes of data from several formats

- Avro
- Protobuf
- Regex
- CSV
- Plain text
- Raw bytes

### Envelopes

What Materialize actually does with the data it receives depends on the
"envelope" your data provides:

Envelope | Action
---------|-------
**Append-only** | Inserts all received data; does not support updates or deletes.
**Debezium** | Treats data as wrapped in a "diff envelope" which indicates whether the record is an insertion, deletion, or update. The Debezium envelope is only supported by sources published to Kafka by [Debezium].

For more information about envelopes, see [Envelope details](#envelope-details).

## Syntax

### Create source

`CREATE SOURCE` can be used to create streaming or file sources.

{{< diagram "create-source.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
**FROM** _connector&lowbar;spec_ | A specification of how to connect to the external resource providing the data. For more detail, see [Connector specifications](#connector-spec).
**FORMAT** _format&lowbar;spec_ | A description of the format of data in the source. For more detail, see [Format specifications](#format-spec).
**ENVELOPE** _envelope_ | The envelope type.<br/><br/> &#8226; **NONE** implies that each record appends to the source. <br/><br/>&#8226; **DEBEZIUM** requires records have the [appropriate fields](#format-implications), which allow deletes, inserts, and updates. The Debezium envelope is only supported by sources published to Kafka by [Debezium].<br/><br/>For more information, see [Debezium envelope details](#debezium-envelope-details).

### Connector specifications

{{< diagram "connector-spec.html" >}}

Field | Value
------|-----
**FILE** _path_ | The absolute path to the file you want to use as the source.
**KAFKA BROKER** _host_ | The Kafka broker's host name.
**TOPIC** _topic_ | The Kafka topic to ingest from.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).

#### `WITH` options

The following options are valid within the `WITH` clause.

Field | Value | Description
------|-------|------------
`tail` | `bool` | Continually check the file for new content; as new content arrives, process it using other `WITH` options. (Only valid for file sources).

### Format specifications

{{< diagram "format-spec.html" >}}

Field | Value
------|-----
**AVRO...** | Format the source using Avro.
_avro&lowbar;schema&lowbar;spec_ | The Avro schema. For more details, see [Avro Schema Specifications](#avro-schema-specifications).
**PROTOBUF...** | Format the source using Protobuf.
_message&lowbar;name_ | The top-level Protobuf message name, in the format `<package>.<message name>`. For example, `billing.Batch`
_schema&lowbar;spec_ | The format/schema for the source's data. For more details, see [Standard schema specifications](#standard-schema-specifications).
**REGEX** _regex_ | Format the source's data as a string, applying _regex_, whose capture groups define the columns of the relation. For more detail, see [Regex format details](#regex-format-details).
**CSV WITH** _n_ | Format the source's data as a CSV with _n_ columns. Any data without _n_ columns is not propagated to the source.
**DELIMITED BY** _char_ | Delimit the CSV by _char_. ASCII comma by default (`','`). This must be an ASCII character; other Unicode code points are not supported.
**TEXT** | Format the source's data as ASCII-encoded text.
**BYTES** | Format the source's data as unformatted bytes.

For more information about formats, see [Format details](#format-details).

### Avro schema specifications

{{< diagram "avro-schema-spec.html" >}}

Field | Value
------|-----
_url_ | The URL of the Confluent schema registry to get schema information from.
_schema&lowbar;spec_ | The format/schema for the source's data. For more details, see [Standard schema specifications](#standard-schema-specifications).

### Standard schema specifications

{{< diagram "schema-spec.html" >}}

Field | Value
------|-----
_schema&lowbar;file&lowbar;path_ | The absolute path to a file containing the schema.
_inline&lowbar;schema_ | A string representing the schema.

## External source details

External sources provide the actual bytes of data to Materialize.

### Kafka source details

Materialize expects each source to use to one Kafka topic, which is&mdash;in
  turn&mdash;generated by a single table in an upstream database.

### File source details

- `path` values must be the file's absolute path, e.g.
    ```sql
    CREATE SOURCE server_source FROM FILE '/Users/sean/server.log'...
    ```
- All data in file sources are treated as [`string`](./data-types/string).

## Format details

### Avro format details

Avro-formatted external sources require providing you providing the schema in
one of two ways:

- Using the [Confluent Schema Registry](#using-a-confluent-schema-registry)
- Providing the Avro schema [in-line when creating the
  source](#inlining-the-avro-schema).

### Protobuf format details

Protobuf-formatted external sources require:

- The `FileDescriptorSet`, which encodes the Protobuf messages' schema. You
  can generate the `FileDescriptorSet` with `protoc`, e.g.
    ```shell
    protoc --include_imports --descriptor_set_out=SCHEMA billing.proto
    ```
- The name of the top-level message to decode, in the following format:

    <!--clojure was chosen because of its specific formatting in this scenario-->
    ```clojure
    .<package name>.<top-level message>
    ```

    For example, if our `FileDescriptorSet` were from a `.proto` file in the
    `billing` package, and our top-level message was called `Batch`, our
    _message&lowbar;name_ value would be:

    ```nofmt
    billing.Batch
    ```

### Regex format details

Regex-formatted sources let you apply a structure to arbitrary strings passed in
from file sources. This is particularly useful when processing unstructured log
files.

- To parse regex strings, Materialize uses
  [rust-lang/regex](https://github.com/rust-lang/regex). For more detail, refer to its [documented syntax](https://docs.rs/regex/latest/regex/#syntax).
- To create a column in the source, create a capture group, i.e. a parenthesized
  expression, e.g. `([0-9a-f]{8})`.
    - Name columns by creating named captured groups, e.g. `?P<offset>` in
      `(?P<offset>[0-9a-f]{8})` creates a column named `offset`.
    - Unnamed capture groups are named `column1`, `column2`, etc.
- We discard all data not included in a capture group. You can create
  non-capturing groups using `?:` as the leading pattern in the group, e.g.
  `(?:[0-9a-f]{4} ){8}`.

### CSV format details

CSV-formatted sources read lines from a CSV file.

- Every line of the CSV is treated as a row, i.e. there is no concept of
  headers.
- Columns in the source are named `column1`, `column2`, etc.
- You must specify the number of columns using `WITH ( format = 'csv', columns =
  n )`. Any row with a different number of columns gets discarded, though
  Materialize will log an error.

### Text format details

Text-formatted sources reads lines from a file.

- Data from text-formatted sources is treated as newline-delimited.
- Data is assumed to be UTF-8 encoded, and discarded if it cannot be converted to UTF-8.

### Raw byte format details

Raw byte-formatted sources provide Materialize the raw bytes received from the
source without applying any formatting or decoding.

## Envelope details

Envelopes determine whether an incoming record inserts new data, updates or deletes existing data, or both.

### Append-only envelope details

Materialize's default behavior doesn't require any additional details from the
data, and simply treats each record received from the source as a new entry.
This means that sources using an append-only envelope do not support updates or
deletes.

Treating data from the external source as append-only does not change the
expected format.

### Debezium envelope details

The Debezium envelope provides a "diff envelope", which describes the decoded
records' old and new values; this is roughly equivalent to the notion of Change
Data Capture, or CDC. Materialize can use the data in this diff envelope to
process data as representing inserts, updates, or deletes.

This envelope is called the Debezium envelope because the only tool that we're
aware of that includes this data is [Debezium].

To use the Debezium envelope with Materialize, you must configure Debezium with
your database.

- [MySQL](https://debezium.io/documentation/reference/0.10/connectors/mysql.html)
- [PostgreSQL](https://debezium.io/documentation/reference/0.10/connectors/postgresql.html)

The Debezium envelope is only supported by sources published to Kafka by
Debezium.

#### Format implications

Using the Debezium envelopes changes the schema of your Avro-encoded Kafka
topics to include something akin to the following field:

```json
{
    "type": "record",
    "name": "envelope",
    "fields": [
        {
        "name": "before",
        "type": [
            {
            "name": "row",
            "type": "record",
            "fields": [
                {"name": "a", "type": "long"},
                {"name": "b", "type": "long"}
            ]
            },
            "null"
        ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
}
```

Note that:

- If you use the Confluent Schema Registry to receive your schemas, you don't
  need to manually create this field; Debezium will have taken care of it for
  you.
- The following section depends on the column's names and types, and is unlikely
  to match our example:
    ```json
    ...
    "fields": [
            {"name": "a", "type": "long"},
            {"name": "b", "type": "long"}
        ]
    ...
    ```

## Examples

### Using a Confluent schema registry

```sql
CREATE SOURCE events
FROM KAFKA BROKER 'localhost:9092' TOPIC 'events'
FORMAT AVRO
    USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```

This creates a source that...

- Automatically determines its schema from the Confluent Schema Registry.
- Decodes data received from the `events` topic published by Kafka running on
  `localhost:9092`.
- Decodes using an Avro schema.
- Is eligible to use the Debezium envelope because it's Avro-encoded and
  published by Kafka; however, this still depends on whether or not the upstream
  database publishes its data from a Debezium-enabled database.

### Inlining the Avro schema

```sql
CREATE SOURCE user
FROM KAFKA BROKER 'localhost:9092' TOPIC 'user'
FORMAT AVRO
USING SCHEMA '{
  "type": "record",
  "name": "envelope",
  "fields": [
    {
      "name": "before",
      "type": [
        {
          "type": "record",
          "name": "Envelope",
          "namespace": "user",
          "fields": [
              ....
          ],
          "connect.name": "user.Envelope"
        }'
    ENVELOPE DEBEZIUM;
```

This creates a source that...

- Has its schema defined inline, and decodes data using that schema.
- Decodes data received from the `user` topic published by Kafka running on
  `localhost:9092`.
- Uses the Debezium envelope, meaning it supports delete, updates, and inserts.

### Creating a source from Protobufs

Assuming you've already generated a `FileDescriptorSet` named `SCHEMA`:

```sql
CREATE SOURCE batches
KAFKA BROKER 'localhost:9092' TOPIC 'billing'
FORMAT PROTOBUF MESSAGE '.billing.Batch'
    USING '[path to SCHEMA]';
```

This creates a source that...

- Is append-only.
- Decodes data received from the `billing` topic published by Kafka running on
  `localhost:9092`.
- Decodes data as the `Batch` message from the `billing` package, as described
  in the [generated `FileDescriptorSet`](#protobuf-format-details).

### Creating a source from a dynamic, unstructured file

In this example, we'll assume we have [`xxd`](https://linux.die.net/man/1/xxd)
creating hex dumps for some incoming files. Its output might look like this:

```nofmt
00000000: 7f45 4c46 0201 0100 0000 0000 0000 0000  .ELF............
00000010: 0300 3e00 0100 0000 105b 0000 0000 0000  ..>......[......
00000020: 4000 0000 0000 0000 7013 0200 0000 0000  @.......p.......
```

We'll create a source that takes in these entire lines and extracts the file
offset, as well as the decoded value.

```sql
CREATE SOURCE hex
FROM FILE '/xxd.log'
FORMAT REGEX '(?P<offset>[0-9a-f]{8}): (?:[0-9a-f]{4} ){8} (?P<decoded>.*)$'
WITH (
    tail=true
);
```

This creates a source that...

- Is append-only.
- Has two columns: `offset` and `decoded`.
- Discards the second group, i.e. `(?:[0-9a-f]{4} ){8}`.
- Materialize dynamically checks for new entries.

Using the above example, this source would generate:

```nofmt
 offset  |     decoded
---------+------------------
00000000 | .ELF............
00000010 | ..>......[......
00000020 | @.......p.......
```

### Creating a source from a static CSV

```sql
CREATE SOURCE test
FROM FILE '[path to .csv]'
FORMAT CSV WITH 5 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 5 columns (`column1`...`column5`). Materialize will ignore all data
  received without 5 columns.
- Is only read once, i.e. any updates to the underlying CSV file will not
  propagate to Materialize.

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)

[Debezium]: http://debezium.io
