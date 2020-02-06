---
title: "CREATE SOURCES"
description: "`CREATE SOURCES` connects Materialize to Kafka, and defines the data coming from Kafka using a schema registry."
menu:
  main:
    parent: 'sql'
aliases:
    - /docs/sql/create-source
---

`CREATE SOURCES` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

## Conceptual framework

To provide data to Materialze, you must create "sources", which is a catchall
term for a resource Materialize can read data from. For more detail about how
sources work within the rest of Materialze, check out our [architecture
overview](/docs/overview/architecture/).

Materialize supports the following types of sources:

- Streaming sources like Kafka
- File sources like `.csv` or unstructured log files

### Streaming sources

Materialize can ingest data from Kafka topics that publish a change feed from an
underlying relational database, e.g. MySQL. In Materialize's current iteration,
this only works with databases set up to publish a change feed to Kafka through
[Debezium](https://debezium.io), a change data capture (CDC) tool for relational
databases.

Materialize also needs to understand the structure of the source, so it relies
on receiving the topic's schema from either a Confluent Schema Registry or a
user-supplied Avro schema. Debezium handles this automatically by using
Confluent Schema Registry, so most users do not need to do anything for this
step.

After you create the source, Materialize automatically collects all of the data
that streams in, which is then used to supply your queries and views with data.

### File sources

Materialize can ingest data set from sources like `.csv` files or unstructured
log file, in one of two ways:

- Static files, which Materialize reads once and makes available for views.
- Dynamic files, which Materialize regularly reads and propagates new values to
  views that use the source. We call this "tailing a file" because it's similar
  to running `tail` and looking for new lines at its end.

## Syntax

### Create multiple sources

`CREATE SOURCES` can only be used to create streaming sources.

{{< diagram "create-sources.html" >}}

Field | Use
------|-----
**LIKE** _schema&lowbar;expr_ | If using a Confluent schema registry, create sources from all Kafka sources that match _schema&lowbar;expr_. If not used, Materialize creates sources for all topics published at _kafka&lowbar;src_.<br/><br/>The name for all sources is the name of the table that originated from within your database.
**FROM** _kafka&lowbar;src_ | The Kafka source you want to use (begins with `kafka://`)
**REGISTRY** _registry&lowbar;src_ | Use the Confluent schema registry at _registry&lowbar;src_ to define the structure of the Kafka source.
_avro&lowbar;schema_ | The [Avro schema](https://avro.apache.org/docs/current/spec.html) for the topic.

### Create single source

`CREATE SOURCE` can be used to create streaming or file sources.

{{< diagram "create-source.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
**FROM** _format&lowbar;spec_ | A specification of how to connect to the external resource providing the data. For more detail, see [Connector specifications](#connector-spec).
**FORMAT** _format&lowbar;spec_ | A description of the format of data in the source. For more detail, see [Format specifications](#format-spec).
**ENVELOPE** _envelope_ | The envelope type, either **NONE** or **DEBEZIUM**. **NONE** implies that each record appends to the source. **DEBEZIUM** requires records to have `before` and `after` fields, allowing deletes, inserts, and updates. It is currently only supported for Avro-format Kafka sources.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).

### Connector specifications

{{< diagram "connector-spec.html" >}}

The following options are valid within the `WITH` clause.

Field | Value
------|-----
`tail` | Continually check the file for new content; as new content arrives, process it using other `WITH` options. (Only valid for file sources).

### Format specifications

{{< diagram "format-spec.html" >}}

### Avro schema specifications

{{< diagram "avro-schema-spec.html" >}}

### Standard schema specifications

{{< diagram "schema-spec.html" >}}

Field | Value
------|-----
message_name | The protobuf message name.
regex | A regular expression whose capture groups define the columns of the relation. For more detail, see [Regex on file sources](#regex-on-file-sources).
n | The number of columns expected in each record of the CSV source.
char | The delimiter of the CSV source. ASCII comma by default (`','`). This must be an ASCII character; other Unicode code points are not supported.
url | The URL of the Confluent schema registry to get schema information from.
schema_file_path | The absolute path to a file containing the schema.
inline_schema | A string representing the schema.

## Streaming source details

### Overview

Materialize receives data from Kafka sources, which are set up to publish a
change feed from a relational database (also known as "change data capture" or
CDC).

For Materialize to meaningfully process arbitrary changes from the upstream
database, it requires a "diff envelope", which describes records' old and new
values. In the current landscape, the only CDC tool that provides a diff
envelope from relational databases is [Debezium](https://debezium.io/).

This means to create sources within Materialize, you must:

- Configure Debezium with your database
  ([MySQL](https://debezium.io/documentation/reference/0.10/connectors/mysql.html)/[PostgreSQL](https://debezium.io/documentation/reference/0.10/connectors/postgresql.html))
  and Kafka.
- Define the Kafka sources; these are 1:1 with Kafka topics.
- Provide a schema for Materialize to structure the source's data, preferably
  through Confluent Schema Registry. The source's schema will be mirrored in the
  table-like API offered to you when reading from the source.

   Note that this is handled automatically through Debezium.

After creating the source and connecting to Kafka, Materialize receives all of
the data published to the Kafka topic.

### Source requirements

Materialize optionally allows the data it receives from Kafka to have a
structure that contains both the old and new values for any fields within a
record (a "diff envelope"), which lets Differential dataflow process arbitrary
changes to the underlying data, e.g. expressing a record's deletion.

The only tool that we're aware of that provide data to Kafka in this form is
Debezium through its envelope structure that contains a `before` and `after`
field (although CockroachDB's change data capture feature is close to
operational, as well).

You can opt in to this behavior with the `ENVELOPE DEBEZIUM` option.

### Source schema

When creating a source, Materialize looks up stream's structure using a
Confluent Schema Registry or with the Avro schema you define. The stream's
schema is then used to create the source within Materialize, which is
essentially equivalent to a table in the language of RDBMSes.

Once Materialize has determined the structure of the stream, it connects the
underlying Kafka stream directly to its Differential dataflow engine.

### Data storage

As data streams in from Kafka, Materialize's internal Differential instance
builds an arrangement (which is roughly equivalent to an index in the language
of RDBMSes). When the source is initially created this will receive all of the
data that the Kafka stream contains for the topic, e.g. the last 24 hours of
data, and construct its initial arrangement from that. As new data streams in,
Materialize will collect that, as well.

However, Materialize has a separate garbage collection period for arrangements,
which works independently from the Kafka stream's retention period.

When [creating views](../create-views), they are populated with all of the data
from their sources that are available from the arrangements within Differential.

## File source details

File sources have the following caveats, in addition to any details of the
specific file type you're using:

- The specified path must be an absolute path to the file.
- All data in file sources are treated as [`string`](./data-types/string).

### CSV sources

- Every line of the CSV is treated as a row, i.e. there is no concept of
  headers.
- Columns in the source are named `column1`, `column2`, etc.
- You must specify the number of columns using `WITH ( format = 'csv', columns =
  n )`. Any row with a different number of columns gets discarded, though
  Materialize will log an error.

### Regex on file sources

By using the `regex` flag in the `WITH` option list, you can apply a structure
to a string of text that gets read from a file. This is particularly useful when
processing unstructured log files.

- To parse regex strings, Materialize uses
  [rust-lang/regex](https://github.com/rust-lang/regex). The API this library
  provides is similar to Python's built-in regex library.
- To create a column in the source, create a capture group, i.e. a parenthesized
  expression, e.g. `([0-9a-f]{8})`.
    - Name columns by creating named captured groups, e.g. `?P<offset>` in
      `(?P<offset>[0-9a-f]{8})` creates a column named `offset`.
    - Unnamed capture groups are named `column1`, `column2`, etc.
- We discard all data not included in a capture group. You can create
  non-capturing groups using `?:` as the leading pattern in the group, e.g.
  `(?:[0-9a-f]{4} ){8}`.

## Examples

### Using a Confluent schema registry

```sql
CREATE SOURCES
LIKE 'mysql.simple.%'
FROM 'kafka://kafka:9092'
USING SCHEMA REGISTRY 'http://schema-registry:8081';
```

### Manually defining Avro schema

```sql
CREATE SOURCE 'mysql.simple.user'
FROM KAFKA BROKER 'kafka:9092' TOPIC 'user'
FORMAT AVRO
USING SCHEMA '{
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
}'
ENVELOPE DEBEZIUM;
```

### Creating a source from a static CSV

```sql
CREATE SOURCE test
FROM FILE '/test.csv'
FORMAT CSV WITH 5 COLUMNS;
```

### Creating a source from a dynamic, unstructured file

In this example, we'll assume we have [`xxd`](https://linux.die.net/man/1/xxd) creating hex dumps for some incoming files. Its output might look like this:

```nofmt
00000000: 7f45 4c46 0201 0100 0000 0000 0000 0000  .ELF............
00000010: 0300 3e00 0100 0000 105b 0000 0000 0000  ..>......[......
00000020: 4000 0000 0000 0000 7013 0200 0000 0000  @.......p.......
```

We'll create a source that takes in these entire lines and extracts the file offset, as well as the decoded value.

```sql
CREATE SOURCE hex
FROM FILE '/xxd.log'
FORMAT REGEX '(?P<offset>[0-9a-f]{8}): (?:[0-9a-f]{4} ){8} (?P<decoded>.*)$'
WITH (
    tail=true
);
```

This creates a source...

- With two columns: `offset` and `decoded`.
- That discards the second group, i.e. `(?:[0-9a-f]{4} ){8}`.
- That Materialize dynamically polls for new entries.

Using the above example, this would generate:

```nofmt
 offset  |     decoded
---------+------------------
00000000 | .ELF............
00000010 | ..>......[......
00000020 | @.......p.......
```

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)
