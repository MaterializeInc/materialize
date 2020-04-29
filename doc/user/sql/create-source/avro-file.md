---
title: "CREATE SOURCE: Avro from local file"
description: "Learn how to connect Materialize to an Avro-formatted local file"
menu:
  main:
    parent: 'create-source'
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

This document details how to connect Materialize to an [Avro Object Container
File](https://avro.apache.org/docs/current/spec.html#Object+Container+Files).
For other options, view [`CREATE SOURCE`](../).

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [API Components:
Sources](../../../overview/api-components#sources).

## Syntax

{{< diagram "create-source-avro-file.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
_col&lowbar;name_ | Override default column name with the provided [identifier](../../identifiers). If used, a _col&lowbar;name_ must be provided for each column in the created source.
**FILE** _path_ | The absolute path to the file you want to use as the source.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).
**AVRO OCF** | Parse the source as an [Avro Object Container File](https://avro.apache.org/docs/1.9.2/spec.html#Object+Container+Files). The file's embedded schema will define the columns of the relation.
**ENVELOPE** _envelope_ | The envelope type.<br/><br/> &#8226; **NONE** creates an append-only source. This means that records will only be appended and cannot be updated or deleted. <br/><br/>&#8226; **DEBEZIUM** creates a source that can reflect all CRUD operations from the source. This option requires records have the [appropriate fields](#format-implications), and is generally only supported by sources published to Kafka by [Debezium].<br/><br/>For more information, see [Debezium envelope details](#debezium-envelope-details).

#### `WITH` options

The following options are valid within the `WITH` clause.

Field | Value | Description
------|-------|------------
`tail` | `bool` | Continually check the file for new content; as new content arrives, process it using other `WITH` options.

## Details

This document assumes you're sending Avro-encoded data to a local file, which
you want Materialize to consume.

### File source details

`path` values must be the file's absolute path, e.g.

```sql
CREATE SOURCE server_source FROM FILE '/Users/sean/server.log'...
```

### Avro OCF format details

Materialize will structure data from the file using the Avro OCF's embedded
schema. For more details, see [Apache Avro 1.9.2 Specification: Object Container
Files](https://avro.apache.org/docs/1.9.2/spec.html#Object+Container+Files).

### Envelope details

Envelopes determine whether an incoming record inserts new data, updates or
deletes existing data, or both. For more information, see [API Components:
Envelopes](../../../overview/api-components#envelopes).

Avro-encoded data is the only type of data that can support the Debezium
envelope.

### Debezium envelope details

The Debezium envelope provides a "diff envelope", which describes the decoded
records' old and new values; this is roughly equivalent to the notion of Change
Data Capture, or CDC. Materialize can use the data in this diff envelope to
process data as representing inserts, updates, or deletes.

This envelope is called the Debezium envelope because it's been developed to
explicitly work with [Debezium].

To use the Debezium envelope with Materialize, you must configure Debezium with
your database.

- [MySQL](https://debezium.io/documentation/reference/0.10/connectors/mysql.html)
- [PostgreSQL](https://debezium.io/documentation/reference/0.10/connectors/postgresql.html)

The Debezium envelope is most easily supported by sources published to Kafka by
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

Note that the following section depends on the column's names and types, and is
unlikely to match our example:

```json
...
"fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "long"}
    ]
...
```

## Examples

```sql
CREATE SOURCE events
FROM FILE '[path to .ocf]'
WITH (tail = true)
FORMAT AVRO OCF
ENVELOPE NONE;
```

This creates a source that...

- Automatically determines its schema from the OCF file's embedded schema.
- Materialize dynamically checks for new entries.
- Is append-only.

[Debezium]: http://debezium.io
