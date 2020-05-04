---
title: "CREATE SOURCE: Text or bytes over Kafka"
description: "Learn how to connect Materialize to an Avro-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

This document details how to connect Materialize to a plain-text- or
raw-byte-formatted Kafka topic. For other options, view [`CREATE  SOURCE`](../).

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [API Components:
Sources](../../../overview/api-components#sources).

## Syntax

{{< diagram "create-source-text-kafka.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
_col&lowbar;name_ | Override default column name with the provided [identifier](../../identifiers). If used, a _col&lowbar;name_ must be provided for each column in the created source.
**KAFKA BROKER** _host_ | The Kafka broker's host name.
**TOPIC** _topic_ | The Kafka topic you want to subscribe to.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).
**ENVELOPE** _envelope_ | The envelope type.<br/><br/> &#8226; **NONE** creates an append-only source. This means that records will only be appended and cannot be updated or deleted.<br/><br/>&#8226; **UPSERT** creates a source that treats later records with the same key as an earlier record as updates to the earlier record. For more information see [Upsert envelope details](#upsert-envelope-details).

{{% create-source/kafka-with-options %}}

## Details

This document assumes you're using Kafka to send plain text or raw bytes to
Materialize.

{{% create-source/kafka-source-details format="avro" %}}

### Text format details

Text-formatted data:
- is assumed to be UTF-8 encoded, and discarded if it cannot be converted
  to UTF-8.
- is treated as having one column, which, by default, is named `text`.

### Raw byte format details

No formatting or decoding is applied to raw byte-formatted data.

Raw byte-formatted sources is treated as having one column, which, by default,
is named `bytes`.

### Upsert envelope details

Envelopes determine whether an incoming record inserts new data, updates or
deletes existing data, or both. For more information, see [API Components:
Envelopes](../../../overview/api-components#envelopes).

Specifying `ENVELOPE UPSERT` creates a source that supports Kafka's standard
key-value convention, and supports inserts, updates, and deletes within
Materialize. The source is also compatible with Kafka's log-compaction feature.

#### Inserts, updates, deletes

When Materialize receives a message, it checks the message's key and offset.

- If Materialize does not contain a record with a matching key, it inserts the
  message's payload.

- If the key matches another record with an earlier offset, Materialize updates
  the record with the message's payload.

    - If the payload is _null_, Materialize deletes the record.

#### Key columns

- Sources with the upsert envelope also decode a message's key, and let you
  interact with it like the source's other columns. These columns are placed
  before the decoded payload columns in the source.

    - If the format of the key is either plain text or raw bytes, the key is
      treated as single column. The default key column name is `key0`.

    - If the key format is Avro, its field names will be converted to column
      names, and they're placed before the decoded payload columns.

    Note that the diagrams on this page do not detail using Avro-formatted keys
    with text- or byte-formatted payloads. However, you can integrate both of
    these features using the `format_spec` outlined in [`CREATE SOURCE`: Avro
    over Kafka](../avro-kafka/#format-specification).

- By default, the key is decoded using the same format as the payload. However,
  you can explicitly set the key's format using **UPSERT FORMAT...**.

## Examples

### Upsert on a Kafka topic

```sql
CREATE SOURCE current_predictions
FROM KAFKA BROKER 'localhost:9092' TOPIC 'kv_feed'
FORMAT TEXT
USING SCHEMA FILE '/scratch/kv_feed.json'
ENVELOPE UPSERT;
```

This creates a source that...

- Has its schema in a file on disk, and decodes payload data using that schema.
- Decodes data received from the `kv_feed` topic published by Kafka running on
  `localhost:9092`.
- Uses message keys to determine what should be inserted, deleted, and updated.
- Treats both message keys and values as text.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
