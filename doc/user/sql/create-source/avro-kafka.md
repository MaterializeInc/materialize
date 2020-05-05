---
title: "CREATE SOURCE: Avro over Kafka"
description: "Learn how to connect Materialize to an Avro-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
aliases:
    - /docs/sql/create-source/kafka
    - /docs/sql/create-source/avro
    - /docs/sql/create-source/avro-source
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

This document details how to connect Materialize to an Avro-formatted Kafka
topic. For other options, view [`CREATE  SOURCE`](../).

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [API Components:
Sources](../../../overview/api-components#sources).

## Syntax

{{< diagram "create-source-avro-kafka.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
_col&lowbar;name_ | Override default column name with the provided [identifier](../../identifiers). If used, a _col&lowbar;name_ must be provided for each column in the created source.
**KAFKA BROKER** _host_ | The Kafka broker's host name.
**TOPIC** _topic_ | The Kafka topic you want to subscribe to.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).
**ENVELOPE** _envelope_ | The enve
**FORMAT** _format&lowbar;spec_ | The payload [format specification](#format-specification).lope type.<br/><br/> &#8226; **NONE** creates an append-only source. This means that records will only be appended and cannot be updated or deleted. <br/><br/>&#8226; **DEBEZIUM** creates a source that can reflect all CRUD operations from the source. This option requires records have the [appropriate fields](#format-implications), and is generally only supported by sources published to Kafka by [Debezium]. For more information, see [Debezium envelope details](#debezium-envelope-details).<br/><br/>&#8226; **UPSERT** creates a source that treats later records with the same key as an earlier record as updates to the earlier record. For more information see [Upsert envelope details](#upsert-envelope-details).

{{% create-source/kafka-with-options %}}

### Format specification

{{< diagram "format-spec-avro-kafka.html" >}}

Field | Use
------|-----
**CONFLUENT SCHEMA REGISTRY ...** _url_ | The URL of the Confluent schema registry to get schema information from.
**SCHEMA FILE ...** _schema&lowbar;file&lowbar;path_ | The absolute path to a file containing the schema.
**SCHEMA ...** _inline&lowbar;schema_ | A string representing the schema.

## Details

This document assumes you're using Kafka to send Avro-encoded data to
Materialize.

{{% create-source/kafka-source-details format="avro" %}}

### Avro format details

Avro-formatted external sources require providing you providing the schema in
one of three ways:

- Using the [Confluent Schema Registry](#using-a-confluent-schema-registry).

    - When using a Schema Registry, Materialize looks for the payload schema using
    the
    [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html).
- Providing a path to a file that contains the Avro Schema.
- Providing the Avro schema [in-line when creating the
  source](#inlining-the-avro-schema).

### Envelope details

Envelopes determine whether an incoming record inserts new data, updates or
deletes existing data, or both. For more information, see [API Components:
Envelopes](../../../overview/api-components#envelopes).

Avro-encoded data sent through Kafka is the only type of data that can support
the Debezium envelope.

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

    Note that the diagrams on this page do not detail using text- or
    byte-formatted keys with Avro-formatted payloads. However, you can integrate
    both of these features using the `format_spec` outlined in [`CREATE SOURCE`:
    Avro over Kafka](../avro-kafka/#format-specification).
- By default, the key is decoded using the same format as the payload. However,
  you can explicitly set the key's format using **UPSERT FORMAT...**.
- If you are using the Confluent Schema Registry, Materialize looks for the key
  and payload schemas using the
  [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html).

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
    ...
  ],
}'
    ENVELOPE DEBEZIUM;
```

This creates a source that...

- Has its schema defined inline, and decodes data using that schema.
- Decodes data received from the `user` topic published by Kafka running on
  `localhost:9092`.
- Uses the Debezium envelope, meaning it supports delete, updates, and inserts.

### Upsert on a Kafka topic with string keys and Avro values

```sql
CREATE SOURCE current_predictions
FROM KAFKA BROKER 'localhost:9092' TOPIC 'current_predictions'
FORMAT AVRO
USING SCHEMA FILE '/scratch/current_predictions.json'
ENVELOPE UPSERT;
```

This creates a source that...

- Has its schema in a file on disk, and decodes both messages and payload data using that schema.
- Decodes data received from the `current_predictions` topic published by Kafka running on
  `localhost:9092`.
- Uses message keys to determine what should be inserted, deleted, and updated.

### Connecting to a Kafka broker using SSL authentication

```sql
CREATE MATERIALIZED SOURCE data_v1
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'top-secret'
    WITH (
      security_protocol='SSL',
      ssl_key_location='/secrets/materialized.key',
      ssl_certificate_location='/secrets/materialized.crt',
      ssl_ca_location='/secrets/ca.crt',
      ssl_key_password='mzmzmz'
    )
  FORMAT AVRO
    USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081';
```

This creates a source that...

- Connects to a Kafka broker and a Confluent Schema Registry that require SSL
  authentication, and whose certificates were both signed by the same CA
  certificate.
- Automatically determines its schema from the Confluent Schema Registry.
- Decodes data received from the `top-secret` topic published by Kafka running on
  `localhost:9092`.
- Is append-only.


### Connecting to a Kafka broker using Kerberos

```sql
CREATE MATERIALIZED SOURCE data_v1
  FROM KAFKA BROKER 'broker.tld:9092' TOPIC 'tps-reports'
    WITH (
      security_protocol = 'sasl_plaintext',
		  sasl_kerberos_keytab = '/secrets/materialized.keytab',
		  sasl_kerberos_service_name = 'kafka',
		  sasl_kerberos_principal = 'materialized@CI.MATERIALIZE.IO'
    )
  FORMAT AVRO
    USING SCHEMA FILE '/tps-reports-schema.json'
```

This creates a source that...

- Connects to a Kerberized Kafka broker whose service principal name is
  `kafka/broker.tld@CI.MATERIALIZE.IO`.
- Has its schema in a file on disk, and decodes payload data using that schema.
- Decodes data received from the `top-secret` topic published by Kafka running on
  `broker.tld:9092`.
- Is append-only.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)

[Debezium]: http://debezium.io
