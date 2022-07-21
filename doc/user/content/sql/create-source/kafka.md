---
title: "CREATE SOURCE: Kafka"
description: "Connecting Materialize to a Kafka or Redpanda broker"
menu:
  main:
    parent: 'create-source'
    name: Kafka
    weight: 10
aliases:
    - /sql/create-source/avro-kafka
    - /sql/create-source/json-kafka
    - /sql/create-source/protobuf-kafka
    - /sql/create-source/text-kafka
    - /sql/create-source/csv-kafka
---

{{% create-source/intro %}}
This page describes how to connect Materialize to a Kafka broker to read data from individual topics.
{{% /create-source/intro %}}

{{< note >}}
The same syntax, supported formats and features can be used to connect to a [Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

## Syntax

{{< diagram "create-source-kafka.svg" >}}

#### `format_spec`

{{< diagram "format-spec.svg" >}}

#### `key_strat`

{{< diagram "key-strat.svg" >}}

#### `val_strat`

{{< diagram "val-strat.svg" >}}

#### `strat`

{{< diagram "strat.svg" >}}

#### `key_constraint`

{{< diagram "key-constraint.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-connector-details connector="kafka" envelopes="debezium upsert append-only" %}}

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`client_id`                          | `text`    | Use the supplied value as the Kafka client identifier.
`group_id_prefix`                    | `text`    | Use the specified prefix in the consumer group ID. The resulting `group.id` looks like `<group_id_prefix>materialize-X-Y`, where `X` and `Y` are values that allow multiple concurrent Kafka consumers from the same topic.
`ignore_source_keys`                 | `boolean` | Default: `false`. If `true`, do not perform optimizations assuming uniqueness of primary keys in schemas.
`isolation_level`                    | `text`    | Default: `read_committed`. Controls how to read messages that were transactionally written to Kafka. Supported options are `read_committed` to read only committed messages and `read_uncommitted` to read all messages, including those that are part of an open transaction or were aborted.
`statistics_interval_ms`             | `int`     | `librdkafka` statistics emit interval in `ms`. A value of 0 disables statistics. Statistics can be queried using the `mz_kafka_source_statistics` system table. Accepts values [0, 86400000].
`timestamp_frequency_ms`             | `int`     | Default: `1000`. Sets the timestamping frequency in `ms`. Reflects how frequently the source advances its timestamp. This measure reflects how stale data in views will be. Lower values result in more-up-to-date views but may reduce throughput.
`topic_metadata_refresh_interval_ms` | `int`     | Default: `300000`. Sets the frequency in `ms` at which the system checks for new partitions. Accepts values [0,3600000].
`enable_auto_commit`                 | `boolean` | Default: `false`. Controls whether or not Materialize commits read offsets back into Kafka. This is purely for consumer progress monitoring and does not cause Materialize to resume reading from where it left off across restarts.
`fetch_message_max_bytes` | `int` | Default: `134217728`. Controls the initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched. Accepts values [1, 1000000000].

## Supported formats

|<div style="width:290px">Format</div> | [Append-only envelope] | [Upsert envelope] | [Debezium envelope] |
---------------------------------------|:----------------------:|:-----------------:|:-------------------:|
| [Avro]                               | ✓                      | ✓                 | ✓                   |
| [JSON]                               | ✓                      | ✓                 |                     |
| [Protobuf]                           | ✓                      | ✓                 |                     |
| [Text/bytes]                         | ✓                      | ✓                 |                     |
| [CSV]                                | ✓                      |                   |                     |

### Key-value encoding

By default, the message key is decoded using the same format as the message value. However, you can set the key and value encodings explicitly using the `KEY FORMAT ... VALUE FORMAT` [syntax](#syntax).

## Features

### Handling upserts

To create a source that uses the standard key-value convention to support inserts, updates, and deletes within Materialize, you can use `ENVELOPE UPSERT`:

```sql
CREATE SOURCE current_predictions
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'events'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081'
  ENVELOPE UPSERT;
```

Note that:

- Using this envelope is required to consume [log compacted topics](https://docs.confluent.io/platform/current/kafka/design.html#log-compaction).

#### Defining primary keys

{{< warning >}}
Materialize will **not enforce** the constraint and will produce wrong results if it's not unique.
{{</ warning >}}

Primary keys are **automatically** inferred for Kafka sources using the `UPSERT` or `DEBEZIUM` envelopes. For other source configurations, you can manually define a column (or set of columns) as a primary key using the `PRIMARY KEY (...) NOT ENFORCED` [syntax](#key_constraint). This enables optimizations and constructs that rely on a key to be present when it cannot be inferred.

### Using Debezium

{{< debezium-json >}}

Materialize provides a dedicated envelope (`ENVELOPE DEBEZIUM`) to decode Kafka messages produced by [Debezium](https://debezium.io/). To create a source that interprets Debezium messages:

```sql
CREATE SOURCE kafka_repl
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'pg_repl.public.table1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
  ENVELOPE DEBEZIUM;
```

Note that:

- If log compaction is enabled for your Debezium topic, you must use `ENVELOPE DEBEZIUM UPSERT`.

Any materialized view defined on top of this source will be incrementally updated as new change events stream in through Kafka, as a result of `INSERT`, `UPDATE` and `DELETE` operations in the original database.

For more details and a step-by-step guide on using Kafka+Debezium for Change Data Capture (CDC), check out [Using Debezium](/integrations/debezium/).

### Exposing source metadata

In addition to the message value, Materialize can expose the message key, headers and other source metadata fields to SQL.

#### Key

The message key is exposed via the `INCLUDE KEY` option. Composite keys are also supported {{% gh 7645 %}}.

```sql
CREATE SOURCE kafka_metadata
  FROM 'localhost:9092' TOPIC 'data'
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY AS renamed_id;
```

Note that:

- This option requires specifying the key and value encodings explicitly using the `KEY FORMAT ... VALUE FORMAT` [syntax](#syntax).

- The `UPSERT` envelope always includes keys.

- The `DEBEZIUM` envelope is incompatible with this option.

#### Headers

Message headers are exposed via the `INCLUDE HEADERS` option, and are included as a column (named `headers` by default) containing a [`list`](/sql/types/list/) of ([`text`](/sql/types/text/), [`bytea`](/sql/types/bytea/)) pairs.

```sql
CREATE SOURCE kafka_metadata
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081'
  INCLUDE HEADERS
  ENVELOPE NONE;
```

To retrieve the headers in a message, you can unpack the value:

```sql
SELECT key,
       field1,
       field2,
       headers[1].value AS kafka_header
FROM mv_kafka_metadata;

  key  |  field1  |  field2  |  kafka_header
-------+----------+----------+----------------
  foo  |  fooval  |   1000   |     hvalue
  bar  |  barval  |   5000   |     <null>
```

, or lookup by key:

```sql
SELECT key,
       field1,
       field2,
       thekey,
       value
FROM (SELECT key,
             field1,
             field2,
             unnest(headers).key AS thekey,
             unnest(headers).value AS value
      FROM mv_kafka_metadata) AS km
WHERE thekey = 'kvalue';

  key  |  field1  |  field2  |  thekey  |  value
-------+----------+----------+----------+--------
  foo  |  fooval  |   1000   |  kvalue  |  hvalue
```

Note that:

- The `DEBEZIUM` envelope is incompatible with this option.

#### Partition, offset, timestamp

These metadata fields are exposed via the `INCLUDE PARTITION`, `INCLUDE OFFSET` and `INCLUDE TIMESTAMP` options.

```sql
CREATE SOURCE kafka_metadata
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081'
  INCLUDE PARTITION, OFFSET, TIMESTAMP AS ts
  ENVELOPE NONE;
```

```sql
SELECT "offset" FROM kafka_metadata WHERE ts > '2021-01-01';

offset
------
15
14
13
```

Note that:

- Using the `INCLUDE OFFSET` option with Debezium requires `UPSERT` semantics.

### Setting start offsets

To start consuming a Kafka stream from a specific offset, you can use the `start_offset` option.

```sql
CREATE SOURCE kafka_offset
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  -- Start reading from the earliest offset in the first partition,
  -- the second partition at 10, and the third partition at 100
  WITH (start_offset=[0,10,100])
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081';
```

Note that:

- If fewer offsets than partitions are provided, the remaining partitions will start at offset 0. This is true if you provide `start_offset=1` or `start_offset=[1, ...]`.
- If more offsets than partitions are provided, then any partitions added later will incorrectly be read from that offset. So, if you have a single partition, but you provide `start_offset=[1,2]`, when you add the second partition you will miss the first 2 records of data.
- Using an offset with a source envelope that can supply updates or deletes requires that Materialize handle possibly nonsensical events (e.g. an update for a row that was never inserted). For that reason, starting at an offset requires either a `NONE` envelope or a `(DEBEZIUM) UPSERT` envelope.

#### Time-based offsets

It's also possible to set a start offset based on Kafka timestamps, using the `kafka_time_offset` option. This approach sets the start offset for each available partition based on the Kafka timestamp and the source behaves as if `start_offset` was provided directly.

It's important to note that `kafka_time_offset` is a property of the source: it will be calculated _once_ at the time the `CREATE SOURCE` statement is issued. This means that the computed start offsets will be the **same** for all views depending on the source and **stable** across restarts.

If you need to limit the amount of data maintained as state after source creation, consider using [temporal filters](/sql/patterns/temporal-filters/) instead.

#### `WITH` options

Field               | Value | Description
--------------------|-------|--------------------
`start_offset`      | `int` | Read partitions from the specified offset. You cannot update the offsets once a source has been created; you will need to recreate the source. Offset values must be zero or positive integers, and the source must use either `ENVELOPE NONE` or `(DEBEZIUM) UPSERT`.
`kafka_time_offset` | `int` | Use the specified value to set `start_offset` based on the Kafka timestamp. Negative values will be interpreted as relative to the current system time in milliseconds (e.g. `-1000` means 1000 ms ago). The offset for each partition will be the earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition. If no such offset exists for a partition, the partition's end offset will be used.

#### `KEY STRATEGY` and `VALUE STRATEGY`

It is possible to define how an Avro reader schema will be chosen for Avro sources by
using the `KEY STRATEGY` and `VALUE STRATEGY` keywords, as shown in the syntax diagram.

A strategy of `LATEST` (the default) will choose the latest writer schema from the schema registry to use as a reader schema. `ID` or `INLINE` will allow specifying a schema from the registry by ID or inline in the `CREATE SOURCE` statement, respectively.

## Authentication

### SSL

To connect to a Kafka broker that requires [SSL authentication](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html), use the provided [`WITH` options](#ssl-with-options).

```sql
CREATE SECRET materialized_key AS '...';
CREATE SOURCE kafka_ssl
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'top-secret' WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET materialized_key,
      ssl_certificate_pem = '...',
      ssl_ca_pem = '...',
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081' WITH (
      ssl_key_pem = SECRET materialized_key,
      ssl_certificate_pem = '...',
      ssl_ca_pem = '...'
  );
```


#### SSL `WITH` options

Field                      | Value  | Description
---------------------------|--------|---------------------------
`security_protocol`        | `text` | Use `ssl` to connect to the Kafka cluster.
`ssl_certificate_pem`      | `text` | Your SSL certificate. Required for SSL client authentication.
`ssl_key_pem`              | `text` | Your SSL certificate's key. Required for SSL client authentication.
`ssl_ca_location`          | `text` | The absolute path to the certificate authority (CA) certificate. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.

#### Confluent Schema Registry SSL `WITH` options

Field | Value | Description
------|-------|------------
`ssl_certificate_pem` | `text` | Your SSL certificate. Required for SSL client authentication.
`ssl_key_pem` | `text` | Your SSL certificate's key. Required for SSL client authentication.
`ssl_ca_pem` | `text` | The certificate authority (CA) certificate. Used for both SSL client and server authentication.
`username` | `text` | The username used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.
`password` | `text` | The password used to connect to the schema registry with basic HTTP authentication. This is compatible with the `ssl` options, which control the transport between Materialize and the CSR.

### SASL

To connect to a Kafka broker that requires [SASL authentication](https://docs.confluent.io/platform/current/kafka/authentication_sasl/auth-sasl-overview.html), use the provided [`WITH` options](#sasl-with-options).

#### SASL/PLAIN

```sql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';
CREATE SECRET schema_registry_password AS '<SCHEMA_REGISTRY_PASSWORD>';
CREATE SOURCE kafka_sasl
  FROM KAFKA BROKER 'broker.tld:9092' TOPIC 'top-secret' WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = '<BROKER_USERNAME>',
      sasl_password = SECRET kafka_password,
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://schema-registry.tld' WITH (
      username = '<SCHEMA_REGISTRY_USERNAME>',
      password = SECRET schema_registry_password
  );
```

This is the configuration required to connect to Kafka brokers running on [Confluent Cloud](https://docs.confluent.io/cloud/current/faq.html#what-client-and-protocol-versions-are-supported).

#### SASL `WITH` options

Field                                   | Value  | Description
----------------------------------------|--------|----------------------------------------
`security_protocol`                     | `text` | Use `plaintext`, `ssl`, `sasl_plaintext` or `sasl_ssl` to connect to the Kafka cluster.
`sasl_mechanisms`                       | `text` | The SASL mechanism to use for authentication. Supported: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
`sasl_username`                         | `text` | Your SASL username, if any. Required if `sasl_mechanisms` is `PLAIN`.
`sasl_password`                         | secret | Your SASL password, if any. Required if `sasl_mechanisms` is `PLAIN`.

## Examples

### Creating a source

{{< tabs tabID="1" >}}
{{< tab "Avro">}}

```sql
CREATE SOURCE avro_source
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081';
```

{{< /tab >}}
{{< tab "JSON">}}

```sql
CREATE SOURCE json_source
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  FORMAT BYTES;
```

```sql
CREATE MATERIALIZED VIEW jsonified_kafka_source AS
  SELECT
    data->>'field1' AS field_1,
    data->>'field2' AS field_2,
    data->>'field3' AS field_3
  FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM json_source);
```

{{< /tab >}}
{{< tab "Protobuf">}}

```sql
CREATE SOURCE proto_source
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'billing'
  WITH (cache = true)
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY 'https://schema-registry:8081';
```

{{< /tab >}}
{{< tab "Text/bytes">}}

```sql
CREATE SOURCE text_source
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  FORMAT TEXT
  ENVELOPE UPSERT;
```

{{< /tab >}}
{{< tab "CSV">}}

```sql
CREATE SOURCE csv_source (col_foo, col_bar, col_baz)
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  FORMAT CSV WITH 3 COLUMNS;
```

{{< /tab >}}
{{< /tabs >}}

## Related pages

- [`CREATE SOURCE`](../)
- [Using Debezium](/integrations/debezium/)

[Avro]: /sql/create-source/#avro
[JSON]: /sql/create-source/#json
[Protobuf]: /sql/create-source/#protobuf
[Text/bytes]: /sql/create-source/#textbytes
[CSV]: /sql/create-source/#csv

[Append-only envelope]: /sql/create-source/#append-only-envelope
[Upsert envelope]: /sql/create-source/#upsert-envelope
[Debezium envelope]: /sql/create-source/#debezium-envelope
