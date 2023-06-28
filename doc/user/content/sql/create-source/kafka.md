---
title: "CREATE SOURCE: Kafka"
description: "Connecting Materialize to a Kafka or Redpanda broker"
pagerank: 40
aliases:
    - /sql/create-source/avro-kafka
    - /sql/create-source/json-kafka
    - /sql/create-source/protobuf-kafka
    - /sql/create-source/text-kafka
    - /sql/create-source/csv-kafka
---

{{% create-source/intro %}}
To connect to a Kafka broker (and optionally a schema registry), you first need to [create a connection](#creating-a-connection) that specifies access and authentication parameters. Once created, a connection is **reusable** across multiple `CREATE SOURCE` and `CREATE SINK` statements.
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

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-connector-details connector="kafka" envelopes="debezium upsert append-only" %}}

### `CONNECTION` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`TOPIC`                              | `text`    | The Kafka topic you want to subscribe to.

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`SIZE`                               | `text`    | The [size](../#sizing-a-source) for the source. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`. Required if the `IN CLUSTER` option is not specified.

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
CREATE SOURCE kafka_upsert
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT
  WITH (SIZE = '3xsmall');
```

Note that:

- Using this envelope is required to consume [log compacted topics](https://docs.confluent.io/platform/current/kafka/design.html#log-compaction).

#### Null keys

If a message with a `NULL` key is detected, Materialize sets the source into an
error state. To recover an errored source, you must produce a record with a
`NULL` value and a `NULL` key to the topic, to force a retraction.

As an example, you can use [`kcat`](https://docs.confluent.io/platform/current/clients/kafkacat-usage.html)
to produce an empty message:

```bash
echo ":" | kcat -b $BROKER -t $TOPIC -Z -K: \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-256 \
  -X sasl.username=$KAFKA_USERNAME \
  -X sasl.password=$KAFKA_PASSWORD
```

### Using Debezium

{{< debezium-json >}}

Materialize provides a dedicated envelope (`ENVELOPE DEBEZIUM`) to decode Kafka messages produced by [Debezium](https://debezium.io/). To create a source that interprets Debezium messages:

```sql
CREATE SOURCE kafka_repl
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'pg_repl.public.table1')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM
  WITH (SIZE = '3xsmall');
```

Any materialized view defined on top of this source will be incrementally updated as new change events stream in through Kafka, as a result of `INSERT`, `UPDATE` and `DELETE` operations in the original database.

For more details and a step-by-step guide on using Kafka+Debezium for Change Data Capture (CDC), check [Using Debezium](/integrations/debezium/).

### Exposing source metadata

In addition to the message value, Materialize can expose the message key, headers and other source metadata fields to SQL.

#### Key

The message key is exposed via the `INCLUDE KEY` option. Composite keys are also supported {{% gh 7645 %}}.

```sql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY AS renamed_id
  WITH (SIZE = '3xsmall');
```

Note that:

- This option requires specifying the key and value encodings explicitly using the `KEY FORMAT ... VALUE FORMAT` [syntax](#syntax).

- The `UPSERT` envelope always includes keys.

- The `DEBEZIUM` envelope is incompatible with this option.

#### Headers

Message headers can be exposed via the `INCLUDE HEADERS` option. They are included
as a column (named `headers` by default) containing a [`list`](/sql/types/list/)
of records of type `(key text, value bytea)`.

The following example demonstrates use of the `INCLUDE HEADERS` option.

```sql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE HEADERS
  ENVELOPE NONE
  WITH (SIZE = '3xsmall');
```

To retrieve the value of an individual header in a message, you can use standard
SQL techniques for working with [`list`](/sql/types/list) and
[`bytea`](/sql/types/bytea) types. The following example parses the UTF-8
encoded `client_id` header of the messages from the Kafka topic. Messages
without a `client_id` header result in null values (`"\N"`) for the parsed
attribute.

```sql
SELECT
    id,
    seller,
    item,
    (
        SELECT convert_from((h).value, 'utf8') AS client_id
        FROM unnest(headers) AS h
        WHERE (h).key = 'client_id'
    )
FROM kafka_metadata;

 id | seller |        item        | client_id
----+--------+--------------------+-----------
  2 |   1592 | Custom Art         |        23
  7 |   1509 | Custom Art         |        42
  3 |   1411 | City Bar Crawl     |      "\N"
```

Note that:

- The `DEBEZIUM` envelope is incompatible with this option.

#### Partition, offset, timestamp

These metadata fields are exposed via the `INCLUDE PARTITION`, `INCLUDE OFFSET` and `INCLUDE TIMESTAMP` options.

```sql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE PARTITION, OFFSET, TIMESTAMP AS ts
  ENVELOPE NONE
  WITH (SIZE = '3xsmall');
```

```sql
SELECT "offset" FROM kafka_metadata WHERE ts > '2021-01-01';

offset
------
15
14
13
```

### Setting start offsets

To start consuming a Kafka stream from a specific offset, you can use the `START OFFSET` option.

```sql
CREATE SOURCE kafka_offset
  FROM KAFKA CONNECTION kafka_connection (
    TOPIC 'data',
    -- Start reading from the earliest offset in the first partition,
    -- the second partition at 10, and the third partition at 100.
    START OFFSET (0, 10, 100)
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  WITH (SIZE = '3xsmall');
```

Note that:

- If fewer offsets than partitions are provided, the remaining partitions will start at offset 0. This is true if you provide `START OFFSET (1)` or `START OFFSET (1, ...)`.
- If more offsets than partitions are provided, then any partitions added later will incorrectly be read from that offset. So, if you have a single partition, but you provide `START OFFSET (1, 2)`, when you add the second partition you will miss the first 2 records of data.

#### Time-based offsets

It's also possible to set a start offset based on Kafka timestamps, using the `START TIMESTAMP` option. This approach sets the start offset for each available partition based on the Kafka timestamp and the source behaves as if `START OFFSET` was provided directly.

It's important to note that `START TIMESTAMP` is a property of the source: it will be calculated _once_ at the time the `CREATE SOURCE` statement is issued. This means that the computed start offsets will be the **same** for all views depending on the source and **stable** across restarts.

If you need to limit the amount of data maintained as state after source creation, consider using [temporal filters](/sql/patterns/temporal-filters/) instead.

#### `CONNECTION` options

Field               | Value | Description
--------------------|-------|--------------------
`START OFFSET`      | `int` | Read partitions from the specified offset. You cannot update the offsets once a source has been created; you will need to recreate the source. Offset values must be zero or positive integers.
`START TIMESTAMP`   | `int` | Use the specified value to set `START OFFSET` based on the Kafka timestamp. Negative values will be interpreted as relative to the current system time in milliseconds (e.g. `-1000` means 1000 ms ago). The offset for each partition will be the earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition. If no such offset exists for a partition, the partition's end offset will be used.

#### `KEY STRATEGY` and `VALUE STRATEGY`

It is possible to define how an Avro reader schema will be chosen for Avro sources by
using the `KEY STRATEGY` and `VALUE STRATEGY` keywords, as shown in the syntax diagram.

A strategy of `LATEST` (the default) will choose the latest writer schema from the schema registry to use as a reader schema. `ID` or `INLINE` will allow specifying a schema from the registry by ID or inline in the `CREATE SOURCE` statement, respectively.

### Monitoring source progress

By default, Kafka sources expose progress metadata as a subsource that you can
use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field          | Type                                     | Meaning
---------------|------------------------------------------|--------
`partition`    | `numrange`                               | The upstream Kafka partition.
`offset`       | [`uint8`](/sql/types/uint/#uint8-info)   | The greatest offset consumed from each upstream Kafka partition.

And can be queried using:

```sql
SELECT
  partition, "offset"
FROM
  (
    SELECT
      -- Take the upper of the range, which is null for non-partition rows
      -- Cast partition to u64, which is more ergonomic
      upper(partition)::uint8 AS partition, "offset"
    FROM
      <src_name>_progress
  )
WHERE
  -- Remove all non-partition rows
  partition IS NOT NULL;
```

As long as any offset continues increasing, Materialize is consuming data from
the upstream Kafka broker. For more details on monitoring source ingestion
progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

### Monitoring consumer lag

To support Kafka tools that monitor consumer lag, Kafka sources commit offsets
once the messages up through that offset have been durably recorded in
Materialize's storage layer.

However, rather than relying on committed offsets, Materialize suggests using
our native [progress monitoring](#monitoring-source-progress), which contains
more up-to-date information.

{{< note >}}
Some Kafka monitoring tools may indicate that Materialize's consumer groups have
no active members. This is **not a cause for concern**.

Materialize does not participate in the consumer group protocol nor does it
recover on restart by reading the committed offsets. The committed offsets are
provided solely for the benefit of Kafka monitoring tools.
{{< /note >}}

Committed offsets are associated with a consumer group specific to the source.
The ID of the consumer group has a prefix with the following format:

```
materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}
```

You should not make assumptions about the number of consumer groups that
Materialize will use to consume from a given source. The only guarantee is that
the ID of each consumer group will begin with the above prefix.

The rendered consumer group ID prefix for each Kafka source in the system is
available in the `group_id_base` column of the [`mz_kafka_sources`] table. To
look up the `group_id_base` for a source by name, use:

```sql
SELECT group_id_base
FROM mz_internal.mz_kafka_sources ks
JOIN mz_sources s ON s.id = ks.id
WHERE s.name = '<src_name>'
```

## Required permissions

The access control lists (ACLs) on the Kafka cluster must allow Materialize
to perform the following operations on the following resources:

Operation type | Resource type    | Resource name
---------------|------------------|--------------
Read           | Topic            | The specified `TOPIC` option

To allow Materialize to [commit offsets](#monitoring-consumer-lag) to the Kafka
broker, Materialize additionally requires access to the following operations:

Operation type | Resource type    | Resource name
---------------|------------------|--------------
Read           | Group            | `materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}*`

## Examples

### Creating a connection

A connection describes how to connect and authenticate to an external system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE` statements. For more details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection) documentation page.

#### Broker

{{< tabs tabID="1" >}}
{{< tab "SSL">}}
```sql
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'rp-f00000bar.data.vectorized.cloud:30365',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```
{{< /tab >}}
{{< tab "SASL">}}

```sql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000-kafka.upstash.io:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```
{{< /tab >}}
{{< /tabs >}}

If your Kafka broker is not exposed to the public internet, you can [tunnel the connection](/sql/create-connection/#network-security-connections) through an AWS PrivateLink service or an SSH bastion host:

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink">}}

```sql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```sql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'broker1:9092' USING AWS PRIVATELINK privatelink_svc,
        'broker2:9092' USING AWS PRIVATELINK privatelink_svc (PORT 9093)
    )
);
```

For step-by-step instructions on creating AWS PrivateLink connections and configuring an AWS PrivateLink service to accept connections from Materialize, check [this guide](/ops/network-security/privatelink/).
{{< /tab >}}
{{< tab "SSH tunnel">}}

```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

```sql
CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh_connection,
    'broker2:9092' USING SSH TUNNEL ssh_connection
    )
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).
{{< /tab >}}
{{< /tabs >}}

#### Confluent Schema Registry

{{< tabs tabID="1" >}}
{{< tab "SSL">}}
```sql
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://rp-f00000bar.data.vectorized.cloud:30993',
    SSL KEY = SECRET csr_ssl_key,
    SSL CERTIFICATE = SECRET csr_ssl_crt,
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
);
```
{{< /tab >}}
{{< tab "Basic HTTP Authentication">}}
```sql
CREATE SECRET IF NOT EXISTS csr_username AS '<CSR_USERNAME>';
CREATE SECRET IF NOT EXISTS csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
  URL '<CONFLUENT_REGISTRY_URL>',
  USERNAME = SECRET csr_username,
  PASSWORD = SECRET csr_password
);
```
{{< /tab >}}
{{< /tabs >}}

If your Confluent Schema Registry server is not exposed to the public internet, you can [tunnel the connection](/sql/create-connection/#network-security-connections) through an AWS PrivateLink service or an SSH bastion host:

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink">}}

```sql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```sql
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and configuring an AWS PrivateLink service to accept connections from Materialize, check [this guide](/ops/network-security/privatelink/).
{{< /tab >}}
{{< tab "SSH tunnel">}}
```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

```sql
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    SSH TUNNEL ssh_connection
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).
{{< /tab >}}
{{< /tabs >}}

### Creating a source

{{< tabs tabID="1" >}}
{{< tab "Avro">}}

**Using Confluent Schema Registry**

```sql
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  WITH (SIZE = '3xsmall');
```

{{< /tab >}}
{{< tab "JSON">}}

```sql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT BYTES
  WITH (SIZE = '3xsmall');
```

```sql
CREATE MATERIALIZED VIEW typed_kafka_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM json_source);
```

{{< /tab >}}
{{< tab "Protobuf">}}

**Using Confluent Schema Registry**

```sql
CREATE SOURCE proto_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  WITH (SIZE = '3xsmall');
```

**Using an inline schema**

If you're not using a schema registry, you can use the `MESSAGE...SCHEMA` clause to specify a Protobuf schema descriptor inline. Protobuf does not serialize a schema with the message, so before creating a source you must:

* Compile the Protobuf schema into a descriptor file using [`protoc`](https://grpc.io/docs/protoc-installation/):

  ```proto
  // example.proto
  syntax = "proto3";
  message Batch {
      int32 id = 1;
      // ...
  }
  ```

  ```bash
  protoc --include_imports --descriptor_set_out=example.pb example.proto
  ```

* Encode the descriptor file into a SQL byte string:

  ```bash
  $ printf '\\x' && xxd -p example.pb | tr -d '\n'
  \x0a300a0d62696...
  ```

* Create the source using the encoded descriptor bytes from the previous step
  (including the `\x` at the beginning):

  ```sql
  CREATE SOURCE proto_source
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
    FORMAT PROTOBUF MESSAGE 'Batch' USING SCHEMA '\x0a300a0d62696...'
    WITH (SIZE = '3xsmall');
  ```

  For more details about Protobuf message names and descriptors, check the
  [Protobuf format](../#protobuf) documentation.

{{< /tab >}}
{{< tab "Text/bytes">}}

```sql
CREATE SOURCE text_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT TEXT
  ENVELOPE UPSERT
  WITH (SIZE = '3xsmall');
```

{{< /tab >}}
{{< tab "CSV">}}

```sql
CREATE SOURCE csv_source (col_foo, col_bar, col_baz)
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT CSV WITH 3 COLUMNS
  WITH (SIZE = '3xsmall');
```

{{< /tab >}}
{{< /tabs >}}

### Sizing a source

To provision a specific amount of CPU and memory to a source on creation, use the `SIZE` option:

```sql
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  WITH (SIZE = '3xsmall');
```

To resize the source after creation:

```sql
ALTER SOURCE avro_source SET (SIZE = 'large');
```

The smallest source size (`3xsmall`) is a resonable default to get started. For more details on sizing sources, check the [`CREATE SOURCE`](../#sizing-a-source) documentation page.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/sql/show-sources)
- [`DROP SOURCE`](/sql/drop-source)
- [Using Debezium](/integrations/debezium/)

[Avro]: /sql/create-source/#avro
[JSON]: /sql/create-source/#json
[Protobuf]: /sql/create-source/#protobuf
[Text/bytes]: /sql/create-source/#textbytes
[CSV]: /sql/create-source/#csv

[Append-only envelope]: /sql/create-source/#append-only-envelope
[Upsert envelope]: /sql/create-source/#upsert-envelope
[Debezium envelope]: /sql/create-source/#debezium-envelope
[`mz_kafka_sources`]: /sql/system-catalog/mz_internal/#mz_kafka_sources
