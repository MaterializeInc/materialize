---
title: "CREATE SOURCE: Kafka/Redpanda (Legacy Syntax)"
description: "Connecting Materialize to a Kafka or Redpanda broker"
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_kafka
    name: Kafka/Redpanda (Legacy Syntax)
    weight: 11
aliases:
    - /sql/create-source/avro-kafka
    - /sql/create-source/json-kafka
    - /sql/create-source/protobuf-kafka
    - /sql/create-source/text-kafka
    - /sql/create-source/csv-kafka
---

{{< source-versioning-disambiguation is_new=false
other_ref="[new reference page](/sql/create-source/kafka-v2/)" >}}

{{% create-source/intro %}}

To connect to a Kafka/Redpanda broker (and optionally a schema registry), you
first need to [create a connection](#prerequisite-creating-a-connection) that specifies
access and authentication parameters. Once created, a connection is **reusable**
across multiple `CREATE SOURCE` and `CREATE SINK` statements. {{%
/create-source/intro %}}

{{< note >}}
The same syntax, supported formats and features can be used to connect to a
[Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

## Syntax

{{< tabs >}}

{{< tab "Format Avro" >}}
### Format Avro

Materialize can decode Avro messages by integrating with a schema registry to
retrieve a schema, and automatically determine the columns and data types to use
in the source.

{{% include-syntax file="examples/create_source_kafka" example="syntax-avro" %}}

{{< include-md file="shared-content/kafka-format-avro-details.md" >}}

{{< /tab >}}

{{< tab "Format JSON" >}}
### Format JSON

Materialize can decode JSON messages into a single column named `data` with type
`jsonb`. Refer to the [`jsonb` type](/sql/types/jsonb) documentation for the
supported operations on this type.

{{% include-syntax file="examples/create_source_kafka" example="syntax-json" %}}

{{< include-md file="shared-content/kafka-format-json-details.md" >}}

{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}
### Format Text/Bytes

Materialize can:
- Parse **new-line delimited** data as plain text. Data is assumed to be **valid
  unicode** (UTF-8), and discarded if it cannot be converted to UTF-8.
  Text-formatted sources have a single column, by default named `text`. For details on casting, check the [`text`](/sql/types/text/) documentation.

- Read raw bytes without applying any formatting or decoding. Raw byte-formatted
sources have a single column, by default named `data`. For details on encodings
and casting, check the [`bytea`](/sql/types/bytea/) documentation.


{{% include-syntax file="examples/create_source_kafka" example="syntax-text-bytes" %}}

{{< /tab >}}

{{< tab "Format CSV" >}}
### Format CSV

Materialize can parse CSV-formatted data. The data in CSV sources is read as
[`text`](/sql/types/text).

{{% include-syntax file="examples/create_source_kafka" example="syntax-csv" %}}

{{< /tab >}}

{{< tab "Format Protobuf" >}}
### Format Protobuf

Materialize can decode Protobuf messages by integrating with a schema registry
or parsing an inline schema to retrieve a `.proto` schema definition. It can
then automatically define the columns and data types to use in the source.

{{% include-syntax file="examples/create_source_kafka" example="syntax-protobuf" %}}

{{< include-md file="shared-content/kafka-format-protobuf-details.md" >}}

{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}
### KEY FORMAT VALUE FORMAT
By default, the message key is decoded using the same format as the message
value. However, you can set the key and value encodings explicitly using the
`KEY FORMAT ... VALUE FORMAT`.

{{% include-syntax file="examples/create_source_kafka" example="syntax-key-value-format" %}}

{{< /tab >}}

{{< /tabs >}}

## Envelopes

{{< include-md file="shared-content/kafka-envelopes.md" >}}

## Features



### Spilling to disk

Kafka sources that use `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` require storing
the current value for _each key_ in the source to produce retractions when keys
are updated. When using [standard cluster sizes](/sql/create-cluster/#available-sizes),
Materialize will automatically offload this state to disk, seamlessly handling
key spaces that are larger than memory.

Spilling to disk is not available with [legacy cluster sizes](/sql/create-cluster/#legacy-sizes).

### Exposing source metadata

{{< include-md file="shared-content/kafka-include-metadata.md" >}}

### Setting start offsets

To start consuming a Kafka stream from a specific offset, you can use the `START
OFFSET` option.

```mzsql
CREATE SOURCE kafka_offset
  FROM KAFKA CONNECTION kafka_connection (
    TOPIC 'data',
    -- Start reading from the earliest offset in the first partition,
    -- the second partition at 10, and the third partition at 100.
    START OFFSET (0, 10, 100)
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

Note that:

- If fewer offsets than partitions are provided, the remaining partitions will
  start at offset 0. This is true if you provide `START OFFSET (1)` or `START
  OFFSET (1, ...)`.

- Providing more offsets than partitions is not supported.

#### Time-based offsets

It's also possible to set a start offset based on Kafka timestamps, using the
`START TIMESTAMP` option. This approach sets the start offset for each
available partition based on the Kafka timestamp and the source behaves as if
`START OFFSET` was provided directly.

It's important to note that `START TIMESTAMP` is a property of the source: it
will be calculated _once_ at the time the `CREATE SOURCE` statement is issued.
This means that the computed start offsets will be the **same** for all views
depending on the source and **stable** across restarts.

If you need to limit the amount of data maintained as state after source
creation, consider using [temporal filters](/sql/patterns/temporal-filters/)
instead.


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

```mzsql
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
The ID of the consumer group consists of the prefix configured with the [`GROUP
ID PREFIX` option](#syntax) followed by a Materialize-generated
suffix.

You should not make assumptions about the number of consumer groups that
Materialize will use to consume from a given source. The only guarantee is that
the ID of each consumer group will begin with the configured prefix.

The consumer group ID prefix for each Kafka source in the system is available in
the `group_id_prefix` column of the [`mz_kafka_sources`] table. To look up the
`group_id_prefix` for a source by name, use:

```mzsql
SELECT group_id_prefix
FROM mz_internal.mz_kafka_sources ks
JOIN mz_sources s ON s.id = ks.id
WHERE s.name = '<src_name>'
```

## Required Kafka ACLs

The access control lists (ACLs) on the Kafka cluster must allow Materialize
to perform the following operations on the following resources:

Operation type | Resource type    | Resource name
---------------|------------------|--------------
Read           | Topic            | The specified `TOPIC` option
Read           | Group            | All group IDs starting with the specified [`GROUP ID PREFIX` option](#syntax)

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-source" %}}

## Examples

### Prerequisite: Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection) documentation page.

#### Broker

{{< tabs tabID="1" >}}
{{< tab "SSL">}}
```mzsql
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9093',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```
{{< /tab >}}
{{< tab "SASL">}}

```mzsql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```
{{< /tab >}}
{{< /tabs >}}

If your Kafka broker is not exposed to the public internet, you can [tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion host:

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{% include-headless "/headless/aws-privatelink-cloud-only-note" %}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'broker1:9092' USING AWS PRIVATELINK privatelink_svc,
        'broker2:9092' USING AWS PRIVATELINK privatelink_svc (PORT 9093)
    )
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh_connection,
    'broker2:9092' USING SSH TUNNEL ssh_connection
    )
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).
{{< /tab >}}
{{< /tabs >}}

#### Confluent Schema Registry

{{< tabs tabID="1" >}}
{{< tab "SSL">}}
```mzsql
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9093',
    SSL KEY = SECRET csr_ssl_key,
    SSL CERTIFICATE = SECRET csr_ssl_crt,
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
);
```
{{< /tab >}}
{{< tab "Basic HTTP Authentication">}}
```mzsql
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

If your Confluent Schema Registry server is not exposed to the public internet,
you can [tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion host:

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{% include-headless "/headless/aws-privatelink-cloud-only-note" %}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```mzsql
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).
{{< /tab >}}
{{< tab "SSH tunnel">}}
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

```mzsql
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    SSH TUNNEL ssh_connection
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).
{{< /tab >}}
{{< /tabs >}}

#### AWS Glue Schema Registry

{{< private-preview />}}

An [AWS Glue Schema Registry connection](/sql/create-connection/#aws-glue-schema-registry)
authenticates through a separate [AWS connection](/sql/create-connection/#aws),
which supplies the credentials and region:

```mzsql
CREATE CONNECTION aws_connection TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::123456789000:role/MaterializeGlue'
);

CREATE CONNECTION glue_connection TO AWS GLUE SCHEMA REGISTRY (
    AWS CONNECTION = aws_connection,
    REGISTRY = 'default-registry'
);
```

The AWS connection must be allowed to read schemas from the registry. See
[Permissions](/sql/create-connection/#glue-permissions) for the required IAM
actions.

### Creating a source

{{< tabs tabID="1" >}}
{{< tab "Avro">}}

**Using Confluent Schema Registry**

```mzsql
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

**Using AWS Glue Schema Registry** {{< private-preview-inline />}}

```mzsql
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_connection (
    SCHEMA NAME = 'test_schema'
  );
```

{{< /tab >}}
{{< tab "JSON">}}

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

```mzsql
CREATE VIEW typed_kafka_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM json_source;
```

JSON-formatted messages are ingested as a JSON blob. We recommend creating a
parsing view on top of your Kafka source that maps the individual fields to
columns with the required data types. To avoid doing this tedious task
manually, you can use [this **JSON parsing widget**](/sql/types/jsonb/#parsing)!

{{< /tab >}}
{{< tab "Text/bytes">}}

```mzsql
CREATE SOURCE text_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT TEXT
  ENVELOPE UPSERT;
```

{{< /tab >}}
{{< tab "CSV">}}

```mzsql
CREATE SOURCE csv_source (col_foo, col_bar, col_baz)
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT CSV WITH 3 COLUMNS;
```

{{< /tab >}}
{{< tab "Protobuf">}}

**Using Confluent Schema Registry**

```mzsql
CREATE SOURCE proto_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

**Using an inline schema**

If you're not using a schema registry, you can use the `MESSAGE...SCHEMA` clause
to specify a Protobuf schema descriptor inline. Protobuf does not serialize a
schema with the message, so before creating a source you must:

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

  ```mzsql
  CREATE SOURCE proto_source
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
    FORMAT PROTOBUF MESSAGE 'Batch' USING SCHEMA '\x0a300a0d62696...';
  ```

{{< /tab >}}
{{< /tabs >}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/sql/show-sources)
- [`DROP SOURCE`](/sql/drop-source)
- [Using Debezium](/integrations/debezium/)
