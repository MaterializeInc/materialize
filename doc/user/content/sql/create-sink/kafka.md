---
title: "CREATE SINK: Kafka"
description: "Connecting Materialize to a Kafka or Redpanda broker sink"
pagerank: 40
aliases:
    - /sql/create-sink/

---

{{% create-sink/intro %}}
To use a Kafka broker (and optionally a schema registry) as a sink, make sure that a connection that specifies access and authentication parameters to that broker already exists; otherwise, you first need to [create a connection](#creating-a-connection). Once created, a connection is **reusable** across multiple `CREATE SINK` and `CREATE SOURCE` statements.
{{% /create-sink/intro %}}

{{< note >}}
The same syntax, supported formats and features can be used to connect to a [Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

Sink source type      | Description
----------------------|------------
**Source**            | Simply pass all data received from the source to the sink without modifying it.
**Table**             | Stream all changes to the specified table out to the sink.
**Materialized view** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values. Note that this feature only works with [materialized views](/sql/create-materialized-view), and _does not_ work with [non-materialized views](/sql/create-view).

## Syntax

{{< diagram "create-sink-kafka.svg" >}}

#### `sink_format_spec`

{{< diagram "sink-format-spec.svg" >}}

#### `kafka_sink_connection`

{{< diagram "kafka-sink-connection.svg" >}}


#### `csr_connection`

{{< diagram "csr-connection.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a sink of the same name already exists. <br/><br/>If _not_ specified, throw an error if a sink of the same name already exists. _(Default)_
_sink&lowbar;name_ | A name for the sink. This name is only used within Materialize.
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this sink. If not specified, the `SIZE` option must be specified.
_item&lowbar;name_ | The name of the source, table or materialized view you want to send to the sink.
**CONNECTION** _connection_name_ | The name of the connection to use in the sink. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection) documentation page.
**KEY (** _key&lowbar;column_ **)** | An optional list of columns to use for the Kafka key. If unspecified, the Kafka key is left unset.
**ENVELOPE DEBEZIUM** | The generated schemas have a [Debezium-style diff envelope](../#debezium-envelope) to capture changes in the input view or source.
**ENVELOPE UPSERT** | The sink emits data with upsert semantics: updates and inserts for the given key are expressed as a value, and deletes are expressed as a null value payload in Kafka. For more detail, see [Handling upserts](/sql/create-sink/kafka/#handling-upserts).

### `CONNECTION` options

Field                | Value  | Description
---------------------|--------|------------
`TOPIC`              | `text` | The prefix used to generate the Kafka topic name to create and write to.

### CSR `CONNECTION` options

Field                | Value  | Description
---------------------|--------|------------
`AVRO KEY FULLNAME`  | `text` | Default: `row`. Sets the Avro fullname on the generated key schema, if a `KEY` is specified. When used, a value must be specified for `AVRO VALUE FULLNAME`.
`AVRO VALUE FULLNAME`| `text` | Default: `envelope`. Sets the Avro fullname on the generated value schema. When `KEY` is specified, `AVRO KEY FULLNAME` must additionally be specified.

### `WITH` options

Field                | Value  | Description
---------------------|--------|------------
`SNAPSHOT`           | `bool` | Default: `true`. Whether to emit the consolidated results of the query before the sink was created at the start of the sink. To see only results after the sink is created, specify `WITH (SNAPSHOT = false)`.
`SIZE`               | `text`    | The [size](#sizing-a-sink) for the sink. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`. Required if the `IN CLUSTER` option is not specified.

## Supported formats

|<div style="width:290px">Format</div> | [Upsert envelope] | [Debezium envelope] |
---------------------------------------|:-----------------:|:-------------------:|
| [Avro]                               | ✓                 | ✓                   |
| [JSON]                               | ✓                 | ✓                   |

### Avro namespaces

For Avro-formatted sinks, you can specify the [fullnames](https://avro.apache.org/docs/current/specification/#names) for the Avro schemas Materialize generates using the `AVRO KEY FULLNAME` and `AVRO VALUE FULLNAME` [syntax](#syntax).

## Features

### Handling upserts

To create a sink that uses the standard key-value convention to support inserts, updates, and deletes in the sink topic, you can use `ENVELOPE UPSERT`:

```sql
CREATE SINK avro_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT
  WITH (SIZE = '3xsmall');
```

[//]: # "TODO(morsapaes) Add information about upsert key selection"

### Exactly-once processing

By default, Kafka sinks provide [exactly-once processing guarantees](https://kafka.apache.org/documentation/#semantics), which ensures that messages are not duplicated or dropped in failure scenarios.

To achieve this, Materialize stores some internal metadata in an additional *progress topic*. This topic is shared among all sinks that use a particular [Kafka connection](/sql/create-connection/#kafka). The name of the progress topic can be specified when [creating a connection](/sql/create-connection/#kafka-options); otherwise, a default is chosen based on the Materialize environment `id` and the connection `id`. In either case, Materialize will attempt to create the topic if it does not exist. The contents of this topic are not user-specified.

#### End-to-end exactly-once processing

Exactly-once semantics are an end-to-end property of a system, but Materialize only controls the initial produce step. To ensure _end-to-end_ exactly-once message delivery, you should ensure that:

- The broker is configured with replication factor greater than 3, with unclean leader election disabled (`unclean.leader.election.enable=false`).
- All downstream consumers are configured to only read committed data (`isolation.level=read_committed`).
- The consumers' processing is idempotent, and offsets are only committed when processing is complete.

For more details, see [the Kafka documentation](https://kafka.apache.org/documentation/).

## Examples

### Creating a connection

A connection describes how to connect and authenticate to an external system you want Materialize to write data to.

Once created, a connection is **reusable** across multiple `CREATE SINK` statements. For more details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection) documentation page.

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

#### Confluent Schema Registry

{{< tabs tabID="1" >}}
{{< tab "SSL">}}

```sql
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_ssl TO CONFLUENT SCHEMA REGISTRY (
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

CREATE CONNECTION csr_basic_http
  FOR CONFLUENT SCHEMA REGISTRY
  URL '<CONFLUENT_REGISTRY_URL>',
  USERNAME = SECRET csr_username,
  PASSWORD = SECRET csr_password;
```

{{< /tab >}}
{{< /tabs >}}

### Creating a sink

When sinking into Kafka, Materialize will write all the changes from the
specified source, table, or materialized view into the sink topic. If the topic
does not exist, Materialize will attempt to create it.

{{< note >}}
{{% kafka-sink-drop  %}}
{{</ note >}}

{{< tabs tabID="1" >}}
{{< tab "Avro">}}

```sql
CREATE SINK avro_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM
  WITH (SIZE = '3xsmall');
```

{{< /tab >}}
{{< tab "JSON">}}

```sql
CREATE SINK json_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_json_topic')
  FORMAT JSON
  ENVELOPE DEBEZIUM
  WITH (SIZE = '3xsmall');
```

{{< /tab >}}
{{< /tabs >}}

#### Sizing a sink

To provision a specific amount of CPU and memory to a sink on creation, use the `SIZE` option:

```sql
CREATE SINK avro_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM
  WITH (SIZE = '3xsmall');
```

To resize the sink after creation:

```sql
ALTER SINK avro_sink SET (SIZE = 'large');
```

The smallest sink size (`3xsmall`) is a resonable default to get started. For more details on sizing sources, check the [`CREATE SINK`](../#sizing-a-sink) documentation page.

## Related pages

- [`SHOW SINKS`](/sql/show-sinks)
- [`DROP SINK`](/sql/drop-sink)
