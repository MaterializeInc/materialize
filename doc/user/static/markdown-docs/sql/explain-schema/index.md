# EXPLAIN SCHEMA

`EXPLAIN KEY SCHEMA` or `EXPLAIN VALUE SCHEMA` is used to see the generated schemas for a `CREATE SINK` statement



`EXPLAIN KEY SCHEMA` or `EXPLAIN VALUE SCHEMA` shows the generated schemas for a `CREATE SINK` statement without creating the sink.

> **Warning:** `EXPLAIN` is not part of Materialize's stable interface and is not subject to
> our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
> change arbitrarily in future versions of Materialize.
>


## Syntax



```mzsql
EXPLAIN (KEY | VALUE) SCHEMA [AS JSON]
FOR CREATE SINK [<sink_name>]
[IN CLUSTER <cluster_name>]
FROM <item_name>
INTO KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, COMPRESSION TYPE <compression_type>]
  [, TRANSACTIONAL ID PREFIX '<transactional_id_prefix>']
  [, PARTITION BY = <expression>]
  [, PROGRESS GROUP ID PREFIX '<progress_group_id_prefix>']
  [, TOPIC REPLICATION FACTOR <replication_factor>]
  [, TOPIC PARTITION COUNT <partition_count>]
  [, TOPIC CONFIG <topic_config>]
)
[KEY ( <key_column> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
[FORMAT <sink_format_spec> | KEY FORMAT <sink_format_spec> VALUE FORMAT <sink_format_spec>]
[ENVELOPE (DEBEZIUM | UPSERT)]
[WITH (<with_option> [, ...])]

```

| Syntax element | Description |
| --- | --- |
| **KEY** \| **VALUE** | Specifies whether to explain the key schema or value schema for the sink.  |
| **AS JSON** | Optional. Format the explanation output as a JSON object. If not specified, the output is formatted as text.  |
| **FOR CREATE SINK** `[<sink_name>]` | The `CREATE SINK` statement to explain. The sink name is optional.  |
| **IN CLUSTER** `<cluster_name>` | Optional. The [cluster](/sql/create-cluster) to maintain this sink.  |
| **FROM** `<item_name>` | The name of the source, table, or materialized view you want to send to the sink.  |
| **INTO KAFKA CONNECTION** `<connection_name>` | The name of the Kafka connection to use in the sink. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection) documentation page.  |
| **TOPIC** `'<topic>'` | The name of the Kafka topic to write to.  |
| **COMPRESSION TYPE** `<compression_type>` | Optional. The type of compression to apply to messages before they are sent to Kafka: `none`, `gzip`, `snappy`, `lz4`, or `zstd`.  |
| **TRANSACTIONAL ID PREFIX** `'<transactional_id_prefix>'` | Optional. The prefix of the transactional ID to use when producing to the Kafka topic.  |
| **PARTITION BY** `= <expression>` | Optional. A SQL expression returning a hash that can be used for partition assignment. See [Partitioning](/sql/create-sink/kafka/#partitioning) for details.  |
| **PROGRESS GROUP ID PREFIX** `'<progress_group_id_prefix>'` | Optional. The prefix of the consumer group ID to use when reading from the progress topic.  |
| **TOPIC REPLICATION FACTOR** `<replication_factor>` | Optional. The replication factor to use when creating the Kafka topic (if the Kafka topic does not already exist).  |
| **TOPIC PARTITION COUNT** `<partition_count>` | Optional. The partition count to use when creating the Kafka topic (if the Kafka topic does not already exist).  |
| **TOPIC CONFIG** `<topic_config>` | Optional. Any topic-level configs to use when creating the Kafka topic (if the Kafka topic does not already exist). See the [Kafka documentation](https://kafka.apache.org/documentation/#topicconfigs) for available configs.  |
| **KEY** ( `<key_column>` [, ...] ) [**NOT ENFORCED**] | Optional. A list of columns to use as the Kafka message key. If unspecified, the Kafka key is left unset. When using the upsert envelope, the key must be unique. Use **NOT ENFORCED** to disable validation of key uniqueness. See [Upsert key selection](/sql/create-sink/kafka/#upsert-key-selection) for details.  |
| **HEADERS** `<headers_column>` | Optional. A column containing headers to add to each Kafka message emitted by the sink. The column must be of type `map[text => text]` or `map[text => bytea]`. See [Headers](/sql/create-sink/kafka/#headers) for details.  |
| **FORMAT** `<sink_format_spec>` | Optional. Specifies the format to use for both keys and values: `AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name>`, `JSON`, `TEXT`, or `BYTES`. See [Formats](/sql/create-sink/kafka/#formats) for details.  |
| **KEY FORMAT** `<sink_format_spec>` **VALUE FORMAT** `<sink_format_spec>` | Optional. Specifies the key format and value formats separately. See [Formats](/sql/create-sink/kafka/#formats) for details.  |
| **ENVELOPE** (`DEBEZIUM` \| `UPSERT`) | Optional. Specifies how changes to the sink's upstream relation are mapped to Kafka messages. Valid envelope types:  \| Envelope \| Description \| \|----------\|-------------\| \| `DEBEZIUM` \| The generated schemas have a [Debezium-style diff envelope](/sql/create-sink/kafka/#debezium) to capture changes in the input view or source. \| \| `UPSERT` \| The sink emits data with [upsert semantics](/sql/create-sink/kafka/#upsert). Requires a unique key specified using the `KEY` option. \|  |
| **WITH** (`<with_option>` [, ...]) | Optional. The following `<with_option>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `SNAPSHOT = <snapshot>` \| Default: `true`. Whether to emit the consolidated results of the query before the sink was created at the start of the sink. To see only results after the sink is created, specify `WITH (SNAPSHOT = false)`. \|  |


## Details
When creating a an Avro-formatted Kafka sink, Materialize automatically generates Avro schemas for the message key and value and publishes them to a schema registry.
This command shows what the generated schemas would look like, without creating the sink.

## Examples

```mzsql
CREATE TABLE t (c1 int, c2 text);
COMMENT ON TABLE t IS 'materialize comment on t';
COMMENT ON COLUMN t.c2 IS 'materialize comment on t.c2';

EXPLAIN VALUE SCHEMA FOR
  CREATE SINK
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'test_avro_topic')
  KEY (c1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
```

```
                   Schema
--------------------------------------------
 {                                         +
   "type": "record",                       +
   "name": "envelope",                     +
   "doc": "materialize comment on t",      +
   "fields": [                             +
     {                                     +
       "name": "c1",                       +
       "type": [                           +
         "null",                           +
         "int"                             +
       ]                                   +
     },                                    +
     {                                     +
       "name": "c2",                       +
       "type": [                           +
         "null",                           +
         "string"                          +
       ],                                  +
       "doc": "materialize comment on t.c2"+
     }                                     +
   ]                                       +
 }
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>USAGE</code> privileges on the schemas that all items in the query are contained
in.</li>
</ul>
