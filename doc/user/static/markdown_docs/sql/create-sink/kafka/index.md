<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [CREATE
SINK](/docs/sql/create-sink/)

</div>

# CREATE SINK: Kafka/Redpanda

<div class="note">

**NOTE:** The `CREATE SINK` syntax, supported formats, and features are
the same for Kafka and Redpanda broker. For simplicity, this page uses
“Kafka” to refer to both Kafka and Redpanda.

</div>

[`CREATE SINK`](/docs/sql/create-sink/) connects Materialize to an
external system you want to write data to, and provides details about
how to encode that data.

To use a Kafka broker (and optionally a schema registry) as a sink, make
sure that a connection that specifies access and authentication
parameters to that broker already exists; otherwise, you first need to
[create a connection](#creating-a-connection). Once created, a
connection is **reusable** across multiple `CREATE SINK` and
`CREATE SOURCE` statements.

| Sink source type | Description |
|----|----|
| **Source** | Simply pass all data received from the source to the sink without modifying it. |
| **Table** | Stream all changes to the specified table out to the sink. |
| **Materialized view** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values. Note that this feature only works with [materialized views](/docs/sql/create-materialized-view), and *does not* work with [non-materialized views](/docs/sql/create-view). |

## Syntax

<div class="code-tabs" heading-level="3">

<div class="tab-content">

<div id="tab-format-avro" class="tab-pane" title="Format Avro">

<div class="highlight">

``` chroma
CREATE SINK [IF NOT EXISTS] <sink_name>
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
[KEY ( <key_col1> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name> [
  (
    [AVRO KEY FULLNAME '<avro_key_fullname>']
    [, AVRO VALUE FULLNAME '<avro_value_fullname>']
    [, NULL DEFAULTS <null_defaults>]
    [, DOC ON <doc_on_option> [, ...]]
    [, KEY COMPATIBILITY LEVEL '<key_compatibility_level>']
    [, VALUE COMPATIBILITY LEVEL '<value_compatibility_level>']
  )
]
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;sink_name&gt;</code></td>
<td>The name for the sink.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a sink with the
same name already exists. Instead, issue a notice and skip the sink
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this sink.</td>
</tr>
<tr>
<td><code>&lt;item_name&gt;</code></td>
<td>The name of the source, table, or materialized view you want to send
to the sink.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the sink. For details on
creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td>The name of the Kafka topic to write to.</td>
</tr>
<tr>
<td><strong>COMPRESSION TYPE</strong>
<code>&lt;compression_type&gt;</code></td>
<td>Optional. The type of compression to apply to messages before they
are sent to Kafka: <code>none</code>, <code>gzip</code>,
<code>snappy</code>, <code>lz4</code>, or <code>zstd</code>.<br />
Default:
<p><code>lz4</code></p></td>
</tr>
<tr>
<td><strong>TRANSACTIONAL ID PREFIX</strong>
<code>'&lt;transactional_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the transactional ID to use when producing
to the Kafka topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>PARTITION BY</strong> = <code>&lt;expression&gt;</code></td>
<td>Optional. A SQL expression returning a hash that can be used for
partition assignment. See <a href="#partitioning">Partitioning</a> for
details.</td>
</tr>
<tr>
<td><strong>PROGRESS GROUP ID PREFIX</strong>
<code>'&lt;progress_group_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the consumer group ID to use when reading
from the progress topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>TOPIC REPLICATION FACTOR</strong>
<code>&lt;replication_factor&gt;</code></td>
<td>Optional. The replication factor to use when creating the Kafka
topic (if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC PARTITION COUNT</strong>
<code>&lt;partition_count&gt;</code></td>
<td>Optional. The partition count to use when creating the Kafka topic
(if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC CONFIG</strong> <code>&lt;topic_config&gt;</code></td>
<td>Optional. Any topic-level configs to use when creating the Kafka
topic (if the Kafka topic does not already exist). See the <a
href="https://kafka.apache.org/documentation/#topicconfigs">Kafka
documentation</a> for available configs.<br />
Default: empty.</td>
</tr>
<tr>
<td><strong>KEY</strong> ( <code>&lt;key_col1&gt;</code> [, …] )
[<strong>NOT ENFORCED</strong>]</td>
<td>Optional. A list of columns to use as the Kafka message key. If
unspecified, the Kafka key is left unset. When using the upsert
envelope, the key must be unique. Use <strong>NOT ENFORCED</strong> to
disable validation of key uniqueness. See <a
href="#upsert-key-selection">Upsert key selection</a> for details.</td>
</tr>
<tr>
<td><strong>HEADERS</strong> <code>&lt;headers_column&gt;</code></td>
<td>Optional. A column containing headers to add to each Kafka message
emitted by the sink. The column must be of type
<code>map[text =&gt; text]</code> or <code>map[text =&gt; bytea]</code>.
See <a href="#headers">Headers</a> for details.</td>
</tr>
<tr>
<td><strong>FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY
CONNECTION</strong> <code>&lt;csr_connection_name&gt;</code></td>
<td>Encode messages using Avro format with schemas published to the
Confluent Schema Registry.</td>
</tr>
<tr>
<td><strong>AVRO KEY FULLNAME</strong>
<code>'&lt;avro_key_fullname&gt;'</code></td>
<td>Optional. Default: <code>row</code>. Sets the Avro fullname on the
generated key schema, if a <code>KEY</code> is specified. When used, a
value must be specified for <code>AVRO VALUE FULLNAME</code>.</td>
</tr>
<tr>
<td><strong>AVRO VALUE FULLNAME</strong>
<code>'&lt;avro_value_fullname&gt;'</code></td>
<td>Optional. Default: <code>envelope</code>. Sets the Avro fullname on
the generated value schema. When <code>KEY</code> is specified,
<code>AVRO KEY FULLNAME</code> must additionally be specified.</td>
</tr>
<tr>
<td><strong>NULL DEFAULTS</strong>
<code>&lt;null_defaults&gt;</code></td>
<td>Optional. Default: <code>false</code>. Whether to automatically
default nullable fields to <code>null</code> in the generated
schemas.</td>
</tr>
<tr>
<td><strong>DOC ON</strong> <code>&lt;doc_on_option&gt;</code> [,
…]</td>
<td><p>Optional. Add a documentation comment to the generated Avro
schemas.</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>TYPE &lt;type_name&gt;</code></td>
<td>Names a SQL type or relation, e.g. <code>my_app.point</code>.</td>
</tr>
<tr>
<td><code>COLUMN &lt;column_name&gt;</code></td>
<td>Names a column of a SQL type or relation, e.g.
<code>my_app.point.x</code>.</td>
</tr>
</tbody>
</table>
<p>The <code>KEY</code> and <code>VALUE</code> options specify whether
the comment applies to the key schema or the value schema. If neither
<code>KEY</code> or <code>VALUE</code> is specified, the comment applies
to both types of schemas.</p>
<p>See <a href="#avro-schema-documentation">Avro schema
documentation</a> for details on how documentation comments are added to
the generated Avro schemas.</p></td>
</tr>
<tr>
<td><strong>KEY COMPATIBILITY LEVEL</strong>
<code>'&lt;key_compatibility_level&gt;'</code></td>
<td>Optional. If specified, set the <a
href="https://docs.confluent.io/platform/7.6/schema-registry/fundamentals/schema-evolution.html#schema-evolution-and-compatibility">Compatibility
Level</a> for the generated key schema to one of: <code>BACKWARD</code>,
<code>BACKWARD_TRANSITIVE</code>, <code>FORWARD</code>,
<code>FORWARD_TRANSITIVE</code>, <code>FULL</code>,
<code>FULL_TRANSITIVE</code>, <code>NONE</code>.</td>
</tr>
<tr>
<td><strong>VALUE COMPATIBILITY LEVEL</strong>
<code>'&lt;value_compatibility_level&gt;'</code></td>
<td>Optional. If specified, set the <a
href="https://docs.confluent.io/platform/7.6/schema-registry/fundamentals/schema-evolution.html#schema-evolution-and-compatibility">Compatibility
Level</a> for the generated value schema to one of:
<code>BACKWARD</code>, <code>BACKWARD_TRANSITIVE</code>,
<code>FORWARD</code>, <code>FORWARD_TRANSITIVE</code>,
<code>FULL</code>, <code>FULL_TRANSITIVE</code>, <code>NONE</code>.</td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how changes to the sink’s upstream relation
are mapped to Kafka messages. Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>DEBEZIUM</code></td>
<td>The generated schemas have a <a
href="#debezium-envelope">Debezium-style diff envelope</a> to capture
changes in the input view or source.</td>
</tr>
<tr>
<td><code>UPSERT</code></td>
<td>The sink emits data with <a href="#upsert-envelope">upsert
semantics</a>. Requires a unique key specified using the
<code>KEY</code> option.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>WITH</strong> (<code>&lt;with_option&gt;</code> [, …])</td>
<td><p>Optional. The following <code>&lt;with_option&gt;</code>s are
supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>SNAPSHOT = &lt;snapshot&gt;</code></td>
<td>Default: <code>true</code>. Whether to emit the consolidated results
of the query before the sink was created at the start of the sink. To
see only results after the sink is created, specify
<code>WITH (SNAPSHOT = false)</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

<div id="tab-format-json" class="tab-pane" title="Format JSON">

<div class="highlight">

``` chroma
CREATE SINK [IF NOT EXISTS] <sink_name>
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
[KEY ( <key_col1> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
FORMAT JSON
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;sink_name&gt;</code></td>
<td>The name for the sink.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a sink with the
same name already exists. Instead, issue a notice and skip the sink
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this sink.</td>
</tr>
<tr>
<td><code>&lt;item_name&gt;</code></td>
<td>The name of the source, table, or materialized view you want to send
to the sink.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the sink. For details on
creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td>The name of the Kafka topic to write to.</td>
</tr>
<tr>
<td><strong>COMPRESSION TYPE</strong>
<code>&lt;compression_type&gt;</code></td>
<td>Optional. The type of compression to apply to messages before they
are sent to Kafka: <code>none</code>, <code>gzip</code>,
<code>snappy</code>, <code>lz4</code>, or <code>zstd</code>.<br />
Default:
<p><code>lz4</code></p></td>
</tr>
<tr>
<td><strong>TRANSACTIONAL ID PREFIX</strong>
<code>'&lt;transactional_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the transactional ID to use when producing
to the Kafka topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>PARTITION BY</strong> = <code>&lt;expression&gt;</code></td>
<td>Optional. A SQL expression returning a hash that can be used for
partition assignment. See <a href="#partitioning">Partitioning</a> for
details.</td>
</tr>
<tr>
<td><strong>PROGRESS GROUP ID PREFIX</strong>
<code>'&lt;progress_group_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the consumer group ID to use when reading
from the progress topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>TOPIC REPLICATION FACTOR</strong>
<code>&lt;replication_factor&gt;</code></td>
<td>Optional. The replication factor to use when creating the Kafka
topic (if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC PARTITION COUNT</strong>
<code>&lt;partition_count&gt;</code></td>
<td>Optional. The partition count to use when creating the Kafka topic
(if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC CONFIG</strong> <code>&lt;topic_config&gt;</code></td>
<td>Optional. Any topic-level configs to use when creating the Kafka
topic (if the Kafka topic does not already exist). See the <a
href="https://kafka.apache.org/documentation/#topicconfigs">Kafka
documentation</a> for available configs.<br />
Default: empty.</td>
</tr>
<tr>
<td><strong>KEY</strong> ( <code>&lt;key_col1&gt;</code> [, …] )
[<strong>NOT ENFORCED</strong>]</td>
<td>Optional. A list of columns to use as the Kafka message key. If
unspecified, the Kafka key is left unset. When using the upsert
envelope, the key must be unique. Use <strong>NOT ENFORCED</strong> to
disable validation of key uniqueness. See <a
href="#upsert-key-selection">Upsert key selection</a> for details.</td>
</tr>
<tr>
<td><strong>HEADERS</strong> <code>&lt;headers_column&gt;</code></td>
<td>Optional. A column containing headers to add to each Kafka message
emitted by the sink. The column must be of type
<code>map[text =&gt; text]</code> or <code>map[text =&gt; bytea]</code>.
See <a href="#headers">Headers</a> for details.</td>
</tr>
<tr>
<td><strong>FORMAT JSON</strong></td>
<td>Encode messages using JSON format.</td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how changes to the sink’s upstream relation
are mapped to Kafka messages. Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>DEBEZIUM</code></td>
<td>The generated schemas have a <a
href="#debezium-envelope">Debezium-style diff envelope</a> to capture
changes in the input view or source.</td>
</tr>
<tr>
<td><code>UPSERT</code></td>
<td>The sink emits data with <a href="#upsert-envelope">upsert
semantics</a>. Requires a unique key specified using the
<code>KEY</code> option.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>WITH</strong> (<code>&lt;with_option&gt;</code> [, …])</td>
<td><p>Optional. The following <code>&lt;with_option&gt;</code>s are
supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>SNAPSHOT = &lt;snapshot&gt;</code></td>
<td>Default: <code>true</code>. Whether to emit the consolidated results
of the query before the sink was created at the start of the sink. To
see only results after the sink is created, specify
<code>WITH (SNAPSHOT = false)</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

<div id="tab-format-textbytes" class="tab-pane"
title="Format TEXT/BYTES">

<div class="highlight">

``` chroma
CREATE SINK [IF NOT EXISTS] <sink_name>
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
FORMAT TEXT | BYTES
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;sink_name&gt;</code></td>
<td>The name for the sink.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a sink with the
same name already exists. Instead, issue a notice and skip the sink
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this sink.</td>
</tr>
<tr>
<td><code>&lt;item_name&gt;</code></td>
<td>The name of the source, table, or materialized view you want to send
to the sink. Note that <code>TEXT</code> and <code>BYTES</code> format
options only support single-column encoding.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the sink. For details on
creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td>The name of the Kafka topic to write to.</td>
</tr>
<tr>
<td><strong>COMPRESSION TYPE</strong>
<code>&lt;compression_type&gt;</code></td>
<td>Optional. The type of compression to apply to messages before they
are sent to Kafka: <code>none</code>, <code>gzip</code>,
<code>snappy</code>, <code>lz4</code>, or <code>zstd</code>.<br />
Default:
<p><code>lz4</code></p></td>
</tr>
<tr>
<td><strong>TRANSACTIONAL ID PREFIX</strong>
<code>'&lt;transactional_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the transactional ID to use when producing
to the Kafka topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>PARTITION BY</strong> = <code>&lt;expression&gt;</code></td>
<td>Optional. A SQL expression returning a hash that can be used for
partition assignment. See <a href="#partitioning">Partitioning</a> for
details.</td>
</tr>
<tr>
<td><strong>PROGRESS GROUP ID PREFIX</strong>
<code>'&lt;progress_group_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the consumer group ID to use when reading
from the progress topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>TOPIC REPLICATION FACTOR</strong>
<code>&lt;replication_factor&gt;</code></td>
<td>Optional. The replication factor to use when creating the Kafka
topic (if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC PARTITION COUNT</strong>
<code>&lt;partition_count&gt;</code></td>
<td>Optional. The partition count to use when creating the Kafka topic
(if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC CONFIG</strong> <code>&lt;topic_config&gt;</code></td>
<td>Optional. Any topic-level configs to use when creating the Kafka
topic (if the Kafka topic does not already exist). See the <a
href="https://kafka.apache.org/documentation/#topicconfigs">Kafka
documentation</a> for available configs.<br />
Default: empty.</td>
</tr>
<tr>
<td><strong>FORMAT TEXT</strong></td>
<td>Encode messages as plain text. Only supports single-column
encoding.</td>
</tr>
<tr>
<td><strong>FORMAT BYTES</strong></td>
<td>Encode messages as raw bytes. Only supports single-column encoding
and scalar data types.</td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how changes to the sink’s upstream relation
are mapped to Kafka messages. Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>DEBEZIUM</code></td>
<td>The generated schemas have a <a
href="#debezium-envelope">Debezium-style diff envelope</a> to capture
changes in the input view or source.</td>
</tr>
<tr>
<td><code>UPSERT</code></td>
<td>The sink emits data with <a href="#upsert-envelope">upsert
semantics</a>. Requires a unique key specified using the
<code>KEY</code> option.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>WITH</strong> (<code>&lt;with_option&gt;</code> [, …])</td>
<td><p>Optional. The following <code>&lt;with_option&gt;</code>s are
supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>SNAPSHOT = &lt;snapshot&gt;</code></td>
<td>Default: <code>true</code>. Whether to emit the consolidated results
of the query before the sink was created at the start of the sink. To
see only results after the sink is created, specify
<code>WITH (SNAPSHOT = false)</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

<div id="tab-key-format-value-format" class="tab-pane"
title="KEY FORMAT VALUE FORMAT">

By default, the message key is encoded using the same format as the
message value. However, you can set the key and value encodings
explicitly using the `KEY FORMAT ... VALUE FORMAT`.

<div class="highlight">

``` chroma
CREATE SINK [IF NOT EXISTS] <sink_name>
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
[KEY ( <key_col1> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
KEY FORMAT <key_format> VALUE FORMAT <value_format>
-- <key_format> and <value_format> can be:
-- AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name> [
--     (
--       [AVRO KEY FULLNAME '<avro_key_fullname>']
--       [, AVRO VALUE FULLNAME '<avro_value_fullname>']
--       [, NULL DEFAULTS <null_defaults>]
--       [, DOC ON <doc_on_option> [, ...]]
--       [, KEY COMPATIBILITY LEVEL '<key_compatibility_level>']
--       [, VALUE COMPATIBILITY LEVEL '<value_compatibility_level>']
--     )
-- ]
-- | JSON | TEXT | BYTES
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;sink_name&gt;</code></td>
<td>The name for the sink.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a sink with the
same name already exists. Instead, issue a notice and skip the sink
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this sink.</td>
</tr>
<tr>
<td><code>&lt;item_name&gt;</code></td>
<td>The name of the source, table, or materialized view you want to send
to the sink.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the sink. For details on
creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td>The name of the Kafka topic to write to.</td>
</tr>
<tr>
<td><strong>COMPRESSION TYPE</strong>
<code>&lt;compression_type&gt;</code></td>
<td>Optional. The type of compression to apply to messages before they
are sent to Kafka: <code>none</code>, <code>gzip</code>,
<code>snappy</code>, <code>lz4</code>, or <code>zstd</code>.<br />
Default:
<p><code>lz4</code></p></td>
</tr>
<tr>
<td><strong>TRANSACTIONAL ID PREFIX</strong>
<code>'&lt;transactional_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the transactional ID to use when producing
to the Kafka topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>PARTITION BY</strong> = <code>&lt;expression&gt;</code></td>
<td>Optional. A SQL expression returning a hash that can be used for
partition assignment. See <a href="#partitioning">Partitioning</a> for
details.</td>
</tr>
<tr>
<td><strong>PROGRESS GROUP ID PREFIX</strong>
<code>'&lt;progress_group_id_prefix&gt;'</code></td>
<td>Optional. The prefix of the consumer group ID to use when reading
from the progress topic.<br />
Default:
<code>materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}</code>.</td>
</tr>
<tr>
<td><strong>TOPIC REPLICATION FACTOR</strong>
<code>&lt;replication_factor&gt;</code></td>
<td>Optional. The replication factor to use when creating the Kafka
topic (if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC PARTITION COUNT</strong>
<code>&lt;partition_count&gt;</code></td>
<td>Optional. The partition count to use when creating the Kafka topic
(if the Kafka topic does not already exist).<br />
Default: Broker’s default.</td>
</tr>
<tr>
<td><strong>TOPIC CONFIG</strong> <code>&lt;topic_config&gt;</code></td>
<td>Optional. Any topic-level configs to use when creating the Kafka
topic (if the Kafka topic does not already exist). See the <a
href="https://kafka.apache.org/documentation/#topicconfigs">Kafka
documentation</a> for available configs.<br />
Default: empty.</td>
</tr>
<tr>
<td><strong>KEY</strong> ( <code>&lt;key_col1&gt;</code> [, …] )
[<strong>NOT ENFORCED</strong>]</td>
<td>Optional. A list of columns to use as the Kafka message key. If
unspecified, the Kafka key is left unset. When using the upsert
envelope, the key must be unique. Use <strong>NOT ENFORCED</strong> to
disable validation of key uniqueness. See <a
href="#upsert-key-selection">Upsert key selection</a> for details.</td>
</tr>
<tr>
<td><strong>HEADERS</strong> <code>&lt;headers_column&gt;</code></td>
<td>Optional. A column containing headers to add to each Kafka message
emitted by the sink. The column must be of type
<code>map[text =&gt; text]</code> or <code>map[text =&gt; bytea]</code>.
See <a href="#headers">Headers</a> for details.</td>
</tr>
<tr>
<td><strong>KEY FORMAT</strong> <code>&lt;key_format&gt;</code></td>
<td>Set the key encoding explicitly. Supported formats:
<code>AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION &lt;csr_connection_name&gt;</code>,
<code>JSON</code>, <code>TEXT</code>, <code>BYTES</code>.</td>
</tr>
<tr>
<td><strong>VALUE FORMAT</strong> <code>&lt;value_format&gt;</code></td>
<td>Set the value encoding explicitly. Supported formats:
<code>AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION &lt;csr_connection_name&gt;</code>,
<code>JSON</code>, <code>TEXT</code>, <code>BYTES</code>.</td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how changes to the sink’s upstream relation
are mapped to Kafka messages. Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>DEBEZIUM</code></td>
<td>The generated schemas have a <a
href="#debezium-envelope">Debezium-style diff envelope</a> to capture
changes in the input view or source.</td>
</tr>
<tr>
<td><code>UPSERT</code></td>
<td>The sink emits data with <a href="#upsert-envelope">upsert
semantics</a>. Requires a unique key specified using the
<code>KEY</code> option.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>WITH</strong> (<code>&lt;with_option&gt;</code> [, …])</td>
<td><p>Optional. The following <code>&lt;with_option&gt;</code>s are
supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>SNAPSHOT = &lt;snapshot&gt;</code></td>
<td>Default: <code>true</code>. Whether to emit the consolidated results
of the query before the sink was created at the start of the sink. To
see only results after the sink is created, specify
<code>WITH (SNAPSHOT = false)</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

## Headers

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

Materialize always adds a header with key `materialize-timestamp` to
each message emitted by the sink. The value of this header indicates the
logical time at which the event described by the message occurred.

The `HEADERS` option allows specifying the name of a column containing
additional headers to add to each message emitted by the sink. When the
option is unspecified, no additional headers are added. When specified,
the named column must be of type `map[text => text]` or
`map[text => bytea]`.

Header keys starting with `materialize-` are reserved for Materialize’s
internal use. Materialize will ignore any headers in the map whose key
starts with `materialize-`.

**Known limitations:**

- Materialize does not permit adding multiple headers with the same key.
- Materialize cannot omit the headers column from the message value.
- Materialize only supports using the `HEADERS` option with the [upsert
  envelope](#upsert-envelope).

## Formats

The `FORMAT` option controls the encoding of the message key and value
that Materialize writes to Kafka.

To use a different format for keys and values, use
`KEY FORMAT .. VALUE FORMAT ..` to choose independent formats for each.

### Avro

**Syntax:** `FORMAT AVRO`

When using the Avro format, the value of each Kafka message is an Avro
record containing a field for each column of the sink’s upstream
relation. The names and ordering of the fields in the record match the
names and ordering of the columns in the relation.

If the `KEY` option is specified, the key of each Kafka message is an
Avro record containing a field for each key column, in the same order
and with the same names.

If a column name is not a valid Avro name, Materialize adjusts the name
according to the following rules:

- Replace all non-alphanumeric characters with underscores.
- If the name begins with a number, add an underscore at the start of
  the name.
- If the adjusted name is not unique, add the smallest number possible
  to the end of the name to make it unique.

For example, consider a table with two columns named `col-a` and
`col@a`. Materialize will use the names `col_a` and `col_a1`,
respectively, in the generated Avro schema.

When using a Confluent Schema Registry:

- Materialize will automatically publish Avro schemas for the key, if
  present, and the value to the registry.

- You can specify the
  [fullnames](https://avro.apache.org/docs/++version++/specification/#names)
  for the Avro schemas Materialize generates using the
  `AVRO KEY FULLNAME` and `AVRO VALUE FULLNAME` [syntax](#syntax).

- You can automatically have nullable fields in the Avro schemas default
  to `null` by using the [`NULL DEFAULTS` option](#syntax).

- You can [add `doc` fields](#avro-schema-documentation) to the Avro
  schemas.

SQL types are converted to Avro types according to the following
conversion table:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>SQL type</th>
<th>Avro type</th>
</tr>
</thead>
<tbody>
<tr>
<td><a href="../../types/integer"><code>bigint</code></a></td>
<td><code>"long"</code></td>
</tr>
<tr>
<td><a href="../../types/boolean"><code>boolean</code></a></td>
<td><code>"boolean"</code></td>
</tr>
<tr>
<td><a href="../../types/bytea"><code>bytea</code></a></td>
<td><code>"bytes"</code></td>
</tr>
<tr>
<td><a href="../../types/date"><code>date</code></a></td>
<td><code>{"type": "int", "logicalType": "date"}</code></td>
</tr>
<tr>
<td><a href="../../types/float"><code>double precision</code></a></td>
<td><code>"double"</code></td>
</tr>
<tr>
<td><a href="../../types/integer"><code>integer</code></a></td>
<td><code>"int"</code></td>
</tr>
<tr>
<td><a href="../../types/interval"><code>interval</code></a></td>
<td><code>{"type": "fixed", "size": 16, "name": "com.materialize.sink.interval"}</code></td>
</tr>
<tr>
<td><a href="../../types/jsonb"><code>jsonb</code></a></td>
<td><code>{"type": "string", "connect.name": "io.debezium.data.Json"}</code></td>
</tr>
<tr>
<td><a href="../../types/map"><code>map</code></a></td>
<td><code>{"type": "map", "values": ...}</code></td>
</tr>
<tr>
<td><a href="../../types/list"><code>list</code></a></td>
<td><code>{"type": "array", "items": ...}</code></td>
</tr>
<tr>
<td><a href="../../types/numeric"><code>numeric(p,s)</code></a></td>
<td><code>{"type": "bytes", "logicalType": "decimal", "precision": p, "scale": s}</code></td>
</tr>
<tr>
<td><a href="../../types/oid"><code>oid</code></a></td>
<td><code>{"type": "fixed", "size": 4, "name": "com.materialize.sink.uint4"}</code></td>
</tr>
<tr>
<td><a href="../../types/float"><code>real</code></a></td>
<td><code>"float"</code></td>
</tr>
<tr>
<td><a href="../../types/record"><code>record</code></a></td>
<td><code>{"type": "record", "name": ..., "fields": ...}</code></td>
</tr>
<tr>
<td><a href="../../types/integer"><code>smallint</code></a></td>
<td><code>"int"</code></td>
</tr>
<tr>
<td><a href="../../types/text"><code>text</code></a></td>
<td><code>"string"</code></td>
</tr>
<tr>
<td><a href="../../types/time"><code>time</code></a></td>
<td><code>{"type": "long", "logicalType": "time-micros"}</code></td>
</tr>
<tr>
<td><a href="../../types/uint"><code>uint2</code></a></td>
<td><code>{"type": "fixed", "size": 2, "name": "com.materialize.sink.uint2"}</code></td>
</tr>
<tr>
<td><a href="../../types/uint"><code>uint4</code></a></td>
<td><code>{"type": "fixed", "size": 4, "name": "com.materialize.sink.uint4"}</code></td>
</tr>
<tr>
<td><a href="../../types/uint"><code>uint8</code></a></td>
<td><code>{"type": "fixed", "size": 8, "name": "com.materialize.sink.uint8"}</code></td>
</tr>
<tr>
<td><a href="../../types/timestamp"><code>timestamp (p)</code></a></td>
<td>If precision <code>p</code> is less than or equal to 3:<br />
<code>{"type": "long", "logicalType: "timestamp-millis"}</code><br />
Otherwise:<br />
<code>{"type": "long", "logicalType: "timestamp-micros"}</code></td>
</tr>
<tr>
<td><a
href="../../types/timestamp"><code>timestamptz (p)</code></a></td>
<td>Same as <code>timestamp (p)</code>.</td>
</tr>
<tr>
<td><a href="../../types/array">Arrays</a></td>
<td><code>{"type": "array", "items": ...}</code></td>
</tr>
</tbody>
</table>

If a SQL column is nullable, and its type converts to Avro type `t`
according to the above table, the Avro type generated for that column
will be `["null", t]`, since nullable fields are represented as unions
in Avro.

In the case of a sink on a materialized view, Materialize may not be
able to infer the non-nullability of columns in all cases, and will
conservatively assume the columns are nullable, thus producing a union
type as described above. If this is not desired, the materialized view
may be created using [non-null
assertions](../../create-materialized-view#non-null-assertions).

#### Avro schema documentation

Materialize allows control over the `doc` attribute for record fields
and types in the generated Avro schemas for the sink.

For the container record type (named `row` for the key schema and
`envelope` for the value schema, unless overridden by the
[`AVRO ... FULLNAME` options](#syntax)), Materialize searches for
documentation in the following locations, in order:

1.  For the key schema, a [`KEY DOC ON TYPE` option](#syntax) naming the
    sink’s upstream relation. For the value schema, a
    [`VALUE DOC ON TYPE` option](#syntax) naming the sink’s upstream
    relation.
2.  A [comment](/docs/sql/comment-on) on the sink’s upstream relation.

For record types within the container record type, Materialize searches
for documentation in the following locations, in order:

1.  For the key schema, a [`KEY DOC ON TYPE` option](#syntax) naming the
    SQL type corresponding to the record type. For the value schema, a
    [`VALUE DOC ON TYPE` option](#syntax) naming the SQL type
    corresponding to the record type.
2.  A [`DOC ON TYPE` option](#syntax) naming the SQL type corresponding
    to the record type.
3.  A [comment](/docs/sql/comment-on) on the SQL type corresponding to
    the record type.

Similarly, for each field of each record type in the Avro schema,
Materialize documentation in the following locations, in order:

1.  For the key schema, a [`KEY DOC ON COLUMN` option](#syntax) naming
    the SQL column corresponding to the field. For the value schema, a
    [`VALUE DOC ON COLUMN` option](#syntax) naming the column
    corresponding to the field.
2.  A [`DOC ON COLUMN` option](#syntax) naming the SQL column
    corresponding to the field.
3.  A [comment](/docs/sql/comment-on) on the SQL column corresponding to
    the field.

For each field or type, Materialize uses the documentation from the
first location that exists. If no documentation is found for a given
field or type, the `doc` attribute is omitted for that field or type.

### JSON

**Syntax:** `FORMAT JSON`

When using the JSON format, the value of each Kafka message is a JSON
object containing a field for each column of the sink’s upstream
relation. The names and ordering of the fields in the record match the
names and ordering of the columns in the relation.

If the `KEY` option is specified, the key of each Kafka message is a
JSON object containing a field for each key column, in the same order
and with the same names.

SQL values are converted to JSON values according to the following
conversion table:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>SQL type</th>
<th>Conversion</th>
</tr>
</thead>
<tbody>
<tr>
<td>[<code>array</code>][<code>arrays</code>]</td>
<td>Values are converted to JSON arrays.</td>
</tr>
<tr>
<td><a href="../../types/integer"><code>bigint</code></a></td>
<td>Values are converted to JSON numbers.</td>
</tr>
<tr>
<td><a href="../../types/boolean"><code>boolean</code></a></td>
<td>Values are converted to <code>true</code> or
<code>false</code>.</td>
</tr>
<tr>
<td><a href="../../types/integer"><code>integer</code></a></td>
<td>Values are converted to JSON numbers.</td>
</tr>
<tr>
<td><a href="../../types/list"><code>list</code></a></td>
<td>Values are converted to JSON arrays.</td>
</tr>
<tr>
<td><a href="../../types/numeric"><code>numeric</code></a></td>
<td>Values are converted to a JSON string containing the decimal
representation of the number.</td>
</tr>
<tr>
<td><a href="../../types/record"><code>record</code></a></td>
<td>Records are converted to JSON objects. The names and ordering of the
fields in the object match the names and ordering of the fields in the
record.</td>
</tr>
<tr>
<td><a href="../../types/integer"><code>smallint</code></a></td>
<td>values are converted to JSON numbers.</td>
</tr>
<tr>
<td><a href="../../types/timestamp"><code>timestamp</code></a><br />
<a href="../../types/timestamp"><code>timestamptz</code></a></td>
<td>Values are converted to JSON strings containing the fractional
number of milliseconds since the Unix epoch. The fractional component
has microsecond precision (i.e., three digits of precision). Example:
<code>"1720032185.312"</code></td>
</tr>
<tr>
<td><a href="../../types/uint"><code>uint2</code></a></td>
<td>Values are converted to JSON numbers.</td>
</tr>
<tr>
<td><a href="../../types/uint"><code>uint4</code></a></td>
<td>Values are converted to JSON numbers.</td>
</tr>
<tr>
<td><a href="../../types/uint"><code>uint8</code></a></td>
<td>Values are converted to JSON numbers.</td>
</tr>
<tr>
<td>Other</td>
<td>Values are cast to <a href="../../types/text"><code>text</code></a>
and then converted to JSON strings.</td>
</tr>
</tbody>
</table>

### Text/Bytes

The `TEXT` and `BYTES` format options only support single-column
encoding and cannot be used for keys or values with multiple columns.

Additionally, the `BYTES` format only works with scalar data types.

## Envelopes

The sink’s envelope determines how changes to the sink’s upstream
relation are mapped to Kafka messages.

There are two fundamental types of change events:

- An **insertion** event is the addition of a new row to the upstream
  relation.
- A **deletion** event is the removal of an existing row from the
  upstream relation.

When a `KEY` is specified, an insertion event and deletion event that
occur at the same time are paired together into a single **update**
event that contains both the old and new value for the given key.

### Upsert

**Syntax:** `ENVELOPE UPSERT`

The upsert envelope:

- Requires that you specify a unique key for the sink’s upstream
  relation using the `KEY` option. See [upsert key
  selection](#upsert-key-selection) for details.
- For an insertion event, emits the row without additional decoration.
- For an update event, emits the new row without additional decoration.
  The old row is not emitted.
- For a deletion event, emits a message with a `null` value (i.e., a
  *tombstone*).

Consider using the upsert envelope if:

- You need to follow standard Kafka conventions for upsert semantics.
- You want to enable key-based compaction on the sink’s Kafka topic
  while retaining the most recent value for each key.

### Debezium

**Syntax:** `ENVELOPE DEBEZIUM`

The Debezium envelope wraps each event in an object containing a
`before` and `after` field to indicate whether the event was an
insertion, deletion, or update event:

<div class="highlight">

``` chroma
// Insertion event.
{"before": null, "after": {"field1": "val1", ...}}

// Deletion event.
{"before": {"field1": "val1", ...}, "after": null}

// Update event.
{"before": {"field1": "oldval1", ...}, "after": {"field1": "newval1", ...}}
```

</div>

Note that the sink will only produce update events if a `KEY` is
specified.

Consider using the Debezium envelope if:

- You have downstream consumers that want update events to contain both
  the old and new value of the row.
- There is no natural `KEY` for the sink.

## Features

### Automatic topic creation

If the specified Kafka topic does not exist, Materialize will attempt to
create it using the broker’s default number of partitions, default
replication factor, default compaction policy, and default retention
policy, unless any specific overrides are provided as part of the
[connection options](#syntax).

If the connection’s [progress topic](#exactly-once-processing) does not
exist, Materialize will attempt to create it with a single partition,
the broker’s default replication factor, compaction enabled, and both
size- and time-based retention disabled. The replication factor can be
overridden using the `PROGRESS TOPIC REPLICATION FACTOR` option when
creating a connection
[`CREATE CONNECTION`](/docs/sql/create-connection).

To customize topic-level configuration, including compaction settings
and other values, use the `TOPIC CONFIG` option in the [connection
options](#syntax) to set any relevant kafka [topic
configs](https://kafka.apache.org/documentation/#topicconfigs).

If you manually create the topic or progress topic in Kafka before
running `CREATE SINK`, observe the following guidance:

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Topic</th>
<th>Configuration</th>
<th>Guidance</th>
</tr>
</thead>
<tbody>
<tr>
<td>Data topic</td>
<td>Partition count</td>
<td>Your choice, based on your performance and ordering
requirements.</td>
</tr>
<tr>
<td>Data topic</td>
<td>Replication factor</td>
<td>Your choice, based on your durability requirements.</td>
</tr>
<tr>
<td>Data topic</td>
<td>Compaction</td>
<td>Your choice, based on your downstream applications’ requirements. If
using the <a href="#upsert">Upsert envelope</a>, enabling compaction is
typically the right choice.</td>
</tr>
<tr>
<td>Data topic</td>
<td>Retention</td>
<td>Your choice, based on your downstream applications’
requirements.</td>
</tr>
<tr>
<td>Progress topic</td>
<td>Partition count</td>
<td><strong>Must be set to 1.</strong> Using multiple partitions can
cause Materialize to violate its <a
href="#exactly-once-processing">exactly-once guarantees</a>.</td>
</tr>
<tr>
<td>Progress topic</td>
<td>Replication factor</td>
<td>Your choice, based on your durability requirements.</td>
</tr>
<tr>
<td>Progress topic</td>
<td>Compaction</td>
<td>We recommend enabling compaction to avoid accumulating unbounded
state. Disabling compaction may cause performance issues, but will not
cause correctness issues.</td>
</tr>
<tr>
<td>Progress topic</td>
<td>Retention</td>
<td><strong>Must be disabled.</strong> Enabling retention can cause
Materialize to violate its <a
href="#exactly-once-processing">exactly-once guarantees</a>.</td>
</tr>
<tr>
<td>Progress topic</td>
<td>Tiered storage</td>
<td>We recommend disabling tiered storage to allow for more aggressive
data compaction. Fully compacted data requires minimal storage,
typically only tens of bytes per sink, making it cost-effective to
maintain directly on local disk.</td>
</tr>
<tr>
<td><div class="warning">
<strong>WARNING!</strong> Dropping a Kafka sink doesn’t drop the
corresponding topic. For more information, see the <a
href="https://kafka.apache.org/documentation/">Kafka documentation</a>.
</div></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>

### Exactly-once processing

By default, Kafka sinks provide [exactly-once processing
guarantees](https://kafka.apache.org/documentation/#semantics), which
ensures that messages are not duplicated or dropped in failure
scenarios.

To achieve this, Materialize stores some internal metadata in an
additional *progress topic*. This topic is shared among all sinks that
use a particular [Kafka connection](/docs/sql/create-connection/#kafka).
The name of the progress topic can be specified when [creating a
connection](/docs/sql/create-connection/#kafka); otherwise, a default
name of `_materialize-progress-{REGION ID}-{CONNECTION ID}` is used. In
either case, Materialize will attempt to create the topic if it does not
exist. The contents of this topic are not user-specified.

#### End-to-end exactly-once processing

Exactly-once semantics are an end-to-end property of a system, but
Materialize only controls the initial produce step. To ensure
*end-to-end* exactly-once message delivery, you should ensure that:

- The broker is configured with replication factor greater than 3, with
  unclean leader election disabled
  (`unclean.leader.election.enable=false`).
- All downstream consumers are configured to only read committed data
  (`isolation.level=read_committed`).
- The consumers’ processing is idempotent, and offsets are only
  committed when processing is complete.

For more details, see [the Kafka
documentation](https://kafka.apache.org/documentation/).

### Partitioning

By default, Materialize assigns a partition to each message using the
following strategy:

1.  Encode the message’s key in the specified format.
2.  If the format uses a Confluent Schema Registry, strip out the schema
    ID from the encoded bytes.
3.  Hash the remaining encoded bytes using
    [SeaHash](https://docs.rs/seahash/latest/seahash/).
4.  Divide the hash value by the topic’s partition count and assign the
    remainder as the message’s partition.

If a message has no key, all messages are sent to partition 0.

To configure a custom partitioning strategy, you can use the
`PARTITION BY` option. This option allows you to specify a SQL
expression that computes a hash for each message, which determines what
partition to assign to the message:

<div class="highlight">

``` chroma
-- General syntax.
CREATE SINK ... INTO KAFKA CONNECTION <name> (PARTITION BY = <expression>) ...;

-- Example.
CREATE SINK ... INTO KAFKA CONNECTION <name> (
    PARTITION BY = kafka_murmur2(name || address)
) ...;
```

</div>

The expression:

- Must have a type that can be assignment cast to
  [`uint8`](../../types/uint).
- Can refer to any column in the sink’s underlying relation when using
  the [upsert envelope](#upsert-envelope).
- Can refer to any column in the sink’s key when using the [Debezium
  envelope](#debezium-envelope).

Materialize uses the computed hash value to assign a partition to each
message as follows:

1.  If the hash is `NULL` or computing the hash produces an error,
    assign partition 0.
2.  Otherwise, divide the hash value by the topic’s partition count and
    assign the remainder as the message’s partition (i.e.,
    `partition_id = hash % partition_count`).

Materialize provides several [hash
functions](/docs/sql/functions/#hash-functions) which are commonly used
in Kafka partition assignment:

- `crc32`
- `kafka_murmur2`
- `seahash`

For a full example of using the `PARTITION BY` option, see [Custom
partioning](#custom-partitioning).

## Required privileges

To execute the `CREATE SINK` command, you need:

- `CREATE` privileges on the containing schema.
- `SELECT` privileges on the item being written out to an external
  system.
  - NOTE: if the item is a materialized view, then the view owner must
    also have the necessary privileges to execute the view definition.
- `CREATE` privileges on the containing cluster if the sink is created
  in an existing cluster.
- `CREATECLUSTER` privileges on the system if the sink is not created in
  an existing cluster.
- `USAGE` privileges on all connections and secrets used in the sink
  definition.
- `USAGE` privileges on the schemas that all connections and secrets in
  the statement are contained in.

See also [Required Kafka ACLs](#required-kafka-acls).

## Required Kafka ACLs

The access control lists (ACLs) on the Kafka cluster must allow
Materialize to perform the following operations on the following
resources:

| Operation type | Resource type | Resource name |
|----|----|----|
| Read, Write | Topic | Consult `mz_kafka_connections.sink_progress_topic` for the sink’s connection |
| Write | Topic | The specified [`TOPIC` option](#syntax) |
| Write | Transactional ID | All transactional IDs beginning with the specified [`TRANSACTIONAL ID PREFIX` option](#syntax) |
| Read | Group | All group IDs beginning with the specified [`PROGRESS GROUP ID PREFIX` option](#syntax) |

When using [automatic topic creation](#automatic-topic-creation),
Materialize additionally requires access to the following operations:

| Operation type  | Resource type | Resource name                |
|-----------------|---------------|------------------------------|
| DescribeConfigs | Cluster       | n/a                          |
| Create          | Topic         | The specified `TOPIC` option |

## Kafka transaction markers

Materialize uses [Kafka
transactions](https://www.confluent.io/blog/transactions-apache-kafka/).
When Kafka transactions are used, special control messages known as
**transaction markers** are published to the topic. Transaction markers
inform both the broker and clients about the status of a transaction.
When a topic is read using a standard Kafka consumer, these markers are
not exposed to the application, which can give the impression that some
offsets are being skipped.

## Troubleshooting

### Upsert key selection

The `KEY` that you specify for an upsert envelope sink must be a unique
key of the sink’s upstream relation.

Materialize will attempt to validate the uniqueness of the specified
key. If validation fails, you’ll receive an error message like one of
the following:

```
ERROR:  upsert key could not be validated as unique
DETAIL: Materialize could not prove that the specified upsert envelope key
("col1") is a unique key of the upstream relation. There are no known
valid unique keys for the upstream relation.

ERROR:  upsert key could not be validated as unique
DETAIL: Materialize could not prove that the specified upsert envelope key
("col1") is a unique key of the upstream relation. The following keys
are known to be unique for the upstream relation:
  ("col2")
  ("col3", "col4")
```

The first error message indicates that Materialize could not prove the
existence of any unique keys for the sink’s upstream relation. The
second error message indicates that Materialize could prove that `col2`
and `(col3, col4)` were unique keys of the sink’s upstream relation, but
could not provide the uniqueness of the specified upsert key of `col1`.

There are three ways to resolve this error:

- Change the sink to use one of the keys that Materialize determined to
  be unique, if such a key exists and has the appropriate semantics for
  your use case.

- Create a materialized view that deduplicates the input relation by the
  desired upsert key:

  <div class="highlight">

  ``` chroma
  -- For each row with the same key `k`, the `ORDER BY` clause ensures we
  -- keep the row with the largest value of `v`.
  CREATE MATERIALIZED VIEW deduped AS
  SELECT DISTINCT ON (k) v
  FROM original_input
  ORDER BY k, v DESC;

  -- Materialize can now prove that `k` is a unique key of `deduped`.
  CREATE SINK s
  FROM deduped
  INTO KAFKA CONNECTION kafka_connection (TOPIC 't')
  KEY (k)
  FORMAT JSON ENVELOPE UPSERT;
  ```

  </div>

  <div class="note">

  **NOTE:** Maintaining the `deduped` materialized view requires memory
  proportional to the number of records in `original_input`. Be sure to
  assign `deduped` to a cluster with adequate resources to handle your
  data volume.

  </div>

- Use the `NOT ENFORCED` clause to disable Materialize’s validation of
  the key’s uniqueness:

  <div class="highlight">

  ``` chroma
  CREATE SINK s
  FROM original_input
  INTO KAFKA CONNECTION kafka_connection (TOPIC 't')
  -- We have outside knowledge that `k` is a unique key of `original_input`, but
  -- Materialize cannot prove this, so we disable its key uniqueness check.
  KEY (k) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT;
  ```

  </div>

  You should only disable this verification if you have outside
  knowledge of the properties of your data that guarantees the
  uniqueness of the key you have specified.

  <div class="warning">

  **WARNING!** If the key is not in fact unique, downstream consumers
  may not be able to correctly interpret the data in the topic, and
  Kafka key compaction may incorrectly garbage collect records from the
  topic.

  </div>

## Examples

### Creating a connection

A connection describes how to connect and authenticate to an external
system you want Materialize to write data to.

Once created, a connection is **reusable** across multiple `CREATE SINK`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/docs/sql/create-connection) documentation page.

#### Broker

<div class="code-tabs">

<div class="tab-content">

<div id="tab-ssl" class="tab-pane" title="SSL">

<div class="highlight">

``` chroma
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9093',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```

</div>

</div>

<div id="tab-sasl" class="tab-pane" title="SASL">

<div class="highlight">

``` chroma
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```

</div>

</div>

</div>

</div>

#### Confluent Schema Registry

<div class="code-tabs">

<div class="tab-content">

<div id="tab-ssl" class="tab-pane" title="SSL">

<div class="highlight">

``` chroma
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_ssl TO CONFLUENT SCHEMA REGISTRY (
    URL 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9093',
    SSL KEY = SECRET csr_ssl_key,
    SSL CERTIFICATE = SECRET csr_ssl_crt,
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
);
```

</div>

</div>

<div id="tab-basic-http-authentication" class="tab-pane"
title="Basic HTTP Authentication">

<div class="highlight">

``` chroma
CREATE SECRET IF NOT EXISTS csr_username AS '<CSR_USERNAME>';
CREATE SECRET IF NOT EXISTS csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_basic_http
  FOR CONFLUENT SCHEMA REGISTRY
  URL '<CONFLUENT_REGISTRY_URL>',
  USERNAME = SECRET csr_username,
  PASSWORD = SECRET csr_password;
```

</div>

</div>

</div>

</div>

### Creating a sink

#### Upsert envelope

<div class="code-tabs">

<div class="tab-content">

<div id="tab-avro" class="tab-pane" title="Avro">

<div class="highlight">

``` chroma
CREATE SINK avro_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  KEY (key_col)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

</div>

</div>

<div id="tab-json" class="tab-pane" title="JSON">

<div class="highlight">

``` chroma
CREATE SINK json_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_json_topic')
  KEY (key_col)
  FORMAT JSON
  ENVELOPE UPSERT;
```

</div>

</div>

</div>

</div>

#### Debezium envelope

<div class="code-tabs">

<div class="tab-content">

<div id="tab-avro" class="tab-pane" title="Avro">

<div class="highlight">

``` chroma
CREATE SINK avro_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM;
```

</div>

</div>

</div>

</div>

#### Topic configuration

<div class="highlight">

``` chroma
CREATE SINK custom_topic_sink
  IN CLUSTER my_io_cluster
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (
    TOPIC 'test_avro_topic',
    TOPIC PARTITION COUNT 4,
    TOPIC REPLICATION FACTOR 2,
    TOPIC CONFIG MAP['cleanup.policy' => 'compact']
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

</div>

#### Schema compatibility levels

<div class="highlight">

``` chroma
CREATE SINK compatibility_level_sink
  IN CLUSTER my_io_cluster
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (
    TOPIC 'test_avro_topic',
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection (
    KEY COMPATIBILITY LEVEL 'BACKWARD',
    VALUE COMPATIBILITY LEVEL 'BACKWARD_TRANSITIVE'
  )
  ENVELOPE UPSERT;
```

</div>

#### Documentation comments

Consider the following sink, `docs_sink`, built on top of a relation `t`
with several [SQL comments](/docs/sql/comment-on) attached.

<div class="highlight">

``` chroma
CREATE TABLE t (key int NOT NULL, value text NOT NULL);
COMMENT ON TABLE t IS 'SQL comment on t';
COMMENT ON COLUMN t.value IS 'SQL comment on t.value';

CREATE SINK docs_sink
FROM t
INTO KAFKA CONNECTION kafka_connection (TOPIC 'doc-commont-example')
KEY (key)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection (
    DOC ON TYPE t = 'Top-level comment for container record in both key and value schemas',
    KEY DOC ON COLUMN t.key = 'Comment on column only in key schema',
    VALUE DOC ON COLUMN t.key = 'Comment on column only in value schema'
)
ENVELOPE UPSERT;
```

</div>

When `docs_sink` is created, Materialize will publish the following Avro
schemas to the Confluent Schema Registry:

- Key schema:

  <div class="highlight">

  ``` chroma
  {
    "type": "record",
    "name": "row",
    "doc": "Top-level comment for container record in both key and value schemas",
    "fields": [
      {
        "name": "key",
        "type": "int",
        "doc": "Comment on column only in key schema"
      }
    ]
  }
  ```

  </div>

- Value schema:

  <div class="highlight">

  ``` chroma
  {
    "type": "record",
    "name": "envelope",
    "doc": "Top-level comment for container record in both key and value schemas",
    "fields": [
      {
        "name": "key",
        "type": "int",
        "doc": "Comment on column only in value schema"
      },
      {
        "name": "value",
        "type": "string",
        "doc": "SQL comment on t.value"
      }
    ]
  }
  ```

  </div>

See [Avro schema documentation](#avro-schema-documentation) for details
about the rules by which Materialize attaches `doc` fields to records.

#### Custom partitioning

Suppose your Materialize deployment stores data about customers and
their orders. You want to emit the order data to Kafka with upsert
semantics so that only the latest state of each order is retained.
However, you want the data to be partitioned by only customer ID (i.e.,
not order ID), so that all orders for a given customer go to the same
partition.

Create a sink using the `PARTITION BY` option to accomplish this:

<div class="highlight">

``` chroma
CREATE SINK customer_orders
  FROM ...
  INTO KAFKA CONNECTION kafka_connection (
    TOPIC 'customer-orders',
    -- The partition hash includes only the customer ID, so the partition
    -- will be assigned only based on the customer ID.
    PARTITION BY = seahash(customer_id::text)
  )
  -- The key includes both the customer ID and order ID, so Kafka's compaction
  -- will keep only the latest message for each order ID.
  KEY (customer_id, order_id)
  FORMAT JSON
  ENVELOPE UPSERT;
```

</div>

## Related pages

- [`SHOW SINKS`](/docs/sql/show-sinks)
- [`DROP SINK`](/docs/sql/drop-sink)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-sink/kafka.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2026 Materialize Inc.

</div>
