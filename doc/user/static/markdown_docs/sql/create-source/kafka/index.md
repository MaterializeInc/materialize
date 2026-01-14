<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [CREATE
SOURCE](/docs/sql/create-source/)

</div>

# CREATE SOURCE: Kafka/Redpanda

[`CREATE SOURCE`](/docs/sql/create-source/) connects Materialize to an
external system you want to read data from, and provides details about
how to decode and interpret that data.

To connect to a Kafka/Redpanda broker (and optionally a schema
registry), you first need to [create a
connection](#prerequisite-creating-a-connection) that specifies access
and authentication parameters. Once created, a connection is
**reusable** across multiple `CREATE SOURCE` and `CREATE SINK`
statements.

<div class="note">

**NOTE:** The same syntax, supported formats and features can be used to
connect to a [Redpanda](/docs/integrations/redpanda/) broker.

</div>

## Syntax

<div class="code-tabs">

<div class="tab-content">

<div id="tab-format-avro" class="tab-pane" title="Format Avro">

### Format Avro

Materialize can decode Avro messages by integrating with a schema
registry to retrieve a schema, and automatically determine the columns
and data types to use in the source.

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name>
  [KEY STRATEGY <key_strategy>]
  [VALUE STRATEGY <value_strategy>]
[INCLUDE
    KEY [AS <name>]
  | PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE
  | DEBEZIUM
  | UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS <name>] ) ]
]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
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
<td><code>&lt;src_name&gt;</code></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the source. For details
on creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><code>'&lt;topic&gt;'</code></td>
<td>The Kafka topic you want to subscribe to.</td>
</tr>
<tr>
<td><strong>GROUP ID PREFIX</strong>
<code>&lt;group_id_prefix&gt;</code></td>
<td>Optional. The prefix of the consumer group ID to use. See <a
href="#monitoring-consumer-lag">Monitoring consumer lag</a>.<br />
Default:
<code>materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}</code></td>
</tr>
<tr>
<td><strong>START OFFSET</strong> (<code>&lt;partition_offset&gt;</code>
[, …])</td>
<td>Optional. Read partitions from the specified offset. You cannot
update the offsets once a source has been created; you will need to
recreate the source. Offset values must be zero or positive integers.
See <a href="#setting-start-offsets">Setting start offsets</a> for
details.</td>
</tr>
<tr>
<td><strong>START TIMESTAMP</strong> <code>&lt;timestamp&gt;</code></td>
<td>Optional. Use the specified value to set <code>START OFFSET</code>
based on the Kafka timestamp. Negative values will be interpreted as
relative to the current system time in milliseconds (e.g.
<code>-1000</code> means 1000 ms ago). See <a
href="#time-based-offsets">Time-based offsets</a> for details.</td>
</tr>
<tr>
<td><code>&lt;csr_connection_name&gt;</code></td>
<td>The Confluent Schema Registry connection to use in the source.</td>
</tr>
<tr>
<td><strong>KEY STRATEGY</strong> <code>&lt;key_strategy&gt;</code></td>
<td><p>Optional. Define how an Avro reader schema will be chosen for the
message key.</p>
<table>
<thead>
<tr>
<th>Strategy</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>LATEST</strong></td>
<td>(Default) Use the latest writer schema from the schema registry as
the reader schema.</td>
</tr>
<tr>
<td><strong>ID</strong></td>
<td>Use a specific schema from the registry.</td>
</tr>
<tr>
<td><strong>INLINE</strong></td>
<td>Use the inline schema.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>VALUE STRATEGY</strong>
<code>&lt;value_strategy&gt;</code></td>
<td><p>Optional. Define how an Avro reader schema will be chosen for the
message value.</p>
<table>
<thead>
<tr>
<th>Strategy</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>LATEST</strong></td>
<td>(Default) Use the latest writer schema from the schema registry as
the reader schema.</td>
</tr>
<tr>
<td><strong>ID</strong></td>
<td>Use a specific schema from the registry.</td>
</tr>
<tr>
<td><strong>INLINE</strong></td>
<td>Use the inline schema.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>INCLUDE</strong> <code>&lt;include_option&gt;</code></td>
<td><p>Optional. If specified, include the additional information as
column(s) in the table. The following
<code>&lt;include_option&gt;</code>s are supported:</p>
<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>KEY [AS &lt;name&gt;]</strong></td>
<td>Include a column containing the Kafka message key. If the key is
encoded using a format that includes schemas, the column will take its
name from the schema. For unnamed formats (e.g. <code>TEXT</code>), the
column will be named <code>key</code>. The column can be renamed with
the optional <strong>AS</strong> <em>name</em> statement.</td>
</tr>
<tr>
<td><strong>PARTITION [AS &lt;name&gt;]</strong></td>
<td>Include a <code>partition</code> column containing the Kafka message
partition. The column can be renamed with the optional
<strong>AS</strong> <em>name</em> clause.</td>
</tr>
<tr>
<td><strong>OFFSET [AS &lt;name&gt;]</strong></td>
<td>Include an <code>offset</code> column containing the Kafka message
offset. The column can be renamed with the optional <strong>AS</strong>
<em>name</em> clause.</td>
</tr>
<tr>
<td><strong>TIMESTAMP [AS &lt;name&gt;]</strong></td>
<td>Include a <code>timestamp</code> column containing the Kafka message
timestamp. The column can be renamed with the optional
<strong>AS</strong> <em>name</em> clause.<br />
<br />
Note that the timestamp of a Kafka message depends on how the topic and
its producers are configured. See the <a
href="https://docs.confluent.io/3.0.0/streams/concepts.html?#time">Confluent
documentation</a> for details.</td>
</tr>
<tr>
<td><strong>HEADERS [AS &lt;name&gt;]</strong></td>
<td>Include a <code>headers</code> column containing the Kafka message
headers as a list of records of type
<code>(key text, value bytea)</code>. The column can be renamed with the
optional <strong>AS</strong> <em>name</em> clause.</td>
</tr>
<tr>
<td><strong>HEADER &lt;key&gt; AS &lt;name&gt;
[<strong>BYTES</strong>]</strong></td>
<td>Include a <em>name</em> column containing the Kafka message header
<em>key</em> parsed as a UTF-8 string. To expose the header value as
<code>bytea</code>, use the <code>BYTES</code> option.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how Materialize interprets incoming records.
Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>NONE</code></td>
<td>Append-only envelope (default). Each message is inserted as a new
row.</td>
</tr>
<tr>
<td><code>DEBEZIUM</code></td>
<td>Decode Kafka messages produced by <a
href="https://debezium.io/">Debezium</a>.</td>
</tr>
<tr>
<td><code>UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS &lt;name&gt;] ) ]</code></td>
<td>Use the standard key-value convention to support inserts, updates,
and deletes. Required to consume <a
href="https://docs.confluent.io/platform/current/kafka/design.html#log-compaction">log
compacted topics</a>.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<code>&lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress collection for the source. If
this is not specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. See <a
href="#monitoring-source-progress">Monitoring source progress</a> for
details.</td>
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
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

#### Schema versioning

The *latest* schema is retrieved using the
[`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html)
strategy at the time the `CREATE SOURCE` statement is issued.

#### Schema evolution

As long as the writer schema changes in a [compatible
way](https://avro.apache.org/docs/++version++/specification/#schema-resolution),
Materialize will continue using the original reader schema definition by
mapping values from the new to the old schema version. To use the new
version of the writer schema in Materialize, you need to **drop and
recreate** the source.

#### Name collision

To avoid [case-sensitivity](/docs/sql/identifiers/#case-sensitivity)
conflicts with Materialize identifiers, we recommend double-quoting all
field names when working with Avro-formatted sources.

#### Supported types

Materialize supports all [Avro
types](https://avro.apache.org/docs/++version++/specification/), *except
for* recursive types and union types in arrays.

</div>

<div id="tab-format-json" class="tab-pane" title="Format JSON">

### Format JSON

Materialize can decode JSON messages into a single column named `data`
with type `jsonb`. Refer to the [`jsonb` type](/docs/sql/types/jsonb)
documentation for the supported operations on this type.

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT JSON
[INCLUDE
    PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
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
<td><code>&lt;src_name&gt;</code></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the source. For details
on creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td><strong>Required.</strong> The Kafka topic you want to subscribe
to.</td>
</tr>
<tr>
<td><strong>GROUP ID PREFIX</strong>
<code>&lt;group_id_prefix&gt;</code></td>
<td>Optional. The prefix of the consumer group ID to use. See <a
href="#monitoring-consumer-lag">Monitoring consumer lag</a>.<br />
Default:
<code>materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}</code></td>
</tr>
<tr>
<td><strong>START OFFSET</strong> (<code>&lt;partition_offset&gt;</code>
[, …])</td>
<td>Optional. Read partitions from the specified offset. You cannot
update the offsets once a source has been created; you will need to
recreate the source. Offset values must be zero or positive integers.
See <a href="#setting-start-offsets">Setting start offsets</a> for
details.</td>
</tr>
<tr>
<td><strong>START TIMESTAMP</strong> <code>&lt;timestamp&gt;</code></td>
<td>Optional. Use the specified value to set <code>START OFFSET</code>
based on the Kafka timestamp. Negative values will be interpreted as
relative to the current system time in milliseconds (e.g.
<code>-1000</code> means 1000 ms ago). See <a
href="#time-based-offsets">Time-based offsets</a> for details.</td>
</tr>
<tr>
<td><strong>FORMAT JSON</strong></td>
<td>Decode JSON-formatted messages. JSON-formatted messages are ingested
as a JSON blob. We recommend creating a parsing view on top of your
Kafka source that maps the individual fields to columns with the
required data types.</td>
</tr>
<tr>
<td><strong>INCLUDE</strong> <code>&lt;include_option&gt;</code></td>
<td><p>Optional. If specified, include the additional information as
column(s) in the table. The following
<code>&lt;include_option&gt;</code>s are supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>PARTITION [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka partition as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>OFFSET [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka offset as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>TIMESTAMP [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka timestamp as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>HEADERS [AS &lt;name&gt;]</code></td>
<td>Expose all message headers as a column with type
<code>record(key: text, value: bytea?) list</code>. See <a
href="#headers">Headers</a> for details.</td>
</tr>
<tr>
<td><code>HEADER '&lt;key&gt;' AS &lt;name&gt; [BYTES]</code></td>
<td>Expose a specific message header as a column. The <code>bytea</code>
value is automatically parsed into a UTF-8 string unless
<code>BYTES</code> is specified. See <a href="#headers">Headers</a> for
details.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how Materialize interprets incoming records.
Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>NONE</code></td>
<td>Append-only envelope (default). Each message is inserted as a new
row. See <a
href="/docs/sql/create-source/kafka/#append-only-envelope">Append-only
envelope</a> for details.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<code>&lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress collection for the source. If
this is not specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. See <a
href="#monitoring-source-progress">Monitoring source progress</a> for
details.</td>
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
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

If your JSON messages have a consistent shape, we recommend creating a
parsing [view](/docs/concepts/views) that maps the individual fields to
columns with the required data types:

<div class="highlight">

``` chroma
-- extract jsonb into typed columns
CREATE VIEW my_typed_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM my_jsonb_source;
```

</div>

To avoid doing this task manually, you can use [this **JSON parsing
widget**](/docs/sql/types/jsonb/#parsing).

#### Schema registry integration

Retrieving schemas from a schema registry is not supported yet for
JSON-formatted sources. This means that Materialize cannot decode
messages serialized using the [JSON
Schema](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer-and-deserializer)
serialization format (`JSON_SR`).

</div>

<div id="tab-format-textbytes" class="tab-pane"
title="Format TEXT/BYTES">

### Format Text/Bytes

Materialize can:

- Parse **new-line delimited** data as plain text. Data is assumed to be
  **valid unicode** (UTF-8), and discarded if it cannot be converted to
  UTF-8. Text-formatted sources have a single column, by default named
  `text`. For details on casting, check the
  [`text`](/docs/sql/types/text/) documentation.

- Read raw bytes without applying any formatting or decoding. Raw
  byte-formatted sources have a single column, by default named `data`.
  For details on encodings and casting, check the
  [`bytea`](/docs/sql/types/bytea/) documentation.

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT TEXT | BYTES
[INCLUDE
    PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
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
<td><code>&lt;src_name&gt;</code></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the source. For details
on creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td><strong>Required.</strong> The Kafka topic you want to subscribe
to.</td>
</tr>
<tr>
<td><strong>GROUP ID PREFIX</strong>
<code>&lt;group_id_prefix&gt;</code></td>
<td>Optional. The prefix of the consumer group ID to use. See <a
href="#monitoring-consumer-lag">Monitoring consumer lag</a>.<br />
Default:
<code>materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}</code></td>
</tr>
<tr>
<td><strong>START OFFSET</strong> (<code>&lt;partition_offset&gt;</code>
[, …])</td>
<td>Optional. Read partitions from the specified offset. You cannot
update the offsets once a source has been created; you will need to
recreate the source. Offset values must be zero or positive integers.
See <a href="#setting-start-offsets">Setting start offsets</a> for
details.</td>
</tr>
<tr>
<td><strong>START TIMESTAMP</strong> <code>&lt;timestamp&gt;</code></td>
<td>Optional. Use the specified value to set <code>START OFFSET</code>
based on the Kafka timestamp. Negative values will be interpreted as
relative to the current system time in milliseconds (e.g.
<code>-1000</code> means 1000 ms ago). See <a
href="#time-based-offsets">Time-based offsets</a> for details.</td>
</tr>
<tr>
<td><strong>FORMAT TEXT|BYTES</strong></td>
<td><ul>
<li><p>If <code>TEXT</code>, decode new-line delimited data as plain
text. Data is assumed to be valid unicode (UTF-8), and discarded if it
cannot be converted to UTF-8. Text-formatted sources have a single
column, by default named <code>text</code>.</p></li>
<li><p>If <code>BYTES</code>, read raw bytes without applying any
formatting or decoding. Raw byte-formatted sources have a single column,
by default named <code>data</code>.</p></li>
</ul></td>
</tr>
<tr>
<td><strong>INCLUDE</strong> <code>&lt;include_option&gt;</code></td>
<td><p>Optional. If specified, include the additional information as
column(s) in the table. The following
<code>&lt;include_option&gt;</code>s are supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>PARTITION [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka partition as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>OFFSET [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka offset as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>TIMESTAMP [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka timestamp as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>HEADERS [AS &lt;name&gt;]</code></td>
<td>Expose all message headers as a column with type
<code>record(key: text, value: bytea?) list</code>. See <a
href="#headers">Headers</a> for details.</td>
</tr>
<tr>
<td><code>HEADER '&lt;key&gt;' AS &lt;name&gt; [BYTES]</code></td>
<td>Expose a specific message header as a column. The <code>bytea</code>
value is automatically parsed into a UTF-8 string unless
<code>BYTES</code> is specified. See <a href="#headers">Headers</a> for
details.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how Materialize interprets incoming records.
Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>NONE</code></td>
<td>Append-only envelope (default). Each message is inserted as a new
row. See <a
href="/docs/sql/create-source/kafka/#append-only-envelope">Append-only
envelope</a> for details.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><code>EXPOSE PROGRESS AS &lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress collection for the source. If
this is not specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. See <a
href="#monitoring-source-progress">Monitoring source progress</a> for
details.</td>
</tr>
<tr>
<td><code>WITH (&lt;with_option&gt; [, ...])</code></td>
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
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

<div id="tab-format-csv" class="tab-pane" title="Format CSV">

### Format CSV

Materialize can parse CSV-formatted data. The data in CSV sources is
read as [`text`](/docs/sql/types/text).

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name> ( <col_name> [, ...] )
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT CSV WITH <n> COLUMNS | WITH HEADER [ ( <col_name> [, ...] ) ]
[INCLUDE
    PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
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
<td><code>&lt;src_name&gt; ( &lt;col_name&gt; [, ...] )</code></td>
<td>The name for the source and the column names. Column names are
required for CSV-formatted sources.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the source. For details
on creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td><strong>Required.</strong> The Kafka topic you want to subscribe
to.</td>
</tr>
<tr>
<td><strong>GROUP ID PREFIX</strong>
<code>&lt;group_id_prefix&gt;</code></td>
<td>Optional. The prefix of the consumer group ID to use. See <a
href="#monitoring-consumer-lag">Monitoring consumer lag</a>.<br />
Default:
<code>materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}</code></td>
</tr>
<tr>
<td><strong>START OFFSET</strong> (<code>&lt;partition_offset&gt;</code>
[, …])</td>
<td>Optional. Read partitions from the specified offset. You cannot
update the offsets once a source has been created; you will need to
recreate the source. Offset values must be zero or positive integers.
See <a href="#setting-start-offsets">Setting start offsets</a> for
details.</td>
</tr>
<tr>
<td><strong>START TIMESTAMP</strong> <code>&lt;timestamp&gt;</code></td>
<td>Optional. Use the specified value to set <code>START OFFSET</code>
based on the Kafka timestamp. Negative values will be interpreted as
relative to the current system time in milliseconds (e.g.
<code>-1000</code> means 1000 ms ago). See <a
href="#time-based-offsets">Time-based offsets</a> for details.</td>
</tr>
<tr>
<td><strong>FORMAT CSV WITH</strong>
<code>&lt;csv_format_option&gt;</code></td>
<td><p>CSV format options:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>WITH &lt;n&gt; COLUMNS</code></td>
<td>Treat the source data as if it has <code>&lt;n&gt;</code> columns.
By default, columns are named <code>column1</code>,
<code>column2</code>…<code>columnN</code>, but you can override these
names by specifying column names in the source definition.</td>
</tr>
<tr>
<td><code>WITH HEADER [ ( &lt;col_name&gt; [, ...] ) ]</code></td>
<td>Materialize determines the</td>
</tr>
<tr>
<td>number of columns and the name of each column using the header row.
The</td>
<td></td>
</tr>
<tr>
<td>header is not ingested as data. Optionally, you can provide a list
of</td>
<td></td>
</tr>
<tr>
<td>column names to validate against the header or override the
source</td>
<td></td>
</tr>
<tr>
<td>column names.</td>
<td></td>
</tr>
</tbody>
</table>
<p>Any row that does not match the number of columns determined by the
format is ignored, and Materialize logs an error.</p></td>
</tr>
<tr>
<td><strong>INCLUDE</strong> <code>&lt;include_option&gt;</code></td>
<td><p>Optional. If specified, include the additional information as
column(s) in the table. The following
<code>&lt;include_option&gt;</code>s are supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>PARTITION [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka partition as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>OFFSET [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka offset as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>TIMESTAMP [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka timestamp as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>HEADERS [AS &lt;name&gt;]</code></td>
<td>Expose all message headers as a column with type
<code>record(key: text, value: bytea?) list</code>. See <a
href="#headers">Headers</a> for details.</td>
</tr>
<tr>
<td><code>HEADER '&lt;key&gt;' AS &lt;name&gt; [BYTES]</code></td>
<td>Expose a specific message header as a column. The <code>bytea</code>
value is automatically parsed into a UTF-8 string unless
<code>BYTES</code> is specified. See <a href="#headers">Headers</a> for
details.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how Materialize interprets incoming records.
CSV format only supports <code>NONE</code>:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>NONE</code></td>
<td>Append-only envelope (default). Each message is inserted as a new
row. See <a
href="/docs/sql/create-source/kafka/#append-only-envelope">Append-only
envelope</a> for details.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<code>&lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress collection for the source. If
this is not specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. See <a
href="#monitoring-source-progress">Monitoring source progress</a> for
details.</td>
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
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

<div id="tab-format-protobuf" class="tab-pane" title="Format Protobuf">

### Format Protobuf

Materialize can decode Protobuf messages by integrating with a schema
registry or parsing an inline schema to retrieve a `.proto` schema
definition. It can then automatically define the columns and data types
to use in the source.

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name>
  | FORMAT PROTOBUF MESSAGE '<message_name>' USING SCHEMA '<schema_bytes>'
[INCLUDE
    KEY [AS <name>]
  | PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE
  | UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS <name>] ) ]
]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
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
<td><code>&lt;src_name&gt;</code></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the source. For details
on creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><code>'&lt;topic&gt;'</code></td>
<td>The Kafka topic you want to subscribe to.</td>
</tr>
<tr>
<td><strong>GROUP ID PREFIX</strong>
<code>&lt;group_id_prefix&gt;</code></td>
<td>Optional. The prefix of the consumer group ID to use. See <a
href="#monitoring-consumer-lag">Monitoring consumer lag</a>.<br />
Default:
<code>materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}</code></td>
</tr>
<tr>
<td><strong>START OFFSET</strong> (<code>&lt;partition_offset&gt;</code>
[, …])</td>
<td>Optional. Read partitions from the specified offset. You cannot
update the offsets once a source has been created; you will need to
recreate the source. Offset values must be zero or positive integers.
See <a href="#setting-start-offsets">Setting start offsets</a> for
details.</td>
</tr>
<tr>
<td><strong>START TIMESTAMP</strong> <code>&lt;timestamp&gt;</code></td>
<td>Optional. Use the specified value to set <code>START OFFSET</code>
based on the Kafka timestamp. Negative values will be interpreted as
relative to the current system time in milliseconds (e.g.
<code>-1000</code> means 1000 ms ago). See <a
href="#time-based-offsets">Time-based offsets</a> for details.</td>
</tr>
<tr>
<td><strong>FORMAT PROTOBUF</strong>
<code>&lt;decode-option&gt;</code></td>
<td><p>Decode Protobuf-formatted messages. The
<code>&lt;decode-option&gt;</code> can be:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>USING CONFLUENT SCHEMA REGISTRY CONNECTION &lt;csr_connection_name&gt;</code></td>
<td>Use schemas from the Confluent Schema Registry. This format applies
to both key and value.</td>
</tr>
<tr>
<td><code>MESSAGE '&lt;message_name&gt;' USING SCHEMA '&lt;schema_bytes&gt;'</code></td>
<td>Use an inline schema. <code>&lt;message_name&gt;</code> is the name
of the Protobuf message type, and <code>&lt;schema_bytes&gt;</code> is
the Protobuf schema definition as a string. This format applies to both
key and value.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>INCLUDE</strong> <code>&lt;include_option&gt;</code></td>
<td><p>Optional. If specified, include the additional information as
column(s) in the table. The following
<code>&lt;include_option&gt;</code>s are supported:</p>
<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>KEY [AS &lt;name&gt;]</strong></td>
<td>Include a column containing the Kafka message key. If the key is
encoded using a format that includes schemas, the column will take its
name from the schema. For unnamed formats (e.g. <code>TEXT</code>), the
column will be named <code>key</code>. The column can be renamed with
the optional <strong>AS</strong> <em>name</em> statement.</td>
</tr>
<tr>
<td><strong>PARTITION [AS &lt;name&gt;]</strong></td>
<td>Include a <code>partition</code> column containing the Kafka message
partition. The column can be renamed with the optional
<strong>AS</strong> <em>name</em> clause.</td>
</tr>
<tr>
<td><strong>OFFSET [AS &lt;name&gt;]</strong></td>
<td>Include an <code>offset</code> column containing the Kafka message
offset. The column can be renamed with the optional <strong>AS</strong>
<em>name</em> clause.</td>
</tr>
<tr>
<td><strong>TIMESTAMP [AS &lt;name&gt;]</strong></td>
<td>Include a <code>timestamp</code> column containing the Kafka message
timestamp. The column can be renamed with the optional
<strong>AS</strong> <em>name</em> clause.<br />
<br />
Note that the timestamp of a Kafka message depends on how the topic and
its producers are configured. See the <a
href="https://docs.confluent.io/3.0.0/streams/concepts.html?#time">Confluent
documentation</a> for details.</td>
</tr>
<tr>
<td><strong>HEADERS [AS &lt;name&gt;]</strong></td>
<td>Include a <code>headers</code> column containing the Kafka message
headers as a list of records of type
<code>(key text, value bytea)</code>. The column can be renamed with the
optional <strong>AS</strong> <em>name</em> clause.</td>
</tr>
<tr>
<td><strong>HEADER &lt;key&gt; AS &lt;name&gt;
[<strong>BYTES</strong>]</strong></td>
<td>Include a <em>name</em> column containing the Kafka message header
<em>key</em> parsed as a UTF-8 string. To expose the header value as
<code>bytea</code>, use the <code>BYTES</code> option.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how Materialize interprets incoming records.
Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>NONE</code></td>
<td>Append-only envelope (default). Each message is inserted as a new
row.</td>
</tr>
<tr>
<td><code>UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS &lt;name&gt;] ) ]</code></td>
<td>Use the standard key-value convention to support inserts, updates,
and deletes. Required to consume <a
href="https://docs.confluent.io/platform/current/kafka/design.html#log-compaction">log
compacted topics</a>.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<code>&lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress collection for the source. If
this is not specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. See <a
href="#monitoring-source-progress">Monitoring source progress</a> for
details.</td>
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
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

Unlike Avro, Protobuf does not serialize a schema with the message, so
Materialize expects:

- A `FileDescriptorSet` that encodes the Protobuf message schema. You
  can generate the `FileDescriptorSet` with
  [`protoc`](https://grpc.io/docs/protoc-installation/), for example:

  <div class="highlight">

  ``` chroma
  protoc --include_imports --descriptor_set_out=SCHEMA billing.proto
  ```

  </div>

- A top-level message name and its package name, so Materialize knows
  which message from the `FileDescriptorSet` is the top-level message to
  decode, in the following format:

  <div class="highlight">

  ``` chroma
  <package name>.<top-level message>
  ```

  </div>

  For example, if the `FileDescriptorSet` were from a `.proto` file in
  the `billing` package, and the top-level message was called `Batch`,
  the *message_name* value would be `billing.Batch`.

#### Schema versioning

The *latest* schema is retrieved using the
[`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html)
strategy at the time the `CREATE SOURCE` statement is issued.

#### Schema evolution

As long as the `.proto` schema definition changes in a [compatible
way](https://developers.google.com/protocol-buffers/docs/overview#updating-defs),
Materialize will continue using the original schema definition by
mapping values from the new to the old schema version. To use the new
version of the schema in Materialize, you need to **drop and recreate**
the source.

#### Supported types

Materialize supports all
[well-known](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf)
Protobuf types from the `proto2` and `proto3` specs, *except for*
recursive `Struct` values and map types.

#### Multiple message schemas

When using a schema registry with Protobuf sources, the registered
schemas must contain exactly one `Message` definition.

</div>

<div id="tab-key-format-value-format" class="tab-pane"
title="KEY FORMAT VALUE FORMAT">

### KEY FORMAT VALUE FORMAT

By default, the message key is decoded using the same format as the
message value. However, you can set the key and value encodings
explicitly using the `KEY FORMAT ... VALUE FORMAT`.

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
KEY FORMAT <key_format> VALUE FORMAT <value_format>
-- <key_format> and <value_format> can be:
-- AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
--     [KEY STRATEGY <strategy>]
--     [VALUE STRATEGY <strategy>]
-- | CSV WITH <num> COLUMNS DELIMITED BY <char>
-- | JSON | TEXT | BYTES
-- | PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
-- | PROTOBUF MESSAGE '<message_name>' USING SCHEMA '<schema_bytes>'
[INCLUDE
    KEY [AS <name>]
  | PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE
  | DEBEZIUM
  | UPSERT [(VALUE DECODING ERRORS = INLINE [AS name])]
]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
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
<td><code>&lt;src_name&gt;</code></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the Kafka connection to use in the source. For details
on creating connections, check the <a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TOPIC</strong> <code>'&lt;topic&gt;'</code></td>
<td><strong>Required.</strong> The Kafka topic you want to subscribe
to.</td>
</tr>
<tr>
<td><strong>GROUP ID PREFIX</strong>
<code>&lt;group_id_prefix&gt;</code></td>
<td>Optional. The prefix of the consumer group ID to use. See <a
href="#monitoring-consumer-lag">Monitoring consumer lag</a>.<br />
Default:
<code>materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}</code></td>
</tr>
<tr>
<td><strong>START OFFSET</strong> (<code>&lt;partition_offset&gt;</code>
[, …])</td>
<td>Optional. Read partitions from the specified offset. You cannot
update the offsets once a source has been created; you will need to
recreate the source. Offset values must be zero or positive integers.
See <a href="#setting-start-offsets">Setting start offsets</a> for
details.</td>
</tr>
<tr>
<td><strong>START TIMESTAMP</strong> <code>&lt;timestamp&gt;</code></td>
<td>Optional. Use the specified value to set <code>START OFFSET</code>
based on the Kafka timestamp. Negative values will be interpreted as
relative to the current system time in milliseconds (e.g.
<code>-1000</code> means 1000 ms ago). See <a
href="#time-based-offsets">Time-based offsets</a> for details.</td>
</tr>
<tr>
<td><strong>KEY FORMAT</strong>
<code>&lt;key_format_spec&gt;</code></td>
<td><strong>Required.</strong> Set the key encoding explicitly.
Supported formats:
<code>AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION &lt;csr_connection_name&gt;</code>,
<code>JSON</code>,
<code>PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION &lt;csr_connection_name&gt;</code>,
<code>PROTOBUF MESSAGE '&lt;message_name&gt;' USING SCHEMA '&lt;schema_bytes&gt;'</code>,
<code>TEXT</code>, <code>BYTES</code>.</td>
</tr>
<tr>
<td><strong>VALUE FORMAT</strong>
<code>&lt;value_format_spec&gt;</code></td>
<td><strong>Required.</strong> Set the value encoding explicitly.
Supported formats:
<code>AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION &lt;csr_connection_name&gt;</code>,
<code>JSON</code>,
<code>PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION &lt;csr_connection_name&gt;</code>,
<code>PROTOBUF MESSAGE '&lt;message_name&gt;' USING SCHEMA '&lt;schema_bytes&gt;'</code>,
<code>TEXT</code>, <code>BYTES</code>. By default, the message key is
decoded using the same format as the message value.</td>
</tr>
<tr>
<td><strong>INCLUDE</strong> <code>&lt;include_option&gt;</code></td>
<td><p>Optional. If specified, include the additional information as
column(s) in the table. The following
<code>&lt;include_option&gt;</code>s are supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>KEY [AS &lt;name&gt;]</code></td>
<td>Expose the message key as a column. Composite keys are also
supported. The <code>UPSERT</code> envelope always includes keys. The
<code>DEBEZIUM</code> envelope is incompatible with this option. See <a
href="#exposing-source-metadata">Exposing source metadata</a> for
details.</td>
</tr>
<tr>
<td><code>PARTITION [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka partition as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>OFFSET [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka offset as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>TIMESTAMP [AS &lt;name&gt;]</code></td>
<td>Expose the Kafka timestamp as a column. See <a
href="#partition-offset-timestamp">Partition, offset, timestamp</a> for
details.</td>
</tr>
<tr>
<td><code>HEADERS [AS &lt;name&gt;]</code></td>
<td>Expose all message headers as a column with type
<code>record(key: text, value: bytea?) list</code>. The
<code>DEBEZIUM</code> envelope is incompatible with this option. See <a
href="#headers">Headers</a> for details.</td>
</tr>
<tr>
<td><code>HEADER '&lt;key&gt;' AS &lt;name&gt; [BYTES]</code></td>
<td>Expose a specific message header as a column. The <code>bytea</code>
value is automatically parsed into a UTF-8 string unless
<code>BYTES</code> is specified. The <code>DEBEZIUM</code> envelope is
incompatible with this option. See <a href="#headers">Headers</a> for
details.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>ENVELOPE</strong> <code>&lt;envelope&gt;</code></td>
<td><p>Optional. Specifies how Materialize interprets incoming records.
Valid envelope types:</p>
<table>
<thead>
<tr>
<th>Envelope</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>NONE</code></td>
<td>Append-only envelope (default). Each message is inserted as a new
row. See <a
href="/docs/sql/create-source/kafka/#append-only-envelope">Append-only
envelope</a> for details.</td>
</tr>
<tr>
<td><code>DEBEZIUM</code></td>
<td>Decode Kafka messages produced by <a
href="https://debezium.io/">Debezium</a>.</td>
</tr>
<tr>
<td><code>UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS &lt;name&gt;] ) ]</code></td>
<td>Use the standard key-value convention to support inserts, updates,
and deletes. Required to consume <a
href="https://docs.confluent.io/platform/current/kafka/design.html#log-compaction">log
compacted topics</a>.</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<code>&lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress collection for the source. If
this is not specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. See <a
href="#monitoring-source-progress">Monitoring source progress</a> for
details.</td>
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
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

## Envelopes

In addition to determining how to decode incoming records, Materialize
also needs to understand how to interpret them. Whether a new record
inserts, updates, or deletes existing data in Materialize depends on the
`ENVELOPE` specified in the `CREATE SOURCE` statement.

### Append-only envelope

**Syntax:** `ENVELOPE NONE`

The append-only envelope treats all records as inserts. This is the
**default** envelope, if no envelope is specified.

### Upsert envelope

To create a source that uses the standard key-value convention to
support inserts, updates, and deletes within Materialize, you can use
`ENVELOPE UPSERT`. For example:

<div class="highlight">

``` chroma
CREATE SOURCE kafka_upsert
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

</div>

The upsert envelope treats all records as having a **key** and a
**value**, and supports inserts, updates and deletes within Materialize:

- If the key does not match a preexisting record, it inserts the
  record’s key and value.

- If the key matches a preexisting record and the value is *non-null*,
  Materialize updates the existing record with the new value.

- If the key matches a preexisting record and the value is *null*,
  Materialize deletes the record.

<div class="note">

**NOTE:**

- Using this envelope is required to consume [log compacted
  topics](https://docs.confluent.io/platform/current/kafka/design.html#log-compaction).

- This envelope can lead to high memory and disk utilization in the
  cluster maintaining the source. We recommend using a standard-sized
  cluster, rather than a legacy-sized cluster, to automatically spill
  the workload to disk. See [spilling to disk](#spilling-to-disk) for
  details.

</div>

#### Null keys

If a message with a `NULL` key is detected, Materialize sets the source
into an error state. To recover an errored source, you must produce a
record with a `NULL` value and a `NULL` key to the topic, to force a
retraction.

As an example, you can use
[`kcat`](https://docs.confluent.io/platform/current/clients/kafkacat-usage.html)
to produce an empty message:

<div class="highlight">

``` chroma
echo ":" | kcat -b $BROKER -t $TOPIC -Z -K: \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-256 \
  -X sasl.username=$KAFKA_USERNAME \
  -X sasl.password=$KAFKA_PASSWORD
```

</div>

#### Value decoding errors

By default, if an error happens while decoding the value of a message
for a specific key, Materialize sets the source into an error state. You
can configure the source to continue ingesting data in the presence of
value decoding errors using the `VALUE DECODING ERRORS = INLINE` option:

<div class="highlight">

``` chroma
CREATE SOURCE kafka_upsert
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT (VALUE DECODING ERRORS = INLINE);
```

</div>

When this option is specified the source will include an additional
column named `error` with type `record(description: text)`.

This column and all value columns will be nullable, such that if the
most recent value for the given Kafka message key cannot be decoded,
this `error` column will contain the error message. If the most recent
value for a key has been successfully decoded, this column will be
`NULL`.

To use an alternative name for the error column, use `INLINE AS ..` to
specify the column name to use:

<div class="highlight">

``` chroma
ENVELOPE UPSERT (VALUE DECODING ERRORS = (INLINE AS my_error_col))
```

</div>

It might be convenient to implement a parsing view on top of your Kafka
upsert source that excludes keys with decoding errors:

<div class="highlight">

``` chroma
CREATE VIEW kafka_upsert_parsed
SELECT *
FROM kafka_upsert
WHERE error IS NULL;
```

</div>

### Debezium envelope

<div class="note">

**NOTE:** Currently, Materialize only supports Avro-encoded Debezium
records. If you're interested in JSON support, please reach out in the
community Slack or submit a [feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests).

</div>

Materialize provides a dedicated envelope (`ENVELOPE DEBEZIUM`) to
decode Kafka messages produced by [Debezium](https://debezium.io/). For
example:

<div class="highlight">

``` chroma
CREATE SOURCE kafka_repl
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'my_table1')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM;
```

</div>

Any materialized view defined on top of this source will be
incrementally updated as new change events stream in through Kafka, as a
result of `INSERT`, `UPDATE` and `DELETE` operations in the original
database.

This envelope treats all records as [change
events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events)
with a diff structure that indicates whether each record should be
interpreted as an insert, update or delete within Materialize:

|  |  |
|----|----|
| **Insert** | If the `before` field is *null*, the record represents an upstream [`create` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events), and Materialize inserts the record’s key and value. |
| **Update** | If the `before` and `after` fields are *non-null*, the record represents an upstream [`update` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events), and Materialize updates the existing record with the new value. |
| **Delete** | If the `after` field is *null*, the record represents an upstream [`delete` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events), and Materialize deletes the record. |

<div class="note">

**NOTE:**

- This envelope can lead to high memory utilization in the cluster
  maintaining the source. Materialize can automatically offload
  processing to disk as needed. See [spilling to
  disk](#spilling-to-disk) for details.

- Materialize expects a specific message structure that includes the row
  data before and after the change event, which is **not guaranteed**
  for every Debezium connector. For more details, check the [Debezium
  integration guide](/docs/integrations/debezium/).

</div>

#### Truncation

The Debezium envelope does not support upstream [`truncate`
events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events).

#### Debezium metadata

The envelope exposes the `before` and `after` value fields from change
events.

#### Duplicate handling

Debezium may produce duplicate records if the connector is interrupted.
Materialize makes a best-effort attempt to detect and filter out
duplicates.

## Features

### Spilling to disk

Kafka sources that use `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` require
storing the current value for *each key* in the source to produce
retractions when keys are updated. When using [standard cluster
sizes](/docs/sql/create-cluster/#size), Materialize will automatically
offload this state to disk, seamlessly handling key spaces that are
larger than memory.

Spilling to disk is not available with [legacy cluster
sizes](/docs/sql/create-cluster/#legacy-sizes).

### Exposing source metadata

In addition to the message value, Materialize can expose the message
key, headers and other source metadata fields to SQL.

#### Key

The message key is exposed via the `INCLUDE KEY` option. Composite keys
are also supported.

<div class="highlight">

``` chroma
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY AS renamed_id;
```

</div>

Note that:

- This option requires specifying the key and value encodings explicitly
  using the `KEY FORMAT ... VALUE FORMAT` [syntax](#syntax).

- The `UPSERT` envelope always includes keys.

- The `DEBEZIUM` envelope is incompatible with this option.

#### Headers

Message headers can be retained in Materialize and exposed as part of
the source data.

Note that:

- The `DEBEZIUM` envelope is incompatible with this option.

**All headers**

All of a message’s headers can be exposed using `INCLUDE HEADERS`,
followed by an `AS <header_col>`.

This introduces column with the name specified or `headers` if none was
specified. The column has the type
`record(key: text, value: bytea?) list`, i.e. a list of records
containing key-value pairs, where the keys are `text` and the values are
nullable `bytea`s.

<div class="highlight">

``` chroma
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE HEADERS
  ENVELOPE NONE;
```

</div>

To simplify turning the headers column into a `map` (so individual
headers can be searched), you can use the
[`map_build`](/docs/sql/functions/#map_build) function:

<div class="highlight">

``` chroma
SELECT
    id,
    seller,
    item,
    convert_from(map_build(headers)->'client_id', 'utf-8') AS client_id,
    map_build(headers)->'encryption_key' AS encryption_key,
FROM kafka_metadata;
```

</div>

```
 id | seller |        item        | client_id |    encryption_key
----+--------+--------------------+-----------+----------------------
  2 |   1592 | Custom Art         |        23 | \x796f75207769736821
  3 |   1411 | City Bar Crawl     |        42 | \x796f75207769736821
```

**Individual headers**

Individual message headers can be exposed via the
`INCLUDE HEADER key AS name` option.

The `bytea` value of the header is automatically parsed into an UTF-8
string. To expose the raw `bytea` instead, the `BYTES` option can be
used.

<div class="highlight">

``` chroma
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE HEADER 'c_id' AS client_id, HEADER 'key' AS encryption_key BYTES,
  ENVELOPE NONE;
```

</div>

Headers can be queried as any other column in the source:

<div class="highlight">

``` chroma
SELECT
    id,
    seller,
    item,
    client_id::numeric,
    encryption_key
FROM kafka_metadata;
```

</div>

```
 id | seller |        item        | client_id |    encryption_key
----+--------+--------------------+-----------+----------------------
  2 |   1592 | Custom Art         |        23 | \x796f75207769736821
  3 |   1411 | City Bar Crawl     |        42 | \x796f75207769736821
```

Note that:

- Messages that do not contain all header keys as specified in the
  source DDL will cause an error that prevents further querying the
  source.

- Header values containing badly formed UTF-8 strings will cause an
  error in the source that prevents querying it, unless the `BYTES`
  option is specified.

#### Partition, offset, timestamp

These metadata fields are exposed via the `INCLUDE PARTITION`,
`INCLUDE OFFSET` and `INCLUDE TIMESTAMP` options.

<div class="highlight">

``` chroma
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE PARTITION, OFFSET, TIMESTAMP AS ts
  ENVELOPE NONE;
```

</div>

<div class="highlight">

``` chroma
SELECT "offset" FROM kafka_metadata WHERE ts > '2021-01-01';
```

</div>

```
offset
------
15
14
13
```

### Setting start offsets

To start consuming a Kafka stream from a specific offset, you can use
the `START OFFSET` option.

<div class="highlight">

``` chroma
CREATE SOURCE kafka_offset
  FROM KAFKA CONNECTION kafka_connection (
    TOPIC 'data',
    -- Start reading from the earliest offset in the first partition,
    -- the second partition at 10, and the third partition at 100.
    START OFFSET (0, 10, 100)
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

</div>

Note that:

- If fewer offsets than partitions are provided, the remaining
  partitions will start at offset 0. This is true if you provide
  `START OFFSET (1)` or `START OFFSET (1, ...)`.

- Providing more offsets than partitions is not supported.

#### Time-based offsets

It’s also possible to set a start offset based on Kafka timestamps,
using the `START TIMESTAMP` option. This approach sets the start offset
for each available partition based on the Kafka timestamp and the source
behaves as if `START OFFSET` was provided directly.

It’s important to note that `START TIMESTAMP` is a property of the
source: it will be calculated *once* at the time the `CREATE SOURCE`
statement is issued. This means that the computed start offsets will be
the **same** for all views depending on the source and **stable** across
restarts.

If you need to limit the amount of data maintained as state after source
creation, consider using [temporal
filters](/docs/sql/patterns/temporal-filters/) instead.

### Monitoring source progress

By default, Kafka sources expose progress metadata as a subsource that
you can use to monitor source **ingestion progress**. The name of the
progress subsource can be specified when creating a source using the
`EXPOSE PROGRESS AS` clause; otherwise, it will be named
`<src_name>_progress`.

The following metadata is available for each source as a progress
subsource:

| Field | Type | Meaning |
|----|----|----|
| `partition` | `numrange` | The upstream Kafka partition. |
| `offset` | [`uint8`](/docs/sql/types/uint/#uint8-info) | The greatest offset consumed from each upstream Kafka partition. |

And can be queried using:

<div class="highlight">

``` chroma
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

</div>

As long as any offset continues increasing, Materialize is consuming
data from the upstream Kafka broker. For more details on monitoring
source ingestion progress and debugging related issues, see
[Troubleshooting](/docs/ops/troubleshooting/).

### Monitoring consumer lag

To support Kafka tools that monitor consumer lag, Kafka sources commit
offsets once the messages up through that offset have been durably
recorded in Materialize’s storage layer.

However, rather than relying on committed offsets, Materialize suggests
using our native [progress monitoring](#monitoring-source-progress),
which contains more up-to-date information.

<div class="note">

**NOTE:**

Some Kafka monitoring tools may indicate that Materialize’s consumer
groups have no active members. This is **not a cause for concern**.

Materialize does not participate in the consumer group protocol nor does
it recover on restart by reading the committed offsets. The committed
offsets are provided solely for the benefit of Kafka monitoring tools.

</div>

Committed offsets are associated with a consumer group specific to the
source. The ID of the consumer group consists of the prefix configured
with the [`GROUP ID PREFIX` option](#syntax) followed by a
Materialize-generated suffix.

You should not make assumptions about the number of consumer groups that
Materialize will use to consume from a given source. The only guarantee
is that the ID of each consumer group will begin with the configured
prefix.

The consumer group ID prefix for each Kafka source in the system is
available in the `group_id_prefix` column of the \[`mz_kafka_sources`\]
table. To look up the `group_id_prefix` for a source by name, use:

<div class="highlight">

``` chroma
SELECT group_id_prefix
FROM mz_internal.mz_kafka_sources ks
JOIN mz_sources s ON s.id = ks.id
WHERE s.name = '<src_name>'
```

</div>

## Required Kafka ACLs

The access control lists (ACLs) on the Kafka cluster must allow
Materialize to perform the following operations on the following
resources:

| Operation type | Resource type | Resource name |
|----|----|----|
| Read | Topic | The specified `TOPIC` option |
| Read | Group | All group IDs starting with the specified [`GROUP ID PREFIX` option](#syntax) |

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster if the source is created
  in an existing cluster.
- `CREATECLUSTER` privileges on the system if the source is not created
  in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the source
  definition.
- `USAGE` privileges on the schemas that all connections and secrets in
  the statement are contained in.

## Examples

### Prerequisite: Creating a connection

A connection describes how to connect and authenticate to an external
system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple
`CREATE SOURCE` statements. For more details on creating connections,
check the [`CREATE CONNECTION`](/docs/sql/create-connection)
documentation page.

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

If your Kafka broker is not exposed to the public internet, you can
[tunnel the
connection](/docs/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion
host:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-aws-privatelink-materialize-cloud" class="tab-pane"
title="AWS PrivateLink (Materialize Cloud)">

<div class="note">

**NOTE:** Connections using AWS PrivateLink is for Materialize Cloud
only.

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'broker1:9092' USING AWS PRIVATELINK privatelink_svc,
        'broker2:9092' USING AWS PRIVATELINK privatelink_svc (PORT 9093)
    )
);
```

</div>

For step-by-step instructions on creating AWS PrivateLink connections
and configuring an AWS PrivateLink service to accept connections from
Materialize, check [this
guide](/docs/ops/network-security/privatelink/).

</div>

<div id="tab-ssh-tunnel" class="tab-pane" title="SSH tunnel">

<div class="highlight">

``` chroma
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh_connection,
    'broker2:9092' USING SSH TUNNEL ssh_connection
    )
);
```

</div>

For step-by-step instructions on creating SSH tunnel connections and
configuring an SSH bastion server to accept connections from
Materialize, check [this guide](/docs/ops/network-security/ssh-tunnel/).

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

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9093',
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

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
  URL '<CONFLUENT_REGISTRY_URL>',
  USERNAME = SECRET csr_username,
  PASSWORD = SECRET csr_password
);
```

</div>

</div>

</div>

</div>

If your Confluent Schema Registry server is not exposed to the public
internet, you can [tunnel the
connection](/docs/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion
host:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-aws-privatelink-materialize-cloud" class="tab-pane"
title="AWS PrivateLink (Materialize Cloud)">

<div class="note">

**NOTE:** Connections using AWS PrivateLink is for Materialize Cloud
only.

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    AWS PRIVATELINK privatelink_svc
);
```

</div>

For step-by-step instructions on creating AWS PrivateLink connections
and configuring an AWS PrivateLink service to accept connections from
Materialize, check [this
guide](/docs/ops/network-security/privatelink/).

</div>

<div id="tab-ssh-tunnel" class="tab-pane" title="SSH tunnel">

<div class="highlight">

``` chroma
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    SSH TUNNEL ssh_connection
);
```

</div>

For step-by-step instructions on creating SSH tunnel connections and
configuring an SSH bastion server to accept connections from
Materialize, check [this guide](/docs/ops/network-security/ssh-tunnel/).

</div>

</div>

</div>

### Creating a source

<div class="code-tabs">

<div class="tab-content">

<div id="tab-avro" class="tab-pane" title="Avro">

**Using Confluent Schema Registry**

<div class="highlight">

``` chroma
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

</div>

</div>

<div id="tab-json" class="tab-pane" title="JSON">

<div class="highlight">

``` chroma
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

</div>

<div class="highlight">

``` chroma
CREATE VIEW typed_kafka_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM json_source;
```

</div>

JSON-formatted messages are ingested as a JSON blob. We recommend
creating a parsing view on top of your Kafka source that maps the
individual fields to columns with the required data types. To avoid
doing this tedious task manually, you can use [this **JSON parsing
widget**](/docs/sql/types/jsonb/#parsing)!

</div>

<div id="tab-textbytes" class="tab-pane" title="Text/bytes">

<div class="highlight">

``` chroma
CREATE SOURCE text_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT TEXT
  ENVELOPE UPSERT;
```

</div>

</div>

<div id="tab-csv" class="tab-pane" title="CSV">

<div class="highlight">

``` chroma
CREATE SOURCE csv_source (col_foo, col_bar, col_baz)
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT CSV WITH 3 COLUMNS;
```

</div>

</div>

<div id="tab-protobuf" class="tab-pane" title="Protobuf">

**Using Confluent Schema Registry**

<div class="highlight">

``` chroma
CREATE SOURCE proto_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

</div>

**Using an inline schema**

If you’re not using a schema registry, you can use the
`MESSAGE...SCHEMA` clause to specify a Protobuf schema descriptor
inline. Protobuf does not serialize a schema with the message, so before
creating a source you must:

- Compile the Protobuf schema into a descriptor file using
  [`protoc`](https://grpc.io/docs/protoc-installation/):

  <div class="highlight">

  ``` chroma
  // example.proto
  syntax = "proto3";
  message Batch {
      int32 id = 1;
      // ...
  }
  ```

  </div>

  <div class="highlight">

  ``` chroma
  protoc --include_imports --descriptor_set_out=example.pb example.proto
  ```

  </div>

- Encode the descriptor file into a SQL byte string:

  <div class="highlight">

  ``` chroma
  $ printf '\\x' && xxd -p example.pb | tr -d '\n'
  \x0a300a0d62696...
  ```

  </div>

- Create the source using the encoded descriptor bytes from the previous
  step (including the `\x` at the beginning):

  <div class="highlight">

  ``` chroma
  CREATE SOURCE proto_source
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
    FORMAT PROTOBUF MESSAGE 'Batch' USING SCHEMA '\x0a300a0d62696...';
  ```

  </div>

</div>

</div>

</div>

## Related pages

- [`CREATE SECRET`](/docs/sql/create-secret)
- [`CREATE CONNECTION`](/docs/sql/create-connection)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/docs/sql/show-sources)
- [`DROP SOURCE`](/docs/sql/drop-source)
- [Using Debezium](/docs/integrations/debezium/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/kafka.md"
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
