<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# CREATE SOURCE

A [source](/docs/concepts/sources/) describes an external system you
want Materialize to read data from, and provides details about how to
decode and interpret that data. To create a source, you must specify a
[connector](#connectors), a [format](#formats) and an
[envelope](#envelopes). Like other relations, sources are
[namespaced](../namespaces/) by a database and schema.

## Connectors

Materialize bundles **native connectors** that allow ingesting data from
the following external systems:

<div class="multilinkbox">

<div class="linkbox">

<div class="title">

Databases (CDC)

</div>

- [PostgreSQL](/docs/ingest-data/postgres/)
- [MySQL](/docs/ingest-data/mysql/)
- [SQL Server](/docs/ingest-data/sql-server/)
- [CockroachDB](/docs/ingest-data/cdc-cockroachdb/)
- [MongoDB](https://github.com/MaterializeIncLabs/materialize-mongodb-debezium)

</div>

<div class="linkbox">

<div class="title">

Message Brokers

</div>

- [Kafka](/docs/ingest-data/kafka/)
- [Redpanda](/docs/sql/create-source/kafka)

</div>

<div class="linkbox">

<div class="title">

Webhooks

</div>

- [Amazon EventBridge](/docs/ingest-data/webhooks/amazon-eventbridge/)
- [Segment](/docs/ingest-data/webhooks/segment/)
- [Other webhooks](/docs/sql/create-source/webhook)

</div>

</div>

For details on the syntax, supported formats and features of each
connector, check out the dedicated `CREATE SOURCE` documentation pages.

**Sample data**

To get started with no external dependencies, you can use the [load
generator source](/docs/sql/create-source/load-generator/) to produce
sample data that is suitable for demo and performance test scenarios.

## Formats

To read from an external data source, Materialize must be able to
determine how to decode raw bytes from different formats into data
structures it can understand at runtime. This is handled by specifying a
`FORMAT` in the `CREATE SOURCE` statement.

### Avro

**Syntax:** `FORMAT AVRO`

Materialize can decode Avro messages by integrating with a schema
registry to retrieve a schema, and automatically determine the columns
and data types to use in the source.

##### Schema versioning

The *latest* schema is retrieved using the
[`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html)
strategy at the time the `CREATE SOURCE` statement is issued. In the
future, we expect to support specifying a different subject name
strategy.

##### Schema evolution

As long as the writer schema changes in a [compatible
way](https://avro.apache.org/docs/++version++/specification/#schema-resolution),
Materialize will continue using the original reader schema definition by
mapping values from the new to the old schema version. To use the new
version of the writer schema in Materialize, you need to **drop and
recreate** the source.

##### Name collision

To avoid [case-sensitivity](/docs/sql/identifiers/#case-sensitivity)
conflicts with Materialize identifiers, we recommend double-quoting all
field names when working with Avro-formatted sources.

##### Supported types

Materialize supports all [Avro
types](https://avro.apache.org/docs/++version++/specification/), *except
for* recursive types and union types in arrays.

### JSON

**Syntax:** `FORMAT JSON`

Materialize can decode JSON messages into a single column named `data`
with type `jsonb`. Refer to the [`jsonb` type](/docs/sql/types/jsonb)
documentation for the supported operations on this type.

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

To avoid doing this tedious task manually, you can use [this **JSON
parsing widget**](/docs/sql/types/jsonb/#parsing)!

##### Schema registry integration

Retrieving schemas from a schema registry is not supported yet for
JSON-formatted sources. This means that Materialize cannot decode
messages serialized using the [JSON
Schema](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer-and-deserializer)
serialization format (`JSON_SR`).

### Protobuf

**Syntax:** `FORMAT PROTOBUF`

Materialize can decode Protobuf messages by integrating with a schema
registry or parsing an inline schema to retrieve a `.proto` schema
definition. It can then automatically define the columns and data types
to use in the source. Unlike Avro, Protobuf does not serialize a schema
with the message, so Materialize expects:

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

##### Schema versioning

The *latest* schema is retrieved using the
[`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html)
strategy at the time the `CREATE SOURCE` statement is issued. In the
future, we expect to support specifying a different subject name
strategy.

##### Schema evolution

As long as the `.proto` schema definition changes in a [compatible
way](https://developers.google.com/protocol-buffers/docs/overview#updating-defs),
Materialize will continue using the original schema definition by
mapping values from the new to the old schema version. To use the new
version of the schema in Materialize, you need to **drop and recreate**
the source.

##### Supported types

Materialize supports all
[well-known](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf)
Protobuf types from the `proto2` and `proto3` specs, *except for*
recursive `Struct` values and map types.

##### Multiple message schemas

When using a schema registry with Protobuf sources, the registered
schemas must contain exactly one `Message` definition. In the future, we
expect to support schemas with multiple messages
([discussion#29603](https://github.com/MaterializeInc/materialize/discussions/29603)).

### Text/bytes

#### Text

**Syntax:** `FORMAT TEXT`

Materialize can parse **new-line delimited** data as plain text. Data is
assumed to be **valid unicode** (UTF-8), and discarded if it cannot be
converted to UTF-8. Text-formatted sources have a single column, by
default named `text`.

For details on casting, check the [`text`](/docs/sql/types/text/)
documentation.

#### Bytes

**Syntax:** `FORMAT BYTES`

Materialize can read raw bytes without applying any formatting or
decoding. Raw byte-formatted sources have a single column, by default
named `data`.

For details on encodings and casting, check the
[`bytea`](/docs/sql/types/bytea/) documentation.

### CSV

**Syntax:** `FORMAT CSV`

Materialize can parse CSV-formatted data using different methods to
determine the number of columns to create and their respective names:

| Method | Description |
|----|----|
| **HEADER** | Materialize determines the *number of columns* and the *name* of each column using the header row. The header is **not** ingested as data. |
| **HEADER (** *name_list* **)** | Same behavior as **HEADER**, with additional validation of the column names against the *name list* specified. This allows decoding files that have headers but may not be populated yet, as well as overriding the source column names. |
| *n* **COLUMNS** | Materialize treats the source data as if it has *n* columns. By default, columns are named `column1`, `column2`…`columnN`. |

The data in CSV sources is read as [`text`](/docs/sql/types/text). You
can then handle the conversion to other types using explicit
[casts](/docs/sql/functions/cast/) when creating views.

##### Invalid rows

Any row that doesn’t match the number of columns determined by the
format is ignored, and Materialize logs an error.

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

**Syntax:** `ENVELOPE UPSERT`

The upsert envelope treats all records as having a **key** and a
**value**, and supports inserts, updates and deletes within Materialize:

- If the key does not match a preexisting record, it inserts the
  record’s key and value.

- If the key matches a preexisting record and the value is *non-null*,
  Materialize updates the existing record with the new value.

- If the key matches a preexisting record and the value is *null*,
  Materialize deletes the record.

### Debezium envelope

**Syntax:** `ENVELOPE DEBEZIUM`

Materialize provides a dedicated envelope to decode messages produced by
[Debezium](https://debezium.io/). This envelope treats all records as
[change
events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events)
with a diff structure that indicates whether each record should be
interpreted as an insert, update or delete within Materialize:

- If the `before` field is *null*, the record represents an upstream
  [`create`
  event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events)
  and Materialize inserts the record’s key and value.

- If the `before` and `after` fields are *non-null*, the record
  represents an upstream [`update`
  event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events)
  and Materialize updates the existing record with the new value.

- If the `after` field is *null*, the record represents an upstream
  [`delete`
  event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events)
  and Materialize deletes the record.

Materialize expects a specific message structure that includes the row
data before and after the change event, which is **not guaranteed** for
every Debezium connector. For more details, check the [Debezium
integration guide](/docs/integrations/debezium/).

##### Truncation

The Debezium envelope does not support upstream [`truncate`
events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events).

##### Debezium metadata

The envelope exposes the `before` and `after` value fields from change
events. In the future, we expect to support additional metadata with
information about the original context of the events, like
`source.ts_ms`, `source.database` and `source.table`.

##### Duplicate handling

Debezium may produce duplicate records if the connector is interrupted.
Materialize makes a best-effort attempt to detect and filter out
duplicates.

## Best practices

### Sizing a source

Some sources are low traffic and require relatively few resources to
handle data ingestion, while others are high traffic and require hefty
resource allocations. The cluster in which you place a source determines
the amount of CPU, memory, and disk available to the source.

It’s a good idea to size up the cluster hosting a source when:

- You want to **increase throughput**. Larger sources will typically
  ingest data faster, as there is more CPU available to read and decode
  data from the upstream external system.

- You are using the [upsert envelope](#upsert-envelope) or [Debezium
  envelope](#debezium-envelope), and your source contains **many unique
  keys**. These envelopes maintain state proportional to the number of
  unique keys in the upstream external system. Larger sizes can store
  more unique keys.

Sources share the resource allocation of their cluster with all other
objects in the cluster. Colocating multiple sources onto the same
cluster can be more resource efficient when you have many low-traffic
sources that occasionally need some burst capacity.

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

## Related pages

- [Sources](/docs/concepts/sources/)
- [`SHOW SOURCES`](/docs/sql/show-sources/)
- [`SHOW COLUMNS`](/docs/sql/show-columns/)
- [`SHOW CREATE SOURCE`](/docs/sql/show-create-source/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/_index.md"
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

© 2025 Materialize Inc.

</div>
