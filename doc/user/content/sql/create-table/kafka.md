---
title: "CREATE TABLE: Kafka/Redpanda"
description: "Reference page for `CREATE TABLE`. `CREATE TABLE` creates a table that is persisted in durable storage."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'create-table'
    name: "Kafka/Redpanda"
    identifier: 'create-table-kafka'
---

{{< source-versioning-disambiguation is_new=true other_ref="[old reference page](/sql/create-source-v1/kafka/)" >}}

`CREATE TABLE` defines a table that is persisted in durable storage. In
Materialize, you can create tables populated from Kafka sources.
Source-populated tables are read-only tables; they cannot be written to by the
user. These tables are populated by [data ingestion from a source](/ingest-data/).

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, and views; and you
can create views/materialized views/indexes on tables.

## Syntax

Use the format-specific syntax to create a table from a Kafka/Redpanda source:

{{< note >}}

- {{< include-md file="shared-content/kafka-redpanda-shorthand.md" >}}

- {{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

- {{< include-md file="shared-content/create-table-from-source-snapshotting.md"
  >}}

{{</ note >}}

{{< tabs level=3 >}}

{{< tab "Format Avro" >}}

{{% include-example file="examples/create_table/example_kafka_table_avro"
 example="syntax" %}}
{{% include-example file="examples/create_table/example_kafka_table_avro"
 example="syntax-options" %}}
{{< /tab >}}

{{< tab "Format JSON" >}}
{{% include-example file="examples/create_table/example_kafka_table_json"
 example="syntax" %}}
{{% include-example file="examples/create_table/example_kafka_table_json"
 example="syntax-options" %}}
{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}
{{% include-example file="examples/create_table/example_kafka_table_text"
 example="syntax" %}}
{{% include-example file="examples/create_table/example_kafka_table_text"
 example="syntax-options" %}}
{{< /tab >}}

{{< tab "Format CSV" >}}
{{% include-example file="examples/create_table/example_kafka_table_csv"
 example="syntax" %}}
{{% include-example file="examples/create_table/example_kafka_table_csv"
 example="syntax-options" %}}
{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}
{{% include-example file="examples/create_table/example_kafka_table_key_value"
 example="syntax" %}}
{{% include-example file="examples/create_table/example_kafka_table_key_value"
 example="syntax-options" %}}
{{< /tab >}}

{{< /tabs >}}

## Details

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Source-populated tables and snapshotting

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

### Monitoring source progress

{{% include-md file="shared-content/kafka-monitoring.md"%}}

### Required privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-table.md" >}}

### Envelopes

{{< tabs level=4 >}}
{{< tab "Append-only envelope" >}}

Append-only envelope (`ENVELOPE NONE`) is the **default** envelope. With
append-only envelope, Materialize treats all records as inserts.

{{< /tab >}}
{{< tab "Upsert envelope" >}}

Upsert envelope (`ENVELOPE UPSERT`) is available with the following `CREATE
TABLE` syntax:
- `CREATE TABLE ... FORMAT AVRO ...`.
- `CREATE TABLE ... KEY FORMAT <key_fmt> VALUE FORMAT <value_fmt>` for formats
  `AVRO`, `JSON`,`Text`, `Bytes`.

With upsert envelope, Materialize treats all records as having a **key** and a
**value**. The key and value determine how Materialize treats the record; i.e.,
as an insert, update, or delete:

|    |   |
 ----|---
 **Insert**| If the key does not match a preexisting record, Materialize inserts the record's key and value.
 **Update** | If the key matches a preexisting record and the value is _non-null_, Materialize updates the existing record with the new value.
 **Delete** | If the key matches a preexisting record and the value is _null_, Materialize deletes the record.

##### Considerations

A table created with `ENVELOPE UPSERT` automatically includes the key as a
column in the new table.

Using `ENVELOPE UPSERT` requires storing the current value of _each record
key_ to produce retractions when keys are updated. When using [standard cluster
sizes](/sql/create-cluster/#size), Materialize will automatically offload this
state to disk, seamlessly handling key spaces that are larger than memory.

##### Null keys

With upsert envelope, if a message with a `NULL` key is detected, Materialize
sets the table into an error state where you cannot select from the table:

```none
Error: Envelope error: Upsert: Null key: record with NULL key in Upsert source; to retract this error, produce a record upstream with a NULL key and Null value
```

To recover, you must force a retraction by producing a record with a `NULL` key
and a `NULL` value to the topic. For example, you can use
[`kcat`](https://docs.confluent.io/platform/current/clients/kafkacat-usage.html)
to produce an empty message with a `NULL` key and a `NULL` value (using a `:`
key-value separator):

```bash
echo ":" | kcat \
  -b $YOUR_BROKER \
  -t $YOUR_TOPIC \
  -Z \
  -K: \
  -X security.protocol=$YOUR_PROTOCOL \
  -X sasl.mechanisms=$YOUR_SASL_MECHANISM \
  -X sasl.username=$YOUR_KAFKA_USERNAME \
  -X sasl.password=$YOUR_KAFKA_PASSWORD
```

##### Value decoding errors

By default, if an error happens while decoding the value of a message for a
specific key, Materialize sets the source into an error state. You can
configure the source to continue ingesting data in the presence of value
decoding errors using the `VALUE DECODING ERRORS = INLINE` option:

```mzsql
CREATE SOURCE kafka_upsert
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT (VALUE DECODING ERRORS = INLINE);
```

When this option is specified the source will include an additional column named
`error` with type `record(description: text)`.

This column and all value columns will be nullable, such that if the most recent value
for the given Kafka message key cannot be decoded, this `error` column will contain
the error message. If the most recent value for a key has been successfully decoded,
this column will be `NULL`.

To use an alternative name for the error column, use `INLINE AS ..` to specify the
column name to use:

```mzsql
ENVELOPE UPSERT (VALUE DECODING ERRORS = (INLINE AS my_error_col))
```

It might be convenient to implement a parsing view on top of your Kafka upsert source that
excludes keys with decoding errors:

```mzsql
CREATE VIEW kafka_upsert_parsed
SELECT *
FROM kafka_upsert
WHERE error IS NULL;
```

{{< /tab >}}
{{< tab "Debezium envelope" >}}

Materialize provides a dedicated envelope to decode Avro-encoded messages
produced by [Debezium](https://debezium.io/). The Debezium envelope exposes the
`before` and `after` value fields from change events.

Debezium envelope treats all records as [change
events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events)
with a diff structure that indicates whether each record should be treated
as an insert, update or delete within Materialize:

|    |   |
 ----|---
 **Insert** | If the `before` field is _null_, the record represents an upstream [`create` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events), and Materialize inserts the record's key and value.
 **Update** | If the `before` and `after` fields are _non-null_, the record represents an upstream [`update` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events), and Materialize updates the existing record with the new value.
 **Delete** | If the `after` field is _null_, the record represents an upstream [`delete` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events), and Materialize deletes the record.

##### Considerations

Using `ENVELOPE DEBEZIUM` requires storing the current value of _each record
key_ to produce retractions when keys are updated. When using [standard cluster
sizes](/sql/create-cluster/#size), Materialize will automatically offload this
state to disk, seamlessly handling key spaces that are larger than memory.

Materialize expects a specific message structure that includes the row data before and after the change event, which is **not guaranteed** for every Debezium connector. For more details, check the [Debezium integration guide](/integrations/debezium/).

##### Limitations

- The Debezium envelope does not support upstream [`truncate`
  events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events).

- The Debezium envelope may produce duplicate records if the connector is
  interrupted. Materialize makes a best-effort attempt to detect and filter out
  duplicates.

{{< /tab >}}
{{< /tabs >}}



### Spilling to disk

Using `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` require storing the current value
of _each record key_ to produce retractions when keys are updated. When
using [standard cluster sizes](/sql/create-cluster/#size), Materialize will
automatically offload this state to disk, seamlessly handling key spaces that
are larger than memory.

Spilling to disk is not available with [legacy cluster sizes](/sql/create-cluster/#legacy-sizes).


## Examples

### Create a table (Kafka Source: Format AVRO)

{{< tip >}}
The same syntax may be used for Redpanda.
{{</ tip >}}

#### Envelope None/Append-only

{{% include-example file="examples/create_table/example_kafka_table_avro"
 example="create-table" %}}

{{% include-example file="examples/create_table/example_kafka_table_avro"
 example="read-from-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

#### Envelope Upsert

### Create a table (Kafka Source: Format JSON)

{{% include-example file="examples/create_table/example_kafka_table_json"
 example="create-table" %}}

{{% include-example file="examples/create_table/example_kafka_table_json"
 example="read-from-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{% include-example file="examples/create_table/example_kafka_table_json"
 example="create-a-view-from-table" %}}

### Create a table (Kafka Source: Format TEXT)

{{% include-example file="examples/create_table/example_kafka_table_text"
 example="create-table" %}}

{{% include-example file="examples/create_table/example_kafka_table_text"
 example="read-from-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

### Create a table (Kafka Source: Format CSV)

{{% include-example file="examples/create_table/example_kafka_table_csv"
 example="create-table" %}}

{{% include-example file="examples/create_table/example_kafka_table_csv"
 example="read-from-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{% include-example file="examples/create_table/example_kafka_table_csv"
 example="create-table-with-colnames" %}}

{{% include-example file="examples/create_table/example_kafka_table_csv"
 example="read-from-table-with-colnames" %}}

### Create a table (Kafka Source: Key Format Value Format)

{{% include-example file="examples/create_table/example_kafka_table_key_value"
 example="create-table" %}}

{{% include-example file="examples/create_table/example_kafka_table_key_value"
 example="read-from-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{% include-example file="examples/create_table/example_kafka_table_key_value"
 example="create-a-view-from-table" %}}

### Include metadata in table

#### Include key

You can include the message key using the `INCLUDE KEY` option available with
the following syntax and envelope combinations:

| Syntax | Notes |
| ------ | ----- |
| `CREATE TABLE ... FORMAT AVRO` with the default envelope | |
| `CREATE TABLE ... FORMAT AVRO` with the envelope `UPSERT`| Since the `UPSERT` envelope includes the keys by default, you can omit the option unless you need to alias the column name. |
| `CREATE TABLE ... KEY FORMAT VALUE FORMAT` with the default envelope | |
| `CREATE TABLE ... KEY FORMAT VALUE FORMAT` with the envelope `UPSERT` | Since the `UPSERT` envelope includes the keys by default, you can omit the option unless you need to alias the column name. |

{{< annotation type="Error: column <key> specified more than once" >}}

If the key is also part of the message value (e.g., a key of `{"user_id":
"abc123"}` and a value of `{"user_id": "abc123", ...}`), you can use the
`INCLUDE KEY AS <alias>` option to avoid name collision.

{{< /annotation >}}

Composite keys are also supported.

{{% include-example file="examples/create_table/example_kafka_table_avro"
 example="include-key-envelope-append-only" %}}

#### Headers

Message headers can be retained in Materialize and exposed as part of the source data.

Note that:
- The `DEBEZIUM` envelope is incompatible with this option.

**All headers**

All of a message's headers can be exposed using `INCLUDE HEADERS`, followed by
an `AS <header_col>`.

This introduces column with the name specified or `headers` if none was
specified. The column has the type `record(key: text, value: bytea?) list`,
i.e. a list of records containing key-value pairs, where the keys are `text`
and the values are nullable `bytea`s.

```mzsql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE HEADERS
  ENVELOPE NONE;
```

To simplify turning the headers column into a `map` (so individual headers can
be searched), you can use the [`map_build`](/sql/functions/#map_build) function:

```mzsql
SELECT
    id,
    seller,
    item,
    convert_from(map_build(headers)->'client_id', 'utf-8') AS client_id,
    map_build(headers)->'encryption_key' AS encryption_key,
FROM kafka_metadata;
```

<p></p>

```nofmt
 id | seller |        item        | client_id |    encryption_key
----+--------+--------------------+-----------+----------------------
  2 |   1592 | Custom Art         |        23 | \x796f75207769736821
  3 |   1411 | City Bar Crawl     |        42 | \x796f75207769736821
```

**Individual headers**

Individual message headers can be exposed via the `INCLUDE HEADER key AS name`
option.

The `bytea` value of the header is automatically parsed into an UTF-8 string. To
expose the raw `bytea` instead, the `BYTES` option can be used.

```mzsql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE HEADER 'c_id' AS client_id, HEADER 'key' AS encryption_key BYTES,
  ENVELOPE NONE;
```

Headers can be queried as any other column in the source:

```mzsql
SELECT
    id,
    seller,
    item,
    client_id::numeric,
    encryption_key
FROM kafka_metadata;
```

<p></p>

```nofmt
 id | seller |        item        | client_id |    encryption_key
----+--------+--------------------+-----------+----------------------
  2 |   1592 | Custom Art         |        23 | \x796f75207769736821
  3 |   1411 | City Bar Crawl     |        42 | \x796f75207769736821
```

Note that:

- Messages that do not contain all header keys as specified in the source DDL
  will cause an error that prevents further querying the source.

- Header values containing badly formed UTF-8 strings will cause an error in the
  source that prevents querying it, unless the `BYTES` option is specified.

#### Partition, offset, timestamp

These metadata fields are exposed via the `INCLUDE PARTITION`, `INCLUDE OFFSET`
and `INCLUDE TIMESTAMP` options.

```mzsql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE PARTITION, OFFSET, TIMESTAMP AS ts
  ENVELOPE NONE;
```

```mzsql
SELECT "offset" FROM kafka_metadata WHERE ts > '2021-01-01';
```

<p></p>

```nofmt
offset
------
15
14
13
```

## Related pages

- [`INSERT`]
- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP TABLE`](/sql/drop-table)
- [Ingest data](/ingest-data/)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
