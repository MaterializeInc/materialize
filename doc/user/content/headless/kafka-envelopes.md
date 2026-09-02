---
headless: true
---
In addition to determining how to decode incoming records, Materialize also
needs to understand how to interpret them. Whether a new record inserts,
updates, or deletes existing data in Materialize depends on the `ENVELOPE`
specified. For where the `ENVELOPE` clause goes, see [Syntax](#syntax).

### Append-only envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE NONE</code></p>

The append-only envelope treats all records as inserts. This is the **default** envelope, if no envelope is specified.

### Upsert envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE UPSERT</code></p>

The upsert envelope uses the standard key-value convention to support inserts,
updates, and deletes within Materialize. It treats all records as having a
**key** and a **value**:

- If the key does not match a preexisting record, it inserts the record's key and value.

- If the key matches a preexisting record and the value is _non-null_, Materialize updates
  the existing record with the new value.

- If the key matches a preexisting record and the value is _null_, Materialize deletes the record.

{{< note >}}

- Using this envelope is required to consume [log compacted topics](https://docs.confluent.io/platform/current/kafka/design.html#log-compaction).

- This envelope can lead to high memory and disk utilization in the cluster
  maintaining the source. We recommend using a standard-sized cluster, rather
  than a legacy-sized cluster, to automatically spill the workload to disk. See
  [spilling to disk](/sql/create-source/kafka/#spilling-to-disk) for details.

{{< /note >}}

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

#### Value decoding errors

By default, if an error happens while decoding the value of a message for a
specific key, Materialize sets the source into an error state. You can
configure the source to continue ingesting data in the presence of value
decoding errors using the `VALUE DECODING ERRORS = INLINE` option:

```mzsql
ENVELOPE UPSERT (VALUE DECODING ERRORS = INLINE)
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

### Debezium envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE DEBEZIUM</code></p>

{{< debezium-json >}}

Materialize provides a dedicated envelope (`ENVELOPE DEBEZIUM`) to decode Kafka
messages produced by [Debezium](https://debezium.io/).

Any materialized view defined on top of a Debezium source will be incrementally
updated as new change events stream in through Kafka, as a result of `INSERT`,
`UPDATE` and `DELETE` operations in the original database.

This envelope treats all records as [change events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events) with a diff structure that indicates whether each record should be interpreted as an insert, update or delete within Materialize:

|    |   |
 ----|---
 **Insert** | If the `before` field is _null_, the record represents an upstream [`create` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events), and Materialize inserts the record's key and value.
 **Update** | If the `before` and `after` fields are _non-null_, the record represents an upstream [`update` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events), and Materialize updates the existing record with the new value.
 **Delete** | If the `after` field is _null_, the record represents an upstream [`delete` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events), and Materialize deletes the record.


{{< note>}}

- This envelope can lead to high memory utilization in the cluster maintaining
  the source. Materialize can automatically offload processing to
  disk as needed. See [spilling to disk](/sql/create-source/kafka/#spilling-to-disk) for details.

- Materialize expects a specific message structure that includes the row data
  before and after the change event, which is **not guaranteed** for every
  Debezium connector. For more details, check the [Debezium integration
  guide](/integrations/debezium/).

{{</ note >}}

#### Truncation

The Debezium envelope does not support upstream [`truncate` events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events).

#### Debezium metadata

The envelope exposes the `before` and `after` value fields from change events.

#### Duplicate handling

Debezium may produce duplicate records if the connector is interrupted. Materialize makes a best-effort attempt to detect and filter out duplicates.
