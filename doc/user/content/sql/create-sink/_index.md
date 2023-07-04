---
title: "CREATE SINK"
description: "`CREATE SINK` connects Materialize to an external data sink."
disable_list: true
pagerank: 30
menu:
  main:
    parent: reference
    name: Sinks
    identifier: 'create-sink'
    weight: 30
---

A [sink](../../get-started/key-concepts/#sinks) describes an external system you
want Materialize to write data to, and provides details about how to encode
that data. To create a sink, you must specify a [connector](#connectors), a
[format](#formats) and an [envelope](#envelopes).

## Connectors

Materialize bundles **native sink connectors** for the following external
systems:

{{< multilinkbox >}}
{{< linkbox title="Message Brokers" >}}
- [Kafka](/sql/create-sink/kafka)
- [Redpanda](/sql/create-sink/kafka)
{{</ linkbox >}}
{{</ multilinkbox >}}

For details on the syntax, supported formats and features of each connector,
check out the dedicated `CREATE SINK` documentation pages.

## Formats

To write to an external data sink, you must specify the format Materialize
should use to encode ouput data. This is handled by specifying a `FORMAT` in
the `CREATE SINK` statement.

### Avro

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT AVRO</code></p>

Materialize can encode output data as Avro messages, and automatically publish a
schema to a schema registry based on the columns and data types in the source,
table or materialized view you want to send to the sink.

### JSON

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT JSON</code></p>

Materialize can encode output data as JSON messages. Publishing schemas to a
schema registry is not supported yet for JSON-formatted sinks {{% gh 7186 %}}.

## Envelopes

In addition to determining how to encode output data, Materialize also needs to
understand the desired behaviour of sinked events in the downstream external
system. Whether a change is simply emitted as an event with a diff structure,
or actively inserts, updates, or deletes existing data downstream depends on
the `ENVELOPE` specified in the `CREATE SINK` statement.

### Upsert envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE UPSERT</code></p>

The upsert envelope treats all records as having a **key** and a **value**, and
supports inserts, updates and deletes in the sink:

- If the key does not match a preexisting record downstream, it inserts the
  record's key and value.

- If the key matches a preexisting record downstream and the value
  is _non-null_, Materialize updates the existing record with the new value.

- If the key matches a preexisting record downstream and the value is _null_,
  Materialize deletes the record.

[//]: # "TODO(morsapaes) Add information about upsert key selection"

### Debezium envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE DEBEZIUM</code></p>

Materialize provides a dedicated envelope that describes the decoded records'
old and new values with a Debezium-like diff structure, representing inserts,
updates, or deletes to the underlying source, table or materialized view being
written to the sink:

- If the `before` field is _null_, the record represents an insert.

- If the `before` and `after` fields are _non-null_, the record represents an
  update.

- If the `after` field is _null_, the record represents a delete.

[//]: # "TODO(morsapaes) Add more specific information about envelope
semantics + example output."

## Best practices

### Sizing a sink

Some sinks require relatively few resources to handle data ingestion, while
others are high traffic and require hefty resource allocations. You choose the
amount of CPU and memory available to a sink using the `SIZE` option, and
adjust the provisioned size after sink creation using the [`ALTER SINK`](/sql/alter-sink) command.

Sinks that specify the `SIZE` option are linked to a single-purpose cluster
dedicated to maintaining that sink.

You can also choose to place a sink in an existing
[cluster](/get-started/key-concepts/#clusters) by using the `IN CLUSTER` option.
Sinks in a cluster share the resource allocation of the cluster with all other
objects in the cluster.

Colocating multiple sinks onto the same cluster can be more resource efficient
when you have many low-traffic sinks that occasionally need some burst
capacity.

[//]: # "TODO(morsapaes) Add best practices for sizing sinks."

## Related pages

- [Key Concepts](../../get-started/key-concepts/)
- [`SHOW SINKS`](/sql/show-sinks/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SINK`](/sql/show-create-sink/)
