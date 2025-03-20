---
title: Sources
description: Learn about sources in Materialize.
menu:
  main:
    parent: concepts
    weight: 10
    identifier: 'concepts-sources'
aliases:
  - /get-started/key-concepts/#sources
---

## Overview

Sources describe external systems you want Materialize to read data from, and
provide details about how to decode and interpret that data. A simplistic way to
think of this is that sources represent streams and their schemas; this isn't
entirely accurate, but provides an illustrative mental model.

In terms of SQL, sources are similar to a combination of tables and
clients.

- Like tables, sources are structured components that users can read from.
- Like clients, sources are responsible for reading data. External
  sources provide all of the underlying data to process.

By looking at what comprises a source, we can develop a sense for how this
combination works.

[//]: # "TODO(morsapaes) Add details about source persistence."

## Source components

Sources consist of the following components:

Component      | Use                                                                                               | Example
---------------|---------------------------------------------------------------------------------------------------|---------
**Connector**  | Provides actual bytes of data to Materialize                                                      | Kafka
**Format**     | Structures of the external source's bytes, i.e. its schema                                        | Avro
**Envelope**   | Expresses how Materialize should handle the incoming data + any additional formatting information | Upsert

### Connectors

Materialize bundles native connectors for the following external systems:

- [Kafka](/sql/create-source/kafka)
- [Redpanda](/sql/create-source/kafka)
- [PostgreSQL](/sql/create-source/postgres)
- [MySQL](/sql/create-source/mysql/)
- [Webhooks](/sql/create-source/webhook/)

For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.

### Formats

Materialize can decode incoming bytes of data from several formats:

- Avro
- Protobuf
- Regex
- CSV
- Plain text
- Raw bytes
- JSON

### Envelopes

What Materialize actually does with the data it receives depends on the
"envelope" your data provides:

Envelope | Action
---------|-------
**Append-only** | Inserts all received data; does not support updates or deletes.
**Debezium** | Treats data as wrapped in a "diff envelope" that indicates whether the record is an insertion, deletion, or update. The Debezium envelope is only supported by sources published to Kafka by [Debezium].<br/><br/>For more information, see [`CREATE SOURCE`: Kafka&mdash;Using Debezium](/sql/create-source/kafka/#using-debezium).
**Upsert** | Treats data as having a key and a value. New records with non-null value that have the same key as a preexisting record in the dataflow will replace the preexisting record. New records with null value that have the same key as preexisting record will cause the preexisting record to be deleted. <br/><br/>For more information, see [`CREATE SOURCE`: &mdash;Handling upserts](/sql/create-source/kafka/#handling-upserts).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
