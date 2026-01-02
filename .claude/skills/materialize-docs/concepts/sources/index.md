---
audience: developer
canonical_url: https://materialize.com/docs/concepts/sources/
complexity: beginner
description: Learn about sources in Materialize.
doc_type: concept
keywords:
- CREATE SOURCE
- Envelope
- Message Brokers
- Connector
- Databases (CDC)
- Sources
- Format
product_area: Concepts
status: stable
title: Sources
---

# Sources

## Purpose
Learn about sources in Materialize.

Read this to understand how this concept works in Materialize.


Learn about sources in Materialize.


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


**Databases (CDC)**
- [PostgreSQL](/ingest-data/postgres/)
- [MySQL](/ingest-data/mysql/)
- [SQL Server](/ingest-data/sql-server/)
- [CockroachDB](/ingest-data/cdc-cockroachdb/)
- [MongoDB](/ingest-data/mongodb/)
**Message Brokers**
- [Kafka](/ingest-data/kafka/)
- [Redpanda](/sql/create-source/kafka)
**Webhooks**
- [Amazon EventBridge](/ingest-data/webhooks/amazon-eventbridge/)
- [Segment](/ingest-data/webhooks/segment/)
- [Other webhooks](/sql/create-source/webhook)


For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.


## Sources and clusters

Sources require compute resources in Materialize, and so need to be associated
with a [cluster](/concepts/clusters/). If possible, dedicate a cluster just for
sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)