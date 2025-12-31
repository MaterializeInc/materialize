---
title: CREATE SOURCE
description: Connect Materialize to an external data source
doc_type: reference
product_area: Sources
audience: developer
status: stable
complexity: intermediate
keywords:
  - CREATE SOURCE
  - source
  - ingestion
  - Kafka
  - PostgreSQL
  - MySQL
  - webhooks
  - CDC
canonical_url: https://materialize.com/docs/sql/create-source/
---

# CREATE SOURCE

## Purpose
`CREATE SOURCE` connects Materialize to an external data source. Sources describe external systems you want Materialize to read data from and provide details about how to decode and interpret that data.

If you're setting up data ingestion into Materialize, this is your starting point.

## When to use
- Ingesting streaming data from Kafka, Redpanda, or other message brokers
- Replicating data from PostgreSQL, MySQL, or SQL Server via CDC
- Receiving data via webhooks
- Loading sample data for testing

**Not for**: Querying data without persistence (use SELECT on existing objects) or transforming before ingestion (create source first, then a view)

## Connector-Specific Documentation

Each connector has its own detailed documentation:

### Database CDC
- [PostgreSQL Source](postgres/index.md) — Native CDC from PostgreSQL
- [MySQL Source](mysql/index.md) — Native CDC from MySQL
- [SQL Server Source](sql-server/index.md) — CDC from SQL Server

### Message Brokers
- [Kafka Source](kafka/index.md) — Consume from Kafka and Redpanda

### Webhooks
- [Webhook Source](webhook/index.md) — Receive HTTP webhook data

### Sample Data
- [Load Generator Source](load-generator/index.md) — Generate test data

## Formats

Sources support the following data formats:

- **Avro** (`FORMAT AVRO`) — Schema-based binary format. Integrates with Confluent Schema Registry. Supports schema evolution.
- **JSON** (`FORMAT JSON`) — JSON messages decoded to `jsonb` column named `data`.
- **Protobuf** (`FORMAT PROTOBUF`) — Google Protocol Buffers format. Requires schema registry or inline schema.
- **Text** (`FORMAT TEXT`) — Newline-delimited UTF-8 text. Single column named `text`.
- **Bytes** (`FORMAT BYTES`) — Raw binary data. Single column named `data`.
- **CSV** (`FORMAT CSV`) — Comma-separated values with header or column count specification.

## Envelopes

Envelopes determine how records are interpreted:

- **Append-only** (`ENVELOPE NONE`) — Default. All records are inserts.
- **Upsert** (`ENVELOPE UPSERT`) — Records have key and value. Supports inserts, updates, and deletes based on key.
- **Debezium** (`ENVELOPE DEBEZIUM`) — For Debezium CDC format. Interprets `before`/`after` fields for insert/update/delete.

## Basic Syntax

```sql
CREATE SOURCE source_name
  [ IN CLUSTER cluster_name ]
  FROM connector_type CONNECTION connection_name
  ( connector_options )
  FORMAT format_type [ format_options ]
  [ ENVELOPE envelope_type ]
  [ WITH ( source_options ) ]
```

## Examples

### Kafka source with Avro
```sql
CREATE SOURCE kafka_source
  IN CLUSTER ingest_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'events')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
```

### PostgreSQL source
```sql
CREATE SOURCE pg_source
  IN CLUSTER ingest_cluster
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source')
  FOR TABLES (public.users, public.orders);
```

### Webhook source
```sql
CREATE SOURCE webhook_source
  IN CLUSTER ingest_cluster
  FROM WEBHOOK
    BODY FORMAT JSON;
```

## Requirements

- A [CONNECTION](../create-connection/index.md) must be created first for most source types
- Sources must be associated with a [cluster](../../concepts/clusters/index.md)
- Sources consume cluster resources continuously, even when not queried

## Related Commands

- [ALTER SOURCE](../alter-source/index.md) — Modify source configuration
- [DROP SOURCE](../drop-source/index.md) — Remove a source
- [SHOW SOURCES](../show-sources/index.md) — List sources
- [SHOW CREATE SOURCE](../show-create-source/index.md) — Show source definition

## Key Takeaways

- Each connector (Kafka, PostgreSQL, MySQL, etc.) has specific syntax and options
- Sources are append-only by default; use `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` for updates/deletes
- Sources require a CONNECTION object for authentication
- Sources run in clusters and consume resources continuously
