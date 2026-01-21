---
title: "CREATE SINK: Iceberg"
description: "Connecting Materialize to an Apache Iceberg table"
menu:
  main:
    parent: 'create-sink'
    identifier: csink_iceberg
    name: Iceberg
    weight: 30
---

{{< public-preview />}}

`CREATE SINK` connects Materialize to an external system you want to write data
to, and provides details about how to encode that data.

To use an Iceberg table as a sink, you need:
- An [Iceberg catalog connection](#creating-an-iceberg-catalog-connection) to
  specify access parameters to your Iceberg catalog
- An [AWS connection](/sql/create-connection/#aws) for authentication with
  object storage

Once created, connections are **reusable** across multiple `CREATE SINK` statements.

Sink source type      | Description
----------------------|------------
**Source**            | Pass all data received from the source to the sink.
**Table**             | Stream all changes to the specified table to the sink.
**Materialized view** | Stream all changes to the materialized view to the sink.

## Syntax

```mzsql
CREATE SINK [IF NOT EXISTS] <sink_name>
  [IN CLUSTER <cluster_name>]
  FROM <item_name>
  INTO ICEBERG CATALOG CONNECTION <catalog_connection> (
    NAMESPACE = '<namespace>',
    TABLE = '<table>'
  )
  USING AWS CONNECTION <aws_connection>
  KEY ( <key_col> [, ...] ) [NOT ENFORCED]
  WITH (COMMIT INTERVAL = '<interval>')
```

### Syntax elements

| Element | Description |
|---------|-------------|
| `<sink_name>` | The name for the sink. |
| **IF NOT EXISTS** | Optional. Do not throw an error if a sink with the same name already exists. |
| **IN CLUSTER** `<cluster_name>` | Optional. The [cluster](/sql/create-cluster) to maintain this sink. |
| `<item_name>` | The name of the source, table, or materialized view to sink. |
| **ICEBERG CATALOG CONNECTION** `<catalog_connection>` | The name of the Iceberg catalog connection. |
| **NAMESPACE** `'<namespace>'` | The Iceberg namespace (database) containing the table. |
| **TABLE** `'<table>'` | The name of the Iceberg table to write to. If the table doesn't exist, Materialize will create it. |
| **USING AWS CONNECTION** `<aws_connection>` | The AWS connection for object storage access. |
| **KEY** `(<key_col>, ...)` | **Required.** The columns that uniquely identify rows. Used to generate equality deletes when rows are updated or deleted. |
| **NOT ENFORCED** | Optional. Disable validation of key uniqueness. Use only when you have outside knowledge that the key is unique. |
| **COMMIT INTERVAL** `'<interval>'` | **Required.** How frequently to commit snapshots to Iceberg (e.g., `'10s'`, `'1m'`). |

## How Iceberg sinks work

Iceberg sinks continuously stream changes from your source relation to an
Iceberg table. If the table doesn't exist, Materialize automatically creates it
with a schema matching your source.

At each `COMMIT INTERVAL`, a new snapshot is committed, making the data
available to downstream query engines. Inserts, updates, and deletes are all
reflected in the Iceberg table—the `KEY` columns identify rows for updates and
deletes.

Iceberg sinks provide **exactly-once delivery**: Materialize resumes from the
last committed snapshot after restarts without duplicating data.

### Commit interval tradeoffs

The `COMMIT INTERVAL` setting involves tradeoffs between latency and efficiency:

| Shorter intervals (e.g., `10s`) | Longer intervals (e.g., `5m`) |
|---------------------------------|-------------------------------|
| Lower latency - data visible sooner | Higher latency - data takes longer to appear |
| More small files - can degrade query performance | Fewer, larger files - better query performance |
| Higher catalog overhead | Lower catalog overhead |

**Recommendations:**
- For real-time use cases: `10s` to `1m`
- For batch analytics: `5m` to `15m`
- If query performance degrades due to small files, increase the interval and
  run Iceberg compaction

## Required privileges

To execute the `CREATE SINK` command, you need:

- `CREATE` privilege on the target schema
- `USAGE` privilege on the Iceberg catalog connection
- `USAGE` privilege on the AWS connection
- `SELECT` privilege on the source relation

## Examples

### Creating an Iceberg catalog connection

AWS S3 Tables provides a managed Iceberg catalog:

```mzsql
-- Create an AWS connection for authentication
CREATE CONNECTION aws_connection
  TO AWS (ASSUME ROLE ARN = 'arn:aws:iam::123456789012:role/MaterializeIceberg');

-- Create the Iceberg catalog connection
CREATE CONNECTION s3tables_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'https://s3tables.us-east-1.amazonaws.com/iceberg',
    WAREHOUSE = 'arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket',
    AWS CONNECTION = aws_connection
);
```

### Creating a sink

Basic example:

```mzsql
CREATE SINK orders_iceberg
  IN CLUSTER analytics_cluster
  FROM orders_view
  INTO ICEBERG CATALOG CONNECTION s3tables_catalog (
    NAMESPACE = 'analytics',
    TABLE = 'orders'
  )
  USING AWS CONNECTION aws_connection
  KEY (order_id)
  WITH (COMMIT INTERVAL = '30s');
```

With composite key:

```mzsql
CREATE SINK user_events_iceberg
  IN CLUSTER analytics_cluster
  FROM user_events
  INTO ICEBERG CATALOG CONNECTION s3tables_catalog (
    NAMESPACE = 'events',
    TABLE = 'user_events'
  )
  USING AWS CONNECTION aws_connection
  KEY (user_id, event_timestamp)
  WITH (COMMIT INTERVAL = '1m');
```

### Validating key uniqueness

If Materialize cannot prove your key is unique, you can bypass validation:

```mzsql
CREATE SINK deduped_sink
  IN CLUSTER my_cluster
  FROM my_source
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog (
    NAMESPACE = 'raw',
    TABLE = 'events'
  )
  USING AWS CONNECTION aws_connection
  -- Bypass key uniqueness validation
  KEY (event_id) NOT ENFORCED
  WITH (COMMIT INTERVAL = '10s');
```

{{< warning >}}
If the key is not actually unique, downstream consumers may see incorrect
results when equality deletes are applied.
{{< /warning >}}

## Limitations

- **Schema evolution**: Materialize does not support changing the schema of an
  existing Iceberg table. If the source schema changes, you must drop and
  recreate the sink.
- **Partition evolution**: Partition spec changes are not supported.
- **Partitioning**: Materialize creates unpartitioned tables. Partitioned tables
  are not yet supported.
- **Record types**: Composite/record types are not supported. Use scalar types
  or flatten your data structure.

## Technical reference

For details on the underlying implementation, including data file formats,
equality deletes, progress tracking, and table creation behavior, see the
[Iceberg sink reference](/serve-results/sink/iceberg/#reference).

## Related pages

- [Iceberg sink guide](/serve-results/sink/iceberg/)
- [`SHOW SINKS`](/sql/show-sinks)
- [`DROP SINK`](/sql/drop-sink)
- [`CREATE CONNECTION`](/sql/create-connection)

