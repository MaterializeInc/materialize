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
- An [Iceberg catalog connection](/sql/create-connection/#iceberg-catalog) to
  specify access parameters to your Iceberg catalog
- An [AWS connection](/sql/create-connection/#aws) for authentication with
  object storage

Once created, connections are **reusable** across multiple `CREATE SINK` statements.

## Syntax

{{% include-syntax file="examples/create_sink_iceberg" example="syntax" %}}

## Details

Iceberg sinks continuously stream changes from Materialize to an Iceberg table.
If the table doesn't exist, Materialize automatically creates it with a schema
matching the source object.

At each `COMMIT INTERVAL`, a new snapshot is committed, making the data
available to downstream query engines. See [Commit interval tradeoffs](#commit-interval-tradeoffs).

Iceberg sinks provide **exactly-once delivery**: Materialize resumes from the
last committed snapshot after restarts without duplicating data.

### Unique keys

The `KEY` clause is required for all Iceberg sinks. The columns you specify must
uniquely identify rows. Materialize validates that the key is unique; if it
cannot prove uniqueness, you'll receive an error.

If you have outside knowledge that the key is unique, you can bypass validation
using `NOT ENFORCED`. However, if the key is not actually unique, downstream
consumers may see incorrect results.

### Commit interval tradeoffs

The `COMMIT INTERVAL` setting involves tradeoffs between latency and efficiency:

| Shorter intervals (e.g., `10s`) | Longer intervals (e.g., `5m`) |
|---------------------------------|-------------------------------|
| Lower latency - data visible sooner | Higher latency - data takes longer to appear |
| More small files - can degrade query performance | Fewer, larger files - better query performance |
| Higher catalog overhead | Lower catalog overhead |
| Higher S3 write costs (more PUT requests) | Lower S3 write costs |

**Recommendations:**
- For real-time use cases: `10s` to `1m`
- For batch analytics: `5m` to `15m`
- If query performance degrades due to small files, increase the interval and
  run Iceberg compaction

### How deletes work

Iceberg sinks use a hybrid delete strategy:

- **Position deletes**: Used when a row is inserted and then deleted or updated
  within the same commit interval. Materialize records the exact file path and
  row position.
- **Equality deletes**: Used when deleting or updating a row from a previous
  snapshot. Materialize writes a delete file containing the `KEY` column values.

This means short-lived rows use efficient position deletes, while updates to
older data use equality deletes. Consider running Iceberg compaction periodically
to merge delete files and improve query performance.

### Type mapping

Materialize converts SQL types to Iceberg types:

| SQL type | Iceberg type |
|----------|--------------|
| `boolean` | `boolean` |
| `smallint`, `integer` | `int` |
| `bigint` | `long` |
| `real` | `float` |
| `double precision` | `double` |
| `numeric` | `decimal(38, scale)` |
| `date` | `date` |
| `time` | `time` (microsecond) |
| `timestamp` | `timestamp` (microsecond) |
| `timestamptz` | `timestamptz` (microsecond) |
| `text`, `varchar` | `string` |
| `bytea` | `binary` |
| `uuid` | `fixed(16)` |
| `jsonb` | `string` |
| `list` | `list` |
| `map` | `map` |

## Required privileges

{{< include-md file="shared-content/sql-command-privileges/create-sink.md" >}}

## Examples

### Prerequisites: Create connections

Before creating an Iceberg sink, you need an AWS connection and an Iceberg
catalog connection. AWS S3 Tables provides a managed Iceberg catalog.

```mzsql
-- Create an AWS connection for authentication
CREATE CONNECTION aws_connection
  TO AWS (ASSUME ROLE ARN = 'arn:aws:iam::123456789012:role/MaterializeIceberg');

-- Create the Iceberg catalog connection pointing to S3 Tables
CREATE CONNECTION s3tables_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'https://s3tables.us-east-1.amazonaws.com/iceberg',
    WAREHOUSE = 'arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket',
    AWS CONNECTION = aws_connection
);
```

### Creating a sink

Using the connections created above, the following example creates an Iceberg
sink with a composite key:

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
  ENVELOPE UPSERT
  WITH (COMMIT INTERVAL = '1m');
```

The required `KEY` clause uniquely identifies rows; in this example, it uses a
composite key of `user_id` and `event_timestamp`. Materialize validates that this
key is unique in the source data.

### Bypassing unique key validation

If Materialize cannot prove your key is unique but you have outside knowledge
that it is, you can bypass validation:

```mzsql
CREATE SINK deduped_sink
  IN CLUSTER my_cluster
  FROM my_source
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog (
    NAMESPACE = 'raw',
    TABLE = 'events'
  )
  USING AWS CONNECTION aws_connection
  KEY (event_id) NOT ENFORCED
  ENVELOPE UPSERT
  WITH (COMMIT INTERVAL = '10s');
```

{{< warning >}}
If the key is not actually unique, downstream consumers may see incorrect
results.
{{< /warning >}}

## Limitations

- **Same region required**: Your S3 Tables bucket must be in the same AWS region
  as your Materialize deployment.
- **Schema evolution**: Materialize does not support changing the schema of an
  existing Iceberg table. If the source schema changes, you must drop and
  recreate the sink.
- **Partition evolution**: Partition spec changes are not supported.
- **Partitioning**: Materialize creates unpartitioned tables. Partitioned tables
  are not yet supported.
- **Record types**: Composite/record types are not supported. Use scalar types
  or flatten your data structure.

## Related pages

- [Iceberg sink guide](/serve-results/sink/iceberg/)
- [`SHOW SINKS`](/sql/show-sinks)
- [`DROP SINK`](/sql/drop-sink)
- [`CREATE CONNECTION`](/sql/create-connection)
