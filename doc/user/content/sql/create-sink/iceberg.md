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

Use `CREATE SINK ... INTO ICEBERG CATALOG...` to create Iceberg sinks. Iceberg sinks write data from Materialize into an Iceberg table hosted on
AWS S3 Tables. As data changes in Materialize, your Iceberg tables are
automatically kept up to date.

To create an Iceberg sink, you need:

- An [AWS connection](/sql/create-connection/#aws) for authentication with
  object storage.
- An [Iceberg catalog connection](/sql/create-connection/#iceberg-catalog) to
  specify access parameters to your Iceberg catalog.

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

The `KEY` clause is required for all Iceberg sinks. The Iceberg sink uses upsert semantics based on the `KEY`. The columns you specify must
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
older data use equality deletes.

{{< tip >}}
Consider running Iceberg compaction periodically to merge delete files and improve query performance.
{{< /tip >}}

### Type mapping

{{% include-headless "/headless/iceberg-sinks/type-mapping" %}}

### Restrictions and limitations

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Required privileges

{{% include-headless "/headless/sql-command-privileges/create-sink" %}}

## Examples

### Prerequisites: Create connections

To create an Iceberg sink, you need an AWS connection and an Iceberg catalog
connection.

{{% include-example file="examples/create_connection"
example="example-iceberg-catalog-connection" %}}

### Creating a sink

{{% include-example file="examples/create_sink_iceberg"
example="example-create-iceberg-sink" %}}

The required `KEY` clause uniquely identifies rows; in this example, it uses a
composite key of `user_id` and `event_timestamp`. Materialize validates that
this key is unique in the source data.

### Bypassing unique key validation

If Materialize cannot prove your key is unique but you have outside knowledge
that it is, you can bypass validation by including `NOT ENFORCED` option:

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
  MODE UPSERT
  WITH (COMMIT INTERVAL = '10s');
```

{{< warning >}}
If the key is not actually unique, downstream consumers may see incorrect
results.
{{< /warning >}}


## Related pages

- [Iceberg sink guide](/serve-results/sink/iceberg/)
- [`SHOW SINKS`](/sql/show-sinks)
- [`DROP SINK`](/sql/drop-sink)
- [`CREATE CONNECTION`](/sql/create-connection)
