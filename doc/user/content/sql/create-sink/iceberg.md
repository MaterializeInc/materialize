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

{{< tabs level=3 >}}

{{< tab "MODE UPSERT" >}}

{{% include-syntax file="examples/create_sink_iceberg" example="syntax-upsert" %}}

{{< /tab >}}

{{< tab "MODE APPEND" >}}

{{% include-syntax file="examples/create_sink_iceberg" example="syntax-append" %}}

{{< /tab >}}

{{< /tabs >}}

## Details

Iceberg sinks continuously stream changes from Materialize to an Iceberg table.
Specifically, Materialize writes data as Parquet files to the object storage
backing your Iceberg catalog.

At each `COMMIT INTERVAL`:

1. All pending writes are flushed to Parquet data files. See [Type
   mapping](#type-mapping).
2. In **upsert** mode, delete files are written for any updates or deletes. See
   [Delete handling](#delete-handling). In **append** mode, no delete files are
   written; all changes are data rows. See [Append mode](#append-mode).
3. A new Iceberg snapshot is committed atomically.

When the snapshot is committed, the data is available to downstream query
engines. See [Commit interval tradeoffs](#commit-interval-tradeoffs).

### Iceberg table creation

If the specified Iceberg table does not exist, Materialize creates the table.
The new Iceberg table:
- Uses the schema derived from your Materialize object.
- Uses Iceberg format version 2.

Materialize creates unpartitioned tables. {{< include-from-yaml
data="examples/create_sink_iceberg"
name="restrictions-limitations-partitioned-tables" >}}

See also: [Restrictions and limitations](#restrictions-and-limitations).

### Exactly-once delivery

{{< include-from-yaml data="examples/create_sink_iceberg"
name="exactly-once-delivery" >}}

### Commit interval tradeoffs

The `COMMIT INTERVAL` setting involves tradeoffs between latency and efficiency:

| Shorter intervals (e.g., < `60s`) | Longer intervals (e.g., `5m`) |
|---------------------------------|-------------------------------|
| Lower latency - data visible sooner | Higher latency - data takes longer to appear |
| More small files - can degrade query performance | Fewer, larger files - better query performance |
| Higher catalog overhead | Lower catalog overhead |
| Higher S3 write costs (more PUT requests) | Lower S3 write costs |

**Recommendations:**
- For production: `60s` to `5m`
- For batch analytics: `5m` to `15m`

{{< note >}}
Outside of development environments, commit intervals should be at least `60s`.
Short commit intervals increase catalog overhead and produce many small files.
Small files will result in degraded query performance. It also increases load on
the Iceberg metadata, which can result in a degraded catalog and non-responsive
queries.
{{< /note >}}

### Unique keys

In upsert mode, the Iceberg sink uses upsert semantics based on the `KEY`. The columns you
specify as the `KEY` must uniquely identify rows. Materialize validates that the
key is unique; if it cannot prove uniqueness, you'll receive an error.

If you have outside knowledge that the key is unique, you can bypass validation
using `NOT ENFORCED`. However, if the key is not actually unique, downstream
consumers may see incorrect results.

### Append mode

In append mode (`MODE APPEND`), every change in the Materialize update stream
is written as a data row. No Iceberg delete files are produced. Two extra
columns are appended to the Iceberg table:

| Column | Iceberg type | Description |
|--------|-------------|-------------|
| `_mz_diff` | `int` | `+1` for insertions, `-1` for deletions. |
| `_mz_timestamp` | `long` | The Materialize logical timestamp of the change. |

- An **insert** produces one row with `_mz_diff = +1`.
- A **delete** produces one row with `_mz_diff = -1`.
- An **update** produces two rows: one with `_mz_diff = -1` (the old value) and
  one with `_mz_diff = +1` (the new value). Both carry the same `_mz_timestamp`.

No `KEY` clause is permitted with `MODE APPEND`.

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Restrictions and limitations

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

### Delete handling

{{< note >}}
Delete handling applies to `MODE UPSERT` only. In `MODE APPEND`, all changes
are written as data rows. See [Append mode](#append-mode).
{{< /note >}}

Iceberg sinks use a hybrid delete strategy:

- **Position deletes**: Used when a row is inserted and then deleted or updated
  within the same commit interval. Materialize records the exact file path and
  row position.
- **Equality deletes**: Used when deleting or updating a row from a previous
  snapshot. Materialize writes a delete file containing the `KEY` column values.

This means short-lived rows use efficient position deletes, while updates to
older data use equality deletes.

{{< tip >}}
Consider running [Iceberg compaction](https://iceberg.apache.org/docs/latest/maintenance/#compacting-data-files) periodically to merge delete files and improve query performance.
{{< /tip >}}

## Required privileges

{{% include-headless "/headless/sql-command-privileges/create-sink" %}}

## Troubleshooting

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Examples

### Prerequisites: Create connections

To create an Iceberg sink, you need an AWS connection and an Iceberg catalog
connection.

{{% include-example file="examples/create_connection"
example="example-iceberg-catalog-connection" %}}

### Creating an upsert sink

{{% include-example file="examples/create_sink_iceberg"
example="example-create-iceberg-sink" %}}

In upsert mode, the required `KEY` clause uniquely identifies rows; in this
example, it uses a composite key of `user_id` and `event_timestamp`.
Materialize validates that this key is unique in the source data.

### Bypassing unique key validation

If Materialize cannot prove your key is unique but you have outside knowledge
that it is, you can bypass validation by including `NOT ENFORCED` option:

```mzsql
CREATE SINK deduped_sink
  IN CLUSTER my_cluster
  FROM my_source
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog_connection (
    NAMESPACE = 'raw',
    TABLE = 'events'
  )
  USING AWS CONNECTION aws_connection
  KEY (event_id) NOT ENFORCED
  MODE UPSERT
  WITH (COMMIT INTERVAL = '1m');
```

{{< warning >}}
If the key is not actually unique, downstream consumers may see incorrect
results.
{{< /warning >}}

### Creating an append sink

{{% include-example file="examples/create_sink_iceberg"
example="example-create-iceberg-sink-append" %}}

The Iceberg table will contain all columns from `user_events` plus two
additional columns: `_mz_diff` and `_mz_timestamp`. See [Append
mode](#append-mode).

## Related pages

- [Iceberg sink guide](/serve-results/sink/iceberg/)
- [`SHOW SINKS`](/sql/show-sinks)
- [`DROP SINK`](/sql/drop-sink)
- [`CREATE CONNECTION`](/sql/create-connection)
