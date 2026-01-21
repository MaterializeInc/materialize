---
title: "Apache Iceberg"
description: "How to export results from Materialize to Apache Iceberg tables."
menu:
  main:
    parent: sink
    name: "Apache Iceberg"
    weight: 15
---

{{< public-preview />}}

This guide walks you through the steps required to export results from
Materialize to [Apache Iceberg](https://iceberg.apache.org/) tables. Iceberg
sinks are useful for maintaining a continuously updated analytical table that
can be queried by data warehouses like Snowflake, Databricks, or Spark.

## Before you begin

- Ensure you have access to an AWS account with permissions to create and manage
  IAM policies and roles.
- Ensure you have an AWS S3 Tables bucket configured in your AWS account.

## How it works

Iceberg sinks continuously stream changes from your source, table, or
materialized view to an Iceberg table. If the table doesn't exist, Materialize
automatically creates it with a schema matching your source relation.

At each `COMMIT INTERVAL`, Materialize commits a new snapshot to the Iceberg
table, making the data available to downstream query engines. Inserts, updates,
and deletes from your source are all reflected in the Iceberg table—the `KEY`
columns you specify identify rows for updates and deletes.

Iceberg sinks provide **exactly-once delivery**: after a restart, Materialize
resumes from the last committed snapshot without duplicating data.

## Step 1. Set up AWS permissions

Materialize needs permissions to write data files to the object storage backing
your Iceberg catalog. We **strongly** recommend using [role assumption-based
authentication](/sql/create-connection/#aws-permissions) to manage access.

### Create an IAM policy

Create an [IAM policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that allows Materialize to write to your Iceberg table's storage location:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::<bucket>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "<prefix>/*"
                    ]
                }
            }
        }
    ]
}
```

For AWS S3 Tables, you'll also need permissions to interact with the S3 Tables
API. Add the following to your IAM policy:

```json
{
    "Effect": "Allow",
    "Action": "s3tables:*",
    "Resource": "*"
}
```

You can scope the resource ARN more narrowly to your specific S3 Tables bucket
if desired.

### Create an IAM role

Create an [IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)
with a trust policy that allows Materialize to assume the role:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::664411391173:role/MaterializeConnection"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "PENDING"
                }
            }
        }
    ]
}
```

You'll update the external ID after creating the AWS connection in Materialize.

## Step 2. Create connections

Iceberg sinks require two connections:

1. An **AWS connection** for authentication with object storage
2. An **Iceberg catalog connection** to interact with the Iceberg catalog

### Create an AWS connection

```mzsql
CREATE CONNECTION aws_connection
   TO AWS (ASSUME ROLE ARN = 'arn:aws:iam::<account-id>:role/<role>');
```

Retrieve the external ID and update your IAM role's trust policy:

```mzsql
SELECT external_id
FROM mz_internal.mz_aws_connections awsc
JOIN mz_connections c ON awsc.id = c.id
WHERE c.name = 'aws_connection';
```

### Create an Iceberg catalog connection

Create an Iceberg catalog connection for AWS S3 Tables:

```mzsql
CREATE CONNECTION iceberg_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'https://s3tables.<region>.amazonaws.com/iceberg',
    WAREHOUSE = 'arn:aws:s3tables:<region>:<account-id>:bucket/<table-bucket-name>',
    AWS CONNECTION = aws_connection
);
```

Replace `<region>` with your AWS region (e.g., `us-east-1`) and `<table-bucket-name>`
with the name of your S3 Tables bucket.

## Step 3. Create the sink

Create a sink from a source, table, or materialized view:

```mzsql
CREATE SINK my_iceberg_sink
  IN CLUSTER my_cluster
  FROM my_materialized_view
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog (
    NAMESPACE = 'my_namespace',
    TABLE = 'my_table'
  )
  USING AWS CONNECTION aws_connection
  KEY (id)
  WITH (COMMIT INTERVAL = '10s');
```

### Required options

| Option | Description |
|--------|-------------|
| `NAMESPACE` | The Iceberg namespace (database) containing the table. |
| `TABLE` | The name of the Iceberg table to write to. |
| `KEY` | The columns that uniquely identify rows. Required for handling updates and deletes via equality deletes. |
| `COMMIT INTERVAL` | How frequently to commit snapshots to Iceberg. See [Commit interval tradeoffs](#commit-interval-tradeoffs) below. |

### Commit interval tradeoffs {#commit-interval-tradeoffs}

The `COMMIT INTERVAL` setting controls how frequently Materialize commits
snapshots to your Iceberg table. This involves tradeoffs:

| Shorter intervals (e.g., `10s`) | Longer intervals (e.g., `5m`) |
|---------------------------------|-------------------------------|
| Lower latency - data visible sooner in downstream systems | Higher latency - data takes longer to appear |
| More small files - can degrade query performance over time | Fewer, larger files - better query performance |
| More frequent snapshot commits - higher catalog overhead | Less catalog overhead |
| Lower throughput efficiency | Higher throughput efficiency |

**Recommendations:**
- For real-time dashboards or alerting, use shorter intervals (`10s` to `1m`)
- For batch analytics, use longer intervals (`5m` to `15m`)
- If you notice query performance degradation from small files, increase the
  interval and consider running Iceberg compaction jobs

### Key selection

The `KEY` columns you specify must uniquely identify rows in your source
relation. Materialize uses these columns to generate equality delete files when
rows are updated or deleted.

If Materialize cannot validate that your key is unique, you'll receive an error.
You can use `KEY (...) NOT ENFORCED` to bypass this validation if you have
outside knowledge that the key is unique.

## Querying Iceberg tables

Once your sink is running, you can query the Iceberg table from any system that
supports Iceberg:

- **Snowflake**: Use [Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- **Databricks**: Use the [Iceberg connector](https://docs.databricks.com/en/delta/clone-parquet.html)
- **Spark**: Use [Apache Iceberg for Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- **Trino/Presto**: Use the [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)

## Limitations

- **Schema evolution**: Materialize does not currently support evolving the
  schema of an existing Iceberg table. If you need to change the schema, you
  must drop and recreate the sink.
- **Partition evolution**: Partition spec changes are not supported.
- **Table format**: Only Iceberg v2 format is supported.
- **Record types**: Composite/record types are not currently supported. Use
  scalar types or flatten your data structure.

## Troubleshooting

### Sink creation fails with "input compacted past resume upper"

This error occurs when the source data has been compacted beyond the point where
the sink last committed. This can happen after a Materialize backup/restore
operation. You may need to drop and recreate the sink, which will re-snapshot
the entire source relation.

### Commit conflicts

If another process modifies the Iceberg table while Materialize is committing,
you may see commit conflict errors. Materialize will automatically retry, but
if conflicts persist, ensure no other writers are modifying the same table.

## Reference

This section provides technical details about how Iceberg sinks work under the
hood.

### Data files and snapshots

Materialize writes data as Parquet files to the object storage backing your
Iceberg catalog. At each commit interval:

1. All pending writes are flushed to Parquet data files
2. Delete files are written for any updates or deletes
3. A new Iceberg snapshot is committed atomically

The snapshot makes all changes from that interval visible to readers as a single
atomic unit.

### How deletes work

Iceberg sinks use a hybrid delete strategy that optimizes for different
scenarios:

**Position deletes** are used when a row is inserted and then deleted (or
updated) within the same commit interval. Since Materialize knows exactly where
the row was written, it records the file path and row position. This is
efficient because it targets a specific location.

**Equality deletes** are used when deleting or updating a row that exists in a
previous snapshot (i.e., was written in an earlier commit interval). Materialize
writes a delete file containing the `KEY` column values, and query engines match
these against rows in existing data files at read time.

This hybrid approach means:
- Short-lived rows (inserted and deleted quickly) use efficient position deletes
- Long-lived rows use equality deletes, which may accumulate over time
- The `KEY` columns must uniquely identify rows for equality deletes to work
  correctly
- Consider running [Iceberg compaction](https://iceberg.apache.org/docs/latest/maintenance/#compacting-data-files)
  periodically to merge delete files and improve query performance

### Table creation

When Materialize creates a new Iceberg table, it uses:
- A schema derived from your source relation's columns
- Iceberg format version 2
- No partitioning (unpartitioned table)

### Progress tracking

Materialize stores progress information in Iceberg snapshot metadata properties
(`mz-frontier` and `mz-sink-version`). This enables exactly-once delivery by
allowing Materialize to identify and resume from the last successfully committed
snapshot after a restart.

### Type mapping

Materialize converts SQL types to Iceberg/Parquet types:

| SQL type | Iceberg type |
|----------|--------------|
| `boolean` | `boolean` |
| `smallint` | `int` |
| `integer` | `int` |
| `bigint` | `long` |
| `real` | `float` |
| `double precision` | `double` |
| `numeric` | `decimal(38, scale)` |
| `date` | `date` |
| `time` | `time` (microsecond precision) |
| `timestamp` | `timestamp` (microsecond precision) |
| `timestamptz` | `timestamptz` (microsecond precision) |
| `text` / `varchar` | `string` |
| `bytea` | `binary` |
| `uuid` | `fixed(16)` |
| `jsonb` | `string` |
| `list` | `list` |
| `map` | `map` |

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
