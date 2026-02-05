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

Iceberg sinks allow you to write data from Materialize into [Apache
Iceberg](https://iceberg.apache.org/) tables hosted on [Amazon S3
Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html).
As data changes in Materialize, the corresponding Iceberg tables are
automatically kept up to date.

Apache Iceberg is an open table format for large-scale analytics datasets that brings reliable, performant ACID transactions, schema evolution, and time travel to data lakes. It gives you data warehouse-like reliability, with the cost advantages of object storage.

Amazon S3 Tables is an AWS feature that provides fully managed Apache Iceberg tables as a native S3 storage type, eliminating the need to manage separate metadata catalogs or table maintenance operations. It automatically handles compaction & snapshot management.

This guide walks you through the steps required to set up Iceberg sinks in
Materialize Cloud.

## Before you begin

- Ensure you have access to an AWS account with permissions to create and manage
  IAM policies and roles.
- Ensure you have an AWS S3 Tables bucket configured in your AWS account.
- Ensure you have created a namespace in your S3 Tables bucket. For details, see
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-namespace-create.html.

## How it works

Iceberg sinks continuously stream changes from your Materialize object (i.e.,
source, table, or materialized view) to an Iceberg table. If the Iceberg table
doesn't exist, Materialize automatically creates it with a schema matching your
source Materialize object.

At each `COMMIT INTERVAL`, Materialize commits a new snapshot to the Iceberg
table, making the data available to downstream query engines. Inserts, updates,
and deletes from your source are all reflected in the Iceberg table—the `KEY`
columns you specify identify rows for updates and deletes.

Iceberg sinks provide **exactly-once delivery**: after a restart, Materialize
resumes from the last committed snapshot without duplicating data.

## Step 1. Set up permissions in AWS

Materialize needs permissions to write data files to the object storage backing
your Iceberg catalog. We **strongly** recommend using [role assumption-based
authentication](/sql/create-connection/#aws-permissions) to manage access.

### Create an IAM policy

Create an [IAM
policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that allows Materialize to write to your Iceberg table's storage location and
interact with the S3 Tables API:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3tables:*",
            "Resource": "*"
        }
    ]
}
```

You can scope the `s3tables` resource ARN more narrowly to your specific S3
Tables bucket if desired.

### Create an IAM role

Create an [IAM
role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) specifying
the following `Custom trust policy`:

- `Principal`: The IAM role principal uses the the AWS account ID
  `664411391173`, which is the Materialize Cloud AWS account. For self-managed
  deployments and the Emulator, the AWS account ID differs.

- `ExternalId`: The "Pending" is a placeholder and will be updated after
creating the AWS connection in Materialize.

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

For permissions, add the IAM policy [created earlier](#create-an-iam-policy).

Once you have created the IAM role, copy the role ARN from the AWS console.
You'll use the ARN in the next step.

## Step 2.Create an AWS connection in Materialize

To create an Iceberg sink in Materialize, you need an **AWS connection** for
authentication with object storage (as well as an **Iceberg catalog connection**)

1. Use [`CREATE CONNECTION ... TO AWS`](/sql/create-connection/#aws) to create
   an AWS connection, replacing:
   
   - `<IAM role ARN>` with your IAM role ARN from [step 1](#create-an-iam-role)
   - `<region>` with your AWS region (e.g., `us-east-1`):

    ```mzsql
    CREATE CONNECTION aws_connection TO AWS (
        ASSUME ROLE ARN = '<IAM role ARN>',
        REGION = '<region>'
    );
    ```

    For more details on AWS connection options, see [`CREATE
    CONNECTION`](/sql/create-connection/#aws).

1. Fetch the `external_id` for the connection:

   ```mzsql
   SELECT external_id
   FROM mz_internal.mz_aws_connections awsc
   JOIN mz_connections c ON awsc.id = c.id
   WHERE c.name = 'aws_connection';
   ```

## Step 3. Update the IAM role in AWS

Once you have the `external_id`, update the trust policy for the IAM role
created in [step 1](#create-an-iam-role). Replace `"PENDING"` with your external
ID value. Your IAM trust policy should look like the following (but with your
external ID value):

```json{hl_lines="12"} 
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
                    "sts:ExternalId": "mz_1234abcd-5678-efgh-9012-ijklmnopqrst_u123"
                }
            }
        }
    ]
}
```

## Step 4. Create an Iceberg catalog connection in Materialize

To create an Iceberg sink in Materialize, you need an **Iceberg catalog
connection**.

1. Use [`CREATE CONNECTION ... TO ICEBERG
   CATALOG`](/sql/create-connection/#iceberg-catalog) to create an Iceberg
   catalog connection, replacing:
   - `<region>` with your AWS region (e.g., `us-east-1`) and
   - `<S3 table bucket ARN>` with your AWS S3 Table ARN.

   The command uses the AWS connection you created earlier.

   ```mzsql
   CREATE CONNECTION iceberg_catalog TO ICEBERG CATALOG (
       CATALOG TYPE = 's3tablesrest',
       URL = 'https://s3tables.<region>.amazonaws.com/iceberg',
       WAREHOUSE = '<S3 table bucket ARN>',
       AWS CONNECTION = aws_connection
   );
   ```

## Step 5. Materialize: Create the sink

Create a sink from a source, table, or materialized view. For full syntax
options, see [`CREATE SINK`](/sql/create-sink/iceberg).

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
  MODE UPSERT
  WITH (COMMIT INTERVAL = '10s');
```

### Required options

| Option | Description |
|--------|-------------|
| `MODE UPSERT` | Required. Specifies that the sink uses upsert semantics based on the `KEY`. |
| `NAMESPACE` | The Iceberg namespace (database) containing the table. |
| `TABLE` | The name of the Iceberg table to write to. |
| `KEY` | **Required.** The columns that uniquely identify rows. Used to track updates and deletes. |
| `COMMIT INTERVAL` | **Required.** How frequently to commit snapshots to Iceberg. See [Commit interval tradeoffs](#commit-interval-tradeoffs) below. |

For the full list of syntax options, see the [`CREATE SINK` reference](/sql/create-sink/iceberg).

### Commit interval tradeoffs {#commit-interval-tradeoffs}

The `COMMIT INTERVAL` setting controls how frequently Materialize commits
snapshots to your Iceberg table. This involves tradeoffs:

| Shorter intervals (e.g., `10s`) | Longer intervals (e.g., `5m`) |
|---------------------------------|-------------------------------|
| Lower latency - data visible sooner in downstream systems | Higher latency - data takes longer to appear |
| More small files - can degrade query performance over time | Fewer, larger files - better query performance |
| More frequent snapshot commits - higher catalog overhead | Less catalog overhead |
| Lower throughput efficiency | Higher throughput efficiency |

The minimum commit interval is `1s`.

**Recommendations:**
- For real-time dashboards or alerting, use shorter intervals (`10s` to `1m`)
- For batch analytics, use longer intervals (`5m` to `15m`)
- If you notice query performance degradation from small files, increase the
  interval and consider running Iceberg compaction jobs

### Key selection

The `KEY` clause is required for all Iceberg sinks. The columns you specify must
uniquely identify rows in your source relation. Materialize uses these columns
to track updates and deletes.

Materialize validates that the key is unique. If it cannot prove uniqueness,
you'll receive an error. You can use `KEY (...) NOT ENFORCED` to bypass this
validation if you have outside knowledge that the key is unique. For more
details, see [Unique keys](/sql/create-sink/iceberg/#unique-keys).

## Querying Iceberg tables

Once your sink is running, you can query the Iceberg table from any system that
supports Iceberg:

- **Snowflake**: Use [Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- **Databricks**: Use the [Iceberg connector](https://docs.databricks.com/en/delta/clone-parquet.html)
- **Spark**: Use [Apache Iceberg for Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- **Trino/Presto**: Use the [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)

## Limitations

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

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
hood. See also the [`CREATE SINK` reference](/sql/create-sink/iceberg/#details)
for additional details.

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
