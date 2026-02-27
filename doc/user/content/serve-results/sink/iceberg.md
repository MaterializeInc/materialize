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
Iceberg](https://iceberg.apache.org/)[^1] tables hosted on [Amazon S3
Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html)[^2].
As data changes in Materialize, the corresponding Iceberg tables are
automatically kept up to date.  You can sink data from a materialized view, a
source, or a table.

This guide walks you through the steps required to set up Iceberg sinks in
Materialize Cloud.

[^1]: [Apache Iceberg](https://iceberg.apache.org/) is an open table format for
large-scale analytics datasets.

[^2]: [Amazon S3
Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) is
    an AWS feature that provides fully managed Apache Iceberg tables as a native
    S3 storage type.

## Prerequisites

- An AWS account with permissions to create and manage IAM policies and roles.
- An AWS S3 Table bucket in your AWS account. The S3 Table bucket must be in
  the same AWS region as your Materialize deployment.
- A namespace in the AWS S3 Table bucket. For details on creating namespaces,
  see [AWS S3 documentation: Creating a namespace](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-namespace-create.html).

## Step 1. Set up permissions in AWS

In AWS, set up permissions to allow Materialize to write data files to the
object storage backing your Iceberg catalog. This tutorial uses an IAM policy
and IAM role to grant the required permissions. We **strongly** recommend using
[role assumption-based authentication](/sql/create-connection/#aws-permissions)
to manage access.

### Step 1A. Create an IAM policy

Create an [IAM
policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that allows full access to your S3 Tables API.Replace `<S3 table bucket ARN>`
with the ARN of your S3 table bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3tables:*",
            "Resource": [
                "<S3 table bucket ARN>"
                "<S3 table bucket ARN>/table/*"
            ]
        }
    ]
}
```

### Step 1B. Create an IAM role

Create an [IAM
role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) that
Materialize can assume.

1. For the **Trusted entity type**, specify **Custom trust policy** with the
following:
    - `Principal`: The example uses the [Materialize Cloud IAM
      principal](/sql/create-connection/#aws-permissions). For self-managed
      deployments and the Emulator, the principal will differ.
    - `ExternalId`: `"PENDING"` is a placeholder and will be updated after
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

1. For permissions, add the [IAM policy created
   earlier](#step-1a-create-an-iam-policy) to grant access to the S3 Tables.

Once you have created the IAM role, copy the role ARN from the AWS console.
You'll use the ARN in the next step.

## Step 2. Create an AWS connection in Materialize

In Materialize, create an **AWS connection** to authenticate with the object
storage:

1. Use [`CREATE CONNECTION ... TO AWS`](/sql/create-connection/#aws) to create
   an AWS connection, replacing:

   - `<IAM role ARN>` with your IAM role ARN from [step
     1](#step-1b-create-an-iam-role)
   - `<region>` with your AWS region (e.g., `us-east-1`):

    ```mzsql
    CREATE CONNECTION aws_connection TO AWS (
        ASSUME ROLE ARN = '<IAM role ARN>',
        REGION = '<region>'
    );
    ```

    For more details on AWS connection options, see [`CREATE
    CONNECTION`](/sql/create-connection/#aws).

1. Fetch the `external_id` for your connection, replacing `<IAM role ARN>` with
    your IAM role ARN:

   ```mzsql
   SELECT external_id
   FROM mz_internal.mz_aws_connections awsc
   JOIN mz_connections c ON awsc.id = c.id
   WHERE c.name = 'aws_connection'
   AND awsc.assume_role_arn = '<iam-role-arn>';
   ```

   You will use the `external_id` to update the IAM role in the next step.

## Step 3. Update the IAM role in AWS

Once you have the `external_id`, update the trust policy for the IAM role
created in [step 1](#step-1b-create-an-iam-role). Replace `"PENDING"` with your
external ID value. Your IAM trust policy should look like the following (but
with your external ID value):

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

In Materialize, create an **Iceberg catalog connection** for the Iceberg sink to
use. To create, use [`CREATE CONNECTION ... TO ICEBERG
CATALOG`](/sql/create-connection/#iceberg-catalog), replacing:
- `<region>` with your AWS region (e.g., `us-east-1`) and
- `<S3 table bucket ARN>` with your AWS S3 Table bucket ARN.

The command uses the AWS connection you created earlier.

```mzsql
CREATE CONNECTION iceberg_catalog_connection TO ICEBERG CATALOG (
    CATALOG TYPE = 's3tablesrest',
    URL = 'https://s3tables.<region>.amazonaws.com/iceberg',
    WAREHOUSE = '<S3 table bucket ARN>',
    AWS CONNECTION = aws_connection
);
```

## Step 5. Create the Iceberg sink in Materialize

In Materialize, you can sink from a materialized view, table, or source. Use
[`CREATE SINK`](/sql/create-sink/iceberg) to create an Iceberg sink, replacing:

- `<sink_name>` with a name for your sink.
- `<sink_cluster>` with the name of your sink cluster.
- `<my_materialize_object>` with the name of your materialized view, table, or source.
- `<my_s3_table_bucket_namespace>` with your S3 Table bucket namespace.
- `<my_iceberg_table>` with the name of your Iceberg table. If the Iceberg table
  does not exist, Materialize creates the table. For details, see [`CREATE SINK`
  reference page](/sql/create-sink/iceberg/#iceberg-table-creation).
- `<key>` with the column(s) that uniquely identify rows.
- `<commit_interval>` with your commit interval (e.g., `60s`). The commit
  interval specifies how frequently Materialize commits snapshots to Iceberg.
  The minimum commit interval is `1s`. See [Commit interval
  tradeoffs](#commit-interval-tradeoffs) below.

```mzsql
CREATE SINK <sink_name>
  IN CLUSTER <sink_cluster>
  FROM <my_materialize_object>
  INTO ICEBERG CATALOG CONNECTION iceberg_catalog_connection (
    NAMESPACE = '<my_s3_table_bucket_namespace>',
    TABLE = '<my_iceberg_table>'
  )
  USING AWS CONNECTION aws_connection
  KEY (<key>)
  MODE UPSERT
  WITH (COMMIT INTERVAL = '<commit_interval>');
```

For the full list of syntax options, see the [`CREATE SINK` reference](/sql/create-sink/iceberg).

## Considerations

### Commit interval tradeoffs {#commit-interval-tradeoffs}

The `COMMIT INTERVAL` setting controls how frequently Materialize commits
snapshots to your Iceberg table, making the data available to downstream query
engines. This setting involves tradeoffs:

| Shorter intervals (e.g., < `60s`) | Longer intervals (e.g., `5m`) |
|---------------------------------|-------------------------------|
| Lower latency - data visible sooner in downstream systems | Higher latency - data takes longer to appear |
| More small files - can degrade query performance over time | Fewer, larger files - better query performance |
| More frequent snapshot commits - higher catalog overhead | Less catalog overhead |
| Lower throughput efficiency | Higher throughput efficiency |

**Recommendations:**
- For production, use intervals of `60s` or longer
- For batch analytics, use longer intervals (`5m` to `15m`)

{{< note >}}
Outside of development environments, commit intervals should be at least `60s`.
Short commit intervals increase catalog overhead and produce many small files.
Small files will result in degraded query performance. It also increases load on
the Iceberg metadata, which can result in a degraded catalog, and non-responsive
queries.
{{< /note >}}

### Exactly-once delivery

{{< include-from-yaml data="examples/create_sink_iceberg"
name="exactly-once-delivery" >}}

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Limitations

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
