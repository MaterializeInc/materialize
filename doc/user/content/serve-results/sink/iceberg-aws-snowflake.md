---
title: "Consume from Snowflake on AWS S3 Tables"
description: "How to query Materialize's Iceberg tables on Amazon S3 Tables from Snowflake."
menu:
  main:
    parent: sink-iceberg
    name: "Snowflake on AWS S3 Tables"
    weight: 30
---

Once Materialize writes to an Iceberg table on [Amazon S3
Tables](/serve-results/sink/iceberg-aws/), Snowflake can query that table
directly through a [catalog
integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration).
Materialize and Snowflake never communicate with each other: both are clients of
the Iceberg catalog, which holds the only shared state. Materialize commits
snapshots to the catalog, and Snowflake polls the catalog for new snapshots.

The steps in this guide are specific to Iceberg tables hosted on Amazon S3
Tables. The catalog integration type, authentication method, and IAM permissions
differ for Iceberg tables hosted elsewhere.

{{< important >}}
Create the sink with `MODE APPEND`. Snowflake cannot read Iceberg equality
delete files, which `MODE UPSERT` writes whenever a row from an earlier snapshot
is updated or deleted. For details, see [Sink mode requirement for
Snowflake](#sink-mode-requirement-for-snowflake).
{{< /important >}}

## Prerequisites

- An Iceberg sink writing to Amazon S3 Tables, set up by following the [AWS S3
  Tables guide](/serve-results/sink/iceberg-aws/), created with `MODE APPEND`.

- An AWS account with permissions to create and manage IAM policies and roles.
  This is a second IAM role, separate from the one Materialize assumes to write.

- A Snowflake account, and a role with the [global `CREATE INTEGRATION`
  privilege](https://docs.snowflake.com/en/user-guide/security-access-control-privileges#global-privileges-account-level-privileges)
  (an account-level privilege, typically held by
  [`ACCOUNTADMIN`](https://docs.snowflake.com/en/user-guide/security-access-control-considerations#using-the-accountadmin-role))
  and the `CREATE DATABASE` privilege.

## Sink mode requirement for Snowflake

Snowflake's support for Iceberg tables it does not manage itself excludes
equality delete files: [row-level deletes with equality delete files aren't
supported](https://docs.snowflake.com/en/user-guide/tables-iceberg). This
determines which sink mode you can use:

| Sink mode | Delete files written | Readable by Snowflake |
| --- | --- | --- |
| `MODE APPEND` | None. Changes are data rows tagged with `_mz_diff` and `_mz_timestamp`. | Yes |
| `MODE UPSERT` | Equality deletes, for any update or delete against a row from an earlier snapshot. | No |

With `MODE UPSERT`, Snowflake fails in one of two ways depending on when the
table was registered:

- If the table was registered **before** the first equality delete was written,
  Snowflake stops refreshing the table and continues serving the last snapshot
  it could read. Queries succeed and return stale data without reporting an
  error. Once refresh stops, no further changes appear, including inserts that
  involve no delete files at all.

- If the table was registered **after** equality deletes exist, Snowflake never
  reads any snapshot, and queries fail with `091968 (0A000): Equality deletes on
  Iceberg tables are not supported.`

Materialize reports an upsert sink in this state as `running` with no error,
because Materialize is writing valid Iceberg. The incompatibility is entirely on
the read side. To detect it, monitor from Snowflake, as described in [Monitor Snowflake's refresh
state](#monitor-snowflakes-refresh-state).

## Step 1. Create an IAM role for Snowflake

Create an [IAM
policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)
that grants read access to your S3 Tables catalog, replacing `<S3 table bucket
ARN>` with the ARN of your S3 table bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3tables:GetTableBucket",
                "s3tables:ListNamespaces",
                "s3tables:GetNamespace",
                "s3tables:ListTables",
                "s3tables:GetTable",
                "s3tables:GetTableData",
                "s3tables:GetTableMetadataLocation"
            ],
            "Resource": [
                "<S3 table bucket ARN>",
                "<S3 table bucket ARN>/table/*"
            ]
        }
    ]
}
```

Then create an [IAM
role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) that
Snowflake can assume, and attach the policy to it. For the **Trusted entity
type**, specify **Custom trust policy**. Both the principal and the external ID
are placeholders at this stage: Snowflake generates the real values when you
create the catalog integration in the next step.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<your account ID>:root"
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

Note down the role ARN. You will use it in the next step.

## Step 2. Create the catalog integration in Snowflake

In Snowflake, create a [catalog
integration](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest)
for the S3 Tables Iceberg REST endpoint, replacing:

- `<region>` with the AWS region of your S3 table bucket (for example,
  `us-east-1`),
- `<S3 table bucket ARN>` with your S3 table bucket ARN, and
- `<Snowflake IAM role ARN>` with the role ARN from [step
  1](#step-1-create-an-iam-role-for-snowflake).

```sql
CREATE CATALOG INTEGRATION mz_s3tables_catalog
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  REST_CONFIG = (
    CATALOG_URI = 'https://s3tables.<region>.amazonaws.com/iceberg'
    CATALOG_API_TYPE = AWS_S3TABLES
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
    CATALOG_NAME = '<S3 table bucket ARN>'
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = '<Snowflake IAM role ARN>'
    SIGV4_SIGNING_REGION = '<region>'
  )
  ENABLED = TRUE;
```

`ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS` means S3 Tables issues Snowflake
scoped credentials for the underlying storage, so no [external
volume](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume)
is required.

Next, retrieve the IAM principal Snowflake uses to assume your role:

```sql
DESCRIBE CATALOG INTEGRATION mz_s3tables_catalog;
```

Note down the values of the `API_AWS_IAM_USER_ARN` and `API_AWS_EXTERNAL_ID`
properties. You will use them in the next step.

## Step 3. Update the IAM role's trust policy in AWS

In AWS, edit the trust policy of the IAM role you created in [step
1](#step-1-create-an-iam-role-for-snowflake), replacing the placeholder
principal and external ID with the `API_AWS_IAM_USER_ARN` and
`API_AWS_EXTERNAL_ID` values from the previous step:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::781425929845:user/xxxxxxxx-x"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "XXXXXXXX_SFCRole=NN_xxxxxxxxxxxxxxxxxxxxxxxxxxxx="
                }
            }
        }
    ]
}
```

## Step 4. Create a catalog-linked database in Snowflake

A [catalog-linked
database](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)
discovers the namespaces and tables in your Iceberg catalog and keeps them
registered as they change, including tables Materialize creates later. Replace
`<namespace>` with the namespace your sink writes to:

```sql
CREATE DATABASE mz_iceberg
  LINKED_CATALOG = (
    CATALOG = 'mz_s3tables_catalog',
    ALLOWED_NAMESPACES = ('<namespace>'),
    SYNC_INTERVAL_SECONDS = 30
  );
```

Discovery takes up to one `SYNC_INTERVAL_SECONDS` cycle. To confirm the tables
have been registered:

```sql
SHOW ICEBERG TABLES IN DATABASE mz_iceberg;
```

Each table your sink writes to should appear with `iceberg_table_type` set to
`UNMANAGED`.

## Step 5. Reconstruct current state in Snowflake

{{% include-headless "/headless/iceberg-sinks/append-mode-current-state" %}}

In Snowflake, quote the column and table names as lowercase. Snowflake resolves
unquoted identifiers as uppercase, which will not match the identifiers
Materialize created.

{{< tabs >}}
{{< tab "Consolidate by diff">}}

Group by every column, and keep the groups whose `_mz_diff` values sum to a
positive number:

```sql
SELECT "id", "name", "qty"
  FROM mz_iceberg."<namespace>"."<table>"
 GROUP BY "id", "name", "qty", "price", "updated_at"
HAVING SUM("_mz_diff") > 0;
```

Every column of the table must appear in the `GROUP BY` clause, including the
columns you do not select. Grouping on a subset would merge rows that differ in
the omitted columns and produce incorrect results.

{{< /tab >}}

{{< tab "Latest version per key">}}

Rank the rows within each key, then keep the top-ranked row for each key where
`_mz_diff` is `+1`, replacing `"id"` with the unique key of your relation:

```sql
SELECT "id", "name", "qty"
  FROM (
    SELECT "id", "name", "qty", "_mz_diff",
           ROW_NUMBER() OVER (
             PARTITION BY "id"
             ORDER BY "_mz_timestamp" DESC, "_mz_diff" DESC) AS rn
      FROM mz_iceberg."<namespace>"."<table>"
  )
 WHERE rn = 1 AND "_mz_diff" = 1;
```

Ordering by `"_mz_timestamp" DESC, "_mz_diff" DESC` selects the new version of
an updated row, and the `"_mz_diff" = 1` filter removes keys whose most recent
change was a deletion.

{{< /tab >}}

{{< /tabs >}}

To define current state once and query it by name, wrap either query in a view.
See [Query cost of the changelog in
Snowflake](#query-cost-of-the-changelog-in-snowflake) for how to bound the cost
of doing so as the changelog grows.

To verify the pipeline end to end, compare the result against the same relation
in Materialize. Compare values rather than row counts alone: a table that has
stopped refreshing can hold a plausible number of rows whose values are stale.

## Considerations

### End-to-end latency from Materialize to Snowflake

Changes become visible in Snowflake in batches, not continuously. Two intervals
compose:

- The sink's `COMMIT INTERVAL` determines when a new Iceberg snapshot exists at
  all. Every change written within one interval becomes visible at the same
  moment.
- The catalog-linked database's `SYNC_INTERVAL_SECONDS` (default `30`)
  determines how soon after that Snowflake notices the new snapshot.

With `COMMIT INTERVAL = '1m'` and the default sync interval, a row's visibility
delay therefore depends on where in the commit window it was written, ranging
from a few seconds to roughly the sum of both intervals.

Lowering `COMMIT INTERVAL` reduces latency at a cost:

{{% include-headless "/headless/iceberg-sinks/commit-interval-tradeoffs" %}}

### Query cost of the changelog in Snowflake

The changelog grows with every change, so the query in [step
5](#step-5-reconstruct-current-state-in-snowflake) reads more data over
time. Defining it as a view keeps it always current but recomputes the full
aggregation on each query. To bound query cost instead, materialize the result
as a [dynamic
table](https://docs.snowflake.com/en/user-guide/dynamic-tables/create-iceberg),
which adds its own refresh lag and compute cost. Dynamic tables track changes to
externally managed Iceberg tables at file level, which suits an append-only
source because appends add files without rewriting existing ones.

### Monitor Snowflake's refresh state

A query against a table that has stopped refreshing succeeds and returns the
last snapshot Snowflake could read, so query results do not indicate whether
refresh is healthy. Materialize's sink status does not either. Check Snowflake's
refresh state instead:

```sql
SHOW ICEBERG TABLES IN DATABASE mz_iceberg;
```

In the `auto_refresh_status` column, `executionState` is `RUNNING` when refresh
is healthy. Any other state includes an `invalidExecutionStateReason` explaining
why refresh stopped, along with the metadata file and snapshot ID that failed.
Snowflake also provides
[`SYSTEM$AUTO_REFRESH_STATUS`](https://docs.snowflake.com/en/user-guide/tables-iceberg-auto-refresh)
and
[`ICEBERG_TABLE_SNAPSHOT_REFRESH_HISTORY`](https://docs.snowflake.com/en/sql-reference/functions/iceberg_table_snapshot_refresh_history)
for this purpose.

Automated refresh polls the catalog rather than relying on notifications, and is
billed as Snowpipe usage.

### AWS and Snowflake region placement

Your S3 table bucket must be in the same AWS region as your Materialize
deployment. Placing your Snowflake account in that region as well avoids
cross-region data transfer costs and added latency when Snowflake reads the
table's data files.

## Troubleshooting

### Snowflake queries return data older than expected

Refresh has most likely stopped. Check `executionState` as described in [Monitor Snowflake's refresh
state](#monitor-snowflakes-refresh-state). If
`invalidExecutionStateReason` reports unsupported equality deletes, the sink is
running in upsert mode; recreate it with `MODE APPEND`, as described in [Sink mode requirement for
Snowflake](#sink-mode-requirement-for-snowflake).

Forcing a refresh surfaces the same underlying error as a query error, which can
be useful when diagnosing:

```sql
ALTER ICEBERG TABLE mz_iceberg."<namespace>"."<table>" REFRESH;
```

### Snowflake reports that equality deletes are not supported

Snowflake cannot read the table because the sink writes equality delete files.
Recreating the catalog integration or the catalog-linked database does not
resolve this, because the delete files are in the table itself. Recreate the
sink with `MODE APPEND` writing to a new Iceberg table.

### Tables do not appear in the Snowflake catalog-linked database

Confirm that the namespace is listed in `ALLOWED_NAMESPACES`, that the sink has
committed at least once, and that at least one `SYNC_INTERVAL_SECONDS` cycle has
elapsed. If tables are still missing, verify the IAM role's trust policy matches
the `API_AWS_IAM_USER_ARN` and `API_AWS_EXTERNAL_ID` reported by `DESCRIBE
CATALOG INTEGRATION`.

## Related pages

- [AWS S3 Tables](/serve-results/sink/iceberg-aws/)
- [Apache Iceberg](/serve-results/sink/iceberg/)
- [`CREATE SINK`](/sql/create-sink/iceberg)
- [Snowflake](/serve-results/sink/snowflake/)
- [Snowflake documentation: Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
