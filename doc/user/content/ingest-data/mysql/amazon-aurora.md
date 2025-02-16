---
title: "Ingest data from Amazon Aurora"
description: "How to stream data from Amazon Aurora for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Amazon Aurora"
    identifier: "mysql-amazon-aurora"
---

This page shows you how to stream data from [Amazon Aurora MySQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% mysql-direct/before-you-begin %}}

## A. Configure Amazon Aurora

### 1. Enable GTID-based binlog replication

{{< note >}}
GTID-based replication is supported for Amazon Aurora MySQL v2 and v3, as well
as Aurora Serverless v2.
{{</ note >}}

Before creating a source in Materialize, you **must** configure Amazon Aurora
MySQL for GTID-based binlog replication. This requires enabling binlog replication and
the following additional configuration changes:

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`log_bin`                        | `ON`   |
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   | In the AWS console, this parameter appears as `gtid-mode`.
`enforce_gtid_consistency`       | `ON`   |
`binlog retention hours`         | 168    |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

For guidance on enabling GTID-based binlog replication in Aurora, see the
[Amazon Aurora MySQL documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/mysql-replication-gtid.html).

### 2. Create a user for replication

{{% mysql-direct/create-a-user-for-replication %}}

## B. Configure network security

{{% self-managed/network-connection %}}

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your MySQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% mysql-direct/create-a-cluster %}}

### 2. Start ingesting data

{{% mysql-direct/ingesting-data/allow-materialize-ips %}}

### 3. Monitor the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## Next steps

{{% mysql-direct/next-steps %}}
