---
title: "Ingest data from Azure DB"
description: "How to stream data from Azure DB for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Azure DB"
    indentifier: "mysql-azure-db"
---

This page shows you how to stream data from [Azure DB for MySQL](https://azure.microsoft.com/en-us/products/MySQL)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% mysql-direct/before-you-begin %}}

## A. Configure Azure DB

### 1. Enable GTID-based binlog replication

{{< note >}}
GTID-based replication is supported for Azure DB for MySQL [flexible server](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/overview-single).
It is **not supported** for single server databases.
{{</ note >}}

Before creating a source in Materialize, you **must** configure Azure DB for
MySQL for GTID-based binlog replication. This requires enabling binlog replication and
the following additional configuration changes:

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`log_bin`                        | `ON`   |
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   |
`enforce_gtid_consistency`       | `ON`   |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

For guidance on enabling GTID-based binlog replication in Azure DB, see the
[Azure documentation](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/how-to-data-in-replication?tabs=shell%2Ccommand-line#configure-the-source-mysql-server).

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

[//]: # "TODO(morsapaes) MySQL connections support multiple SSL modes. We should
adapt to that, rather than just state SSL MODE REQUIRED."

{{% mysql-direct/ingesting-data/allow-materialize-ips %}}

[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available(also for PostgreSQL)."

### 3. Monitor the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## Next steps

{{% mysql-direct/next-steps %}}
