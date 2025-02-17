---
title: "Ingest data from Google Cloud SQL"
description: "How to stream data from Google Cloud SQL for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Google Cloud SQL"
    identifier: "mysql-google-cloudsql"
---

This page shows you how to stream data from [Google Cloud SQL for MySQL](https://cloud.google.com/sql/MySQL)
to Materialize using the[MySQL source](/sql/create-source/mysql/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% mysql-direct/before-you-begin %}}

## A. Configure Google Cloud SQL

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure Google Cloud SQL
for MySQL for GTID-based binlog replication. This requires the following
configuration changes:

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`log_bin`                        | `ON`   |
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   |
`enforce_gtid_consistency`       | `ON`   |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

For guidance on enabling GTID-based binlog replication in Cloud SQL, see the [Cloud SQL documentation](https://cloud.google.com/sql/docs/mysql/replication).

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
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% mysql-direct/create-a-cluster %}}

### 2. Start ingesting data

{{% mysql-direct/ingesting-data/allow-materialize-ips %}}

### 3. Monitor the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## D. Explore your data

{{% mysql-direct/next-steps %}}

## Considerations

{{% include-md file="shared-content/mysql-considerations.md" %}}
