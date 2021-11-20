---
title: "Postgres CDC in the cloud"
description: "How to configure Postgres CDC for instances hosted on cloud services"
weight:
aliases:
---

**Required:** PostgreSQL 10+

To use a PostgreSQL instance running on Amazon RDS, AWS Aurora, or Cloud SQL for [Change Data Capture](../cdc-postgres/), you need to ensure that the database is configured to support logical replication.

Note that enabling logical replication may affect performance.

## Amazon RDS

The AWS user account requires the `rds_superuser` role to perform logical replication for the PostgreSQL database on Amazon RDS.

As an account with the `rds_superuser` role, make these changes to the upstream database:

1. Create a custom RDS parameter group with your instance and associate it with your instance. You will not be able to set custom parameters on the default RDS parameter groups.

1. In the custom RDS parameter group, set the `rds.logical_replication` static parameter to `1`.

1. The Materialize replica will need access to connect to the upstream database. This is usually controlled by IP address. If you are hosting your own installation of Materialize, add the replica's IP address in the security group for the RDS instance.

    If you are using Materialize Cloud, you will need to make the RDS instance publicly accessible; Materialize Cloud instances do not have static IP addresses.

1. Restart the database so all changes can take effect.

1. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) with the tables you want to replicate:

    _For specific tables:_

    ```sql
    CREATE PUBLICATION mz_source FOR TABLE table1, table2;
    ```

    _For all tables in Postgres:_

    ```sql
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```

    The `mz_source` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream. We strongly recommend that you **limit** publications only to the tables you need.

For more information, see the [Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication) documentation.

## AWS Aurora

{{< note >}}
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations) logical replication, so it's not possible to use this configuration for Postgres CDC.
{{</ note >}}

As a superuser, make these changes to the upstream database:

1. Create a DB cluster parameter group for your instance. Use the following settings:

    Set **Parameter group family** to your version of Aurora PostgreSQL.

     Set **Type** to **DB Cluster Parameter Group**.

1. In the DB cluster parameter group, set the `rds.logical_replication` static parameter to `1`.

1. In the DB cluster parameter group, set `max_replication_slots`, `max_wal_senders`, `max_logical_replication_workers`, and `max_worker_processes parameters`  based on your expected usage.

    Parameter | Recommended Minimum Value
    ----------|--------------------------
    **max_replication_slots** | The combined number of logical replication publications and subscriptions you plan to create.
    **max_wal_senders** |  The number of logical replication slots that you intend to be active, or the number of active AWS DMS tasks for change data capture.
    **max_logical_replication_workers**  |  The number of logical replication slots that you intend to be active, or the number of active AWS DMS tasks for change data capture.
    **max_worker_processes**  | The combined values of `max_logical_replication_workers`, `autovacuum_max_workers`, and `max_parallel_workers`.

1. The Materialize replica will need access to connect to the upstream database. This is usually controlled by IP address. If you are hosting your own installation of Materialize, add the replica's IP address in the security group for the DB instance.

    If you are using Materialize Cloud, you will need to make the DB instance publicly accessible; Materialize Cloud instances do not have static IP addresses.

1. Restart the database so all changes can take effect.

1. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) with the tables you want to replicate:

    _For specific tables:_

    ```sql
    CREATE PUBLICATION mz_source FOR TABLE table1, table2;
    ```

    _For all tables in Postgres:_

    ```sql
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```

    The `mz_source` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream. We strongly recommend that you **limit** publications only to the tables you need.

For more information, see the [AWS Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure) documentation.

## Cloud SQL

As a user with the `cloudsqlsuperuser` role, make these changes to the upstream database:

1. In the Google Cloud Console, set the `cloudsql.logical_decoding` to `on`. This enables logical replication.

1. The Materialize replica will need access to connect to the upstream database. In the Google Cloud Console, enable access on the upstream database for the Materialize replica's IP address.

      If you are using Materialize Cloud, you will need to make the Cloud SQL instance publically accessible; Materialize Cloud instances do not have static IP addresses.

1. Restart the database to apply your changes.

1. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) with the tables you want to replicate:

    _For specific tables:_

    ```sql
    CREATE PUBLICATION mz_source FOR TABLE table1, table2;
    ```

    _For all tables in Postgres:_

    ```sql
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```

    The `mz_source` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream. We strongly recommend that you **limit** publications only to the tables you need.

For more information, see the [Cloud SQL](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance) documentation.

## Related pages

- [Change Data Capture with PostgreSQL](../cdc-postgres/)
- [`CREATE SOURCE FROM POSTGRES`](/sql/create-source/postgres/)
