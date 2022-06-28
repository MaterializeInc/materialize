---
title: "How to connect AWS Aurora to Materialize"
description: "How to connect AWS Aurora PostgreSQL as a source to Materialize."
menu:
  main:
    parent: "integration-guides"
    name: "AWS Aurora"
---

{{< note >}}
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations) logical replication, so it's not possible to use this configuration for Postgres CDC.
{{</ note >}}

To connect AWS Aurora as a [Postgres Source](/sql/create-source/postgres/), as a superuser, make these changes to the upstream database:

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

1. The Materialize instance will need access to connect to the upstream database. This is usually controlled by IP address. If you are hosting your own installation of Materialize, add the instance's IP address in the security group for the DB instance.

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

    The `mz_source` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream.

For more information, see the [AWS Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure) documentation.

## Related pages

- [Change Data Capture (Postgres)](../cdc-postgres/)
- [`CREATE SOURCE FROM POSTGRES`](/sql/create-source/postgres/)
