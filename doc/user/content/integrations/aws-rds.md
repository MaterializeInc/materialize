---
title: "How to connect AWS RDS to Materialize"
description: "How to connect Amazon RDS managed PostgreSQL as a source to Materialize"
menu:
  main:
    parent: "integration-guides"
    name: "AWS RDS PostgreSQL"
---

To connect AWS RDS PostgreSQL as a [Postgres Source](/sql/create-source/postgres/), the AWS user account requires the `rds_superuser` role to perform logical replication for the PostgreSQL database on Amazon RDS.

As an account with the `rds_superuser` role, make these changes to the upstream database:

1. Create a custom RDS parameter group with your instance and associate it with your instance. You will not be able to set custom parameters on the default RDS parameter groups.

1. In the custom RDS parameter group, set the `rds.logical_replication` static parameter to `1`.

1. The Materialize instance will need access to connect to the upstream database. This is usually controlled by IP address. If you are hosting your own installation of Materialize, add the instance's IP address in the security group for the RDS instance.

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

For more information, see the [Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication) documentation.

## Related pages

- [Change Data Capture (Postgres)](../cdc-postgres/)
- [`CREATE SOURCE FROM POSTGRES`](/sql/create-source/postgres/)
