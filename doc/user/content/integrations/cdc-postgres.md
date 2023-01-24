---
title: "PostgreSQL CDC"
description: "How to propagate Change Data Capture (CDC) data from a PostgreSQL database to Materialize"
aliases:
  - /guides/cdc-postgres/
menu:
  main:
    parent: "integration-guides"
    name: "PostgreSQL CDC"
    weight: 20
---

Change Data Capture (CDC) allows you to track and propagate changes in a Postgres database to downstream consumers based on its Write-Ahead Log (WAL). In this guide, we'll cover how to use Materialize to create and efficiently maintain real-time materialized views on top of CDC data.

There are two ways to connect Materialize to a Postgres database for CDC:

* Using the direct [Postgres source](#direct-postgres-source)

* Using [Kafka + Debezium](#kafka--debezium)

## Direct Postgres source

If Kafka is not part of your stack, you can use the [Postgres source](/sql/create-source/postgres) to connect directly to Materialize. This source uses Postgres’ native replication protocol to continually ingest changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the upstream database.

### Database setup

**Minimum requirements:** PostgreSQL 10+

Before creating a source in Materialize, you need to ensure that the upstream database is configured to support [logical replication](https://www.postgresql.org/docs/10/logical-replication.html).

{{< tabs >}}
{{< tab "Self-hosted">}}

As a _superuser_:

1. Check the [`wal_level` configuration](https://www.postgresql.org/docs/current/wal-configuration.html) setting:

    ```sql
    SHOW wal_level;
    ```

    The default value is `replica`. For CDC, you'll need to set it to `logical` in the database configuration file (`postgresql.conf`). Keep in mind that changing the `wal_level` requires a restart of the Postgres instance and can affect database performance.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "AWS RDS">}}

We recommend following the [AWS RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication) documentation for detailed information on logical replication configuration and best practices.

As a _superuser_ (`rds_superuser`):

1. Create a custom RDS parameter group and associate it with your instance. You will not be able to set custom parameters on the default RDS parameter groups.

1. In the custom RDS parameter group, set the `rds.logical_replication` static parameter to `1`.

1. Add the egress IP addresses associated with your Materialize region to the security group of the RDS instance. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "AWS Aurora">}}

{{< note >}}
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations) logical replication, so it's not possible to use this service with Materialize.
{{</ note >}}

We recommend following the [AWS Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure) documentation for detailed information on logical replication configuration and best practices.

As a _superuser_:

1. Create a DB cluster parameter group for your instance using the following settings:

    Set **Parameter group family** to your version of Aurora PostgreSQL.

    Set **Type** to **DB Cluster Parameter Group**.

1. In the DB cluster parameter group, set the `rds.logical_replication` static parameter to `1`.

1. In the DB cluster parameter group, set reasonable values for `max_replication_slots`, `max_wal_senders`, `max_logical_replication_workers`, and `max_worker_processes parameters`  based on your expected usage.

1. Add the egress IP addresses associated with your Materialize region to the security group of the DB instance. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "Azure DB">}}

We recommend following the [Azure DB for PostgreSQL](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical#pre-requisites-for-logical-replication-and-logical-decoding) documentation for detailed information on logical replication configuration and best practices.

1. In the Azure portal, or using the Azure CLI, [enable logical replication](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#set-up-your-server) for the PostgreSQL instance.

1. Add the egress IP addresses associated with your Materialize region to the list of allowed IP addresses under the "Connections security" menu. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "Cloud SQL">}}

We recommend following the [Cloud SQL for PostgreSQL](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance) documentation for detailed information on logical replication configuration and best practices.

As a _superuser_ (`cloudsqlsuperuser`):

1. In the Google Cloud Console, enable logical replication by setting the `cloudsql.logical_decoding` configuration parameter to `on`.

1. Add the egress IP addresses associated with your Materialize region to the list of allowed IP addresses. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< /tabs >}}

Once logical replication is enabled:

1. Grant the required privileges to the replication user:

    ```sql
    ALTER ROLE "dbuser" WITH REPLICATION;
    ```

    And ensure that this user has the right permissions on the tables you want to replicate:

    ```sql
    GRANT CONNECT ON DATABASE dbname TO dbuser;
    GRANT USAGE ON SCHEMA schema TO dbuser;
    GRANT SELECT ON ALL TABLES IN SCHEMA schema TO dbuser;
    ```

    **Note:** `SELECT` privileges on the tables you want to replicate are needed for the initial table sync.

1. Set the replica identity to `FULL` for the tables you want to replicate:

    ```sql
    ALTER TABLE repl_table REPLICA IDENTITY FULL;
    ```

    This setting determines the amount of information that is written to the WAL in `UPDATE` and `DELETE` operations.

    As a heads-up, you should expect a performance hit in the database from increased CPU usage. For more information, see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html).

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

### Create a source

Postgres sources ingest the raw replication stream data for all tables included in a publication to avoid creating multiple replication slots and minimize the required bandwidth.

When you define a Postgres source, Materialize will automatically create a **subsource** for each original table in the publication (so you don't have to!):

_Create subsources for all tables included in the Postgres publication_

```sql
CREATE SOURCE mz_source
    FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
    FOR ALL TABLES
    WITH (SIZE = '3xsmall');
```

_Create subsources for specific tables included in the Postgres publication_

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (table_1, table_2 AS alias_table_2)
  WITH (SIZE = '3xsmall');
```

{{< note >}}
Materialize performs an initial sync of all tables in the publication before it starts ingesting change events.
{{</ note >}}

### Create a materialized view

Any materialized view that depends on replication subsources will be incrementally updated as change events stream in, as a result of `INSERT`, `UPDATE` and `DELETE` operations in the original Postgres database.

```sql
CREATE MATERIALIZED VIEW cnt_view1 AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM view1
    GROUP BY field1;
```

## Kafka + Debezium

If Kafka is part of your stack, you can use [Debezium](https://debezium.io/) and the [Kafka source](/sql/create-source/kafka/#using-debezium) to propagate CDC data from Postgres to Materialize. Debezium captures row-level changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the upstream database and publishes them as events to Kafka using Kafka Connect-compatible connectors.

### Database setup

**Minimum requirements:** PostgreSQL 10+

Before deploying a Debezium connector, you need to ensure that the upstream database is configured to support [logical replication](https://www.postgresql.org/docs/10/logical-replication.html).

{{< tabs >}}
{{< tab "Self-hosted">}}

As a _superuser_:

1. Check the [`wal_level` configuration](https://www.postgresql.org/docs/current/wal-configuration.html) setting:

    ```sql
    SHOW wal_level;
    ```

    The default value is `replica`. For CDC, you'll need to set it to `logical` in the database configuration file (`postgresql.conf`). Keep in mind that changing the `wal_level` requires a restart of the Postgres instance and can affect database performance.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "AWS RDS">}}

We recommend following the [AWS RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication) documentation for detailed information on logical replication configuration and best practices.

As a _superuser_ (`rds_superuser`):

1. Create a custom RDS parameter group and associate it with your instance. You will not be able to set custom parameters on the default RDS parameter groups.

1. In the custom RDS parameter group, set the `rds.logical_replication` static parameter to `1`.

1. Add the egress IP addresses associated with your Materialize region to the security group of the RDS instance. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "AWS Aurora">}}

{{< note >}}
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations) logical replication, so it's not possible to use this service with Materialize.
{{</ note >}}

We recommend following the [AWS Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure) documentation for detailed information on logical replication configuration and best practices.

As a _superuser_:

1. Create a DB cluster parameter group for your instance using the following settings:

    Set **Parameter group family** to your version of Aurora PostgreSQL.

    Set **Type** to **DB Cluster Parameter Group**.

1. In the DB cluster parameter group, set the `rds.logical_replication` static parameter to `1`.

1. In the DB cluster parameter group, set reasonable values for `max_replication_slots`, `max_wal_senders`, `max_logical_replication_workers`, and `max_worker_processes parameters`  based on your expected usage.

1. Add the egress IP addresses associated with your Materialize region to the security group of the DB instance. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "Azure DB">}}

We recommend following the [Azure DB for PostgreSQL](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical#pre-requisites-for-logical-replication-and-logical-decoding) documentation for detailed information on logical replication configuration and best practices.

1. In the Azure portal, or using the Azure CLI, [enable logical replication](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#set-up-your-server) for the PostgreSQL instance.

1. Add the egress IP addresses associated with your Materialize region to the list of allowed IP addresses under the "Connections security" menu. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< tab "Cloud SQL">}}

We recommend following the [Cloud SQL for PostgreSQL](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance) documentation for detailed information on logical replication configuration and best practices.

As a _superuser_ (`cloudsqlsuperuser`):

1. In the Google Cloud Console, enable logical replication by setting the `cloudsql.logical_decoding` configuration parameter to `on`.

1. Add the egress IP addresses associated with your Materialize region to the list of allowed IP addresses. You can find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

{{< /tab >}}

{{< /tabs >}}

Once logical replication is enabled:

1. Grant enough privileges to ensure Debezium can operate in the database. The specific privileges will depend on how much control you want to give to the replication user, so we recommend following the [Debezium documentation](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-replication-user-privileges).

1. If a table that you want to replicate has a **primary key** defined, you can use your default replica identity value. If a table you want to replicate has **no primary key** defined, you must set the replica identity value to `FULL`:

    ```sql
    ALTER TABLE repl_table REPLICA IDENTITY FULL;
    ```

    This setting determines the amount of information that is written to the WAL in `UPDATE` and `DELETE` operations. Setting it to `FULL` will include the previous values of all the table’s columns in the change events.

    As a heads up, you should expect a performance hit in the database from increased CPU usage. For more information, see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html).

### Deploy Debezium

**Minimum requirements:** Debezium 1.5+

Debezium is deployed as a set of Kafka Connect-compatible connectors, so you first need to define a Postgres connector configuration and then start the connector by adding it to Kafka Connect.

{{< warning >}}
If you deploy the PostgreSQL Debezium connector in [Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-debezium.html), you **must** override the default value of `After-state only` to `false`.
{{</ warning >}}

1. Create a connector configuration file and save it as `register-postgres.json`:

    ```json
    {
        "name": "your-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name":"pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname" : "postgres",
            "database.server.name": "pg_repl",
            "table.include.list": "public.table1",
            "publication.autocreate.mode":"filtered",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schemas.enable": false
        }
    }
    ```

    You can read more about each configuration property in the [Debezium documentation](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-connector-properties). By default, the connector writes events for each table to a Kafka topic named `serverName.schemaName.tableName`.


1. Start the Debezium Postgres connector using the configuration file:

    ```bash
    export CURRENT_HOST='<your-host>'

    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-postgres.json
    ```

1. Check that the connector is running:

    ```bash
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```

    The first time it connects to a Postgres server, Debezium takes a [consistent snapshot](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-snapshots) of the tables selected for replication, so you should see that the pre-existing records in the replicated table are initially pushed into your Kafka topic:

    ```bash
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic pg_repl.public.table1
    ```

### Create a source

{{< debezium-json >}}

Debezium emits change events using an envelope that contains detailed information about upstream database operations, like the `before` and `after` values for each record. To create a source that interprets the [Debezium envelope](/sql/create-source/kafka/#using-debezium) in Materialize:

```sql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'pg_repl.public.table1')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM
    WITH (SIZE = '3xsmall');
```

This allows you to replicate tables with `REPLICA IDENTITY DEFAULT`, `INDEX`, or `FULL`.

#### Transaction support

Debezium provides [transaction metadata](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-transaction-metadata) that can be used to preserve transactional boundaries downstream. We are working on using this topic to support transaction-aware processing in Materialize ([#7537](https://github.com/MaterializeInc/materialize/issues/7537))!

### Create a materialized view

Any materialized view defined on top of this source will be incrementally updated as new change events stream in through Kafka, as a result of `INSERT`, `UPDATE` and `DELETE` operations in the original Postgres database.

```sql
CREATE MATERIALIZED VIEW cnt_table1 AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM kafka_repl
    GROUP BY field1;
```
