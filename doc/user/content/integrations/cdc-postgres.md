---
title: "Materialized Views for PostgreSQL"
description: "How to create real-time materialized views from PostgreSQL changelog data with Materialize."
aliases:
  - /guides/cdc-postgres/
menu:
  main:
    parent: "integration-guides"
    name: "PostgreSQL"
    weight: 20
---

Change Data Capture (CDC) allows you to track and propagate changes in a Postgres database to downstream consumers based on its Write-Ahead Log (WAL). In this guide, we'll cover how to use Materialize to create and efficiently maintain real-time materialized views on top of CDC data.

There are two ways to connect Materialize to a Postgres database for CDC:

* Using the direct [Postgres source](#direct-postgres-source)

* Using [Kafka + Debezium](#kafka--debezium)

## Direct Postgres source

If Kafka is not part of your stack, you can use the [Postgres source](/sql/create-source/postgres) to connect directly to Materialize (v0.8.2+). This source uses Postgres’ native replication protocol to continually ingest changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the upstream database.

### Database setup

**Minimum requirements:** Postgres 10+

Before creating a source in Materialize, you need to ensure that the upstream database is configured to support [logical replication](https://www.postgresql.org/docs/10/logical-replication.html).

As a _superuser_:

1. Check the [`wal_level` configuration](https://www.postgresql.org/docs/current/wal-configuration.html) setting:

    ```sql
    SHOW wal_level;
    ```

    The default value is `replica`. For CDC, you'll need to set it to `logical` in the database configuration file (`postgresql.conf`). Keep in mind that changing the `wal_level` requires a restart of the Postgres instance and can affect database performance.

    **Note:** If you're using Postgres on a Cloud service like Amazon RDS, AWS Aurora, or Cloud SQL, you'll need to take some additional steps. For more information, see [Postgres in the cloud](/integrations/#postgresql).

1. Grant the required privileges to the replication user:

    ```sql
    ALTER ROLE "user" WITH REPLICATION;
    ```

    **Note:** This user also needs `SELECT` privileges on the tables you want to replicate, for the initial table sync.

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

Postgres sources ingest the raw replication stream data for all tables included in a publication to avoid creating multiple replication slots and minimize the required bandwidth. To create a source in Materialize:

```sql
CREATE SOURCE mz_source
    FROM POSTGRES
      CONNECTION 'host=example.com port=5432 user=host dbname=postgres sslmode=require'
      PUBLICATION 'mz_source';
```

{{< note >}}
Materialize performs an initial sync of all tables in the publication before it starts ingesting change events. You should expect increased disk usage during this phase.
{{</ note >}}

The next step is to break down this source into views that reproduce the publication’s original tables and can be used as a base for your materialized view.

#### Create replication views

Once you've created the Postgres source, you can create views that filter the replication stream and take care of converting its elements to the original data types:

_Create views for specific tables included in the Postgres publication_

```sql
CREATE VIEWS FROM SOURCE mz_source (table1, table2);
```

_Create views for all tables_

```sql
CREATE VIEWS FROM SOURCE mz_source;
```

Under the hood, Materialize parses this statement into view definitions for each table (so you don't have to!).

### Create a materialized view

Any materialized view defined on top of this source will be incrementally updated as new change events stream in, as a result of `INSERT`, `UPDATE` and `DELETE` operations in the original Postgres database.

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

**Minimum requirements:** Postgres 10+

Before deploying a Debezium connector, you need to ensure that the upstream database is configured to support [logical replication](https://www.postgresql.org/docs/10/logical-replication.html).

As a _superuser_:

1. Check the [`wal_level` configuration](https://www.postgresql.org/docs/current/wal-configuration.html) setting:

    ```sql
    SHOW wal_level;
    ```

    The default value is `replica`. For CDC, you'll need to set it to `logical` in the database configuration file (`postgresql.conf`). Keep in mind that changing the `wal_level` requires a restart of the Postgres instance and can affect database performance.

    **Note:** If you're using Postgres on a Cloud service like Amazon RDS, AWS Aurora, or Cloud SQL, you'll need to take some additional steps. For more information, see [Postgres Cloud Support Notes](/integrations/#postgresql).

1. Grant enough privileges to ensure Debezium can operate in the database. The specific privileges will depend on how much control you want to give to the replication user, so we recommend following the [Debezium documentation](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-replication-user-privileges).

1. If a table that you want to replicate has a **primary key** defined, you can use your default replica identity value. If a table you want to replicate has **no primary key** defined, you must set the replica identity value to `FULL`:

    ```sql
    ALTER TABLE repl_table REPLICA IDENTITY FULL;
    ```

    This setting determines the amount of information that is written to the WAL in `UPDATE` and `DELETE` operations. Setting it to `FULL` will include the previous values of all the table’s columns in the change events.

    As a heads up, you should expect a performance hit in the database from increased CPU usage. For more information, see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html).

### Deploy Debezium

Debezium is deployed as a set of Kafka Connect-compatible connectors, so you first need to define a Postgres connector configuration and then start the connector by adding it to Kafka Connect.

{{< debezium-warning >}}

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
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'pg_repl.public.table1'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM UPSERT;
```

Enabling `UPSERT` allows you to replicate tables with `REPLICA IDENTITY DEFAULT` or `INDEX`. Although this approach is less resource-intensive for the upstream database, it will require **more memory** in Materialize, as it needs to track state proportional to the number of unique primary keys in the changing data.

If the original Postgres table uses `REPLICA IDENTITY FULL`:

```sql
CREATE SOURCE kafka_repl
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'pg_repl.public.table1'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;
```

When should you use what? `UPSERT` works best when there is a small number of quickly changing rows and is required if log compaction is enabled for your Debezium topic; setting `REPLICA IDENTITY FULL` in the original tables and using the regular Debezium envelope works best for pretty much everything else.

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
