---
title: "SQL Server CDC using Kafka and Debezium"
description: "How to propagate Change Data Capture (CDC) data from a SQL Server database to Materialize"
aliases:
  - /guides/cdc-sql-server/
  - /integrations/cdc-sql-server/
  - /connect-sources/cdc-sql-server/
menu:
  main:
    parent: "sql-server"
    name: "Using Kafka and Debezium"
    identifier: "sql-server-dbz"
    weight: 15
---

Change Data Capture (CDC) allows you to track and propagate changes in a SQL Server database to downstream consumers. In this guide, we’ll cover how to use Materialize to create and efficiently maintain real-time materialized views on top of CDC data.

## Kafka + Debezium

Utilize [Debezium](https://debezium.io/) and the [Kafka source](/sql/create-source/kafka/#using-debezium) to propagate CDC data from SQL Server to Materialize. Debezium captures row-level changes resulting from `INSERT`, `UPDATE`, and `DELETE` operations in the upstream database and publishes them as events to Kafka using Kafka Connect-compatible connectors.

### Database Setup

Before deploying a Debezium connector, ensure that the upstream database is configured to support CDC.

1.  Enable CDC on the SQL Server instance and database:

    ```sql
    -- Enable CDC on the SQL Server instance
    USE MyDB
    GO
    EXEC sys.sp_cdc_enable_db
    GO
    ```

### Deploy Debezium

**Minimum requirements:** Debezium 1.5+

Debezium is deployed as a set of Kafka Connect-compatible connectors. First, define a SQL Server connector configuration, then start the connector by adding it to Kafka Connect.

{{< tabs >}}
{{< tab "Debezium 1.5+">}}

1.  Create a connector configuration file and save it as `register-sqlserver.json`:

    ```json
    {
        "name": "inventory-connector",
        "config": {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "tasks.max": "1",
            "database.server.name": "server1",
            "database.hostname": "sqlserver",
            "database.port": "1433",
            "database.user": "sa",
            "database.password": "Password!",
            "database.names": "testDB",
            "database.history.kafka.bootstrap.servers": "<broker>:9092",
            "database.history.kafka.topic": "schema-changes.inventory",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://<schema-registry>:8081",
            "value.converter.schema.registry.url": "http://<schema-registry>:8081",
            "database.encrypt": "false"
        }
    }
    ```

    You can read more about each configuration property in the [Debezium documentation](https://debezium.io/documentation/reference/1.9/connectors/sqlserver.html).

{{< /tab >}}
{{< tab "Debezium 2.0+">}}

1. Beginning with Debezium 2.0.0, Confluent Schema Registry support is not included in the Debezium containers. To enable the Confluent Schema Registry for a Debezium container, install the following Confluent Avro converter JAR files into the Connect plugin directory:
    * `kafka-connect-avro-converter`
    * `kafka-connect-avro-data`
    * `kafka-avro-serializer`
    * `kafka-schema-serializer`
    * `kafka-schema-registry-client`
    * `common-config`
    * `common-utils`

1.  Create a connector configuration file and save it as `register-sqlserver.json`:

    ```json
    {
        "name": "inventory-connector",
        "config": {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "tasks.max": "1",
            "topic.prefix": "server1",
            "database.hostname": "sqlserver",
            "database.port": "1433",
            "database.user": "sa",
            "database.password": "Password!",
            "database.names": "testDB",
            "schema.history.internal.kafka.bootstrap.servers": "<broker>:9092",
            "schema.history.internal.kafka.topic": "schema-changes.inventory",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://<schema-registry>:8081",
            "value.converter.schema.registry.url": "http://<schema-registry>:8081",
            "database.encrypt": "false"
        }
    }
    ```

    You can read more about each configuration property in the [Debezium documentation](https://debezium.io/documentation/reference/2.4/connectors/sqlserver.html).

{{< /tab >}}
{{< /tabs >}}

2.  Start the Debezium SQL Server connector using the configuration file:

    ```sh
    export CURRENT_HOST='<your-host>'
    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-sqlserver.json
    ```

3.  Check that the connector is running:

    ```
    curl http://$CURRENT_HOST:8083/connectors/inventory-connector/status
    ```

Now, Debezium will capture changes from the SQL Server database and publish them to Kafka.

### Create a Source

Debezium emits change events using an envelope that contains detailed information about upstream database operations, like the `before` and `after` values for each record. To create a source that interprets the [Debezium envelope](/sql/create-source/kafka/#using-debezium) in Materialize:

```sql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'server1.testDB.tableName')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

#### Transaction Support

Debezium provides [transaction metadata](https://debezium.io/documentation/reference/connectors/sqlserver.html#sqlserver-transaction-metadata) that can be used to preserve transactional boundaries downstream. Work is in progress to utilize this topic to support transaction-aware processing in Materialize ([#7537](https://github.com/MaterializeInc/materialize/issues/7537))!

### Create a Materialized View

Any materialized view defined on top of this source will be incrementally updated as new change events stream in through Kafka, resulting from `INSERT`, `UPDATE`, and `DELETE` operations in the original SQL Server database.

```sql
CREATE MATERIALIZED VIEW cnt_table AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM kafka_repl
    GROUP BY field1;
```

## Known Limitations

The integration of Debezium with Microsoft SQL Server and Materialize presents a few known bugs that may affect the replication and processing of data. The tracking issue for these bugs is [#8054](https://github.com/MaterializeInc/materialize/issues/8054).

1.  **Incorrect Data Types Replication:**

    -   `DATETIMEOFFSET` values from SQL Server are replicated as `TEXT` in Materialize ([#8017](https://github.com/MaterializeInc/materialize/issues/8017)).
    -   `DATETIME2` values from SQL Server are replicated as `BIGINT` in Materialize ([#8041](https://github.com/MaterializeInc/materialize/issues/8041)).

2.  **Upstream Bug (DBZ-3915):**

    -   An upstream bug identified as [DBZ-3915](https://issues.redhat.com/browse/DBZ-3915) on RedHat’s issue tracker presents additional challenges.

3.  **Configuration Limitation:**

    -   An operational issue exists which requires halting all updates to the tables intended for replication to Materialize until Debezium is completely configured, all snapshots have been taken, and an additional duration of 10 seconds has elapsed post-configuration. This limitation is significant and may influence the decision to utilize SQL Server and Debezium for this purpose.
