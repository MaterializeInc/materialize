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

Change Data Capture (CDC) allows you to track and propagate changes in a SQL
Server database to downstream consumers. In this guide, we’ll cover how to use
Materialize to create and efficiently maintain real-time views with
incrementally updated results on top of CDC data.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Kafka + Debezium

Use [Debezium](https://debezium.io/) and the [Kafka source](/sql/create-source/kafka/#using-debezium)
to propagate CDC data from SQL Server to Materialize. Debezium captures
row-level changes resulting from `INSERT`, `UPDATE`, and `DELETE` operations in
the upstream database and publishes them as events to Kafka using Kafka
Connect-compatible connectors.

### Database setup

Before deploying a Debezium connector, ensure that the upstream database is
configured to support CDC.

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

Debezium is deployed as a set of Kafka Connect-compatible connectors. First,
define a SQL Server connector configuration, then start the connector by adding
it to Kafka Connect.

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

1. Beginning with Debezium 2.0.0, Confluent Schema Registry support is not
   included in the Debezium containers. To enable the Confluent Schema Registry
   for a Debezium container, install the following Confluent Avro converter JAR
   files into the Connect plugin directory:

    * `kafka-connect-avro-converter`
    * `kafka-connect-avro-data`
    * `kafka-avro-serializer`
    * `kafka-schema-serializer`
    * `kafka-schema-registry-client`
    * `common-config`
    * `common-utils`

    You can read more about this in the [Debezium documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html#deploying-confluent-schema-registry-with-debezium-containers).

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

    You can read more about each configuration property in the
    [Debezium documentation](https://debezium.io/documentation/reference/2.4/connectors/sqlserver.html).
    By default, the connector writes events for each table to a Kafka topic
    named `serverName.databaseName.tableName`.

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

Now, Debezium will capture changes from the SQL Server database and publish them
to Kafka.

### Create a source

Debezium emits change events using an envelope that contains detailed
information about upstream database operations, like the `before` and `after`
values for each record. To create a source that interprets the
[Debezium envelope](/sql/create-source/kafka/#using-debezium) in Materialize:

```mzsql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'server1.testDB.tableName')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

### Create a view

{{% ingest-data/ingest-data-kafka-debezium-view %}}

### Create an index on the view

{{% ingest-data/ingest-data-kafka-debezium-index %}}

## Known limitations

The official Microsoft [documentation for SQL Server CDC](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/known-issues-and-errors-change-data-capture?view=sql-server-ver16)
lists a number of known limitations that we recommend carefully reading through.
In addition to those listed, please also consider:

##### Debezium delivery guarantees

Due to an upstream bug in Debezium affecting snapshot isolation ([DBZ-3915](https://issues.redhat.com/browse/DBZ-3915)),
it is possible that rows inserted close to the initial snapshot time are
reflected twice in the downstream Kafka topic. This will lead to a `rows didn't
match` error in Materialize.

To work around this limitation, we recommend halting any updates to the tables
marked for replication until the Debezium connector is fully configured and the
initial snapshot for all tables has been completed.

##### Supported types

`DATETIMEOFFSET` columns are replicated as `text`, and
`DATETIME2` columns are replicated as `bigint` in Materialize.
