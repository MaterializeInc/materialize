---
title: "MySQL CDC using Kafka and Debezium"
description: "How to propagate Change Data Capture (CDC) data from a MySQL database to Materialize using Kafka and Debezium"
aliases:
  - /guides/cdc-mysql/
  - /integrations/cdc-mysql/
  - /connect-sources/cdc-mysql/
  - /ingest-data/cdc-mysql/
---

{{< warning >}}
You can use [Debezium](https://debezium.io/) to propagate Change
Data Capture(CDC) data to Materialize from a MySQL database, but
we **strongly recommend** using the native [MySQL](/sql/create-source/mysql/)
source instead.
{{</ warning >}}

{{< guided-tour-blurb-for-ingest-data >}}

Change Data Capture (CDC) allows you to track and propagate changes in a MySQL
database to downstream consumers based on its binary log (`binlog`). In this
guide, we’ll cover how to use Materialize to create and efficiently maintain
real-time views with incrementally updated results on top of CDC data.

## Kafka + Debezium

You can use [Debezium](https://debezium.io/) and the [Kafka source](/sql/create-source/kafka/#using-debezium)
to propagate CDC data from MySQL to Materialize in the unlikely event that using
the [native MySQL source](/sql/create-source/mysql/) is not an option. Debezium
captures row-level changes resulting from `INSERT`, `UPDATE` and `DELETE`
operations in the upstream database and publishes them as events to Kafka using
Kafka Connect-compatible connectors.

### A. Configure database

Before deploying a Debezium connector, you need to ensure that the upstream
database is configured to support [row-based replication](https://dev.mysql.com/doc/refman/8.0/en/replication-rbr-usage.html).
As _root_:

1. Check the `log_bin` and `binlog_format` settings:

    ```mysql
    SHOW VARIABLES
    WHERE variable_name IN ('log_bin', 'binlog_format');
    ```

    For CDC, binary logging must be enabled and use the `row` format. If your
    settings differ, you can adjust the database configuration file
    (`/etc/mysql/my.cnf`) to use `log_bin=mysql-bin` and `binlog_format=row`.
    Keep in mind that changing these settings requires a restart of the MySQL
    instance and can [affect database performance](https://dev.mysql.com/doc/refman/8.0/en/replication-sbr-rbr.html#replication-sbr-rbr-rbr-disadvantages).

    **Note:** Additional steps may be required if you're using MySQL on
      [Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.MySQL.BinaryFormat.html).

1. Grant enough privileges to the replication user to ensure Debezium can
   operate in the database:

    ```mysql
    GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "user";

    FLUSH PRIVILEGES;
    ```

### B. Deploy Debezium

**Minimum requirements:** Debezium 1.5+

Debezium is deployed as a set of Kafka Connect-compatible connectors, so you
first need to define a MySQL connector configuration and then start the
connector by adding it to Kafka Connect.

{{< warning >}}

If you deploy the MySQL Debezium connector in [Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-debezium.html),
you **must** override the default value of `After-state only` to `false`.
{{</ warning >}}

{{< tabs >}}
{{< tab "Debezium 1.5+">}}

1. Create a connector configuration file and save it as `register-mysql.json`:

    ```json
    {
      "name": "your-connector",
      "config": {
          "connector.class": "io.debezium.connector.mysql.MySqlConnector",
          "tasks.max": "1",
          "database.hostname": "mysql",
          "database.port": "3306",
          "database.user": "user",
          "database.password": "mysqlpwd",
          "database.server.id":"223344",
          "database.server.name": "dbserver1",
          "database.history.kafka.bootstrap.servers":"kafka:9092",
          "database.history.kafka.topic":"dbserver1.history",
          "database.include.list": "db1",
          "table.include.list": "table1",
          "include.schema.changes": false
        }
    }
    ```

    You can read more about each configuration property in the
    [Debezium documentation](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-connector-properties).

{{< /tab >}}
{{< tab "Debezium 2.0+">}}

1. From Debezium 2.0, Confluent Schema Registry (CSR) support is not bundled in
   Debezium containers. To enable CSR, you must install the following Confluent
   Avro converter JAR files into the Kafka Connect plugin directory (by default,
   `/kafka/connect`):

    * `kafka-connect-avro-converter`
    * `kafka-connect-avro-data`
    * `kafka-avro-serializer`
    * `kafka-schema-serializer`
    * `kafka-schema-registry-client`
    * `common-config`
    * `common-utils`

    You can read more about this in the [Debezium documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html#deploying-confluent-schema-registry-with-debezium-containers).

1. Create a connector configuration file and save it as `register-mysql.json`:

    ```json
    {
      "name": "your-connector",
      "config": {
          "connector.class": "io.debezium.connector.mysql.MySqlConnector",
          "tasks.max": "1",
          "database.hostname": "mysql",
          "database.port": "3306",
          "database.user": "user",
          "database.password": "mysqlpwd",
          "database.server.id":"223344",
          "topic.prefix": "dbserver1",
          "database.include.list": "db1",
          "database.history.kafka.topic":"dbserver1.history",
          "database.history.kafka.bootstrap.servers":"kafka:9092",
          "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
          "schema.history.internal.kafka.topic": "dbserver1.internal.history",
          "table.include.list": "table1",
          "key.converter": "io.confluent.connect.avro.AvroConverter",
          "value.converter": "io.confluent.connect.avro.AvroConverter",
          "key.converter.schema.registry.url": "http://<scheme-registry>:8081",
          "value.converter.schema.registry.url": "http://<scheme-registry>:8081",
          "include.schema.changes": false
        }
    }
    ```

    You can read more about each configuration property in the
    [Debezium documentation](https://debezium.io/documentation/reference/2.4/connectors/mysql.html).
    By default, the connector writes events for each table to a Kafka topic
    named `serverName.databaseName.tableName`.

{{< /tab >}}
{{< /tabs >}}

1. Start the Debezium MySQL connector using the configuration file:

    ```bash
    export CURRENT_HOST='<your-host>'
    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-mysql.json
    ```

1. Check that the connector is running:

    ```bash
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```

    The first time it connects to a MySQL server, Debezium takes a
    [consistent snapshot](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed into your
    Kafka topic:

    ```bash
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic dbserver1.db1.table1
    ```

### C. Create a source

{{< debezium-json >}}

Debezium emits change events using an envelope that contains detailed
information about upstream database operations, like the `before` and `after`
values for each record. To create a source that interprets the
[Debezium envelope](/sql/create-source/kafka/#using-debezium) in Materialize:

```mzsql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'dbserver1.db1.table1')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

### D. Create a view on the source

{{% ingest-data/ingest-data-kafka-debezium-view %}}

### E. Create an index on the view

{{% ingest-data/ingest-data-kafka-debezium-index %}}
