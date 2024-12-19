---
title: "CockroachDB CDC using Kafka and Changefeeds"
description: "How to propagate Change Data Capture (CDC) data from a CockroachDB database to Materialize"
menu:
  main:
    parent: "crdb"
    name: "Using Kafka and Changefeeds"
    identifier: "crdb-changefeeds"
    weight: 16
---

Change Data Capture (CDC) allows you to track and propagate changes in a CockroachDB database to downstream consumers.
In this guide, we’ll cover how to use Materialize to create and efficiently maintain real-time materialized views on
top of CDC data.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Kafka + Changefeeds

Use [Changefeeds](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview) and the [Kafka source](/sql/create-source/kafka/#using-debezium)
to propagate CDC data from CockroachDB to Materialize. Changefeeds capture
row-level changes resulting from `INSERT`, `UPDATE`, and `DELETE` operations in
the upstream database and publishes them as events to Kafka topics. 

### Database setup

Before deploying a Debezium connector, ensure that the upstream database is
configured to support CDC.

1.  Enable rangefeeds for the CockroachDB instance:

    ```sql
    SET CLUSTER SETTING kv.rangefeed.enabled = true;
    ```

### Create Changefeeds

Create one changefeed for each table you want to publish to Materialize. 
Each table will produce data to its own Kafka topic that can be consumed by Materialize. 

```sql
CREATE CHANGEFEED FOR TABLE my_table 
    INTO 'kafka://broker:9092' 
    WITH format = avro,
         confluent_schema_registry = 'http://registry:8081',
         diff,
         envelope=wrapped
```

Materialize recommends creating changefeeds with using format `avro` and enabling `diff` and using envelope `wrapped`.
This will emits change events using an envelope that matches Debezium contains detailed
information about upstream database operations, like the `before` and `after`
values for each record. 
Please refer to the CockroachDB documentation for full details on changefeed configurations.  

### Create a source

To create a source that interprets the changefeeds use the
[Debezium envelope](/sql/create-source/kafka/#using-debezium) in Materialize:

```mzsql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'my_table')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

### Create a view

{{% ingest-data/ingest-data-kafka-debezium-view %}}

### Create an index on the view

{{% ingest-data/ingest-data-kafka-debezium-index %}}
