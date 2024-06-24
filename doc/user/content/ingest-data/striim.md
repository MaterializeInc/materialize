---
title: "Striim Cloud"
description: "How to ingest Striim Change Data Capture (CDC) data into Materialize using the Kafka source"
aliases:
  - /integrations/striim/
---

[Striim](https://www.striim.com/) is a real-time data integration platform that
offers a variety of connectors for databases, messaging systems, and other data
sources. This guide walks through the steps to ingest Striim Change Data
Capture (CDC) into Materialize using the [Kafka source](https://materialize.com/docs/sql/create-source/kafka/).

## Before you begin

{{< note >}}
We are in touch with the Striim team to build a direct CDC connector
to Materialize. If you're interested in this integration, please [contact our team](https://materialize.com/docs/support/)!
{{</ note >}}

As is, integrating Striim Cloud with Materialize requires using a message broker
(like Apache Kafka) as an intermediary, as well as a schema registry
(like Confluent Schema Registry) configured for Avro schema management.

Ensure that you have:

- An active [Striim account](https://go2.striim.com/free-trial) and [Striim Cloud service](https://www.striim.com/docs/en/create-a-striim-cloud-service.html).
- A Kafka cluster up and running with access to a Confluent Schema Registry
  service.

## Step 1. Configure Striim CDC

### Database configuration

Follow Striim's guidance to enable and configure CDC from your source relational
database:

   * [Oracle Database CDC guide](https://www.striim.com/docs/en/oracle-database-cdc.html).
   * [SQL Server CDC guide](https://www.striim.com/docs/en/sql-server-cdc.html).

### Kafka writer configuration

1. In Striim, create a [`KafkaWriter`](https://www.striim.com/docs/en/kafka-writer.html)
   using the version that corresponds to your target Kafka broker.

1. Select the writer created in the previous step as the **Input stream**.

1. Configure the Kafka broker details and the target topic name.

   1. Under **Advanced settings > Kafka Config**, add the
      `value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer`
      configuration to serialize records using the [Confluent wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format).

   1. Add a `MESSAGE KEY` field to specify the primary key of the table. This
      will configure the `KafkaWriter` to use the table’s primary key as the Kafka
      message key.

### Schema registry configuration

1. From the **Formatter** dropdown menu, select `AvroFormatter` to serialize records using the [Avro](https://avro.apache.org/) format.

1. Configure the Schema Registry URL, and its authentication credentials
(if necessary).

1. Set the format to `Table`.

1. Leave the **Schema Registry Subject Name** and **Schema Registry Subject Name
Mapping** fields empty.

### Known limitations

#### Supported types

Some data types might not be correctly supported in Striim's Avro handling (e.g.
[`bigint`](https://community.striim.com/product-q-a-6/suitable-avro-type-not-found-for-field-error-when-using-mysql-striim-kafka-125)).
If you run into Avro serialization issues with the `KafkaWriter`, please reach
out to [Striim support](https://striim.zendesk.com/) or the [Striim community](https://community.striim.com/community).

<br>

Once your Striim Cloud service is configured for CDC, **start the app** to begin
streaming changes from your source relational database into the specified Kafka
cluster. Next, you'll configure Materialize to consume this data.

## Step 2. Start ingesting data

{{< note >}}
If you are prototyping and already have a cluster to host your Kafka source
(e.g. `quickstart`), you don't need to create a new cluster. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/)
   command to create connection objects with access and authentication details
   to your Kafka cluster and schema registry:

   ```mzsql
    CREATE SECRET kafka_password AS '<your-password>';
    CREATE SECRET csr_password AS '<your-password>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker-url>',
        SECURITY PROTOCOL = 'SASL_PLAINTEXT',
        SASL MECHANISMS = 'SCRAM-SHA-256', -- or `PLAIN` or `SCRAM-SHA-512`
        SASL USERNAME = '<your-username>',
        SASL PASSWORD = SECRET kafka_password
        SSH TUNNEL ssh_connection
    );

    CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
        URL '<schema-registry-url>',
        USERNAME = '<your-username>',
        PASSWORD = SECRET csr_password
    );
    ```

1. Use the [`CREATE SOURCE`](/sql/create-source/kafka/) command to connect
   Materialize to your Kafka broker and schema registry using the connections you
   created in the previous step.

   ```mzsql
   CREATE SOURCE src
     FROM KAFKA CONNECTION kafka_connection (TOPIC '<topic-name>')
     KEY FORMAT TEXT
     VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
     VALUE STRATEGY ID <id>
   ENVELOPE UPSERT;
   ```

   **Fetching the `VALUE STRATEGY ID`**

   Striim uses nonstandard subject names in the schema registry, which prevents
   Materialize from finding the schema using the default configuration. You
   must manually fetch and add the schema identifier. To get this value,
   open a terminal and use the following command:

   ```bash
   curl <schema-registry-url>:8081/subjects/<schema-name>/versions/latest | jq .id
   ```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection/)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka/)
