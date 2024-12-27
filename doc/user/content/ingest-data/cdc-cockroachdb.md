---
title: "CockroachDB CDC using Kafka and Changefeeds"
description: "How to propagate Change Data Capture (CDC) data from a CockroachDB database to Materialize"
menu:
  main:
    parent: "crdb"
    name: "Using Kafka and Changefeeds"
    identifier: "crdb-kafka-changefeeds"
    weight: 5
---

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

Change Data Capture (CDC) allows you to track and propagate changes in a
CockroachDB database to downstream consumers. In this guide, we’ll cover how to
use Materialize to create and efficiently maintain real-time views with
incrementally updated results on top of CockroachDB CDC data.

[//]: # "TODO(morsapaes) Add Before you begin section for consistency and
details like the minimum required Cockroach version to follow this."

## A. Configure CockroachDB

### 1. Enable rangefeeds

[//]: # "TODO(morsapaes) Add more detailed steps and best practices, including
checking if rangefeeds are already enabled (true for CockroachDB serverless),
creating a dedicated user for replication, granting it the appropriate
permissions, using CDC queries to reduce the amount of data sent over the wire,
and so on."

As a first step, you must ensure [rangefeeds](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds#enable-rangefeeds)
are enabled in your CockroachDB instance so you can create changefeeds for the
tables you want to replicate to Materialize.

1. As a user with the `admin` role, enable the `kv.rangefeed.enabled`
   [cluster setting](https://www.cockroachlabs.com/docs/stable/set-cluster-setting):

   ```sql
   SET CLUSTER SETTING kv.rangefeed.enabled = true;
   ```

### 2. Configure per-table changefeeds

[//]: # "TODO(morsapaes) Instructions to create a changefeed vary depending on
whether users are on CockroachDB core or enterprise."

[Changefeeds](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview)
capture row-level changes resulting from `INSERT`, `UPDATE`, and `DELETE`
operations against CockroachDB tables and publish them as events to Kafka
(or other Kafka API-compatible broker). You can then use the [Kafka source](/sql/create-source/kafka/#using-debezium)
to consume these changefeed events into Materialize, making the data available
for transformation.

1. [Create a changefeed](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds?)
   for each table you want to replicate:

   ```sql
   CREATE CHANGEFEED FOR TABLE my_table
     INTO 'kafka://broker:9092'
     WITH format = avro,
       confluent_schema_registry = 'http://registry:8081',
       diff,
       envelope = wrapped
   ```

   We recommend creating changefeeds using the Avro format (`format = avro`) and
   the default [diff envelope](https://www.cockroachlabs.com/docs/v24.3/create-changefeed#envelope)
   (`envelope = wrapped`), which is compatible with the message format
   Materialize expects. Each table will produce data to a dedicated Kafka
   topic, which can then be consumed by Materialize.


For detailed instructions on configuring your CockroachDB instance for CDC,
refer to the [CockroachDB documentation](https://www.cockroachlabs.com/docs/stable/create-changefeed).

## B. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your Kafka
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% kafka/cockroachdb/create-a-cluster %}}

### 2. Start ingesting data

[//]: # "TODO(morsapaes) Incorporate all options for network security and
authentication later on. Starting with a simplified version that is consistent
with the PostgreSQL/MySQL guides and can be used to model other Kafka-relate
integration guides after."

Now that you've created an ingestion cluster, you can connect Materialize to
your Kafka broker and start ingesting data. The exact steps depend on your
authentication and networking configurations, so refer to the
[`CREATE CONNECTION`](/sql/create-connection/#kafka) documentation for further
guidance.

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the credentials to connect to your Kafka broker
   and, optionally, schema registry:

    ```mzsql
    CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';
    CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';
    CREATE SECRET csr_password AS '<CSR_PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/#kafka) command to create
   a connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION kafka_connection TO KAFKA (
      BROKER '<host>',
      SSL KEY = SECRET kafka_ssl_key,
      SSL CERTIFICATE = SECRET kafka_ssl_crt
    );
    ```

    If you're using a schema registry, create an additional connection object:

    ```mzsql
    CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
      URL '<csr_url>',
      SSL KEY = SECRET csr_ssl_key,
      SSL CERTIFICATE = SECRET csr_ssl_crt,
      USERNAME = 'foo',
      PASSWORD = SECRET csr_password
    );
    ```

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Kafka broker and start ingesting data from the target topic:

   ```mzsql
   CREATE SOURCE kafka_repl
     IN CLUSTER ingest_kafka
     FROM KAFKA CONNECTION kafka_connection (TOPIC 'my_table')
     -- CockroachDB's default envelope structure for changefeed messages is
     -- compatible with the Debezium format, so you can use ENVELOPE DEBEZIUM
     -- to interpret the data.
     ENVELOPE DEBEZIUM;
   ```

    By default, the source will be created in the active cluster; to use a
    different cluster, use the `IN CLUSTER` clause.

### 3. Monitor the ingestion status

{{% kafka/cockroachdb/check-the-ingestion-status %}}

### 4. Create a view

{{% ingest-data/ingest-data-kafka-debezium-view %}}

### 5. Create an index on the view

{{% ingest-data/ingest-data-kafka-debezium-index %}}

## Next steps

With Materialize ingesting your CockroachDB data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.

- Explore your data with [`SHOW SOURCES`](/sql/show-sources) and [`SELECT`](/sql/select/).

- Compute real-time results in memory with [`CREATE VIEW`](/sql/create-view/)
  and [`CREATE INDEX`](/sql/create-index/) or in durable
  storage with [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

- Serve results to a PostgreSQL-compatible SQL client or driver with [`SELECT`](/sql/select/)
  or [`SUBSCRIBE`](/sql/subscribe/) or to an external message broker with
  [`CREATE SINK`](/sql/create-sink/).

- Check out the [tools and integrations](/integrations/) supported by
  Materialize.
