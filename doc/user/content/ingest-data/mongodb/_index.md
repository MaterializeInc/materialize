---
title: "MongoDB"
description: "Connecting Materialize to a MongoDB database for Change Data Capture (CDC)."
disable_list: true
menu:
  main:
    parent: 'ingest-data'
    identifier: 'mongodb'
    weight: 17
---

This guide outlines how to ingest data from a MongoDB database into Materialize.

## High-level architecture

Since Materialize is a streaming database, it requires an active stream of data
changes to maintain real-time views. MongoDB does not natively push changes to
external systems, so this integration relies on Change Data Capture (CDC).

The architecture consists of four main components working in a pipeline:

- **MongoDB (Source):** Your operational database. It must be configured as a
  Replica Set to generate an Oplog (Operations Log), which records all data
  modifications.

- **Debezium:** A connector that acts as a log reader. It monitors the MongoDB
  Oplog and translates every insert, update, and delete operation into a stream
  of events.

- **Kafka & Schema Registry:** The streaming transport layer. Debezium pushes
  the events to a Kafka topic, while the Schema Registry ensures the data
  structure (schema) is consistent and readable.

- **Materialize (Destination):** Connects to Kafka, reads the change events, and
  incrementally updates its internal tables. This allows you to query the live
  state of your MongoDB data using standard SQL.

### Architecture diagram

```mermaid
flowchart LR
    subgraph "Operational Layer"
        Mongo[("MongoDB (Replica Set)")]
    end

    subgraph "Streaming Layer"
        Debezium["Debezium Connector"]
        Kafka{{"Kafka Broker"}}
        Schema["Schema Registry"]
    end

    subgraph "Analytical Layer"
        MZ[("Materialize")]
    end

    Mongo -- "Reads Oplog" --> Debezium
    Debezium -- "Writes Avro Events" --> Kafka
    Schema -.- "Validates Schemas" -.- Kafka
    Kafka -- "Consumes Topic" --> MZ
    Schema -.- "Decodes Data" -.- MZ
```

## Prerequisites

- **MongoDB:** Version 4.0 or later, configured as a Replica Set.
- **Kafka Cluster:** A running Kafka broker and Schema Registry (e.g.,
  Confluent Platform or Redpanda).
- **Debezium Connect:** A Kafka Connect cluster with the MongoDB connector
  plugin installed.

## Step 1. Configure MongoDB

For Debezium to capture changes, your MongoDB instance must be configured as a
Replica Set. Standalone MongoDB instances do not produce the Oplog (Operations
Log) required for Change Data Capture (CDC).

### 1. Enable replication

Ensure your MongoDB instance is running with replication enabled. Depending on
your deployment method (Docker, Kubernetes, Systemd, or manual CLI), this is
typically achieved by:

- **Configuration File:** Setting the `replication.replSetName` directive in
  your `mongod.conf`.
- **Command Line:** Passing the `--replSet` flag to the startup command.

For specific instructions on enabling replication for your environment, please
refer to the official [MongoDB Replica Set documentation](https://www.mongodb.com/docs/manual/replication/).

### 2. Initiate the Replica Set

Once the MongoDB service is running with replication enabled, the set must be
initialized before the Oplog is generated.

Connect to your MongoDB shell (`mongosh`) and run the initiation command:

```javascript
rs.initiate()
```

Verify that your prompt changes from a standalone status to primary (e.g.,
`rs0 [direct: primary]`).

### 3. Create a Debezium user

Create a user with the necessary permissions to read the database and the oplog.
In this example, we use a root user, but in production, you should scope
permissions to the specific databases you wish to ingest.

```javascript
use admin;
db.createUser({
    user: "debezium_user",
    pwd: "secure_password",
    roles: [{ role: "root", db: "admin" }]
});
```

## Step 2. Set up the streaming layer

You need a streaming platform to act as the buffer between MongoDB and
Materialize. This consists of Kafka, Schema Registry, and Debezium.

### Setup guides

If you do not have these services running, follow these external guides to get
started:

- **Docker (Recommended for Testing):** [Confluent Platform using Docker](https://docs.confluent.io/platform/current/platform-quickstart.html)
- **Debezium:** [Debezium Tutorial with Docker Compose](https://debezium.io/documentation/reference/stable/tutorial.html)

### Critical configuration

Regardless of how you install the services, you must configure them with the
following settings to ensure compatibility with Materialize.

#### 1. Kafka Connect worker configuration

Your Debezium (Kafka Connect) worker must use Avro for serialization to work
seamlessly with the Schema Registry. Ensure the following environment variables
(or properties) are set on your Connect worker:

| Property | Value |
|----------|-------|
| `KEY_CONVERTER` | `io.confluent.connect.avro.AvroConverter` |
| `VALUE_CONVERTER` | `io.confluent.connect.avro.AvroConverter` |
| `CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |

#### 2. Debezium connector configuration

When registering the MongoDB connector, you must use the specific configuration
below.

{{< important >}}
You must set `publish.full.document.only` to `true`. This forces Debezium to
send the entire document state for every change, which allows Materialize to
strictly use the `UPSERT` envelope.
{{< /important >}}

Use the following `curl` command to register the connector:

```bash
curl -X PUT -H "Content-Type:application/json" http://debezium:8083/connectors/mongodb-connector/config \
      -d '{
      "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
      "mongodb.hosts": "rs0/your-mongo-host:27017",
      "mongodb.name": "mongo",
      "mongodb.user": "debezium_user",
      "mongodb.password": "secure_password",
      "database.history.kafka.bootstrap.servers": "kafka:9092",
      "database.history.kafka.topic": "mongodb-history",
      "collection.include.list": "shop.products",
      "publish.full.document.only": true
  }'
```

| Parameter | Description |
|-----------|-------------|
| `mongodb.hosts` | Your MongoDB connection string (e.g., `rs0/mongodb:27017`). |
| `collection.include.list` | Comma-separated list of collections to ingest (e.g., `shop.products`). |
| `database.history.kafka.bootstrap.servers` | The address of your Kafka broker. |

## Step 3. Connect Materialize

Now that your MongoDB data is streaming into Kafka, you can create a source in
Materialize.

### 1. Create connections

First, tell Materialize how to connect to your Kafka cluster and Schema
Registry.

```mzsql
-- Connect to the Kafka Broker
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'kafka:9092',
    SECURITY PROTOCOL = 'PLAINTEXT'
);

-- Connect to the Schema Registry
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://schema-registry:8081'
);
```

### 2. Create the source

Create a source pointing to the specific Kafka topic. We use the `UPSERT`
envelope because Debezium is configured to send the full document state.

```mzsql
CREATE SOURCE mongo_products
FROM KAFKA CONNECTION kafka_connection (TOPIC 'mongo.shop.products')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
ENVELOPE UPSERT;
```

### 3. Query the data

You can now query the data using standard SQL.

```mzsql
SELECT * FROM mongo_products;
```

## Troubleshooting

- **Networking:** Ensure your Debezium container allows outbound traffic to
  your external MongoDB host.

- **Topic Names:** By default, Debezium creates topics in the format
  `serverName.databaseName.collectionName`. If you set `"mongodb.name": "mongo"`,
  your topic will be `mongo.shop.products`.
