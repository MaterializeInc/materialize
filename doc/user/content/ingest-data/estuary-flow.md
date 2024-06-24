---
title: "Estuary Flow"
description: "How to ingest real-time data from Estuary Flow into Materialize"
aliases:
  - /integrations/estuary/
---

[Estuary Flow](https://estuary.dev/) is a real-time data platform specializing in Change Data Capture (CDC). This guide walks through the steps to ingest data from Estuary Flow into Materialize using the Kafka source.
A Collection in Estuary Flow is exposed as a Kafka Topic for consumption.


## Before you begin, ensure that you have:

- An Estuary Flow account.

## Step 1. Create a New Secret & Create Connection

Generate your refresh token from the [Estuary Flow Dashboard](https://dashboard.estuary.dev/admin/api).
   Replace the placeholder token in the commands below with your actual token.

In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/)
   command to create connection objects with access and authentication details
   to Estuary Flow, including a schema registry:

### Create Secret for Estuary Refresh Token

```sql
CREATE SECRET estuary_refresh_token AS 'your_generated_token_here';
```

### Create Kafka Connection

```sql
CREATE CONNECTION estuary_connection TO KAFKA (
    BROKER 'dekaf.estuary.dev',
    SECURITY PROTOCOL = 'SASL_SSL',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = '{}',
    SASL PASSWORD = SECRET estuary_refresh_token
);
```

### Create Schema Registry Connection

```sql
CREATE CONNECTION csr_estuary_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://dekaf.estuary.dev',
    USERNAME = '{}',
    PASSWORD = SECRET estuary_refresh_token
);
```

Once created, a connection is reusable across multiple CREATE SOURCE statements.

## Step 2. Create a Source for an Estuary Flow Collection

Define a source that reads from an Estuary Flow collection (surfaced as a Kafka topic), using the previously created connections and specifying the data format.

### Create Source

```sql
CREATE SOURCE estuary_flow_source
  FROM KAFKA CONNECTION estuary_connection (TOPIC '<name-of-your-flow-collection>')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_estuary_connection
    ENVELOPE UPSERT;
```

Make sure to replace <name-of-your-flow-collection> with the full name of your collection from Estuary Flow; you can grab this value from the Flow dashboard.

Related Pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection/)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka/)

