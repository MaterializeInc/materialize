---
title: "Debezium"
description: "How to propagate Change Data Capture (CDC) data from a database to Materialize using Debezium"
aliases:
  - /third-party/debezium/
  - /integrations/debezium/
  - /connect-sources/debezium/
---

You can use [Debezium](https://debezium.io/) to propagate Change Data Capture (CDC) data from a database to Materialize, for example MySQL or PostgreSQL.

Debezium emits records using an envelope that contains valuable information about the change captured, like the `before` and `after` values for each record. This envelope is a powerful structure that lets Materialize perform more complex analysis to understand all CRUD-like operations happening in the upstream database. For more details on CDC support in Materialize, check the [documentation](/sql/create-source/kafka/#using-debezium).


{{< debezium-json >}}

### CDC guides

For the best CDC experience, we recommend following the step-by-step guides for each upstream database:

* [PostgreSQL](/integrations/cdc-postgres/)

* [MySQL](/integrations/cdc-mysql/)

### Kafka-less setup

If you need to connect Materialize to a PostgreSQL database but Kafka is not part of your stack, you can use the [PostgreSQL direct source](/sql/create-source/postgres). This source uses PostgreSQL’s native replication protocol to continuously propagate upstream changes into Materialize, bypassing the need to deploy and maintain a Kafka instance. For more details and step-by-step instructions, see the integration guide for your PostgreSQL service: [Amazon RDS](/ingest-data/postgres-amazon-rds/), [Amazon Aurora](/ingest-data/postgres-amazon-aurora/), [Azure DB](/ingest-data/postgres-azure-db/), [Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/), [Self-hosted](/ingest-data/postgres-self-hosted/).
