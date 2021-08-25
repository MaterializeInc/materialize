---
title: "Using Debezium"
description: "Get details about using Materialize with Debezium"
menu:
  main:
    parent: 'third-party'
---

You can use [Debezium](https://debezium.io/) to propagate Change Data Capture (CDC) data from a database to Materialize, for example MySQL or PostgreSQL.

Debezium emits records using an envelope that contains valuable information about the change captured, like the `before` and `after` values for each record. This envelope is a powerful structure that lets Materialize perform more complex analysis to understand all CRUD-like operations happening in the upstream database. For more details on CDC support in Materialize, check the [documentation](https://materialize.com/docs/sql/create-source/avro-kafka/#debezium-envelope-details).


{{< note >}}
Currently, Materialize only supports Avro-encoded Debezium records. If you're interested in JSON support, please reach out in the community Slack or leave a comment in [this GitHub issue](https://github.com/MaterializeInc/materialize/issues/5231).
{{</ note >}}

### Database settings

For the best CDC experience, we recommend following the guidelines in the Debezium documentation for each upstream database:

| Database | Settings |
|------|---------|
| MySQL | [Debezium MySQL Connector](https://debezium.io/documentation/reference/0.10/connectors/mysql.html#setting-up-mysql)
| PostgreSQL | [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/0.10/connectors/postgresql.html#setting-up-PostgreSQL)

As an example, for Debezium to emit the envelope Materialize expects when using PostgreSQL, the tables selected for CDC must have the parameter `REPLICA IDENTITY FULL` set:

```sql
ALTER TABLE foo
REPLICA IDENTITY FULL;
```

### Kafka-less setup

If you need to connect Materialize to a PostgreSQL database but Kafka is not part of your stack, you can use the [PostgreSQL direct source](https://materialize.com/docs/sql/create-source/postgres/#postgresql-source-details). This source uses PostgreSQL’s native replication protocol to continuously propagate upstream changes into Materialize, bypassing the need to deploy and maintain a Kafka instance. For more details and step-by-step instructions, check the [documentation](https://materialize.com/docs/sql/create-source/postgres/#postgresql-source-details).
