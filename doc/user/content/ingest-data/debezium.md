---
title: "Debezium"
description: "How to propagate Change Data Capture (CDC) data from a database to Materialize using Debezium"
aliases:
  - /third-party/debezium/
  - /integrations/debezium/
  - /connect-sources/debezium/
---

You can use [Debezium](https://debezium.io/) to propagate Change Data Capture
(CDC) data to Materialize from databases that are not supported via native
connectors. For PostgreSQL and MySQL databases, we **strongly recommend** using
the native [PostgreSQL](/sql/create-source-v1/postgres/) and [MySQL](/sql/create-source-v1/mysql/)
sources instead.

| Database   | Natively supported? | Integration guide                                                                              |
|------------|---------------------| ---------------------------------------------------------------------------------------------- |
| PostgreSQL | ✓                   | {{% ingest-data/postgres-native-support %}}                                                    |
| MySQL      | ✓                   | {{% ingest-data/mysql-native-support %}}                                                       |
| SQL Server | ✓                   | {{% ingest-data/sql-server-native-support %}}                                                  |
| Oracle     |                     | [Kafka + Debezium](https://debezium.io/documentation/reference/stable/connectors/oracle.html)  |
| MongoDB    |                     | [Kafka + Debezium](https://debezium.io/documentation/reference/stable/connectors/mongodb.html) |

### Using Debezium

For databases that are not yet natively supported, like Oracle, SQL Server, or
MongoDB, you can use [Debezium](https://debezium.io/) to propagate Change Data
Capture (CDC) data to Materialize.

{{< debezium-json >}}

Debezium captures row-level changes resulting from `INSERT`, `UPDATE`, and
`DELETE` operations in the upstream database and publishes them as events to
Kafka (and other Kafka API-compatible brokers) using Kafka Connect-compatible
connectors. For more details on CDC support in Materialize, check the
[Kafka source](/sql/create-source-v1/kafka/#using-debezium) reference
documentation.
