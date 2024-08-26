---
title: "Debezium"
description: "How to propagate Change Data Capture (CDC) data from a database to Materialize using Debezium"
aliases:
  - /third-party/debezium/
  - /integrations/debezium/
  - /connect-sources/debezium/
---

{{< tip >}}
Materialize provides [native support for PostgreSQL
and MySQL](#postgresql-and-mysql-native-support). If possible, use Materialize's
native support for these databases instead.
{{< /tip >}}

You can use [Debezium](https://debezium.io/) to propagate Change Data Capture (CDC) data from a database to Materialize, for example MySQL or PostgreSQL.

Debezium emits records using an envelope that contains valuable information about the change captured, like the `before` and `after` values for each record. This envelope is a powerful structure that lets Materialize perform more complex analysis to understand all CRUD-like operations happening in the upstream database. For more details on CDC support in Materialize, check the [documentation](/sql/create-source/kafka/#using-debezium).

{{< debezium-json >}}

### Debezium + Kafka guides

{{< tip >}}
Materialize provides [native support for PostgreSQL
and MySQL](#postgresql-and-mysql-native-support). If possible, use Materialize's
native support for these databases instead.
{{< /tip >}}

The following Debezium + Kafka guides are available:

* [PostgreSQL](/ingest-data/postgres/postgres-debezium/)

* [MySQL](/integrations/cdc-mysql/)

#### PostgreSQL and MySQL Native Support

Materialize provides native support for PostgreSQL and MySQL.
When possible, use Materialize's native support for these databases.

|            |               |
|------------|---------------|
| PostgreSQL | {{% ingest-data/postgres-native-support %}} |
| MySQL      | {{% ingest-data/mysql-native-support %}} |
