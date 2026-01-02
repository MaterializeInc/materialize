---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/debezium/
complexity: intermediate
description: How to propagate Change Data Capture (CDC) data from a database to Materialize
  using Debezium
doc_type: reference
keywords:
- strongly recommend
- Debezium
product_area: Sources
status: stable
title: Debezium
---

# Debezium

## Purpose
How to propagate Change Data Capture (CDC) data from a database to Materialize using Debezium

If you need to understand the syntax and options for this command, you're in the right place.


How to propagate Change Data Capture (CDC) data from a database to Materialize using Debezium


You can use [Debezium](https://debezium.io/) to propagate Change Data Capture
(CDC) data to Materialize from databases that are not supported via native
connectors. For PostgreSQL and MySQL databases, we **strongly recommend** using
the native [PostgreSQL](/sql/create-source/postgres/) and [MySQL](/sql/create-source/mysql/)
sources instead.

| Database   | Natively supported? | Integration guide                                                                              |
|------------|---------------------| ---------------------------------------------------------------------------------------------- |
| PostgreSQL | ✓                   | <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See ingest-data documentation --> --> -->                                                    |
| MySQL      | ✓                   | <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See ingest-data documentation --> --> -->                                                       |
| SQL Server | ✓                   | <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See ingest-data documentation --> --> -->                                                  |
| Oracle     |                     | [Kafka + Debezium](https://debezium.io/documentation/reference/stable/connectors/oracle.html)  |
| MongoDB    |                     | [Kafka + Debezium](/ingest-data/mongodb/) |

### Using Debezium

For databases that are not yet natively supported, like Oracle, SQL Server, or
MongoDB, you can use [Debezium](https://debezium.io/) to propagate Change Data
Capture (CDC) data to Materialize.


Debezium captures row-level changes resulting from `INSERT`, `UPDATE`, and
`DELETE` operations in the upstream database and publishes them as events to
Kafka (and other Kafka API-compatible brokers) using Kafka Connect-compatible
connectors. For more details on CDC support in Materialize, check the
[Kafka source](/sql/create-source/kafka/#using-debezium) reference
documentation.