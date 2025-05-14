---
title: "PostgreSQL"
description: "Connecting Materialize to a PostgreSQL database for Change Data Capture (CDC)."
disable_list: true
menu:
  main:
    parent: 'ingest-data'
    weight: 10
    identifier: 'postgresql'
---

## Change Data Capture (CDC)

Materialize supports PostgreSQL as a real-time data source. The
[PostgreSQL source](/sql/create-source/postgres//) uses PostgreSQL's
[replication protocol](/sql/create-source/postgres/#change-data-capture)
to **continually ingest changes** resulting from CRUD operations in the upstream
database. The native support for PostgreSQL Change Data Capture (CDC) in
Materialize gives you the following benefits:

* **No additional infrastructure:** Ingest PostgreSQL change data into
    Materialize in real-time with no architectural changes or additional
    operational overhead. In particular, you **do not need to deploy Kafka and
    Debezium** for PostgreSQL CDC.

* **Transactional consistency:** The PostgreSQL source ensures that transactions
    in the upstream PostgreSQL database are respected downstream. Materialize
    will **never show partial results** based on partially replicated
    transactions.

* **Incrementally updated materialized views:** Materialized views in PostgreSQL
    are computationally expensive and require manual refreshes. You can use
    Materialize as a read-replica to build views on top of your PostgreSQL data
    that are efficiently maintained and always up-to-date.

## Supported versions and services

{{< note >}}
PostgreSQL-compatible database systems are not guaranteed to work with the
PostgreSQL source out-of-the-box. [Yugabyte](https://www.yugabyte.com/) is
currently supported with **limitations**.
{{< /note >}}

The PostgreSQL source requires **PostgreSQL 11+** and is compatible with most
common PostgreSQL hosted services.

| Integration guides                          |
| ------------------------------------------- |
| {{% ingest-data/postgres-native-support %}} |

If there is a hosted service or PostgreSQL distribution that is not listed above
but you would like to use with Materialize, please submit a [feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)
or reach out in the Materialize [Community
Slack](https://materialize.com/s/chat).

## Considerations

{{% include-md file="shared-content/postgres-considerations.md" %}}
