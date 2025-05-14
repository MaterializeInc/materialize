---
title: "MySQL"
description: "Connecting Materialize to a MySQL database for Change Data Capture (CDC)."
disable_list: true
menu:
  main:
    parent: 'ingest-data'
    identifier: 'mysql'
    weight: 5
---

## Change Data Capture (CDC)

Materialize supports MySQL as a real-time data source. The [MySQL source](/sql/create-source/mysql/)
uses MySQL's [binlog replication protocol](/sql/create-source/mysql/#change-data-capture)
to **continually ingest changes** resulting from CRUD operations in the upstream
database. The native support for MySQL Change Data Capture (CDC) in Materialize
gives you the following benefits:

* **No additional infrastructure:** Ingest MySQL change data into Materialize in
    real-time with no architectural changes or additional operational overhead.
    In particular, you **do not need to deploy Kafka and Debezium** for MySQL
    CDC.

* **Transactional consistency:** The MySQL source ensures that transactions in
    the upstream MySQL database are respected downstream. Materialize will
    **never show partial results** based on partially replicated transactions.

* **Incrementally updated materialized views:** Materialized views are **not
    supported in MySQL**, so you can use Materialize as a
    read-replica to build views on top of your MySQL data that are efficiently
    maintained and always up-to-date.

## Supported versions and services

{{< note >}}
MySQL-compatible database systems are not guaranteed to work with the MySQL
source out-of-the-box. [MariaDB](https://mariadb.org/), [Vitess](https://vitess.io/)
and [PlanetScale](https://planetscale.com/) are currently **not supported**.
{{< /note >}}

The MySQL source requires **MySQL 5.7+** and is compatible with most common
MySQL hosted services.

| Integration guides                          |
| ------------------------------------------- |
| {{% ingest-data/mysql-native-support %}}    |

If there is a hosted service or MySQL distribution that is not listed above but
you would like to use with Materialize, please submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)
or reach out in the Materialize [Community Slack](https://materialize.com/s/chat).

## Considerations

{{% include-md file="shared-content/mysql-considerations.md" %}}
