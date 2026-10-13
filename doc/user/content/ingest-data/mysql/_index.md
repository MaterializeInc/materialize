---
title: "MySQL"
description: "Connecting Materialize to a MySQL database for Change Data Capture (CDC)."
disable_list: true
menu:
    main:
        parent: "ingest-data"
        identifier: "mysql"
        weight: 20
aliases:
  - /self-managed/v25.2/ingest-data/mysql/
---

## Ingest from MySQL via change data capture

Materialize supports MySQL (8.0.1+) as a real-time data source. The [MySQL source](/sql/create-source/mysql-v2/)
uses MySQL's [binlog replication protocol](/sql/create-source/mysql-v2/#change-data-capture)
to **continually ingest changes** resulting from CRUD operations in the upstream
database. The native support for MySQL Change Data Capture (CDC) in Materialize
gives you the following benefits:

- **No additional infrastructure:** Ingest MySQL change data into Materialize in
  real-time with no architectural changes or additional operational overhead.
  In particular, you **do not need to deploy Kafka and Debezium** for MySQL
  CDC.

- **Transactional consistency:** The MySQL source ensures that transactions in
  the upstream MySQL database are respected downstream. Materialize will
  **never show partial results** based on partially replicated transactions.

- **Incrementally updated materialized views:** Materialized views are **not
  supported in MySQL**, so you can use Materialize as a
  read-replica to build views on top of your MySQL data that are efficiently
  maintained and always up-to-date.

### Supported versions and services

{{< note >}}
MySQL-compatible database systems are not guaranteed to work with the MySQL
source out-of-the-box. [MariaDB](https://mariadb.org/), [Vitess](https://vitess.io/)
and [PlanetScale](https://planetscale.com/) are currently **not supported**.
{{< /note >}}

The MySQL source requires **MySQL 8.0.1+** and is compatible with most common
MySQL hosted services.

### Integration guides

To help you get started, the following integration guides are available:

{{% include-headless "/headless/mysql-ingest-guides" %}}

## Supported data types

{{% include-headless "/headless/mysql-supported-data-types" %}}

## How ingestion from MySQL works

{{% include-headless "/headless/mysql-ingestion-mechanics" %}}

## Supported schema and table changes

The following table summarizes how Materialize handles changes to an upstream
table it is ingesting. See the details below the table for recovery commands.

| Change | Effect |
| --- | --- |
| Foreign key or `CHECK` constraint changes | No impact: Materialize ignores these changes. |
| Adding a `NOT NULL`, `UNIQUE`, or `PRIMARY KEY` constraint after the Materialize table was created | No impact. |
| Dropping an excluded column, or a column added after the Materialize table was created | No impact. |
| [Adding a column](#adding-a-column) | Handled automatically. Materialize keeps ingesting the existing columns; incorporate the new column with a new table (current syntax) or by re-adding the subsource (legacy syntax). |
| [Dropping an ingested column](#dropping-a-column) | Table enters an error state. Re-create the table. |
| [Renaming an ingested column](#renaming-a-column) | Table enters an error state. Re-create the table. |
| [Changing an ingested column's data type](#changing-a-columns-data-type) so that it maps to a different Materialize type | Table enters an error state. Re-create the table. |
| Changing an ingested column's data type so that it still maps to the same Materialize type | No impact. |
| [Appending a value to the end of an enum](#changing-a-columns-data-type) | Ingestion continues, but rows that use the new value fail to decode. Re-create the table to pick up the value. |
| [Any other enum change](#changing-a-columns-data-type) | Table enters an error state. Re-create the table. |
| [Dropping a `NOT NULL`, `UNIQUE`, or `PRIMARY KEY` constraint](#changing-constraints) that existed when the Materialize table was created | Table enters an error state. Re-create the table. |
| [Dropping, renaming, truncating, or moving a table to another schema](#table-level-operations) | Table enters an error state. Re-create the table. |

{{% upstream-schema-change-behavior connector="mysql" %}}

## Supported database operations

The following table summarizes how Materialize handles operational events on
the upstream MySQL database. See the details below the table for the error text
and any required configuration.

| Operation | Resolution |
| --- | --- |
| Restarting or patching MySQL (including OS-level restarts) | Supported automatically. |
| Restarting Materialize | Supported automatically. |
| Transient network interruptions between Materialize and MySQL | Supported automatically. |
| The upstream server running out of disk space | Supported automatically. |
| A long-running upstream transaction blocking the initial snapshot | Supported automatically. |
| [Failing over to a replica](#failovers) | Supported automatically, with the checks below. |
| [Binlog files covering the resume point removed](#binlog-files-removed-before-the-resume-point) | Requires re-creating the source. |
| [Resetting the binary log](#resetting-the-binary-log) | Requires re-creating the source. |
| [Changing a required replication setting](#changing-a-required-replication-setting) | Requires re-creating the source. |
| [Restoring the upstream database from a backup](#restoring-the-upstream-database) | Requires re-creating the source. |
| [Out-of-order GTIDs](#out-of-order-gtids) | Requires re-creating the source. |
| [Lowering `binlog_row_metadata`](#lowering-binlog_row_metadata) | Requires re-creating the affected tables. |

{{% include-headless "/headless/mysql-failure-states" %}}
