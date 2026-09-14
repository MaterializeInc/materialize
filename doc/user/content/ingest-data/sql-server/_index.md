---
title: "SQL Server"
description: "Connecting Materialize to a SQL Server database for Change Data Capture (CDC)."
disable_list: true
menu:
  main:
    parent: 'ingest-data'
    identifier: 'sql-server'
    weight: 30
---

## Ingest from SQL Server via change data capture

Materialize supports SQL Server as a real-time data source. The [SQL Server source](/sql/create-source/sql-server/)
uses SQL Server's change data capture feature to **continually ingest changes**
resulting from CRUD operations in the upstream database. The native support for
SQL Server Change Data Capture (CDC) in Materialize gives you the following benefits:

* **No additional infrastructure:** Ingest SQL Server change data into Materialize in
    real-time with no architectural changes or additional operational overhead.
    In particular, you **do not need to deploy Kafka and Debezium** for SQL Server
    CDC.

* **Transactional consistency:** The SQL Server source ensures that transactions in
    the upstream SQL Server database are respected downstream. Materialize will
    **never show partial results** based on partially replicated transactions.

* **Incrementally updated materialized views:** Incrementally updated Materialized
    views are considerably **limited in SQL Server**, so you can use Materialize as
    a read-replica to build views on top of your SQL Server data that are
    efficiently maintained and always up-to-date.

### Supported versions

Materialize supports replicating data from SQL Server 2016 or higher with Change
Data Capture (CDC) support.

### Integration guides

- [Azure SQL Database](/ingest-data/sql-server/azure-db/)
- [Self-hosted SQL Server](/ingest-data/sql-server/self-hosted/)

## Supported data types

{{% include-headless "/headless/sql-server-supported-data-types" %}}

## How ingestion from SQL Server works

{{% include-headless "/headless/sql-server-ingestion-mechanics" %}}

## Supported schema and table changes

The following table summarizes how Materialize handles changes to an upstream
table it is ingesting. See the details below the table for recovery commands.

| Change | Effect |
| --- | --- |
| Foreign key or `CHECK` constraint changes | No impact: Materialize ignores these changes. |
| Dropping an excluded column | No impact. |
| [Adding a column](#adding-a-column) | Handled automatically. Materialize keeps ingesting the existing columns; incorporate the new column with a new table (current syntax) or by re-adding the subsource (legacy syntax). |
| [Dropping an ingested column](#dropping-a-column) | Table enters an error state. Re-create the table. |
| [Renaming an ingested column](#renaming-a-column) | Table enters an error state. Re-create the table. |
| Any [`ALTER COLUMN`](#changing-a-columns-data-type) (type, collation, sparseness, masking, nullability) | Table enters an error state. Re-create the table. |
| Dropping a `UNIQUE` constraint | Table enters an error state. Re-create the table. |
| [Disabling CDC on a table](#disabling-cdc-on-a-table) (`sys.sp_cdc_disable_table`) | Table enters an error state. Drop and re-create just that table; the rest of the source keeps replicating. |
| [Removing the in-use capture instance](#removing-a-capture-instance) | Table enters an error state. Re-create the table. |
| [Dropping or renaming a table, or moving it to another schema](#table-level-operations) | Table enters an error state. Re-create the table. |

{{% upstream-schema-change-behavior connector="sql-server" %}}

## Supported database operations

The following table summarizes how Materialize handles operational events on
the upstream SQL Server database. See the details below the table for the
error text and any required configuration.

| Operation | Resolution |
| --- | --- |
| Restarting or patching SQL Server (including OS-level restarts) | Supported automatically. |
| Restarting Materialize | Supported automatically. |
| Transient network interruptions between Materialize and SQL Server | Supported automatically. |
| Taking the database `OFFLINE` and back `ONLINE` | Supported automatically. |
| Toggling `SINGLE_USER`/`MULTI_USER` or `READ_ONLY`/`READ_WRITE` | Supported automatically. |
| Data-file, filegroup, or index maintenance that rewrites data in place | Supported automatically. |
| [Availability group failover](#always-on-availability-groups) | Supported automatically, with a configuration change. |
| [Point-in-time restore](#point-in-time-restore) | Requires re-creating the source. |
| [CDC disabled at the database level](#cdc-disabled-at-the-database-level) | Requires re-creating the source. |
| [Change-table retention](#change-table-retention) exceeded during an outage | Requires re-creating the source. |

{{% include-headless "/headless/sql-server-failure-states" %}}
