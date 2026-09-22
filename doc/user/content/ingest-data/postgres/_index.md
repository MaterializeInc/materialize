---
title: "PostgreSQL"
description: "Connecting Materialize to a PostgreSQL database for Change Data Capture (CDC)."
disable_list: true
menu:
  main:
    parent: 'ingest-data'
    weight: 10
    identifier: 'postgresql'
aliases:
  - /self-managed/v25.1/ingest-data/postgres/
  - /self-managed/v25.2/ingest-data/postgres/
  - /self-managed/v25.2/ingest-data/postgres/amazon-aurora/
---

## Change Data Capture (CDC)

Materialize supports PostgreSQL as a real-time data source. The
[PostgreSQL source](/sql/create-source/postgres/) uses PostgreSQL's
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

When a source is created, Materialize parallelizes the initial snapshot
across the cluster's workers and, on PostgreSQL 14 and later, splits each
table's read across workers. See [Snapshot
parallelism](/fundamentals/concepts/snapshotting/#parallelism).

### Supported versions and services

The PostgreSQL source requires **PostgreSQL 11+** and is compatible with most
common PostgreSQL hosted services.

### Integration guides

To help you get started, the following integration guides are available:

{{% include-headless "/headless/postgresql-ingest-data-guides" %}}

## Supported data types

{{% include-headless "/headless/postgres-supported-data-types" %}}

## How ingestion from PostgreSQL works

{{% include-headless "/headless/postgres-ingestion-mechanics" %}}

## Supported schema and table changes

The following table summarizes how Materialize handles changes to an upstream
table it is ingesting. See the details below the table for the remediation for
each syntax.

| Change | Effect |
| --- | --- |
| Foreign key, `CHECK`, or `EXCLUSION` constraint changes | No impact: Materialize ignores these changes. |
| Dropping a column that is not ingested | No impact. |
| Adding a `NOT NULL`, `UNIQUE`, or `PRIMARY KEY` constraint | No impact. |
| [Adding a column](#adding-a-column) | Handled automatically. Materialize keeps ingesting the existing columns. To pick up the new column, create a new table (current syntax) or re-add the subsource (legacy syntax). |
| [Dropping an ingested column](#dropping-a-column) | Table enters an error state. Re-create the table. |
| [Renaming an ingested column](#renaming-a-column) | Table enters an error state. Re-create the table. |
| [Changing an ingested column's data type](#changing-a-columns-data-type) | Table enters an error state, unless the column is ingested as `text` via `TEXT COLUMNS`. Re-create the table. |
| [Dropping a `NOT NULL`, `UNIQUE`, or `PRIMARY KEY` constraint](#changing-constraints) that existed when the table was created | Table enters an error state. Re-create the table. |
| [Dropping, renaming, or moving a table](#table-level-operations) | Table enters an error state. Re-create the table. |
| [Removing a table from the publication](#table-level-operations) | Table enters an error state. Re-create the table. |
| [Setting a replica identity other than `FULL`](#table-level-operations) | Table enters an error state. Re-create the table. |
| [Truncating a table](#table-level-operations) | Table enters an error state. Use an unqualified `DELETE FROM` instead. |

{{% upstream-schema-change-behavior connector="postgres" %}}

## Supported database operations

The following table summarizes how Materialize handles operational events on
the upstream PostgreSQL database. See the details below the table for the error
text and any required configuration.

| Operation | Resolution |
| --- | --- |
| Restarting or patching PostgreSQL (including OS-level restarts) | Supported automatically. |
| Restarting Materialize | Supported automatically. |
| Transient network interruptions between Materialize and PostgreSQL | Supported automatically. |
| Resizing the source cluster or changing its replication factor | Supported automatically. |
| The upstream database running out of disk space | Supported automatically, once space is freed. |
| [High-availability failover](#high-availability-failovers) | Requires re-creating the source. On self-managed Materialize, a configuration change can avoid this. |
| [Point-in-time restore](#point-in-time-restore) | Requires re-creating the source. |
| [Promoting a physical replica](#promotion-of-a-physical-replica) | Requires re-creating the source. |
| [Replication slot invalidated](#replication-slot-invalidated) by WAL retention | Requires re-creating the source. |
| [Replication slot dropped or rewound](#replication-slot-dropped-or-rewound) | Requires re-creating the source. |
| [Dropping the publication](#dropping-the-publication) | Requires re-creating the source. |
| [Major version upgrades](#major-version-upgrades) | Requires re-creating the source. |

{{% include-headless "/headless/postgres-failure-states" %}}
