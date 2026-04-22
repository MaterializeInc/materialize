---
title: "Guide: Handle upstream schema changes with zero downtime"
description: "How to add a column, or drop a column, from your source MySQL database, without any downtime in Materialize"

menu:
  main:
    parent: "mysql"
    identifier: "mysql-source-versioning"
    weight: 85
---

{{< private-preview />}}
{{< note >}}
Changing column types is currently unsupported.
{{< /note >}}

Materialize allows you to handle certain types of upstream
table schema changes seamlessly, specifically:

- Adding a column in the upstream database.
- Dropping a column in the upstream database.

This guide walks you through how to handle these changes without any downtime in Materialize.

## Prerequisites

Some familiarity with Materialize. If you've never used Materialize before,
start with our [guide to getting started](/get-started/quickstart/) to learn
how to connect a database to Materialize.

### Set up a MySQL database

For this guide, setup a MySQL 5.7+ database. In your MySQL database, create a
table `t1` and populate it:

```sql
CREATE TABLE t1 (
    a INT
);

INSERT INTO t1 (a) VALUES (10);
```

### Configure your MySQL database

Configure your MySQL database for GTID-based binlog replication using the
[configuration instructions for self-hosted MySQL](/ingest-data/mysql/self-hosted/#a-configure-mysql).

### Connect your source database to Materialize

Create a connection to your MySQL database using the [`CREATE CONNECTION` syntax](/sql/create-connection/).

## Create a source

In Materialize, create a source using the [`CREATE SOURCE`
syntax](/sql/create-source/mysql-v2/).

```mzsql
CREATE SOURCE my_source
  FROM MYSQL CONNECTION mysql_connection;
```

## Create a table from the source

To start ingesting specific tables from your source database, create a
table in Materialize. We'll add it into the `v1` schema.

```mzsql
CREATE SCHEMA v1;

CREATE TABLE v1.t1
    FROM SOURCE my_source (REFERENCE mydb.t1);
```

Once you've created a table from source, the [initial
snapshot](/ingest-data/#snapshotting) of table `v1.t1` will begin.

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. You can monitor progress for the snapshot
operation on the overview page for the source in the Materialize console.

{{< /note >}}

## Create a view on top of the table

For this guide, add a materialized view `matview` (also in schema `v1`) that
sums column `a` from table `t1`.

```mzsql
CREATE MATERIALIZED VIEW v1.matview AS
    SELECT SUM(a) FROM v1.t1;
```

## Handle upstream column addition

### A. Add a column in your upstream MySQL database

In your upstream MySQL database, add a new column `b` to the table `t1`:

```sql
ALTER TABLE t1
    ADD COLUMN b BOOLEAN DEFAULT false;

INSERT INTO t1 (a, b) VALUES (20, true);
```

This operation has no immediate effect in Materialize. In Materialize:

- The table `v1.t1` will continue to ingest only column `a`.
- The materialized view `v1.matview` will continue to have access to column `a`
  only.

### B. Incorporate the new column in Materialize

Unlike SQL Server CDC, MySQL uses binlog-based replication, which automatically
includes all columns. To incorporate the new column into Materialize, create a
new `v2` schema and recreate the table in the new schema:

```mzsql
CREATE SCHEMA v2;

CREATE TABLE v2.t1
    FROM SOURCE my_source (REFERENCE mydb.t1);
```

The [snapshotting](/ingest-data/#snapshotting) of table `v2.t1` will begin.
`v2.t1` will include columns `a` and `b`.

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. You can monitor progress for the snapshot
operation on the overview page for the source in the Materialize console.

{{< /note >}}

When `v2.t1` has finished snapshotting, create a new materialized view in the
new schema. Since `v2.matview` references `v2.t1`, it can now reference column `b`:

```mzsql {hl_lines="4"}
CREATE MATERIALIZED VIEW v2.matview AS
    SELECT SUM(a)
    FROM v2.t1
    WHERE b = true;
```

## Handle upstream column drop

### A. Exclude the column in Materialize

To drop a column safely, first create a new schema in Materialize and recreate
the table excluding the column you intend to drop. In this example, we'll drop
column `b`.

```mzsql
CREATE SCHEMA v3;

CREATE TABLE v3.t1
    FROM SOURCE my_source (REFERENCE mydb.t1) WITH (EXCLUDE COLUMNS (b));
```

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. You can monitor progress for the snapshot
operation on the overview page for the source in the Materialize console.

{{< /note >}}

### B. Drop the column in your upstream MySQL database

In your upstream MySQL database, drop column `b` from table `t1`:

```sql
ALTER TABLE t1 DROP COLUMN b;
```

Dropping column `b` will have no effect on `v3.t1` in Materialize, provided
you completed step A before dropping the column. However, the drop affects
`v2.T` and `v2.matview` from our earlier examples. When the user attempts to
read from either, Materialize will report an error that the source table schema
has been altered.

Once you have finished migrating any views and queries from `v2` to `v3`, you
can clean up the old objects:

```mzsql
DROP TABLE v2.t1;
DROP MATERIALIZED VIEW v2.matview;
DROP SCHEMA v2;
```
