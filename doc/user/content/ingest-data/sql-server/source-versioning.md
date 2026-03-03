---
title: "Guide: Handle upstream schema changes with zero downtime"
description: "How to add a column, or drop a column, from your source SQL Server database, without any downtime in Materialize"

menu:
  main:
    parent: "sql-server"
    identifier: "sqlserver-source-versioning"
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

### Set up a SQL Server database

For this guide, setup a SQL Server 2016+ database. In your SQL Server, create a
table `t1` and populate:

```sql
CREATE TABLE t1 (
    A INT
);

INSERT INTO t1 (A) VALUES
    (10);
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

### Configure your SQL Server Database

Configure your SQL Server database using the [configuration instructions for self hosted SQL Server.](/ingest-data/sql-server/self-hosted/#a-configure-sql-server)

### Connect your source database to Materialize

Create a connection to your SQL Server database using the [`CREATE CONNECTION` syntax.](/sql/create-connection/)

## Create a source using the new syntax

In Materialize, create a source using the [`CREATE SOURCE`
syntax](/sql/create-source/sql-server-v2/).

```mzsql
CREATE SOURCE my_source
  FROM SQL SERVER CONNECTION sqlserver_connection;
```

## Create a table from the source
To start ingesting specific tables from your source database, you can create a
table in Materialize. We'll add it into the v1 schema in Materialize.

```mzsql
CREATE SCHEMA v1;

CREATE TABLE v1.t1
    FROM SOURCE my_source(REFERENCE dbo.t1);
```

Once you've created a table from source, the [initial
snapshot](/ingest-data/#snapshotting) of table `v1.t1` will begin.

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}

## Create a view on top of the table.

For this guide, add a materialized view `matview` (also in schema `v1`) that
sums column `A` from table `t1`.

```mzsql
CREATE MATERIALIZED VIEW v1.matview AS
    SELECT SUM(A) from v1.t1;
```

## Handle upstream column addition

### A. Add a column in your upstream SQL Server database

In your upstream SQL Server database, add a new column `B` to the table `t1`:

```sql
ALTER TABLE t1
    ADD B BIT NULL;

INSERT INTO t1 (A, B) VALUES
    (20, 0);
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

This operation will have no immediate effect in Materialize. In Materialize,
`v1.t1` will continue to ingest only column `A`, and SQL Server will continue to only publish CDC changes for column `A`. The materialized view
`v1.matview` will continue to have access to column `A` as well.

### B. Enable CDC for your table under a new capture instance in your upstream SQL Server database

In order for Materialize to begin receiving data for this new column, you must create a new capture instance for your table (see [Enable Change-Data-Capture for the tables](/ingest-data/sql-server/self-hosted/#4-enable-change-data-capture-for-the-tables)). Materialize will then select the most [recently created capture instance when creating a table from a SQL Server source](/ingest-data/sql-server/#capture-instance-selection), which will contain CDC data for column `B`.

```sql
EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 't1',
  @role_name = 'materialize_role',
  @capture_instance = 'dbo_t1_v2', -- MUST BE SPECIFIED
  @supports_net_changes = 0;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

You must provide a new value for `@capture_instance` that is different from the one initially provided when you enabled CDC. The default value is `<schema_name>_<table_name>`.

{{< note >}}
SQL Server only allows a [maximum of 2 capture instances](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql?view=sql-server-ver17#----capture_instance). If you already have 2 capture instances, you will have to disable one of them, possibly resulting in downtime for your Materialize source.
{{< /note >}}

### C. Incorporate the new column in Materialize

To incorporate the new column into Materialize, create a new `v2` schema and
recreate the table in the new schema, choosing the newly created capture instance:

```mzsql
CREATE SCHEMA v2;

CREATE TABLE v2.t1
    FROM SOURCE my_source(REFERENCE dbo.t1);
```

The [snapshotting](/ingest-data/#snapshotting) of table `v2.t1` will begin.
`v2.t1` will include columns `A` and `B`.

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}


When the new `v2.t1` table has finished snapshotting, create a new materialized
view `matview` in the new schema.  Since the new `v2.matview` is referencing the
new `v2.t1`, it can reference column `B`:

```mzsql {hl_lines="4"}
CREATE MATERIALIZED VIEW v2.matview AS
    SELECT SUM(A)
    FROM v2.t1
    WHERE B = true;
```

## Handle upstream column drop

### A. Exclude the column in Materialize

To drop a column safely, in Materialize, first, create a new `v3` schema, and
recreate table `t1` in the new schema but exclude the column to drop. In this
example, we'll drop the column B.

```mzsql
CREATE SCHEMA v3;
CREATE TABLE v3.t1
    FROM SOURCE my_source(REFERENCE dbo.t1) WITH (EXCLUDE COLUMNS (B));
```

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}

### B. Drop a column in your upstream SQL Server database

In your upstream SQL Server database, drop the column `B` from the table `t1`:

```sql
ALTER TABLE t1 DROP COLUMN B;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

Dropping the column B will have no effect on `v3.t1`. However, the drop affects
`v2.t1` and `v2.matview` from our earlier examples. When the user attempts to
read from either, Materialize will report an error that the source table schema
has been altered.

## Optional
### Swap schemas

When you're ready to fully cut over to the new source version, you can optionally swap the schemas and drop the old objects.

```mzsql
ALTER SCHEMA v1 SWAP WITH v3;

DROP SCHEMA v3 CASCADE;
```

### Disable the old capture instance in your upstream SQL Server database

If you fully cut over to the new source version, and you previously [created a new capture instance for your upstream table](/ingest-data/sql-server/source-versioning/#b-enable-cdc-for-your-table-under-a-new-capture-instance-in-your-upstream-sql-server-database), you may wish to disable the old capture instance.

{{< warning >}}
Ensure that you have no other source tables or third party applications using the old capture instance, as this will break them.
{{< /warning >}}

```sql
EXEC sys.sp_cdc_disable_table
    @source_schema = 'dbo',
    @source_name = 'foo',
    @capture_instance = '<old_capture_instance_name>';
```
