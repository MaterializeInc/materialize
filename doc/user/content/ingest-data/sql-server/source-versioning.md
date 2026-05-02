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
    a INT
);

INSERT INTO t1 (a) VALUES
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
sums column `a` from table `t1`.

```mzsql
CREATE MATERIALIZED VIEW v1.matview AS
    SELECT SUM(a) from v1.t1;
```

## Handle upstream column addition

### A. Add a column in your upstream SQL Server database

In your upstream SQL Server database, add a new column `b` to the table `t1`:

```sql
ALTER TABLE t1
    ADD b BIT NULL;

INSERT INTO t1 (a,b) VALUES
    (20, 1);
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

This operation does not impact the SQL Server CDC output; the SQL Server
continues to publish CDC changes only for column `a`. As such, the addition of a
new column has no immediate effect in Materialize. In Materialize:

- The table `v1.t1` will continue to ingest only column `a`.
- The materialized view `v1.matview` will continue to have access to column `a`
  only.

### B. Enable CDC for your table under a new capture instance in your upstream SQL Server database

In order for Materialize to begin receiving data for this new column, you must
create a new capture instance for your table, explicitly specifing a new
[`@capture_instance`
name](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql?view=sql-server-ver17#----capture_instance).

{{< note >}}

SQL Server only allows a [maximum of 2 capture
instances](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql?view=sql-server-ver17#----capture_instance).
If you already have 2 capture instances, you will have to [disable one of
them](#disable-unused-capture-instance), possibly resulting in downtime for your
Materialize source.

{{< /note >}}

```sql
EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 't1',
  @role_name = 'materialize_role',
  @capture_instance = 'dbo_t1_v2', -- MUST BE SPECIFIED
  @supports_net_changes = 0;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

The newly created capture instance will include CDC data for column `b`. Now,
when you create a new table `t1` from your source in Materialize (see next
step), Materialize will select the most [recently created capture
instance](/ingest-data/sql-server/#capture-instance-selection) (i.e., the
capture instance with the newly added column `b`).

See also:

- [Capture instance
  selection](/ingest-data/sql-server/#capture-instance-selection)
- [Enable Change-Data-Capture for the
tables](/ingest-data/sql-server/self-hosted/#4-enable-change-data-capture-for-the-tables)

### C. Create a new table from the source in Materialize

To incorporate the new column into Materialize, create a new `v2` schema and
recreate the table in the new schema. When creating the table, Materialize uses
the  most [recently created capture
instance](/ingest-data/sql-server/#capture-instance-selection) (i.e., the
capture instance with the newly added column `b`):

```mzsql
CREATE SCHEMA v2;

CREATE TABLE v2.t1
    FROM SOURCE my_source(REFERENCE dbo.t1);
```

The [snapshotting](/ingest-data/#snapshotting) of table `v2.t1` will begin.
`v2.t1` will include columns `a` and `b`.

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}


When the new `v2.t1` table has finished snapshotting, create a new materialized
view `matview` in the new schema.  Since the new `v2.matview` is referencing the
new `v2.t1`, it can reference column `b`:

```mzsql {hl_lines="4"}
CREATE MATERIALIZED VIEW v2.matview AS
    SELECT SUM(a)
    FROM v2.t1
    WHERE b = true;
```

## Handle upstream column drop

### A. Exclude the column in Materialize

To drop a column safely, in Materialize, first, create a new `v3` schema, and
recreate table `t1` in the new schema but exclude the column to drop. In this
example, we'll drop the column `b`.

```mzsql
CREATE SCHEMA v3;
CREATE TABLE v3.t1
    FROM SOURCE my_source(REFERENCE dbo.t1) WITH (EXCLUDE COLUMNS (b));
```

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}

### B. Drop a column in your upstream SQL Server database

In your upstream SQL Server database, drop the column `b` from the table `t1`:

```sql
ALTER TABLE t1 DROP COLUMN b;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

Dropping the column `b` in SQL Server will not affect `v3.t1` (or on `v1.t1`) in
Materialize. However, the drop affects `v2.t1` and `v2.matview` from our earlier
examples. When the user attempts to read from either, Materialize will report an
error that the source table schema has been altered.

## Optional


### Disable unused capture instance

SQL Server only allows a [maximum of 2 capture
instances](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql?view=sql-server-ver17#----capture_instance)
per table.

To find the capture instance(s) for a table:

```sql
SELECT capture_instance
FROM cdc.change_tables
WHERE source_schema = '<schema>'
  AND source_table = '<table>';
```

After you have fully cut over to the new source version for the table, and you
previously [created a new capture instance for your upstream
table](#b-enable-cdc-for-your-table-under-a-new-capture-instance-in-your-upstream-sql-server-database),
you may wish to disable the old capture instance if it is no longer in use.

{{< warning >}}
Ensure that no other source tables or other applications are using the old
capture instance; otherwise, they will break.
{{< /warning >}}

To disable a capture instance for a table:

```sql
EXEC sys.sp_cdc_disable_table
    @source_schema = '<schema>',
    @source_name = '<source_table_name>',
    @capture_instance = '<old_capture_instance_name>';
```
