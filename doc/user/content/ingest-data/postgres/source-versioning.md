---
title: "Guide: Handle upstream schema changes with zero downtime"
description: "How to add a column, or drop a column, from your source PostgreSQL database, without any downtime in Materialize"

menu:
  main:
    parent: "postgresql"
    weight: 85

---

{{< private-preview />}}
{{< note >}}
- Changing column types is currently unsupported.

- {{% include-example file="examples/create_table/example_postgres_table"
example="syntax-version-requirement" %}}
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

### Set up a PostgreSQL database

For this guide, setup a PostgreSQL 11+ database. In your PostgreSQL, create a
table `T` and populate:

```sql
CREATE TABLE T (
    A INT
);

INSERT INTO T (A) VALUES
    (10);
```

### Connect your source database to Materialize

{{% include-from-yaml data="postgres_source_details"
name="postgres-source-prereq" %}}

## Create a source using the new syntax

In Materialize, create a source using the updated [`CREATE SOURCE`
syntax](/sql/create-source/postgres-v2/).

```sql
CREATE SOURCE IF NOT EXISTS my_source
    FROM POSTGRES CONNECTION my_connection (PUBLICATION 'mz_source');
```

Unlike the [legacy syntax](/sql/create-source/postgres/), the new syntax does
not include the `FOR [[ALL] TABLES|SCHEMAS]` clause; i.e., the new syntax does
not create corresponding subsources in Materialize automatically. Instead, the
new syntax requires a separate [`CREATE TABLE ... FROM
SOURCE`](/sql/create-table/), which will create the corresponding tables and
start the snapshotting process. See [Create a table from the
source](#create-a-table-from-the-source).

{{< note >}}
The [legacy syntax](/sql/create-source/postgres/) is still supported. However,
the legacy syntax doesn't support upstream schema changes.
{{< /note >}}

## Create a table from the source
To start ingesting specific tables from your source database, you can create a
table in Materialize. We'll add it into the v1 schema in Materialize.

```sql
CREATE SCHEMA v1;

CREATE TABLE v1.T
    FROM SOURCE my_source(REFERENCE public.T);
```

Once you've created a table from source, the [initial
snapshot](/ingest-data/#snapshotting) of table `v1.T` will begin.

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}

## Create a view on top of the table.

For this guide, add a materialized view `matview` (also in schema `v1`) that
sums column `A` from table `T`.

```sql
CREATE MATERIALIZED VIEW v1.matview AS
    SELECT SUM(A) from v1.T;
```

## Handle upstream column addition

### A. Add a column in your upstream PostgreSQL database

In your upstream PostgreSQL database, add a new column `B` to the table `T`:

```sql
ALTER TABLE T
    ADD COLUMN B BOOLEAN DEFAULT false;

INSERT INTO T (A, B) VALUES
    (20, true);
```

This operation will have no immediate effect in Materialize. In Materialize,
`v1.T` will continue to ingest only column `A`. The materialized view
`v1.matview` will continue to have access to column `A` as well.

### B. Incorporate the new column in Materialize

To incorporate the new column into Materialize, create a new `v2` schema and
recreate the table in the new schema:

```sql
CREATE SCHEMA v2;

CREATE TABLE v2.T
    FROM SOURCE my_source(REFERENCE public.T);
```

The [snapshotting](/ingest-data/#snapshotting) of table `v2.T` will begin.
`v2.T` will include columns `A` and `B`.

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}


When the new `v2.T` table has finished snapshotting, create a new materialized
view `matview` in the new schema.  Since the new `v2.matview` is referencing the
new `v2.T`, it can reference column `B`:

```sql {hl_lines="4"}
CREATE MATERIALIZED VIEW v2.matview AS
    SELECT SUM(A)
    FROM v2.T
    WHERE B = true;
```

## Handle upstream column drop

### A. Exclude the column in Materialize

To drop a column safely, in Materialize, first, create a new `v3` schema, and
recreate table `T` in the new schema but exclude the column to drop. In this
example, we'll drop the column B.

```sql
CREATE SCHEMA v3;
CREATE TABLE v3.T
    FROM SOURCE my_source(REFERENCE public.T) WITH (EXCLUDE COLUMNS (B));
```

{{< note >}}

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.

{{< /note >}}

### B. Drop a column in your upstream PostgreSQL database

In your upstream PostgreSQL database, drop the column `B` from the table `T`:

```sql
ALTER TABLE T DROP COLUMN B;
```

Dropping the column B will have no effect on `v3.T`. However, the drop affects
`v2.T` and `v2.matview` from our earlier examples. When the user attempts to
read from either, Materialize will report an error that the source table schema
has been altered.

## Optional: Swap schemas

When you're ready to fully cut over to the new source version, you can optionally swap the schemas and drop the old objects.

```sql
ALTER SCHEMA v1 SWAP WITH v3;

DROP SCHEMA v3 CASCADE;
```
