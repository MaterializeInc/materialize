---
title: "Guide: Handling upstreams schema changes with 0 downtime"
description: "How to handle schema changes in your upstream source systems without any downtime"
---

{{< private-preview >}}
New in version 26.0.0. Currently in private preview. This feature is currently supported for Postgres, with additional source types coming soon. Changing column types is currently unsupported.
{{</ private-preview >}}

Materialize allows you to handle certain types of upstream schema changes seamlessly. This guide explains how you can incorporate a new column from an upstream database, as well as drop a column in an upstream database, without any downtime in Materialize.

## Prerequisites
If you've never used Materialize before, start with our [guide to getting started](/get-started/quickstart/) to learn how to connect a database to Materialize.

### Setup a source PostgreSQL database
For this guide, setup a source PostgreSQL database, and setup a table with a single column:
```sql
CREATE TABLE T (
    A INT
);

INSERT INTO T (A) VALUES 
    (10);
```

### Connect your source database to Materialize
Follow our [guide to ingest data from a PostgreSQL database](ingest-data/postgres/). Make sure to setup logical replication in your PostgreSQL database, create a publication and replication user, create a cluster in Materialize, and finally create a connection in Materialize.

## Create a source using our updated syntax
Create a source in Materialize using our updated [CREATE SOURCE syntax](/sql/create-source/postgres-v2/).

```sql
CREATE SOURCE IF NOT EXISTS my_source
    FROM POSTGRES CONNECTION my_connection (PUBLICATION mz_source)
```

If you've used Materialize before, you'll notice a subtle change to the syntax. The [legacy syntax](/sql/create-source/postgres/) is still supported. However, the legacy syntax doesn't support upstream schema changes.

## Create a table from the source
To start ingesting specific tables from your source database, you can create a table in Materialize. We'll add it into the v1 schema for this guide.

```sql
CREATE SCHEMA v1;

CREATE TABLE v1.T
    FROM SOURCE my_source(REFERENCE public.T);
```

Once you've created a table, the [initial snapshot](ingest-data/#snapshotting) of table T will begin. No other tables will be ingested into Materialize. As before, you can monitor progress for the snaphot operation on the overview page for the source in the Materialize console.

You can create a materialized view on top of this table. You might notice that we're adding this view into a `v1` schema.
```sql
CREATE MATERIALIZED VIEW v1.matview AS
    SELECT SUM(A) from v1.T
```

## Add a column to your upstream database
In this example, we'll add a new column, `B`. Run the following command in your source PostgreSQL database:
```sql
ALTER TABLE T
    ADD COLUMN B BOOLEAN DEFAULT false

INSERT INTO T (A, B) VALUES 
    (20, true);
```

This operation will have no immediate effect on Materialize. `v1.T` will continue to ingest only column A. The materialized view `v1.matview` will continue to have only column A as well.

## Incorporate the new column
To incorporate the new column into Materialize, you can create a new table from your source database. We'll insert this into the `v2` schema.

```sql
CREATE SCHEMA v2;

CREATE TABLE v2.T
    FROM SOURCE my_source(REFERENCE public.T);
```

`v2.T` will now include columns A and B.

Now, you can create a new materialized view which incorporates the new column:
```sql
CREATE MATERIALIZED VIEW v2.matview AS
    SELECT SUM(A)
    FROM v2.T
    WHERE B = true
```

## Drop a column
To drop a column safely, we'll setup a new version of the source which excludes the column. In this example, we'll drop the column B.

```sql
CREATE SCHEMA v3;
CREATE TABLE v3.T
    FROM SOURCE my_source(REFERENCE public.T) WITH (EXCLUDE COLUMNS (B));
```

You can drop the column from your source postgres database:
```sql
ALTER TABLE T DROP COLUMN B;
```

Dropping the column B will have no effect on `v3.T`.

## Optional: Swap schemas
When you're ready to fully cut over to the new source version, you can optionally swap the schemas and drop the old objects.

```sql
ALTER SCHEMA v1 SWAP WITH v3;

DROP SCHEMA v1 CASCADE;
```