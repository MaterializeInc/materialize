---
title: "Ingest data from Amazon RDS"
description: "How to stream data from Amazon RDS for PostgreSQL to Materialize"
aliases:
  - /guides/cdc-postgres/
  - /integrations/cdc-postgres/
  - /connect-sources/cdc-postgres-direct/
  - /ingest-data/postgres-amazon-rds/
menu:
  main:
    parent: "postgresql"
    name: "Amazon RDS"
    identifier: "pg-amazon-rds"
---

This page shows you how to stream data from [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% postgres-direct/before-you-begin %}}

## A. Configure Amazon RDS

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

As a first step, you need to make sure logical replication is enabled.

1. As a user with the `rds_superuser` role, use `psql` (or your preferred SQL
   client) to connect to your database.

1. Check if logical replication is enabled:

    ```postgres
    SELECT name, setting
      FROM pg_settings
      WHERE name = 'rds.logical_replication';
    ```
    <p></p>

    ```nofmt
            name             | setting
    -------------------------+---------
    rds.logical_replication  | off
    (1 row)
    ```

    - If logical replication is off, continue to the next step.

    - If logical replication is already on, skip to [Create a publication and a
      Materialize user section](#2-create-a-publication-and-a-replication-user).

1. Using the AWS Management Console, [create a DB parameter group in RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.Creating.html).

    - Set **Parameter group family** to your PostgreSQL version.
    - Set **Type** to **DB Parameter Group**.
    - Set **Engine type** to PostgreSQL.

1. Edit the new parameter group and set the `rds.logical_replication` parameter
   to `1`.

1. [Associate the DB parameter group with your database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.Associating.html).

    Use the **Apply Immediately** option to immediately reboot your database and
    apply the change. Keep in mind that rebooting the RDS instance can affect
    database performance.

    Do not move on to the next step until the database **Status**
    is **Available** in the RDS Console.

1. Back in the SQL client connected to PostgreSQL, verify that replication is
   now enabled:

    ```postgres
    SELECT name, setting
      FROM pg_settings
      WHERE name = 'rds.logical_replication';
    ```
    <p></p>

    ``` nofmt
            name             | setting
    -------------------------+---------
    rds.logical_replication  | on
    (1 row)
    ```

    If replication is still not enabled, [reboot the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RebootInstance.html).

### 2. Create a publication and a replication user

{{% postgres-direct/create-a-publication-aws %}}

## B. Configure network security

{{% self-managed/network-connection %}}

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% postgres-direct/create-a-cluster %}}

### 2. Start ingesting data

The following provides general steps for creating a connection using user
credentials. The exact steps depend on your networking configuration, such as
whether you require SSL connections or are using an SSH tunnel, etc. For
additional connection options, such as SSL options, see [`CREATE
CONNECTION`](/sql/create-connection/).

1. In [Materialize console's SQL Shell](/console/), or
   your preferred SQL client connected to Materialize, use the [`CREATE
   SECRET`](/sql/create-secret/) command to securely store the password for the
   `materialize` PostgreSQL user you created
   [earlier](#2-create-a-publication-and-a-replication-user):

    ```mzsql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER 'materialize',
      PASSWORD SECRET pgpass,
      SSL MODE 'require',
      DATABASE '<database>'
      );
    ```

    - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select
      your database in the RDS Console, and look under **Connectivity &
      security**.

    - Replace `<database>` with the name of the database containing the tables
      you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your RDS instance and start ingesting data from the publication you
   created [earlier](#2-create-a-publication-and-a-replication-user):

    ```mzsql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    By default, the source will be created in the active cluster; to use a
    different cluster, use the `IN CLUSTER` clause. To ingest data from
    specific schemas or tables in your publication, use `FOR SCHEMAS
    (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR
    ALL TABLES`.

1. After source creation, you can handle upstream [schema changes](/sql/create-source/postgres/#schema-changes)
   for specific replicated tables using the [`ALTER SOURCE...{ADD | DROP} SUBSOURCE`](/sql/alter-source/#context)
   syntax.

### 3. Monitor the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## Next steps

{{% postgres-direct/next-steps %}}
