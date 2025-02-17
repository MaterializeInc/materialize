---
title: "Ingest data from Amazon Aurora"
description: "How to stream data from Amazon Aurora for PostgreSQL to Materialize"
aliases:
  - /ingest-data/postgres-amazon-aurora/
menu:
  main:
    parent: "postgresql"
    name: "Amazon Aurora"
    identifier: "pg-amazon-aurora"
---

This page shows you how to stream data from [Amazon Aurora for PostgreSQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the[PostgreSQL source](/sql/create-source/postgres/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% postgres-direct/before-you-begin %}}

## A. Configure Amazon Aurora

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Aurora, see the
[Aurora documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure).

{{< note >}}
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
logical replication, so it's not possible to use this service with
Materialize.
{{</ note >}}

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

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to Materialize, use the [`CREATE
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

    - Replace `<host>` with the **Writer** endpoint for your Aurora database. To
      find the endpoint, select your database in the AWS Management Console,
      then click the **Connectivity & security** tab and look for the endpoint
      with type **Writer**.

        <div class="warning">
            <strong class="gutter">WARNING!</strong>
            You must use the <strong>Writer</strong> endpoint for the database. Using a <strong>Reader</strong> endpoint will not work.
        </div>

    - Replace `<database>` with the name of the database containing the tables
      you want to replicate to Materialize.

    For additional connection options, such as SSL options, see [`CREATE
    CONNECTION`](/sql/create-connection/).

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Aurora instance and start ingesting data from the publication you
   created [earlier](#2-create-a-publication-and-a-replication-user).

    ```mzsql
    CREATE SOURCE mz_source
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
