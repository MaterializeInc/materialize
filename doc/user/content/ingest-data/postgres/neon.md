---
title: "Ingest data from Neon"
description: "How to stream data from Neon Postgres to Materialize"
aliases:
  - /ingest-data/postgres-neon/
menu:
  main:
    parent: "postgresql"
    name: "Neon"
    identifier: "pg-neon"
---

This page shows you how to stream data from [Neon Postgres](https://neon.tech) database, to Materialize using the [PostgreSQL source](/sql/create-source/postgres/). 

Neon is a fully managed serverless Postgres provider, separating compute and storage to offer features like autoscaling, branching and bottomless storage.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% postgres-direct/before-you-begin %}}

## A. Configure Neon

Note that the steps in this section are specific to Neon. You can run them by connecting to your Neon database using a `psql` client or the `SQL Editor` in the Neon Console.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Neon:

1. Select your project in the Neon Console.
2. On the Neon **Dashboard**, select **Settings**.
3. Select **Logical Replication**.
4. Click **Enable** to enable logical replication.

{{< note >}}
Enabling logical replication modifies the PostgreSQL `wal_level` configuration parameter, changing it from `replica` to `logical` for all databases in your Neon project. Once the `wal_level` setting is changed to `logical`, it cannot be reverted. Enabling logical replication also restarts all computes in your Neon project, meaning that active connections will be dropped and have to reconnect.
{{< /note >}}

You can verify that logical replication is enabled by running the following query:

```sql
SHOW wal_level;
```

The result should be:

```
 wal_level
-----------
 logical
```

### 2. Create a publication and a replication user

1. Set the [replica identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY) to `FULL` for each table that you want to replicate to Materialize:

   ```sql
   ALTER TABLE <table1> REPLICA IDENTITY FULL;
   ```

   `REPLICA IDENTITY FULL` ensures that the replication stream includes the previous data of changed rows, in the case of `UPDATE` and `DELETE` operations. This setting allows Materialize to ingest Postgres data with minimal in-memory state.


2. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) with the tables you want to replicate:

   ```sql
   CREATE PUBLICATION mz_source FOR TABLE <table1>, <table2>;
   ```

   The `mz_source` publication will contain the set of change events generated from the specified tables and will later be used to ingest the replication stream.

   {{< note >}}
   Be sure to include only the tables you need. If the publication includes additional tables, Materialize wastes resources on ingesting and then immediately discarding the data from those tables.
   {{< /note >}}

3. Create a dedicated Postgres role for replication:

   The default Postgres role created with your Neon project and roles created using the Neon CLI, Console, or API are granted membership in the [neon_superuser](https://neon.tech/docs/manage/roles#the-neonsuperuser-role) role, which has the required `REPLICATION` privilege. While you can also use the default role for replication, we recommend creating a dedicated role for security reasons.

    {{< tabs >}}

{{< tab "CLI">}}

The following CLI command creates a role. To view the CLI documentation for this command, see [Neon CLI commands — roles](https://neon.tech/docs/reference/cli-roles)

```bash
neon roles create --name materialize
```

{{< /tab >}}

{{< tab "Console">}}

To create a role in the Neon Console:

1. Navigate to the [Neon Console](https://console.neon.tech).
2. Select a project.
3. Select **Branches**.
4. Select the branch where you want to create the role.
5. Select the **Roles & Databases** tab.
6. Click **Add Role**.
7. In the role creation dialog, specify the role name as "materialize".
8. Click **Create**. The role is created, and you are provided with the password for the role.

{{< /tab >}}

{{< tab "API">}}

The following Neon API method creates a role. To view the API documentation for this method, refer to the [Neon API reference](https://api-docs.neon.tech/reference/createprojectbranchrole).

```bash
curl 'https://console.neon.tech/api/v2/projects/<project_id>/branches/<branch_id>/roles' \
-H 'Accept: application/json' \
-H "Authorization: Bearer $NEON_API_KEY" \
-H 'Content-Type: application/json' \
-d '{
"role": {
    "name": "materialize"
}
}' | jq
```

{{< /tab >}}

    {{< /tabs >}}


4. Grant schema access to your Postgres role:

   ```sql
   GRANT USAGE ON SCHEMA public TO materialize;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO materialize;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO materialize;
   ```

   Granting `SELECT ON ALL TABLES IN SCHEMA` instead of naming the specific tables avoids having to add privileges later if you add tables to your publication.

## B. (Optional) Configure network security

If you are prototyping or your Neon instance is publicly accessible, **you can skip this step**. For production scenarios, we recommend configuring the network security option below.

### Allow Materialize IPs

If you use Neon's **IP Allow** feature to limit IP addresses that can connect to Neon, you will need to allow inbound traffic from Materialize IP addresses.

1. In the [Materialize console's SQL Shell](https://console.materialize.com/), or your preferred SQL client connected to Materialize, run the following query to find the static egress IP addresses, for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

2. In your Neon project, add the IPs to your **IP Allow** list:

   1. Select your project in the Neon Console.
   2. On the Neon **Dashboard**, select **Settings**.
   3. Select **IP Allow**.
   4. Add each Materialize IP address to the list.

   For more details regarding the **IP Allow** feature, see [Neon documentation](https://neon.tech/docs/introduction/ip-allow).

## C. Ingest data in Materialize

Note that the steps in this section are specific to Materialize. You can run them by using a `psql` client connected to Materialize, or from the [Materialize console's SQL Shell](https://console.materialize.com/).

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your PostgreSQL source (e.g. `quickstart`), **you can skip this step**. For production scenarios, we recommend separating your workloads into multiple clusters for [resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% postgres-direct/create-a-cluster %}}

### 2. Start ingesting data

Now that you've configured your database network and created an ingestion cluster, you can connect Materialize to your Neon Postgres database and start ingesting data.

1. Run the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` Postgres role your created in the Neon project earlier:

    ```mzsql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

    You can access the password for your Neon Postgres role from the **Connection Details** widget on the Neon **Dashboard**.


2. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a connection object with access and authentication details for Materialize to use:

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

    You can find the connection details for your replication role in the **Connection Details** widget on the Neon **Dashboard**. A Neon connection string looks like this:

    ```text
    postgresql://alex:AbC123dEf@ep-cool-darkness-123456.us-east-2.aws.neon.tech/dbname?sslmode=require
    ```

    - Replace `<host>` with your Neon hostname (e.g., `ep-cool-darkness-123456.us-east-2.aws.neon.tech`)
    - Replace `<role_name>` with the name of your Postgres role (e.g., `alex`)
    - Replace `<database>` with the name of the database containing the tables you want to replicate to Materialize (e.g., `dbname`)

3. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize to your Neon Postgres database and start ingesting data from the publication you created earlier:

    ```mzsql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    By default, the source will be created in the active cluster; to use a different cluster, use the `IN CLUSTER` clause. To ingest data from specific schemas or tables in your publication, use `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR ALL TABLES`.

    After source creation, you can handle upstream [schema changes](/sql/create-source/postgres/#schema-changes)
    for specific replicated tables using the [`ALTER SOURCE...{ADD | DROP} SUBSOURCE`](/sql/alter-source/#context) 
    syntax.

### 3. Monitor the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## Next steps

{{% postgres-direct/next-steps %}}
