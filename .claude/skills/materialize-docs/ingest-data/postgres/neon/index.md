---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/postgres/neon/
complexity: beginner
description: How to stream data from Neon to Materialize
doc_type: reference
keywords:
- Ingest data from Neon
- autoscaling
- 'Warning:'
- SELECT YOUR
- SHOW WAL_LEVEL
- ALTER TABLE
- CREATE A
- 'Tip:'
- branching
- bottomless storage
product_area: Sources
status: stable
title: Ingest data from Neon
---

# Ingest data from Neon

## Purpose
How to stream data from Neon to Materialize

If you need to understand the syntax and options for this command, you're in the right place.


How to stream data from Neon to Materialize


> **Tip:** 


[Neon](https://neon.tech) is a fully managed serverless PostgreSQL provider. It
separates compute and storage to offer features like **autoscaling**,
**branching** and **bottomless storage**.

This page shows you how to stream data from a Neon database to Materialize using
the [PostgreSQL source](/sql/create-source/postgres/).

## Before you begin

- Make sure you have [a Neon account](https://neon.tech).

- Make sure you have access to your Neon instance via [`psql`](https://www.postgresql.org/docs/current/app-psql.html)
  or the SQL editor in the Neon Console.

## A. Configure Neon

The steps in this section are specific to Neon. You can run them by connecting
to your Neon database using a `psql` client or the SQL editor in the Neon
Console.

### 1. Enable logical replication

> **Warning:** 
Enabling logical replication applies **globally** to all databases in your Neon
project, and **cannot be reverted**. It also **restarts all computes**, which
means that any active connections are dropped and have to reconnect.


Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

As a first step, you need to make sure logical replication is enabled in Neon.

1. Select your project in the Neon Console.

2. On the Neon **Dashboard**, select **Settings**.

3. Select **Logical Replication**.

4. Click **Enable** to enable logical replication.

You can verify that logical replication is enabled by running:

```sql
SHOW wal_level;
```text

The result should be:

```text
 wal_level
-----------
 logical
```bash

### 2. Create a publication and a replication user

Once logical replication is enabled, the next step is to create a publication
with the tables that you want to replicate to Materialize. You'll also need a
user for Materialize with sufficient privileges to manage replication.

1. For each table that you want to replicate to Materialize, set the
   [replica identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY)
   to `FULL`:

   ```postgres
   ALTER TABLE <table1> REPLICA IDENTITY FULL;
   ```text

   ```postgres
   ALTER TABLE <table2> REPLICA IDENTITY FULL;
   ```text

   `REPLICA IDENTITY FULL` ensures that the replication stream includes the
    previous data of changed rows, in the case of `UPDATE` and `DELETE`
    operations. This setting enables Materialize to ingest Neon data with
    minimal in-memory state. However, you should expect increased disk usage in
    your Neon database.

2. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html)
   with the tables you want to replicate:

   _For specific tables:_

    ```postgres
    CREATE PUBLICATION mz_source FOR TABLE <table1>, <table2>;
    ```text

    _For all tables in the database:_

    ```postgres
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```text

    The `mz_source` publication will contain the set of change events generated
    from the specified tables, and will later be used to ingest the replication
    stream.

    Be sure to include only the tables you need. If the publication includes
    additional tables, Materialize will waste resources on ingesting and then
    immediately discarding the data.

3. Create a dedicated user for Materialize, if you don't already have one. The default user created with your Neon project and users created using the
Neon CLI, Console or API are granted membership in the [`neon_superuser`](https://neon.tech/docs/manage/roles#the-neonsuperuser-role)
role, which has the required `REPLICATION` privilege.

   While you can use the default user for replication, we recommend creating a
   dedicated user for security reasons.

    #### Neon CLI

Use the [`roles create` CLI command](https://neon.tech/docs/reference/cli-roles)
to create a new role.

```bash
neon roles create --name materialize
```bash

#### Neon Console

1. Navigate to the [Neon Console](https://console.neon.tech).
2. Select a project.
3. Select **Branches**.
4. Select the branch where you want to create the role.
5. Select the **Roles & Databases** tab.
6. Click **Add Role**.
7. In the role creation dialog, specify the role name as "materialize".
8. Click **Create**. The role is created, and you are provided with the
password for the role.

#### API

Use the [`roles` endpoint](https://api-docs.neon.tech/reference/createprojectbranchrole)
to create a new role.

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
```text

4. Grant the user the required permissions on the schema(s) you want to
   replicate:

   ```postgres
   GRANT USAGE ON SCHEMA public TO materialize;

   GRANT SELECT ON ALL TABLES IN SCHEMA public TO materialize;

   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO materialize;
   ```text

   Granting `SELECT ON ALL TABLES IN SCHEMA` instead of on specific tables
   avoids having to add privileges later if you add tables to your
   publication.

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your Neon instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend using [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
to limit the IP addresses that can connect to your Neon instance.


#### Cloud

If you use Neon's [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
feature to limit the IP addresses that can connect to your Neon instance, you
will need to allow inbound traffic from Materialize IP addresses.

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to
   Materialize, run the following query to find the static egress IP addresses,
   for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```text

2. In your Neon project, add the IPs to your **IP Allow** list:

   1. Select your project in the Neon Console.
   2. On the Neon **Dashboard**, select **Settings**.
   3. Select **IP Allow**.
   4. Add each Materialize IP address to the list.

#### Self-Managed

> **Note:** 
If you are prototyping and your Neon instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend using [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
to limit the IP addresses that can connect to your Neon instance.


If you use Neon's [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
feature to limit the IP addresses that can connect to your Neon instance, you
will need to allow inbound traffic from Materialize IP addresses.

2. In your Neon project, add the IPs to your **IP Allow** list:

   1. Select your project in the Neon Console.
   2. On the Neon **Dashboard**, select **Settings**.
   3. Select **IP Allow**.
   4. Add Materialize IP addresses to the list.

## C. Ingest data in Materialize

The steps in this section are specific to Materialize. You can run them in the
[Materialize console's SQL Shell](/console/) or your
preferred SQL client connected to Materialize.

### 1. (Optional) Create a cluster

> **Note:** 
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-cluster --> --> -->

### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.

1. Run the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
   password for the `materialize` PostgreSQL user you created [earlier](#2-create-a-publication-and-a-replication-user):

    ```mzsql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```text

    You can access the password for your Neon user from
    the **Connection Details** widget on the Neon **Dashboard**.


2. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER '<user_name>',
      PASSWORD SECRET pgpass,
      SSL MODE 'require',
      DATABASE '<database>'
    );
    ```text

    You can find the connection details for your replication user in
    the **Connection Details** widget on the Neon **Dashboard**. A Neon
    connection string looks like this:

    ```bash
    postgresql://materialize:AbC123dEf@ep-cool-darkness-123456.us-east-2.aws.neon.tech/dbname?sslmode=require
    ```

    - Replace `<host>` with your Neon hostname
      (e.g., `ep-cool-darkness-123456.us-east-2.aws.neon.tech`).
    - Replace `<role_name>` with the dedicated replication user
      (e.g., `materialize`).
    - Replace `<database>` with the name of the database containing the tables
      you want to replicate to Materialize (e.g., `dbname`).

### 3. Start ingesting data

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/pos... -->

### 4. Monitor the ingestion status

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/check-the-ingestion-stat --> --> -->

### 5. Right-size the cluster

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/right-size-the-cluster --> --> -->

## D. Explore your data

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/next-steps --> --> -->

## Considerations

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->