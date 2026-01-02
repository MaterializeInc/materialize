---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/postgres/
complexity: beginner
description: Connecting Materialize to a PostgreSQL database for Change Data Capture
  (CDC).
doc_type: reference
keywords:
- never show partial results
- "do not need to deploy Kafka and\n    Debezium"
- PostgreSQL
- SHOW PARTIAL
- 'No additional infrastructure:'
- continually ingest changes
- CREATE DUPLICATE
- CREATE A
- CREATE TABLE
- 'Transactional consistency:'
product_area: Sources
status: experimental
title: PostgreSQL
---

# PostgreSQL

## Purpose
Connecting Materialize to a PostgreSQL database for Change Data Capture (CDC).

If you need to understand the syntax and options for this command, you're in the right place.


Connecting Materialize to a PostgreSQL database for Change Data Capture (CDC).


## Change Data Capture (CDC)

Materialize supports PostgreSQL as a real-time data source. The
[PostgreSQL source](/sql/create-source/postgres//) uses PostgreSQL's
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

## Supported versions and services

The PostgreSQL source requires **PostgreSQL 11+** and is compatible with most
common PostgreSQL hosted services.

## Integration guides

The following integration guides are available:

<!-- Unresolved shortcode: {{% include-md file="shared-content/postgresql-ing... -->

## Considerations

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->


---

## FAQ: PostgreSQL sources


This page addresses common questions and challenges when working with PostgreSQL
sources in Materialize. For general ingestion questions/troubleshooting, see:
- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/).
- [Troubleshooting/FAQ](/ingest-data/troubleshooting/).

## For my trial/POC, what if I cannot use `REPLICA IDENTITY FULL`?

Materialize requires `REPLICA IDENTITY FULL` on PostgreSQL tables to capture all
column values in change events. If for your trial/POC (Proof-of-concept) you cannot modify your existing tables, here are two common alternatives:

- **Outbox Pattern (shadow tables)**

  > **Note:** 

  With the Outbox pattern, you will need to implement dual writes so that all changes apply to both the original and shadow tables.

  

  With the Outbox pattern, you create duplicate "shadow" tables for the ones you
  want to replicate and set the shadow tables to `REPLICA IDENTITY FULL`. You
  can then use these shadow tables for Materialize instead of the originals.

- **Sidecar Pattern**

  > **Note:** 

  With the Sidecar pattern, you will need to keep the sidecar in sync with the
  source database (e.g., via logical replication or ETL processes).

  

  With the Sidecar pattern, you create a separate PostgreSQL instance as an
  integration layer. That is, in the sidecar instance, you recreate the tables
  you want to replicate, setting these tableswith `REPLICA IDENTITY FULL`. You
  can then use the sidecar for Materialiez instead of your primary database.

## What if my table contains data types that are unsupported in Materialize?

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

See also: [PostgreSQL considerations](/ingest-data/postgres/#considerations).


---

## Guide: Handle upstream schema changes with zero downtime


> **Private Preview:** This feature is in private preview.
> **Note:** 
- Changing column types is currently unsupported.

- <!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->


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
```bash

### Connect your source database to Materialize

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

## Create a source using the new syntax

In Materialize, create a source using the updated [`CREATE SOURCE`
syntax](/sql/create-source/postgres-v2/).

```sql
CREATE SOURCE IF NOT EXISTS my_source
    FROM POSTGRES CONNECTION my_connection (PUBLICATION 'mz_source');
```text

Unlike the [legacy syntax](/sql/create-source/postgres/), the new syntax does
not include the `FOR [[ALL] TABLES|SCHEMAS]` clause; i.e., the new syntax does
not create corresponding subsources in Materialize automatically. Instead, the
new syntax requires a separate [`CREATE TABLE ... FROM
SOURCE`](/sql/create-table/), which will create the corresponding tables and
start the snapshotting process. See [Create a table from the
source](#create-a-table-from-the-source).

> **Note:** 
The [legacy syntax](/sql/create-source/postgres/) is still supported. However,
the legacy syntax doesn't support upstream schema changes.


## Create a table from the source
To start ingesting specific tables from your source database, you can create a
table in Materialize. We'll add it into the v1 schema in Materialize.

```sql
CREATE SCHEMA v1;

CREATE TABLE v1.T
    FROM SOURCE my_source(REFERENCE public.T);
```text

Once you've created a table from source, the [initial
snapshot](/ingest-data/#snapshotting) of table `v1.T` will begin.

> **Note:** 

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.


## Create a view on top of the table.

For this guide, add a materialized view `matview` (also in schema `v1`) that
sums column `A` from table `T`.

```sql
CREATE MATERIALIZED VIEW v1.matview AS
    SELECT SUM(A) from v1.T;
```bash

## Handle upstream column addition

This section covers handle upstream column addition.

### A. Add a column in your upstream PostgreSQL database

In your upstream PostgreSQL database, add a new column `B` to the table `T`:

```sql
ALTER TABLE T
    ADD COLUMN B BOOLEAN DEFAULT false;

INSERT INTO T (A, B) VALUES
    (20, true);
```text

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
```text

The [snapshotting](/ingest-data/#snapshotting) of table `v2.T` will begin.
`v2.T` will include columns `A` and `B`.

> **Note:** 

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.


When the new `v2.T` table has finished snapshotting, create a new materialized
view `matview` in the new schema.  Since the new `v2.matview` is referencing the
new `v2.T`, it can reference column `B`:

```sql {hl_lines="4"}
CREATE MATERIALIZED VIEW v2.matview AS
    SELECT SUM(A)
    FROM v2.T
    WHERE B = true;
```bash

## Handle upstream column drop

This section covers handle upstream column drop.

### A. Exclude the column in Materialize

To drop a column safely, in Materialize, first, create a new `v3` schema, and
recreate table `T` in the new schema but exclude the column to drop. In this
example, we'll drop the column B.

```sql
CREATE SCHEMA v3;
CREATE TABLE v3.T
    FROM SOURCE my_source(REFERENCE public.T) WITH (EXCLUDE COLUMNS (B));
```text

> **Note:** 

During the snapshotting, the data ingestion for the other tables associated with
the source is temporarily blocked. As before, you can monitor progress for the
snapshot operation on the overview page for the source in the Materialize
console.


### B. Drop a column in your upstream PostgreSQL database

In your upstream PostgreSQL database, drop the column `B` from the table `T`:

```sql
ALTER TABLE T DROP COLUMN B;
```text

Dropping the column B will have no effect on `v3.T`. However, the drop affects
`v2.T` and `v2.matview` from our earlier examples. When the user attempts to
read from either, Materialize will report an error that the source table schema
has been altered.

## Optional: Swap schemas

When you're ready to fully cut over to the new source version, you can optionally swap the schemas and drop the old objects.

```sql
ALTER SCHEMA v1 SWAP WITH v3;

DROP SCHEMA v3 CASCADE;
```text


---

## Ingest data from AlloyDB


This page shows you how to stream data from [AlloyDB for PostgreSQL](https://cloud.google.com/alloydb)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

If you don't already have an AlloyDB instance, creating one involves several
steps, including configuring your cluster and setting up network connections.
For detailed instructions, refer to the [AlloyDB documentation](https://cloud.google.com/alloydb/docs).

## A. Configure AlloyDB

This section covers a. configure alloydb.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in AlloyDB, see the
[AlloyDB documentation](https://cloud.google.com/datastream/docs/configure-your-source-postgresql-database#configure_alloydb_for_replication).

### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-oth --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your AlloyDB instance is publicly accessible, **you
can skip this step**. For production scenarios, we recommend configuring one of
the network security options below.


#### Cloud

To establish authorized and secure connections to an AlloyDB instance, an
authentication proxy is necessary. Google Cloud Platform provides [a guide](https://cloud.google.com/alloydb/docs/auth-proxy/connect)
to assist you in setting up this proxy and generating a connection string that
can be utilized with Materialize. Further down, we will provide you with a
tailored approach specific to integrating Materialize.

Next, choose the best network configuration for your setup to connect
Materialize with AlloyDB:

- **Allow Materialize IPs:** If your AlloyDB instance is publicly accessible,
    configure your firewall to allow connections from Materialize IP
    addresses.
- **Use an SSH tunnel:** For private networks, use an SSH tunnel to connect
    Materialize to AlloyDB.

#### Self-Managed

To establish authorized and secure connections to an AlloyDB instance, an
authentication proxy is necessary. Google Cloud Platform provides [a guide](https://cloud.google.com/alloydb/docs/auth-proxy/connect)
to assist you in setting up this proxy and generating a connection string that
can be utilized with Materialize. Further down, we will provide you with a
tailored approach specific to integrating Materialize.

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth
   proxy instance from Materialize IPs.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to
    serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth
   proxy instance from the SSH bastion host.

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

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

#### Allow Materialize IPs

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use an SSH tunnel

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

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


---

## Ingest data from Amazon Aurora


This page shows you how to stream data from [Amazon Aurora for PostgreSQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

> **Warning:** 
There is a known issue with Aurora PostgreSQL 16.1 that can cause logical replication to fail with the following error:
- `postgres: sql client error: db error: ERROR: could not map filenumber "base/16402/3147867235" to relation OID`

This is due to a bug in Aurora's implementation of logical replication in PostgreSQL 16.1, where the system fails to correctly fetch relation metadata from the catalogs. If you encounter these errors, you should upgrade your Aurora PostgreSQL instance to a newer minor version (16.2 or later).

For more information, see [this AWS discussion](https://repost.aws/questions/QU4RXUrLNQS_2oSwV34pmwww/error-could-not-map-filenumber-after-aurora-upgrade-to-16-1).


## A. Configure Amazon Aurora

This section covers a. configure amazon aurora.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Aurora, see the
[Aurora documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure).

> **Note:** 
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
logical replication, so it's not possible to use this service with
Materialize.


### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-aws --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your Aurora instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.


#### Cloud

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's security group to allow connections from a set of
    static Materialize IP addresses.

- **Use AWS PrivateLink**: If your database is running in a private network, you
    can use [AWS PrivateLink](/ingest-data/network-security/privatelink/) to
    connect Materialize to the database. For details, see [AWS PrivateLink](/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

#### Self-Managed

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. In the AWS Management Console, [add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** 
Materialize provides a Terraform module that automates the creation and
configuration of resources for an SSH tunnel. For more details, see the
[Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).


1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
    to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      RDS instance.

    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).
      For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
      to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

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

#### Allow Materialize IPs

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use AWS PrivateLink (Cloud-only)

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->
1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->
   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use an SSH tunnel

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/pos... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

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


---

## Ingest data from Amazon RDS


This page shows you how to stream data from [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

## A. Configure Amazon RDS

This section covers a. configure amazon rds.

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
    ```text
    <p></p>

    ```nofmt
            name             | setting
    -------------------------+---------
    rds.logical_replication  | off
    (1 row)
    ```text

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
    ```text
    <p></p>

    ``` nofmt
            name             | setting
    -------------------------+---------
    rds.logical_replication  | on
    (1 row)
    ```text

    If replication is still not enabled, [reboot the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RebootInstance.html).

### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-aws --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your RDS instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.


#### Cloud

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's security group to allow connections from a set of
    static Materialize IP addresses.

- **Use AWS PrivateLink**: If your database is running in a private network, you
    can use [AWS PrivateLink](/ingest-data/network-security/privatelink/) to
    connect Materialize to the database. For details, see [AWS PrivateLink](/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

#### Self-Managed

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. In the AWS Management Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** 
Materialize provides a Terraform module that automates the creation and
configuration of resources for an SSH tunnel. For more details, see the
[Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).


1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
    to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      RDS instance.

    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).
      For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
      to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

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

#### Allow Materialize IPs

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use AWS PrivateLink (Cloud-only)

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->
1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->
   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use an SSH tunnel

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

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


---

## Ingest data from Azure DB


This page shows you how to stream data from [Azure DB for PostgreSQL](https://azure.microsoft.com/en-us/products/postgresql)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

## A. Configure Azure DB

This section covers a. configure azure db.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Azure DB, see the
[Azure documentation](https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-logical#set-up-your-server).

### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-oth --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your AzureDB instance is publicly accessible, **you
can skip this step**. For production scenarios, we recommend configuring one of
the network security options below.


#### Cloud

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

#### Self-Managed

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from Materialize IPs.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch an Azure VM with a static public IP address](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/virtual-network-deploy-static-pip-arm-portal?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from the SSH bastion host.

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

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

#### Allow Materialize IPs

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
    file="examples/ingest_data... -->

#### Use an SSH tunnel

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

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


---

## Ingest data from Google Cloud SQL


This page shows you how to stream data from [Google Cloud SQL for PostgreSQL](https://cloud.google.com/sql/postgresql)
to Materialize using the[PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

## A. Configure Google Cloud SQL

This section covers a. configure google cloud sql.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Cloud SQL, see the [Cloud SQL
documentation](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance).

### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-oth --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your Google Cloud SQL instance is publicly
accessible, **you can skip this step**. For production scenarios, we recommend
configuring one of the network security options below.


#### Cloud

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

#### Self-Managed

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. Update your Google Cloud SQL firewall rules to allow traffic from Materialize
   IPs.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

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

#### Allow Materialize IPs

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use an SSH tunnel

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

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


---

## Ingest data from Neon


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
    ```text

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


---

## Ingest data from self-hosted PostgreSQL


This page shows you how to stream data from a self-hosted PostgreSQL database to
Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

## A. Configure PostgreSQL

This section covers a. configure postgresql.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical
replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.
Enable your PostgreSQL's logical replication.

1. As a _superuser_, use `psql` (or your preferred SQL client) to connect to
   your PostgreSQL database.

1. Check if logical replication is enabled; that is, check if the `wal_level` is
   set to `logical`:

    ```postgres
    SHOW wal_level;
    ```text

1. If `wal_level` setting is **not** set to `logical`:

    1. In the  database configuration file (`postgresql.conf`), set `wal_level`
       value to `logical`.

    1. Restart the database in order for the new `wal_level` to take effect.
       Restarting can affect database performance.

    1. In the SQL client connected to PostgreSQL, verify that replication is now
  enabled (i.e., verify `wal_level` setting is set to `logical`).

        ```postgres
        SHOW wal_level;
        ```bash

### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-oth --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your PostgreSQL instance is publicly
accessible, **you can skip this step**. For production scenarios, we recommend
configuring one of the network security options below.


#### Cloud

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

#### Self-Managed

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. Update your database firewall rules to allow traffic from Materialize.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an VM to
serve as an SSH bastion host, configure the bastion host to allow traffic only
from Materialize, and then configure your database's private network to allow
traffic from the bastion host.

1. Launch a VM to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

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

#### Allow Materialize IPs

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use AWS PrivateLink (Cloud-only)

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use an SSH tunnel

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

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


---

## PostgreSQL CDC using Kafka and Debezium


> **Warning:** 
You can use [Debezium](https://debezium.io/) to propagate Change Data Capture
(CDC) data to Materialize from a PostgreSQL database, but we **strongly
recommend** using the native [PostgreSQL](/sql/create-source/postgres/) source
instead.


Change Data Capture (CDC) allows you to track and propagate changes in a
PostgreSQL database to downstream consumers based on its Write-Ahead Log
(`WAL`). In this guide, we’ll cover how to use Materialize to create and
efficiently maintain real-time views with incrementally updated results
on top of CDC data.

## Kafka + Debezium

You can use [Debezium](https://debezium.io/) and the [Kafka source](/sql/create-source/kafka/#using-debezium)
to propagate CDC data from PostgreSQL to Materialize in the unlikely event that
using the[native PostgreSQL source](/sql/create-source/postgres/) is not an
option. Debezium captures row-level changes resulting from `INSERT`, `UPDATE`
and `DELETE` operations in the upstream database and publishes them as events
to Kafka using Kafka Connect-compatible connectors.

### A. Configure database

**Minimum requirements:** PostgreSQL 11+

Before deploying a Debezium connector, you need to ensure that the upstream
database is configured to support [logical replication](https://www.postgresql.org/docs/current/logical-replication.html).

#### Self-hosted

As a _superuser_:

1. Check the [`wal_level` configuration](https://www.postgresql.org/docs/current/wal-configuration.html)
   setting:

    ```postgres
    SHOW wal_level;
    ```text

    The default value is `replica`. For CDC, you'll need to set it to `logical`
    in the database configuration file (`postgresql.conf`). Keep in mind that
    changing the `wal_level` requires a restart of the PostgreSQL instance and
    can affect database performance.

1. Restart the database so all changes can take effect.

#### AWS RDS

We recommend following the [AWS RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication)
documentation for detailed information on logical replication configuration and
best practices.

As a _superuser_ (`rds_superuser`):

1. Create a custom RDS parameter group and associate it with your instance. You
   will not be able to set custom parameters on the default RDS parameter groups.

1. In the custom RDS parameter group, set the `rds.logical_replication` static
   parameter to `1`.

1. Add the egress IP addresses associated with your Materialize region to the
   security group of the RDS instance. You can find these addresses by querying
   the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

#### AWS Aurora

> **Note:** 
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
logical replication, so it's not possible to use this service with
Materialize.


We recommend following the [AWS Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure)
documentation for detailed information on logical replication configuration and
best practices.

As a _superuser_:

1. Create a DB cluster parameter group for your instance using the following
   settings:

    Set **Parameter group family** to your version of Aurora PostgreSQL.

    Set **Type** to **DB Cluster Parameter Group**.

1. In the DB cluster parameter group, set the `rds.logical_replication` static
   parameter to `1`.

1. In the DB cluster parameter group, set reasonable values for
   `max_replication_slots`, `max_wal_senders`, `max_logical_replication_workers`,
   and `max_worker_processes parameters`  based on your expected usage.

1. Add the egress IP addresses associated with your Materialize region to the
   security group of the DB instance. You can find these addresses by querying the
   `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

#### Azure DB

We recommend following the [Azure DB for PostgreSQL](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical#pre-requisites-for-logical-replication-and-logical-decoding)
documentation for detailed information on logical replication configuration and
best practices.

1. In the Azure portal, or using the Azure CLI, [enable logical replication](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#set-up-your-server)
   for the PostgreSQL instance.

1. Add the egress IP addresses associated with your Materialize region to the
   list of allowed IP addresses under the "Connections security" menu. You can
   find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

#### Cloud SQL

We recommend following the [Cloud SQL for PostgreSQL](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance)
documentation for detailed information on logical replication configuration and
best practices.

As a _superuser_ (`cloudsqlsuperuser`):

1. In the Google Cloud Console, enable logical replication by setting the
`cloudsql.logical_decoding` configuration parameter to `on`.

1. Add the egress IP addresses associated with your Materialize region to the
list of allowed IP addresses. You can find these addresses by querying the
`mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.

Once logical replication is enabled:

1. Grant enough privileges to ensure Debezium can operate in the database. The
   specific privileges will depend on how much control you want to give to the
   replication user, so we recommend following the [Debezium documentation](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-replication-user-privileges).

1. If a table that you want to replicate has a **primary key** defined, you can
   use your default replica identity value. If a table you want to replicate
   has **no primary key** defined, you must set the replica identity value to
   `FULL`:

    ```postgres
    ALTER TABLE repl_table REPLICA IDENTITY FULL;
    ```text

    This setting determines the amount of information that is written to the WAL
    in `UPDATE` and `DELETE` operations. Setting it to `FULL` will include the
    previous values of all the table’s columns in the change events.

    As a heads up, you should expect a performance hit in the database from
    increased CPU usage. For more information, see the
    [PostgreSQL documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html).

### B. Deploy Debezium

**Minimum requirements:** Debezium 1.5+

Debezium is deployed as a set of Kafka Connect-compatible connectors, so you
first need to define a SQL connector configuration and then start the connector
by adding it to Kafka Connect.

> **Warning:** 
If you deploy the PostgreSQL Debezium connector in [Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-debezium.html),
you **must** override the default value of `After-state only` to `false`.


#### Debezium 1.5+

1. Create a connector configuration file and save it as `register-postgres.json`:

    ```json
    {
        "name": "your-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name":"pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname" : "postgres",
            "database.server.name": "pg_repl",
            "table.include.list": "public.table1",
            "publication.autocreate.mode":"filtered",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schemas.enable": false
        }
    }
    ```text

    You can read more about each configuration property in the [Debezium documentation](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-connector-properties).
    By default, the connector writes events for each table to a Kafka topic
    named `serverName.schemaName.tableName`.

1. Start the PostgreSQL Debezium connector using the configuration file:

    ```bash
    export CURRENT_HOST='<your-host>'

    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-postgres.json
    ```text

1. Check that the connector is running:

    ```bash
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```text

    The first time it connects to a PostgreSQL server, Debezium takes a
    [consistent snapshot](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed into your
    Kafka topic:

    ```bash
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic pg_repl.public.table1
    ```bash

#### Debezium 2.0+

1. Beginning with Debezium 2.0.0, Confluent Schema Registry support is not
   included in the Debezium containers. To enable the Confluent Schema Registry
   for a Debezium container, install the following Confluent Avro converter JAR
   files into the Connect plugin directory:

    * `kafka-connect-avro-converter`
    * `kafka-connect-avro-data`
    * `kafka-avro-serializer`
    * `kafka-schema-serializer`
    * `kafka-schema-registry-client`
    * `common-config`
    * `common-utils`

    You can read more about this in the [Debezium documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html#deploying-confluent-schema-registry-with-debezium-containers).

1. Create a connector configuration file and save it as
   `register-postgres.json`:

    ```json
    {
        "name": "your-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name":"pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname" : "postgres",
            "topic.prefix": "pg_repl",
            "schema.include.list": "public",
            "table.include.list": "public.table1",
            "publication.autocreate.mode":"filtered",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://<scheme-registry>:8081",
            "value.converter.schema.registry.url": "http://<scheme-registry>:8081",
            "value.converter.schemas.enable": false
        }
    }
    ```text

    You can read more about each configuration property in the [Debezium documentation](https://debezium.io/documentation/reference/2.4/connectors/postgresql.html#postgresql-connector-properties).
    By default, the connector writes events for each table to a Kafka topic
    named `serverName.schemaName.tableName`.

1. Start the Debezium Postgres connector using the configuration file:

    ```bash
    export CURRENT_HOST='<your-host>'

    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-postgres.json
    ```text

1. Check that the connector is running:

    ```bash
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```text

    The first time it connects to a Postgres server, Debezium takes a
    [consistent snapshot](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed into your
    Kafka topic:

    ```bash
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic pg_repl.public.table1
    ```bash

### C. Create a source


Debezium emits change events using an envelope that contains detailed
information about upstream database operations, like the `before` and `after`
values for each record. To create a source that interprets the
[Debezium envelope](/sql/create-source/kafka/#using-debezium) in Materialize:

```mzsql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'pg_repl.public.table1')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

This allows you to replicate tables with `REPLICA IDENTITY DEFAULT`, `INDEX`, or
`FULL`.

### D. Create a view on the source

<!-- Unresolved shortcode: {{% ingest-data/ingest-data-kafka-debezium-view %}... -->

### E. Create an index on the view

<!-- Unresolved shortcode: {{% ingest-data/ingest-data-kafka-debezium-index %... -->