---
title: "Ingest data from self-hosted SQL Server"
description: "How to stream data from self-hosted SQL Server database to Materialize"
menu:
  main:
    parent: "sql-server"
    name: "Self-hosted"
    identifier: "sql-server-self-hosted"
aliases:
  - /ingest-data/cdc-sql-server/
---

This page shows you how to stream data from a self-hosted SQL Server database
to Materialize using the [SQL Server source](/sql/create-source/sql-server/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% sql-server-direct/before-you-begin %}}

## A. Configure SQL Server

### 1. Enable Change-Data-Capture for the database

Before creating a source in Materialize, you **must** configure your SQL Server
database for change data capture. This requires running the following stored procedures:

```sql
EXEC sys.sp_cdc_enable_db;
```

For guidance on enabling Change Data Capture, see the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-db-transact-sql).

### 2. Enable `SNAPSHOT` transaction isolation.

In addition to enabling Change-Data-Capture you **must** also enable your
`SNAPSHOT` transaction isolation in your SQL Server database. This requires running
the following SQL:

```sql
ALTER DATABASE <DATABASE_NAME> SET ALLOW_SNAPSHOT_ISOLATION ON;
```

For guidance on enabling `SNAPSHOT` transaction isolation, see the [SQL Server documentation](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server).

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your SQL Server instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.
{{< /note >}}

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your database firewall rules to allow traffic from each IP address
   from the previous step.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

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

    1. In the [SQL Shell](https://console.materialize.com/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
       IP address from the previous step.

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.

{{< /tab >}}

{{< /tabs >}}

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your SQL Server
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% sql-server-direct/create-a-cluster %}}

### 2. Start ingesting data

Now that you've configured your database network, you can connect Materialize to
your SQL Server database and start ingesting data. The exact steps depend on your
networking configuration, so start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}
{{% sql-server-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< /tabs >}}


[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available(also for PostgreSQL)."

### 3. Right-size the cluster

{{% sql-server-direct/right-size-the-cluster %}}

## D. Explore your data

{{% sql-server-direct/next-steps %}}

## Considerations

{{% include-md file="shared-content/sql-server-considerations.md" %}}
