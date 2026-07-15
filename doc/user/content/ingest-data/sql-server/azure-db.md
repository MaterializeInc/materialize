---
title: "Ingest data from Azure SQL Database"
description: "How to stream data from Azure SQL Database to Materialize"
menu:
  main:
    parent: "sql-server"
    name: "Azure SQL Database"
    identifier: "sql-server-azure-db"
    weight: 70
---

This page shows you how to stream data from [Azure SQL Database](https://azure.microsoft.com/en-us/products/azure-sql/database)
to Materialize using the [SQL Server source](/sql/create-source/sql-server/).

{{< note >}}
This guide covers **Azure SQL Database**, the single-database service. For
**Azure SQL Managed Instance**, which runs a SQL Server Agent and exposes
`msdb`, follow the [self-hosted SQL Server guide](/ingest-data/sql-server/self-hosted/)
instead.
{{</ note >}}

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

- Make sure Change Data Capture (CDC) is available on your Azure SQL Database.
  CDC has compute requirements and is not supported on lower service tiers. CDC
  must also be enabled per database and per table. See [Azure SQL documentation](
  https://learn.microsoft.com/en-us/azure/azure-sql/database/change-data-capture-overview?view=azuresql)
  for details on service tiers and CDC configuration.

- Ensure you have access to your database via the [`sqlcmd` client](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility),
  or your preferred SQL client, as a member of `db_owner`.

## A. Configure Azure SQL Database

{{< note >}}

To configure Azure SQL Database for data ingestion into Materialize, you must
connect to the database you want to replicate as a member of `db_owner`, which
can enable CDC and create/manage the user, role, and privileges.

{{</ note >}}

### 1. Create a Materialize user in Azure SQL Database.

Azure SQL Database is a single-database service. Because it does not provide
reusable server logins or access to the master database for granting
server-scoped permissions, create a [contained database user](https://learn.microsoft.com/en-us/sql/relational-databases/security/contained-database-users-making-your-database-portable) directly in the
database you want to replicate.

Connect to the database you want to replicate as a member of `db_owner`, then
create the user (replace `<PASSWORD>` with your own password):

```sql
CREATE USER materialize WITH PASSWORD = '<PASSWORD>';
```

Create a gating role for the capture instances and add the user to it:

```sql
CREATE ROLE materialize_role;
ALTER ROLE materialize_role ADD MEMBER materialize;
```

Grant the privileges Materialize needs:

```sql
-- SELECT on the replicated tables and the CDC change tables.
ALTER ROLE db_datareader ADD MEMBER materialize;

-- Read access to the transaction-state views used to track replication
-- progress, in place of the server-scoped VIEW SERVER STATE used for
-- self-hosted SQL Server.
GRANT VIEW DATABASE STATE TO materialize;
```

{{< note >}}
Unlike self-hosted SQL Server, no explicit grants are issued on the
`sys.fn_cdc_*` functions or the `INFORMATION_SCHEMA` views. They are executable
and readable by default.
{{</ note >}}

### 2. Enable Change-Data-Capture for the database.

Azure SQL Database drives CDC from an internal scheduler, so **no SQL Server
Agent is required**. Enabling CDC requires the database to be on a service tier
that supports it and that you are a member of `db_owner`.

Connect to the database you want to replicate and run:

```sql
EXEC sys.sp_cdc_enable_db;
```

For guidance on enabling Change Data Capture on Azure SQL Database, see the
[Azure documentation](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server?view=azuresqldb-current).

### 3. Enable `SNAPSHOT` transaction isolation.

Enable `SNAPSHOT` transaction isolation for the database. Because Azure SQL
Database connections cannot switch databases, use the `CURRENT` keyword to target
the connected database:

```sql
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
```

### 4. Enable Change-Data-Capture for the tables.

Enable Change Data Capture for each table you wish to replicate, gated by the
role you created above (replace `<SCHEMA_NAME>` and `<TABLE_NAME>` with your
schema and table names):

```sql
EXEC sys.sp_cdc_enable_table
  @source_schema = '<SCHEMA_NAME>',
  @source_name = '<TABLE_NAME>',
  @role_name = 'materialize_role',
  @supports_net_changes = 0;
```

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your Azure SQL Database is publicly accessible, **you
can skip this step**. For production scenarios, we recommend configuring one of
the network security options below.
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

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your [Azure SQL Database firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from each IP address from the previous step.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

This assumes your Azure SQL Database is already reachable over a private IP in a
virtual network via an [Azure Private Endpoint](https://learn.microsoft.com/en-us/azure/azure-sql/database/private-endpoint-overview?view=azuresql),
with the `privatelink.database.windows.net` DNS zone integrated so
`<server>.database.windows.net` resolves to the private IP.

To create the SSH tunnel, you launch an instance to serve as an SSH bastion host
in that network and configure the bastion host to allow traffic from Materialize.
The bastion forwards traffic to the database's private endpoint.

1. [Launch a Linux VM with a static public IP address](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/quick-create-portal)
to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same virtual network as
      the private endpoint (or a peered network).
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic from Materialize.

    1. In the [SQL Shell](/console/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's [firewall rules](https://learn.microsoft.com/en-us/azure/virtual-network/tutorial-filter-network-traffic?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
    to allow SSH traffic from each IP address from the previous step.

1. Set the server's [connection policy](https://learn.microsoft.com/en-us/azure/azure-sql/database/connectivity-architecture?view=azuresql#connection-policy)
   to **Proxy**:

    ```sh
    az sql server conn-policy update \
      --resource-group <resource-group> \
      --server <server-name> \
      --connection-type Proxy
    ```

    With the `Redirect` policy, the gateway tells the client to reconnect
    directly to the backend node on a high port, which bypasses the single port
    the SSH tunnel forwards. `Proxy` keeps all traffic on the gateway at port
    1433, which is the port the tunnel forwards.

    If the connection policy is left as `Redirect`, creating the source or
    validating the connection fails with an error like:

    ```text
    Server requested a connection to an alternative address:
    `<backend-node>.worker.database.windows.net:<high-port>`
    ```

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

### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration. Azure SQL Database **requires an encrypted
connection**, so the SQL Server connection must specify `SSL MODE 'required'`.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}
{{% sql-server-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#ssh-tunnel)
   command to create an SSH tunnel connection:

    ```mzsql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
      address and port of the SSH bastion host you created
      [earlier](#b-optional-configure-network-security).

    - Replace `<SSH_BASTION_USER>` with the username for the key pair you created
      for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:

    ```mzsql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
   `authorized_keys` file, for example:

    ```sh
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
   connection you created using the [`VALIDATE CONNECTION`](/sql/validate-connection)
   command:

    ```mzsql
    VALIDATE CONNECTION ssh_connection;
    ```

    If no validation error is returned, move to the next step.

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
   password for the `materialize` user
   [you created](#1-create-a-materialize-user-in-azure-sql-database):

    ```mzsql
    CREATE SECRET sqlserver_pass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create the
   SQL Server connection, routed through the SSH tunnel. Azure SQL Database
   requires an encrypted connection, so include `SSL MODE 'required'`:

    ```mzsql
    CREATE CONNECTION sqlserver_connection TO SQL SERVER (
        HOST '<host>',
        PORT 1433,
        USER 'materialize',
        PASSWORD SECRET sqlserver_pass,
        DATABASE '<database>',
        SSL MODE 'required',
        SSH TUNNEL ssh_connection
    );
    ```

    - Replace `<host>` with your Azure SQL Database endpoint, and `<database>`
      with the database you'd like to connect to.

{{< /tab >}}

{{< /tabs >}}

### 3. Start ingesting data

{{< note >}}
For a new SQL Server source, if none of the replicating tables
are receiving write queries, snapshotting may take up to an additional 5 minutes
to complete. For details, see [snapshot latency for inactive databases](#snapshot-latency-for-inactive-databases)
{{</ note >}}

{{% include-example file="examples/ingest_data/sql_server/create_source_cloud" example="create-source" %}}

{{% include-example file="examples/ingest_data/sql_server/create_source_cloud" example="create-source-options" %}}

{{% include-example file="examples/ingest_data/sql_server/create_source_cloud"
example="schema-changes" %}}

### 4. Right-size the cluster

{{% sql-server-direct/right-size-the-cluster %}}

## D. Explore your data

{{% sql-server-direct/next-steps %}}

## Considerations

{{% include-headless "/headless/sql-server-considerations" %}}
