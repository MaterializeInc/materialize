---
title: "Ingest data from Azure DB for MySQL"
description: "How to stream data from Azure DB for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Azure DB"
    indentifier: "azure-db-mysql"
    weight: 15
---

This page shows you how to stream data from [Azure DB for MySQL](https://azure.microsoft.com/en-us/products/MySQL)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

## Before you begin

{{% mysql-direct/before-you-begin %}}

## Step 1. Enable GTID-based replication

{{< note >}}
GTID-based replication is supported for Azure DB for MySQL [flexible server](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/overview-single).
It is **not supported** for single server databases.
{{</ note >}}

Before creating a source in Materialize, you **must** configure Azure DB for
MySQL for GTID-based binlog replication. This requires the following
configuration changes:

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   |
`enforce_gtid_consistency`       | `ON`   |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

For guidance on enabling GTID-based binlog replication in Azure DB, see the
[Azure documentation](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/how-to-data-in-replication?tabs=shell%2Ccommand-line#configure-the-source-mysql-server).

## Step 2. Create a user for replication

{{% mysql-direct/create-a-user-for-replication %}}

## Step 3. Configure network security

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

    ```sql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from each IP address from the previous step.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

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

    1. In the [SQL Shell](https://console.materialize.com/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```sql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's [firewall rules](https://learn.microsoft.com/en-us/azure/virtual-network/tutorial-filter-network-traffic?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
    to allow traffic from each IP address from the previous step.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from the SSH bastion host.

{{< /tab >}}

{{< /tabs >}}

## Step 4. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your MySQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% mysql-direct/create-a-cluster %}}

## Step 5. Start ingesting data

[//]: # "TODO(morsapaes) MySQL connections support multiple SSL modes. We should
adapt to that, rather than just state SSL MODE REQUIRED."

Now that you've configured your database network, you can connect Materialize to
your MySQL database and start ingesting data. The exact steps depend on your
networking configuration, so start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` MySQL user
   you created [earlier](#step-2-create-a-user-for-replication):

    ```sql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```sql
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    - Replace `<host>` with your Azure DB endpoint.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Azure instance and start ingesting data:

    ```sql
    CREATE SOURCE mz_source
      FROM mysql CONNECTION mysql_connection
      FOR ALL TABLES;
    ```

    - By default, the source will be created in the active cluster; to use a
      different cluster, use the `IN CLUSTER` clause.

    - To ingest data from specific schemas or tables, use the `FOR SCHEMAS
      (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options
      instead of `FOR ALL TABLES`.

    - To handle unsupported data types, use the `TEXT COLUMNS` or `IGNORE
      COLUMNS` options. Check out the [reference documentation](/sql/create-source/mysql/#supported-types)
      for guidance.

1. After source creation, you can handle upstream [schema changes](/sql/create-source/mysql/#schema-changes)
   by dropping and recreating the source.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#ssh-tunnel)
   command to create an SSH tunnel connection:

    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT`> with the public IP
      address and port of the SSH bastion host you created [earlier](#step-3-configure-network-security).

    - Replace `<SSH_BASTION_USER>` with the username for the key pair you
      created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection you just
   created:

    ```sql
    SELECT
        mz_connections.name,
        mz_ssh_tunnel_connections.*
    FROM
        mz_connections JOIN
        mz_ssh_tunnel_connections USING(id)
    WHERE
        mz_connections.name = 'ssh_connection';
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

    ```sql
    VALIDATE CONNECTION ssh_connection;
    ```

    If no validation error is returned, move to the next step.

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
   password for the `materialize` MySQL user you created [earlier](#step-2-create-a-user-for-replication):

    ```sql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
   another connection object, this time with database access and authentication
   details for Materialize to use:

    ```sql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection,
    );
    ```

    - Replace `<host>` with your Azure DB endpoint.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
to your Azure instance and start ingesting data:

    ```sql
    CREATE SOURCE mz_source
      FROM mysql CONNECTION mysql_connection
      FOR ALL TABLES;
    ```

    - By default, the source will be created in the active cluster; to use a
      different cluster, use the `IN CLUSTER` clause.

    - To ingest data from specific schemas or tables, use the `FOR SCHEMAS
      (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options
      instead of `FOR ALL TABLES`.

    - To handle unsupported data types, use the `TEXT COLUMNS` or `IGNORE
      COLUMNS` options. Check out the [reference documentation](/sql/create-source/mysql/#supported-types)
      for guidance.

{{< /tab >}}

{{< /tabs >}}

[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available(also for PostgreSQL)."

## Step 6. Check the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

## Step 7. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## Next steps

{{% mysql-direct/next-steps %}}
