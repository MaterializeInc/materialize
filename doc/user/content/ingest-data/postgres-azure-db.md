---
title: "Ingest data from Azure DB"
description: "How to stream data from Azure DB for PostgreSQL to Materialize"
menu:
  main:
    parent: "postgresql"
    name: "Azure DB"
    weight: 15
---

This page shows you how to stream data from [Azure DB for PostgreSQL](https://azure.microsoft.com/en-us/products/postgresql) to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

## Before you begin

{{% postgres-direct/before-you-begin %}}

  {{% postgres-direct/postgres-schema-changes %}}

## Step 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) protocol to track changes in your database and propagate them to Materialize.

For guidance on enabling logical replication in Azure DB, see the [Azure documentation](https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-logical#set-up-your-server).

## Step 2. Create a publication

{{% postgres-direct/create-a-publication-other %}}

## Step 3. Configure network security

There are various ways to configure your database's network to allow Materialize to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can configure your database's firewall to allow connections from a set of static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the `psql` shell connected to Materialize, find the static egress IP addresses for the Materialize region you are running in:

    ```sql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql) to allow traffic from each IP address from the previous step.
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

To create an SSH tunnel from Materialize to your database, you launch an instance to serve as an SSH bastion host, configure the bastion host to allow traffic only from Materialize, and then configure your database's private network to allow traffic from the bastion host.

1. [Launch an Azure VM with a static public IP address](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/virtual-network-deploy-static-pip-arm-portal?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json) to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your database.
    - Add a key pair and note the username. You'll use this username when connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the `psql` shell connected to Materialize, get the static egress IP addresses for the Materialize region you are running in:

        ```sql
        SELECT * FROM mz_egress_ips;
        ```

    1. Update your SSH bastion host's [firewall rules](https://learn.microsoft.com/en-us/azure/virtual-network/tutorial-filter-network-traffic?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json) to allow traffic from each IP address from the previous step.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql) to allow traffic from the SSH bastion host.

{{< /tab >}}

{{< /tabs >}}

## Step 4. Create an ingestion cluster

{{% postgres-direct/create-an-ingestion-cluster %}}

## Step 5. Start ingesting data

Now that you've configured your database network and created an ingestion cluster, you can connect Materialize to your PostgreSQL database and start ingesting data. The exact steps depend on your networking configuration, so start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the `psql` shell connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` PostgreSQL user you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a connection object with access and authentication details for Materialize to use:

    ```sql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER 'materialize',
      PASSWORD SECRET pgpass,
      SSL MODE 'require',
      DATABASE '<database>'
      );
    ```

    - Replace `<host>` with your Azure DB endpoint.

    - Replace `<database>` with the name of the database containing the tables you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize to your Azure instance and start ingesting data from the publication you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    To ingest data from specific schemas or tables in your publication, use `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR ALL TABLES`.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. In the `psql` shell connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH tunnel connection:

    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT`> with the public IP address and port of the SSH bastion host you created [earlier](#step-3-configure-network-security).
    - Replace `<SSH_BASTION_USER>` with the username for the key pair you created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:

    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

1. Log in to your SSH bastion host and add Materialize's public keys to the `authorized_keys` file, for example:

    ```sh
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Back in the `psql` shell connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` PostgreSQL user you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```sql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER 'materialize',
      PASSWORD SECRET pgpass,
      DATABASE '<database>',
      SSH TUNNEL ssh_connection
      );
    ```

    - Replace `<host>` with your Azure DB endpoint.

    - Replace `<database>` with the name of the database containing the tables you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize to your Azure instance and start ingesting data from the publication you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    To ingest data from specific schemas or tables in your publication, use `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR ALL TABLES`.

{{< /tab >}}

{{< /tabs >}}

## Step 6. Check the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

## Step 7. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## Next steps

{{% postgres-direct/next-steps %}}
