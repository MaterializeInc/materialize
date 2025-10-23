---
title: "Ingest data from Azure DB"
description: "How to stream data from Azure DB for PostgreSQL to Materialize"
aliases:
  - /ingest-data/postgres-azure-db/
menu:
  main:
    parent: "postgresql"
    name: "Azure DB"
    identifier: "pg-azuredb"
    weight: 25
---

This page shows you how to stream data from [Azure DB for PostgreSQL](https://azure.microsoft.com/en-us/products/postgresql)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% postgres-direct/before-you-begin %}}

## A. Configure Azure DB

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Azure DB, see the
[Azure documentation](https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-logical#set-up-your-server).

### 2. Create a publication and a replication user

{{% postgres-direct/create-a-publication-other %}}

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your AzureDB instance is publicly accessible, **you
can skip this step**. For production scenarios, we recommend configuring one of
the network security options below.
{{</ note >}}

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

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to Materialize, find the static egress
   IP addresses for the Materialize region you are running in:

    ```mzsql
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

    1. In the [Materialize console's SQL
       Shell](/console/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's [firewall rules](https://learn.microsoft.com/en-us/azure/virtual-network/tutorial-filter-network-traffic?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
    to allow traffic from each IP address from the previous step.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from the SSH bastion host.

{{< /tab >}}

{{< /tabs >}}

## C. Ingest data in Materialize

### 1. (Recommended) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% postgres-direct/create-a-cluster %}}

### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ips_cloud"
   example="create-secret" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ips_cloud"
   example="create-connection" indent="true" %}}

   {{% include-example
    file="examples/ingest_data/postgres/create_connection_ips_cloud"
    example="create-connection-options-general" indent="true" %}}

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-ssh-tunnel-connection" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-ssh-tunnel-connection-options" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="get-public-keys-general" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="login-to-ssh-bastion-host" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="validate-ssh-tunnel-connection" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-secret" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-connection" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-connection-options-general" indent="true" %}}

{{< /tab >}}
{{< /tabs >}}

### 3. Start ingesting data

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-options" %}}

{{% include-example file="examples/ingest_data/postgres/create_source_cloud"
example="schema-changes" %}}

### 4. Monitor the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

### 5. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## D. Explore your data

{{% postgres-direct/next-steps %}}

## Considerations

{{% include-md file="shared-content/postgres-considerations.md" %}}
