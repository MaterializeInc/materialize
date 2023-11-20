---
title: "Ingest data from AlloyDB"
description: "How to stream data from AlloyDB to Materialize"
menu:
  main:
    parent: "postgresql"
    name: "AlloyDB for PostgreSQL"
    weight: 25
---

This page shows you how to stream data from [AlloyDB](https://cloud.google.com/alloydb) to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

## Before you begin

{{% postgres-direct/before-you-begin %}}

## Step 1. Create an AlloyDB instance

Creating an AlloyDB instance involves several steps, including configuring your cluster and setting up network connections. For detailed instructions, refer to the [AlloyDB documentation](https://cloud.google.com/alloydb/docs).

## Step 2. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) protocol to track changes in your database and propagate them to Materialize.

For guidance on enabling logical replication in AlloyDB, see the [AlloyDB documentation](https://cloud.google.com/datastream/docs/configure-your-source-postgresql-database#configure_alloydb_for_replication).

## Step 3. Set up the AlloyDB Auth Proxy

To establish authorized and secure connections to an AlloyDB instance, an authentication proxy is necessary. The Google Cloud Platform offers a guide [here](https://cloud.google.com/alloydb/docs/auth-proxy/connect) to assist you in setting up this proxy and generating a connection string that can be utilized with Materialize. Further down, we will provide you with a tailored approach specific to integrating Materialize.

## Step 4. Configure network security

Choose the best network configuration for your setup to connect Materialize with AlloyDB:

- **Allow Materialize IPs:** If your AlloyDB instance is publicly accessible, configure your firewall to allow connections from Materialize IP addresses.
- **Use an SSH tunnel:** For private networks, use an SSH tunnel to connect Materialize to AlloyDB.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the `psql` shell connected to Materialize, find the static egress IP addresses for the Materialize region you are running in:

    ```sql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth proxy instance from each IP address from the previous step.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

To create an SSH tunnel from Materialize to your database, you launch an instance to serve as an SSH bastion host, configure the bastion host to allow traffic only from Materialize, and then configure your database's private network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your database.
    - Add a key pair and note the username. You'll use this username when connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address). You'll use this IP address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the `psql` shell connected to Materialize, get the static egress IP addresses for the Materialize region you are running in:

        ```sql
        SELECT * FROM mz_egress_ips;
        ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each IP address from the previous step.

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth proxy instance from the SSH bastion host.

{{< /tab >}}

{{< /tabs >}}

## Step 5. Create an ingestion cluster

{{% postgres-direct/create-an-ingestion-cluster %}}

## Step 6. Start ingesting data

With the network configured and an ingestion pipeline in place, connect Materialize to your AlloyDB instance and begin the data ingestion process.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}
{{% postgres-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}
{{% postgres-direct/ingesting-data/use-ssh-tunnel %}}
{{< /tab >}}

{{< /tabs >}}

## Step 6. Monitor the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

## Step 7. Optimize the cluster performance

{{% postgres-direct/right-size-the-cluster %}}

## Next steps

{{% postgres-direct/next-steps %}}
