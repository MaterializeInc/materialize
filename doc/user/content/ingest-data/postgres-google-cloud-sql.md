---
title: "Ingest data from Google Cloud SQL"
description: "How to stream data from Google Cloud SQL for PostgreSQL to Materialize"
menu:
  main:
    parent: "postgresql"
    name: "Google Cloud SQL"
    weight: 20
---

This page shows you how to stream data from [Google Cloud SQL for PostgreSQL](https://cloud.google.com/sql/postgresql)
to Materialize using the[PostgreSQL source](/sql/create-source/postgres/).

## Before you begin

{{% postgres-direct/before-you-begin %}}

## Step 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

For guidance on enabling logical replication in Cloud SQL, see the [Cloud SQL
documentation](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance).

## Step 2. Create a publication

{{% postgres-direct/create-a-publication-other %}}

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

1. Update your Google Cloud SQL firewall rules to allow traffic from each IP
   address from the previous step.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

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

    1. In the [SQL Shell](https://console.materialize.com/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```sql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
    IP address from the previous step.

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.

{{< /tab >}}

{{< /tabs >}}

## Step 4. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% postgres-direct/create-a-cluster %}}

## Step 5. Start ingesting data

Now that you've configured your database network and created an ingestion
cluster, you can connect Materialize to your PostgreSQL database and start
ingesting data. The exact steps depend on your networking configuration, so
start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}
{{% postgres-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}
{{% postgres-direct/ingesting-data/use-ssh-tunnel %}}
{{< /tab >}}

{{< /tabs >}}

## Step 6. Check the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

## Step 7. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## Next steps

{{% postgres-direct/next-steps %}}
