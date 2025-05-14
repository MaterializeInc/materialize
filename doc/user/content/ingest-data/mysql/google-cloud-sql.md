---
title: "Ingest data from Google Cloud SQL"
description: "How to stream data from Google Cloud SQL for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Google Cloud SQL"
    identifier: "mysql-google-cloudsql"
---

This page shows you how to stream data from [Google Cloud SQL for MySQL](https://cloud.google.com/sql/MySQL)
to Materialize using the[MySQL source](/sql/create-source/mysql/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% mysql-direct/before-you-begin %}}

## A. Configure Google Cloud SQL

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure Google Cloud SQL
for MySQL for GTID-based binlog replication. This requires the following
configuration changes:

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`log_bin`                        | `ON`   |
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   |
`enforce_gtid_consistency`       | `ON`   |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

For guidance on enabling GTID-based binlog replication in Cloud SQL, see the [Cloud SQL documentation](https://cloud.google.com/sql/docs/mysql/replication).

### 2. Create a user for replication

{{% mysql-direct/create-a-user-for-replication %}}

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your Google Cloud SQL instance is publicly
accessible, **you can skip this step**. For production scenarios, we recommend
configuring one of the network security options below.
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

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
    IP address from the previous step.

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.

{{< /tab >}}

{{< /tabs >}}

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your MySQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% mysql-direct/create-a-cluster %}}

### 2. Start ingesting data

[//]: # "TODO(morsapaes) MySQL connections support multiple SSL modes. We should
adapt to that, rather than just state SSL MODE REQUIRED."

Now that you've configured your database network, you can connect Materialize to
your MySQL database and start ingesting data. The exact steps depend on your
networking configuration, so start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}
{{% mysql-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}
{{% mysql-direct/ingesting-data/use-ssh-tunnel %}}
{{< /tab >}}

{{< /tabs >}}

### 3. Monitor the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## D. Explore your data

{{% mysql-direct/next-steps %}}

## Considerations

{{% include-md file="shared-content/mysql-considerations.md" %}}
