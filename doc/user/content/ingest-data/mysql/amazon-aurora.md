---
title: "Ingest data from Amazon Aurora MySQL"
description: "How to stream data from Amazon Aurora for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Amazon Aurora"
    identifier: "mysql-amazon-aurora"
---

{{< private-preview />}}

This page shows you how to stream data from [Amazon Aurora MySQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% mysql-direct/before-you-begin %}}

## Step 1. Enable GTID-based binlog replication

{{< note >}}
GTID-based replication is supported for Amazon Aurora MySQL v2 and v3, as well
as Aurora Serverless v2.
{{</ note >}}

Before creating a source in Materialize, you **must** configure Amazon Aurora
MySQL for GTID-based binlog replication. This requires enabling binlog replication and
the following additional configuration changes:

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`log_bin`                        | `ON`   |
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   | In the AWS console, this parameter appears as `gtid-mode`.
`enforce_gtid_consistency`       | `ON`   |
`binlog retention hours`         | 168    |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

For guidance on enabling GTID-based binlog replication in Aurora, see the
[Amazon Aurora MySQL documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/mysql-replication-gtid.html).

## Step 2. Create a user for replication

{{% mysql-direct/create-a-user-for-replication %}}

## Step 3. Configure network security

{{< note >}}
Support for AWS PrivateLink connections is planned for a future release.
{{< /note >}}

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's security group to allow connections from a set of
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

1. [Add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Overview.RDSSecurityGroups.html)
    for each IP address from the previous step.

    In each rule:

    - Set **Type** to **MySQL**.
    - Set **Source** to the IP address in CIDR notation.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of resources for an SSH tunnel. For more details, see the
[Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).
{{</ note >}}

1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
    to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      Amazon Aurora MySQL instance.

    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).
      For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
      to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [SQL Shell](https://console.materialize.com/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. For each static egress IP, [add an inbound rule](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
       to your SSH bastion host's security group.

        In each rule:

        - Set **Type** to **MySQL**.
        - Set **Source** to the IP address in CIDR notation.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.

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
{{% mysql-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}
{{% mysql-direct/ingesting-data/use-ssh-tunnel %}}
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
