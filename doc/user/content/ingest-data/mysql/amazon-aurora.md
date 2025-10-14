---
title: "Ingest data from Amazon Aurora"
description: "How to stream data from Amazon Aurora for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Amazon Aurora"
    identifier: "mysql-amazon-aurora"
---

This page shows you how to stream data from [Amazon Aurora MySQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% mysql-direct/before-you-begin %}}

## A. Configure Amazon Aurora

### 1. Enable GTID-based binlog replication

{{< note >}}
GTID-based replication is supported for Amazon Aurora MySQL v2 and v3 as well
as Aurora Serverless v2.
{{</ note >}}

1. Before creating a source in Materialize, you **must** configure Amazon Aurora
   MySQL for GTID-based binlog replication. Ensure the upstream MySQL database  has been configured for GTID-based binlog replication:

   {{% mysql-direct/ingesting-data/mysql-configs
      gtid_mode_note="In the AWS console, this parameter appears as `gtid-mode`."
      binlog_format_note=" "
   %}}

   For guidance on enabling GTID-based binlog replication in Aurora, see the
   [Amazon Aurora MySQL
    documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/mysql-replication-gtid.html).

1. In addition to the step above, you **must** also ensure that
   [binlog retention](/sql/create-source/mysql/#binlog-retention) is set to a
   reasonable value. To check the current value of the `binlog retention hours`
   configuration parameter, connect to your RDS instance and run:

   ```mysql
   CALL mysql.rds_show_configuration;
   ```

   If the value returned is `NULL`, or less than `168` (i.e. 7 days), run:

   ```mysql
   CALL mysql.rds_set_configuration('binlog retention hours', 168);
   ```

   Although 7 days is a reasonable retention period, we recommend using the
   default MySQL retention period (30 days) in order to not compromise
   Materialize’s ability to resume replication in case of failures or
   restarts.

1. To validate that all configuration parameters are set to the expected values
   after the above configuration changes, run:

    ```mysql
    -- Validate "binlog retention hours" configuration parameter
    CALL mysql.rds_show_configuration;
    ```

    ```mysql
    -- Validate parameter group configuration parameters
    SHOW VARIABLES WHERE variable_name IN (
      'log_bin',
      'binlog_format',
      'binlog_row_image',
      'gtid_mode',
      'enforce_gtid_consistency',
      'replica_preserve_commit_order'
    );
    ```

### 2. Create a user for replication

{{% mysql-direct/create-a-user-for-replication %}}

## B. Configure network security

{{% ingest-data/configure-network-security-intro %}}

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. [Add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Overview.RDSSecurityGroups.html)
    to allow traffic from Materialize IPs.

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

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.

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

[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available (also for PostgreSQL)."

### 3. Monitor the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## D. Explore your data

{{% mysql-direct/next-steps %}}

## Considerations

{{% include-md file="shared-content/mysql-considerations.md" %}}
