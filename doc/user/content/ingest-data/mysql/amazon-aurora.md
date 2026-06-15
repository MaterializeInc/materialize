---
title: "Ingest data from Amazon Aurora"
description: "How to stream data from Amazon Aurora for MySQL to Materialize"
menu:
    main:
        parent: "mysql"
        name: "Amazon Aurora"
        identifier: "mysql-amazon-aurora"
        weight: 10
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
   MySQL for GTID-based binlog replication. Ensure the upstream MySQL database has been configured for GTID-based binlog replication:

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
      'replica_preserve_commit_order',
      'binlog_row_metadata' -- must be "FULL" to use new CREATE SOURCE syntax
    );
    ```

### 2. Create a user for replication

{{% mysql-direct/create-a-user-for-replication %}}

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your Aurora instance is publicly accessible, **you
can skip this step**. For production scenarios, we recommend configuring one of
the network security options below.
{{< /note >}}

{{< tabs >}}

{{< tab "Cloud">}}

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
  configure your database's security group to allow connections from a set of
  static Materialize IP addresses.

- **Use AWS PrivateLink**: If your database is running in a private network, you
  can use [AWS PrivateLink](/ingest-data/network-security/privatelink/) to
  connect Materialize to the database. For details, see [AWS PrivateLink](/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your database is running in a private network, you
  can use an SSH tunnel to connect Materialize to the database.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the [SQL Shell](/console/), or your preferred SQL
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

{{< tab "Use AWS PrivateLink">}}

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your Aurora instance without exposing traffic to the public
internet. To use AWS PrivateLink, you create a network load balancer in the
same VPC as your Aurora instance and a VPC endpoint service that Materialize
connects to. The VPC endpoint service then routes requests from Materialize to
Aurora via the network load balancer.

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).
{{</ note >}}

{{< include-headless-with file="/headless/privatelink/privatelink-aws-rds-aurora-setup" instance="Aurora" endpoint="AURORA_ENDPOINT" port="3306" >}}

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
    1. In the [SQL Shell](/console/), or your preferred
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

{{< /tab >}}

{{< tab "Self-Managed">}}

{{% include-md
file="shared-content/self-managed/configure-network-security-intro.md" %}}

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

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Overview.RDSSecurityGroups.html)
   to allow traffic from the SSH bastion host.
    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.

{{< /tab >}}

{{< /tabs >}}

{{< /tab >}}

{{< /tabs >}}

## C. Ingest data in Materialize

### 1. (Optional) Create a source cluster

{{< note >}}
If you are prototyping and already have a cluster to host your MySQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% mysql-direct/create-a-cluster %}}

### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}
{{% mysql-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< tab "Use AWS PrivateLink (Cloud-only)">}}
{{% mysql-direct/ingesting-data/use-aws-privatelink %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}
{{% mysql-direct/ingesting-data/use-ssh-tunnel %}}
{{< /tab >}}

{{< /tabs >}}

### 3. Start ingesting data

{{% include-example file="examples/ingest_data/mysql/create_source_cloud" example="ingest-data-step" %}}

[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available (also for PostgreSQL)."

### 4. Monitor the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

### 5. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## D. Explore your data

{{% mysql-direct/next-steps %}}

## Considerations

{{% include-headless "/headless/mysql-considerations" %}}
