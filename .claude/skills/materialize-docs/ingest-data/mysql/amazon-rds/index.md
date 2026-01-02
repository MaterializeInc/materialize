---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/mysql/amazon-rds/
complexity: beginner
description: How to stream data from Amazon RDS for MySQL to Materialize
doc_type: reference
keywords:
- Ingest data from Amazon RDS
- must
- SHOW VARIABLES
- DB Parameter Group
- Type
- CREATE A
- SELECT THE
- Parameter group family
- 'Tip:'
- CREATE AN
product_area: Sources
status: stable
title: Ingest data from Amazon RDS
---

# Ingest data from Amazon RDS

## Purpose
How to stream data from Amazon RDS for MySQL to Materialize

If you need to understand the syntax and options for this command, you're in the right place.


How to stream data from Amazon RDS for MySQL to Materialize


This page shows you how to stream data from [Amazon RDS for MySQL](https://aws.amazon.com/rds/mysql/)
to Materialize using the [MySQL source](/sql/create-source/mysql).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/before-you-begin --> --> -->

## A. Configure Amazon RDS

This section covers a. configure amazon rds.

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure Amazon RDS for
GTID-based binlog replication. For guidance on enabling GTID-based
binlog replication in RDS, see the [Amazon RDS for MySQL documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-replication-gtid.html).

1. [Enable automated backups in your RDS instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html#USER_WorkingWithAutomatedBackups.Enabling)
by setting the backup retention period to a value greater than `0`.  This
enables binary logging (`log_bin`).

1. [Create a custom RDS parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Creating).

    - Set **Parameter group family** to your MySQL version.
    - Set **Type** to **DB Parameter Group**.

1. Edit the new parameter group to set the configuration parameters to the
   following values:

   <!-- Unresolved shortcode: {{% mysql-direct/ingesting-data/mysql-configs
    ... -->


1. [Associate the RDS parameter group to your database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Associating).

    Use the **Apply Immediately** option. The database must be rebooted in order
    for the parameter group association to take effect. Keep in mind that
    rebooting the RDS instance can affect database performance.

    Do not move on to the next step until the database **Status**
    is **Available** in the RDS Console.

1. In addition to the step above, you **must** also ensure that
   [binlog retention](/sql/create-source/mysql/#binlog-retention) is set to a
   reasonable value. To check the current value of the [`binlog retention hours`](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours)
   configuration parameter, connect to your RDS instance and run:

   ```mysql
   CALL mysql.rds_show_configuration;
   ```text

   If the value returned is `NULL`, or less than `168` (i.e. 7 days), run:

   ```mysql
   CALL mysql.rds_set_configuration('binlog retention hours', 168);
   ```text

   Although 7 days is a reasonable retention period, we recommend using the
   default MySQL retention period (30 days) in order to not compromise
   Materialize’s ability to resume replication in case of failures or
   restarts.

1. To validate that all configuration parameters are set to the expected values
   after the above configuration changes, run:

    ```mysql
    -- Validate "binlog retention hours" configuration parameter
    CALL mysql.rds_show_configuration;
    ```text

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
    ```bash

### 2. Create a user for replication

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/create-a-user-for-replicati --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your RDS instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.


#### Cloud

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

#### Self-Managed

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. In the RDS Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/working-with-security-groups.html#adding-security-group-rule)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **MySQL**.
    - Set **Source** to the IP address in CIDR notation.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** 
Materialize provides a Terraform module that automates the creation and
configuration of resources for an SSH tunnel. For more details, see the
[Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).


1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
   to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      RDS instance.
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

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

### 1. (Optional) Create a cluster

> **Note:** 
If you are prototyping and already have a cluster to host your MySQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/create-a-cluster --> --> -->

### 2. Create a connection


Once you have configured your network, create a connection in Materialize per
your networking configuration.

#### Allow Materialize IPs

<!-- Unresolved shortcode: {{% mysql-direct/ingesting-data/allow-materialize-... -->

#### Use AWS PrivateLink (Cloud-only)

<!-- Unresolved shortcode: {{% mysql-direct/ingesting-data/use-aws-privatelin... -->

#### Use an SSH tunnel

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/ingesting-data/use-ssh-tunn --> --> -->


### 3. Start ingesting data

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/mys... -->

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/mys... -->

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/mys... -->

[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available (also for PostgreSQL)."

### 4. Monitor the ingestion status

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/check-the-ingestion-status --> --> -->

### 5. Right-size the cluster

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/right-size-the-cluster --> --> -->

## D. Explore your data

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/next-steps --> --> -->

## Considerations

<!-- Unresolved shortcode: {{% include-from-yaml data="mysql_source_details"
... -->