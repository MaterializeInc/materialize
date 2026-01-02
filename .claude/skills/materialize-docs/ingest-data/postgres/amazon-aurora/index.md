---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/postgres/amazon-aurora/
complexity: beginner
description: How to stream data from Amazon Aurora for PostgreSQL to Materialize
doc_type: reference
keywords:
- Ingest data from Amazon Aurora
- 'Warning:'
- CREATE A
- SELECT THE
- not
- 'Note:'
- 'Tip:'
- CREATE AN
product_area: Sources
status: stable
title: Ingest data from Amazon Aurora
---

# Ingest data from Amazon Aurora

## Purpose
How to stream data from Amazon Aurora for PostgreSQL to Materialize

If you need to understand the syntax and options for this command, you're in the right place.


How to stream data from Amazon Aurora for PostgreSQL to Materialize


This page shows you how to stream data from [Amazon Aurora for PostgreSQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

> **Warning:** 
There is a known issue with Aurora PostgreSQL 16.1 that can cause logical replication to fail with the following error:
- `postgres: sql client error: db error: ERROR: could not map filenumber "base/16402/3147867235" to relation OID`

This is due to a bug in Aurora's implementation of logical replication in PostgreSQL 16.1, where the system fails to correctly fetch relation metadata from the catalogs. If you encounter these errors, you should upgrade your Aurora PostgreSQL instance to a newer minor version (16.2 or later).

For more information, see [this AWS discussion](https://repost.aws/questions/QU4RXUrLNQS_2oSwV34pmwww/error-could-not-map-filenumber-after-aurora-upgrade-to-16-1).


## A. Configure Amazon Aurora

This section covers a. configure amazon aurora.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Aurora, see the
[Aurora documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure).

> **Note:** 
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
logical replication, so it's not possible to use this service with
Materialize.


### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-aws --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your Aurora instance is publicly accessible, **you can
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

1. In the AWS Management Console, [add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **PostgreSQL**.
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

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.

## C. Ingest data in Materialize

This section covers c. ingest data in materialize.

### 1. (Optional) Create a cluster

> **Note:** 
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-cluster --> --> -->

### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.

#### Allow Materialize IPs

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use AWS PrivateLink (Cloud-only)

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->
1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->
   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

#### Use an SSH tunnel

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

1. <!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/pos... -->

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

   <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

### 3. Start ingesting data

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/pos... -->

### 4. Monitor the ingestion status

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/check-the-ingestion-stat --> --> -->

### 5. Right-size the cluster

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/right-size-the-cluster --> --> -->

## D. Explore your data

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/next-steps --> --> -->

## Considerations

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->