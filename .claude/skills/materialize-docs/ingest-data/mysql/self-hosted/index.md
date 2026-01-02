---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/mysql/self-hosted/
complexity: beginner
description: How to stream data from self-hosted MySQL database to Materialize
doc_type: reference
keywords:
- UPDATE YOUR
- must
- 'you can

  skip this step'
- 'Allow Materialize IPs:'
- Ingest data from self-hosted MySQL
- CREATE A
- SELECT THE
- 'Note:'
- 'Tip:'
- CREATE AN
product_area: Sources
status: stable
title: Ingest data from self-hosted MySQL
---

# Ingest data from self-hosted MySQL

## Purpose
How to stream data from self-hosted MySQL database to Materialize

If you need to understand the syntax and options for this command, you're in the right place.


How to stream data from self-hosted MySQL database to Materialize


This page shows you how to stream data from a self-hosted MySQL database to
Materialize using the [MySQL source](/sql/create-source/mysql/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/before-you-begin --> --> -->

## A. Configure MySQL

This section covers a. configure mysql.

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure your MySQL
database for GTID-based binlog replication. Ensure the upstream MySQL database
has been configured for GTID-based binlog replication:

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/ingesting-data/mysql-config --> --> -->

For guidance on enabling GTID-based binlog replication, see the
[MySQL documentation](https://dev.mysql.com/blog-archive/enabling-gtids-without-downtime-in-mysql-5-7-6/).

### 2. Create a user for replication

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/create-a-user-for-replicati --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your MySQL instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.


#### Cloud

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

#### Self-Managed

<!-- Unresolved shortcode: {{% include-md
file="shared-content/self-managed/c... -->

#### Allow Materialize IPs

1. Update your database firewall rules to allow traffic from Materialize IPs.

#### Use an SSH tunnel

To create an SSH tunnel from Materialize to your database, you launch an VM to
serve as an SSH bastion host, configure the bastion host to allow traffic only
from Materialize, and then configure your database's private network to allow
traffic from the bastion host.

1. Launch a VM to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.

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

#### Use an SSH tunnel

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/ingesting-data/use-ssh-tunn --> --> -->

### 3. Start ingesting data

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/mys... -->

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/mys... -->

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/mys... -->

### 4. Monitor the ingestion status

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/check-the-ingestion-status --> --> -->

### 5. Right-size the cluster

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/right-size-the-cluster --> --> -->

## D. Explore your data

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: mysql-direct/next-steps --> --> -->

## Considerations

<!-- Unresolved shortcode: {{% include-from-yaml data="mysql_source_details"
... -->