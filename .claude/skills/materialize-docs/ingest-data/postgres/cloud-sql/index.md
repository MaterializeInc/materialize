---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/postgres/cloud-sql/
complexity: beginner
description: How to stream data from Google Cloud SQL for PostgreSQL to Materialize
doc_type: reference
keywords:
- 'Use an SSH tunnel:'
- UPDATE YOUR
- 'Note:'
- 'Allow Materialize IPs:'
- you can skip this step
- CREATE A
- SELECT THE
- Ingest data from Google Cloud SQL
- 'Tip:'
- CREATE AN
product_area: Sources
status: stable
title: Ingest data from Google Cloud SQL
---

# Ingest data from Google Cloud SQL

## Purpose
How to stream data from Google Cloud SQL for PostgreSQL to Materialize

If you need to understand the syntax and options for this command, you're in the right place.


How to stream data from Google Cloud SQL for PostgreSQL to Materialize


This page shows you how to stream data from [Google Cloud SQL for PostgreSQL](https://cloud.google.com/sql/postgresql)
to Materialize using the[PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/before-you-begin --> --> -->

## A. Configure Google Cloud SQL

This section covers a. configure google cloud sql.

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Cloud SQL, see the [Cloud SQL
documentation](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance).

### 2. Create a publication and a replication user

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: postgres-direct/create-a-publication-oth --> --> -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your Google Cloud SQL instance is publicly
accessible, **you can skip this step**. For production scenarios, we recommend
configuring one of the network security options below.


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

1. Update your Google Cloud SQL firewall rules to allow traffic from Materialize
   IPs.

#### Use an SSH tunnel

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

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.

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

1. <!-- Unresolved shortcode: {{% include-example
   file="examples/ingest_data/... -->

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