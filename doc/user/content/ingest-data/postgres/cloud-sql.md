---
title: "Ingest data from Google Cloud SQL"
description: "How to stream data from Google Cloud SQL for PostgreSQL to Materialize"
aliases:
  - /ingest-data/postgres-google-cloud-sql/
menu:
  main:
    parent: "postgresql"
    name: "Google Cloud SQL"
    identifier: "pg-google-cloudsql"
---

This page shows you how to stream data from [Google Cloud SQL for PostgreSQL](https://cloud.google.com/sql/postgresql)
to Materialize using the[PostgreSQL source](/sql/create-source/postgres/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}


## Before you begin

{{% postgres-direct/before-you-begin %}}

## A. Configure Google Cloud SQL

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Cloud SQL, see the [Cloud SQL
documentation](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance).

### 2. Create a publication and a replication user

{{% postgres-direct/create-a-publication-other %}}

## B. Configure network security

{{% self-managed/network-connection %}}

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% postgres-direct/create-a-cluster %}}

### 2. Start ingesting data

{{% postgres-direct/ingesting-data/allow-materialize-ips %}}

### 3. Monitor the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

### 4. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## Next steps

{{% postgres-direct/next-steps %}}
