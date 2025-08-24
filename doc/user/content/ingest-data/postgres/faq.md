---
title: "FAQ: PostgreSQL sources"
description: "Frequently asked questions about PostgreSQL sources in Materialize"
menu:
  main:
    parent: "postgresql"
    weight: 90
---

This page addresses common questions and challenges when working with PostgreSQL
sources in Materialize. For general ingestion questions/troubleshooting, see:
- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/).
- [Troubleshooting/FAQ](/ingest-data/troubleshooting/).

## For my trial/POC, what if I cannot use `REPLICA IDENTITY FULL`?

Materialize requires `REPLICA IDENTITY FULL` on PostgreSQL tables to capture all
column values in change events. If for your trial/POC (Proof-of-concept) you cannot modify your existing tables, here are two common alternatives:

- **Outbox Pattern (shadow tables)**

  {{< note >}}

  With the Outbox pattern, you will need to implement dual writes so that all changes apply to both the original and shadow tables.

  {{</ note >}}

  With the Outbox pattern, you create duplicate "shadow" tables for the ones you
  want to replicate and set the shadow tables to `REPLICA IDENTITY FULL`. You
  can then use these shadow tables for Materialize instead of the originals.

- **Sidecar Pattern**

  {{< note >}}

  With the Sidecar pattern, you will need to keep the sidecar in sync with the
  source database (e.g., via logical replication or ETL processes).

  {{</ note >}}

  With the Sidecar pattern, you create a separate PostgreSQL instance as an
  integration layer. That is, in the sidecar instance, you recreate the tables
  you want to replicate, setting these tableswith `REPLICA IDENTITY FULL`. You
  can then use the sidecar for Materialiez instead of your primary database.

## What if my table contains data types that are unsupported in Materialize?

{{< include-md file="shared-content/postgres-unsupported-types.md" >}}

See also: [PostgreSQL considerations](/ingest-data/postgres/#considerations).
