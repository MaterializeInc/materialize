---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/postgres/faq/
complexity: intermediate
description: Frequently asked questions about PostgreSQL sources in Materialize
doc_type: troubleshooting
keywords:
- Outbox Pattern (shadow tables)
- CREATE DUPLICATE
- CREATE A
- Sidecar Pattern
- 'FAQ: PostgreSQL sources'
- 'Note:'
product_area: Sources
status: stable
title: 'FAQ: PostgreSQL sources'
---

# FAQ: PostgreSQL sources

## Purpose
Frequently asked questions about PostgreSQL sources in Materialize

This page provides detailed documentation for this topic.


Frequently asked questions about PostgreSQL sources in Materialize


This page addresses common questions and challenges when working with PostgreSQL
sources in Materialize. For general ingestion questions/troubleshooting, see:
- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/).
- [Troubleshooting/FAQ](/ingest-data/troubleshooting/).

## For my trial/POC, what if I cannot use `REPLICA IDENTITY FULL`?

Materialize requires `REPLICA IDENTITY FULL` on PostgreSQL tables to capture all
column values in change events. If for your trial/POC (Proof-of-concept) you cannot modify your existing tables, here are two common alternatives:

- **Outbox Pattern (shadow tables)**

  > **Note:** 

  With the Outbox pattern, you will need to implement dual writes so that all changes apply to both the original and shadow tables.

  

  With the Outbox pattern, you create duplicate "shadow" tables for the ones you
  want to replicate and set the shadow tables to `REPLICA IDENTITY FULL`. You
  can then use these shadow tables for Materialize instead of the originals.

- **Sidecar Pattern**

  > **Note:** 

  With the Sidecar pattern, you will need to keep the sidecar in sync with the
  source database (e.g., via logical replication or ETL processes).

  

  With the Sidecar pattern, you create a separate PostgreSQL instance as an
  integration layer. That is, in the sidecar instance, you recreate the tables
  you want to replicate, setting these tableswith `REPLICA IDENTITY FULL`. You
  can then use the sidecar for Materialiez instead of your primary database.

## What if my table contains data types that are unsupported in Materialize?

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

See also: [PostgreSQL considerations](/ingest-data/postgres/#considerations).