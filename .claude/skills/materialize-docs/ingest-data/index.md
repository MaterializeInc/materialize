---
title: Ingest Data
description: Guides for ingesting data into Materialize from external systems
doc_type: overview
product_area: Sources
audience: developer
status: stable
complexity: beginner
keywords:
  - ingest data
  - sources
  - CDC
  - Kafka
  - PostgreSQL
  - MySQL
  - webhooks
canonical_url: https://materialize.com/docs/ingest-data/
---

# Ingest Data

## Purpose
This section contains guides for ingesting data into Materialize from various external systems. Materialize supports native CDC (Change Data Capture) from databases and streaming from message brokers.

If you want to connect Materialize to an external data source, start here to find the appropriate guide.

## When to use
- Setting up data ingestion from PostgreSQL, MySQL, or SQL Server
- Connecting to Kafka, Redpanda, or other message brokers
- Configuring webhook sources
- Troubleshooting ingestion issues

## Database Sources (CDC)

Materialize can replicate data directly from databases using native CDC:

### PostgreSQL
- [PostgreSQL Overview](postgres/index.md) — Main guide for PostgreSQL CDC
- [Amazon RDS PostgreSQL](postgres/amazon-rds/index.md)
- [Amazon Aurora PostgreSQL](postgres/amazon-aurora/index.md)
- [Google Cloud SQL](postgres/cloud-sql/index.md)
- [Google AlloyDB](postgres/alloydb/index.md)
- [Azure Database for PostgreSQL](postgres/azure-db/index.md)
- [Neon](postgres/neon/index.md)
- [Self-hosted PostgreSQL](postgres/self-hosted/index.md)
- [PostgreSQL via Debezium](postgres/postgres-debezium/index.md)
- [PostgreSQL Source Versioning](postgres/source-versioning/index.md)
- [PostgreSQL FAQ](postgres/faq/index.md)

### MySQL
- [MySQL Overview](mysql/index.md) — Main guide for MySQL CDC
- [Amazon RDS MySQL](mysql/amazon-rds/index.md)
- [Amazon Aurora MySQL](mysql/amazon-aurora/index.md)
- [Google Cloud SQL MySQL](mysql/google-cloud-sql/index.md)
- [Azure Database for MySQL](mysql/azure-db/index.md)
- [Self-hosted MySQL](mysql/self-hosted/index.md)
- [MySQL via Debezium](mysql/mysql-debezium/index.md)

### SQL Server
- [SQL Server Overview](sql-server/index.md) — Main guide for SQL Server CDC
- [Self-hosted SQL Server](sql-server/self-hosted/index.md)

### Other Databases
- [CockroachDB](cdc-cockroachdb/index.md) — Via Kafka changefeeds
- [MongoDB](mongodb/index.md) — Via Debezium

## Message Brokers

### Kafka
- [Kafka Overview](kafka/index.md) — Main guide for Kafka sources
- [Confluent Cloud](kafka/confluent-cloud/index.md)
- [Amazon MSK](kafka/amazon-msk/index.md)
- [WarpStream](kafka/warpstream/index.md)
- [Self-hosted Kafka](kafka/kafka-self-hosted/index.md)

### Redpanda
- [Redpanda Overview](redpanda/index.md)
- [Redpanda Cloud](redpanda/redpanda-cloud/index.md)

## Webhooks

Receive data via HTTP webhooks:

- [Webhook Quickstart](webhooks/webhook-quickstart/index.md)
- [Amazon EventBridge](webhooks/amazon-eventbridge/index.md)
- [Segment](webhooks/segment/index.md)
- [RudderStack](webhooks/rudderstack/index.md)
- [Stripe](webhooks/stripe/index.md)
- [HubSpot](webhooks/hubspot/index.md)
- [Snowcat Cloud](webhooks/snowcatcloud/index.md)

## Integration Tools

- [Debezium](debezium/index.md) — CDC via Kafka Connect
- [Fivetran](fivetran/index.md) — Data integration platform
- [Striim](striim/index.md) — Real-time data integration

## Network Security

Configure secure network connections:

- [SSH Tunnels](network-security/ssh-tunnel/index.md)
- [AWS PrivateLink](network-security/privatelink/index.md) (Cloud only)
- [Static IPs](network-security/static-ips/index.md)

## Operations

- [Monitoring Data Ingestion](monitoring-data-ingestion/index.md) — Track ingestion health
- [Troubleshooting](troubleshooting/index.md) — Common issues and solutions

## Key Takeaways

- PostgreSQL and MySQL sources use native replication (no Kafka/Debezium required)
- Kafka sources support Avro, JSON, Protobuf, and CSV formats
- Use SSH tunnels or PrivateLink for secure connections to private networks
- Sources require dedicated clusters for compute resources
