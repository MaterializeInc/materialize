---
name: materialize-docs
description: Materialize documentation for SQL syntax, data ingestion, concepts, and best practices. Use when users ask about Materialize queries, sources, sinks, views, or clusters.
---

# Materialize Documentation

This skill provides comprehensive documentation for Materialize, a streaming database for real-time analytics.

## How to Use This Skill

When a user asks about Materialize:

1. **For SQL syntax/commands**: Read files in the `sql/` directory
2. **For core concepts**: Read files in the `concepts/` directory
3. **For data ingestion**: Read files in the `ingest-data/` directory
4. **For transformations**: Read files in the `transform-data/` directory

## Documentation Sections

### Administrations

- **Usage (Self-Managed)**: `administration/usage/index.md`
- **Usage & billing (Cloud)**: `administration/billing/index.md`

### Concepts
Learn about the core concepts in Materialize.

- **Namespaces**: `concepts/namespaces/index.md`
- **Clusters**: `concepts/clusters/index.md`
- **Indexes**: `concepts/indexes/index.md`
- **Reaction Time, Freshness, and Query Latency**: `concepts/reaction-time/index.md`
- **Sinks**: `concepts/sinks/index.md`
- **Sources**: `concepts/sources/index.md`
- **Views**: `concepts/views/index.md`

### Ingest data
Best practices for ingesting data into Materialize from external systems.

- **Amazon EventBridge**: `ingest-data/webhooks/amazon-eventbridge/index.md`
- **AWS PrivateLink connections (Cloud-only)**: `ingest-data/network-security/privatelink/index.md`
- **CockroachDB CDC using Kafka and Changefeeds**: `ingest-data/cdc-cockroachdb/index.md`
- **Debezium**: `ingest-data/debezium/index.md`
- **Fivetran**: `ingest-data/fivetran/index.md`
- **HubSpot**: `ingest-data/webhooks/hubspot/index.md`
- **Kafka**: `ingest-data/kafka/index.md`
- **MongoDB**: `ingest-data/mongodb/index.md`
- **Monitoring data ingestion**: `ingest-data/monitoring-data-ingestion/index.md`
- **MySQL**: `ingest-data/mysql/index.md`
- _(and 12 more files in this section)_

### Install/Upgrade (Self-Managed)
Installation and upgrade guides for Self-Managed Materialize.

- **Appendix: Cluster sizes**: `installation/appendix-cluster-sizes/index.md`
- **Appendix: Materialize CRD Field Descriptions**: `installation/appendix-materialize-crd-field-descriptions/index.md`
- **Appendix: Prepare for swap and upgrade to v26.0**: `installation/upgrade-to-swap/index.md`
- **Appendix: Terraforms**: `installation/appendix-terraforms/index.md`
- **FAQ: Self-managed installation**: `installation/faq/index.md`
- **Install locally on kind (via Helm)**: `installation/install-on-local-kind/index.md`
- **Install on AWS (via Terraform)**: `installation/install-on-aws/index.md`
- **Install on Azure (via Terraform)**: `installation/install-on-azure/index.md`
- **Install on GCP (via Terraform)**: `installation/install-on-gcp/index.md`
- **Materialize Operator Configuration**: `installation/configuration/index.md`
- _(and 3 more files in this section)_

### Manage Materialize

- **Appendix: Alternative cluster architectures**: `manage/appendix-alternative-cluster-architectures/index.md`
- **Disaster recovery (Cloud)**: `manage/disaster-recovery/index.md`
- **Monitoring and alerting**: `manage/monitor/index.md`
- **Operational guidelines**: `manage/operational-guidelines/index.md`
- **Use dbt to manage Materialize**: `manage/dbt/index.md`
- **Use Terraform to manage Materialize**: `manage/terraform/index.md`

### Materialize console
Introduction to the Materialize Console, user interface for Materialize

- **Admin (Cloud-only)**: `console/admin/index.md`
- **Clusters**: `console/clusters/index.md`
- **Connect (Cloud-only)**: `console/connect/index.md`
- **Create new**: `console/create-new/index.md`
- **Database object explorer**: `console/data/index.md`
- **Integrations**: `console/integrations/index.md`
- **Monitoring**: `console/monitoring/index.md`
- **SQL Shell**: `console/sql-shell/index.md`
- **User profile**: `console/user-profile/index.md`

### Overview
Learn how to efficiently transform data using Materialize SQL.

- **Dataflow troubleshooting**: `transform-data/dataflow-troubleshooting/index.md`
- **FAQ: Indexes**: `transform-data/faq/index.md`
- **Idiomatic Materialize SQL**: `transform-data/idiomatic-materialize-sql/index.md`
- **Optimization**: `transform-data/optimization/index.md`
- **Patterns**: `transform-data/patterns/index.md`
- **Troubleshooting**: `transform-data/troubleshooting/index.md`

### Security

- **Appendix**: `security/appendix/index.md`
- **Cloud**: `security/cloud/index.md`
- **Self-managed**: `security/self-managed/index.md`

### Serve results
Serving results from Materialize

- **`SELECT` and `SUBSCRIBE`**: `serve-results/query-results/index.md`
- **Sink results**: `serve-results/sink/index.md`
- **Use BI/data collaboration tools**: `serve-results/bi-tools/index.md`
- **Use foreign data wrapper (FDW)**: `serve-results/fdw/index.md`

### SQL commands
SQL commands reference.

- **Namespaces**: `sql/namespaces/index.md`
- **ALTER CLUSTER**: `sql/alter-cluster/index.md`
- **ALTER CLUSTER REPLICA**: `sql/alter-cluster-replica/index.md`
- **ALTER CONNECTION**: `sql/alter-connection/index.md`
- **ALTER DATABASE**: `sql/alter-database/index.md`
- **ALTER DEFAULT PRIVILEGES**: `sql/alter-default-privileges/index.md`
- **ALTER INDEX**: `sql/alter-index/index.md`
- **ALTER MATERIALIZED VIEW**: `sql/alter-materialized-view/index.md`
- **ALTER NETWORK POLICY (Cloud)**: `sql/alter-network-policy/index.md`
- **ALTER ROLE**: `sql/alter-role/index.md`
- _(and 110 more files in this section)_

### Tools and integrations
Get details about third-party tools and integrations supported by Materialize

- **Client libraries**: `integrations/client-libraries/index.md`
- **Connect to Materialize via HTTP**: `integrations/http-api/index.md`
- **Connect to Materialize via WebSocket**: `integrations/websocket-api/index.md`
- **Connection Pooling**: `integrations/connection-pooling/index.md`
- **Foreign data wrapper (FDW) **: `integrations/fdw/index.md`
- **MCP Server**: `integrations/llm/index.md`
- **mz - Materialize CLI**: `integrations/cli/index.md`
- **mz-debug**: `integrations/mz-debug/index.md`
- **SQL clients**: `integrations/sql-clients/index.md`

## Quick Reference

### Common SQL Commands

| Command | Description |
|---------|-------------|
| `CREATE SOURCE` | Connect to external data sources (Kafka, PostgreSQL, MySQL) |
| `CREATE MATERIALIZED VIEW` | Create incrementally maintained views |
| `CREATE INDEX` | Create indexes on views for faster queries |
| `CREATE SINK` | Export data to external systems |
| `SELECT` | Query data from sources, views, and tables |

### Key Concepts

- **Sources**: Connections to external data systems that stream data into Materialize
- **Materialized Views**: Views that are incrementally maintained as source data changes
- **Indexes**: Arrangements of data in memory for fast point lookups
- **Clusters**: Isolated compute resources for running dataflows
- **Sinks**: Connections that export data from Materialize to external systems
