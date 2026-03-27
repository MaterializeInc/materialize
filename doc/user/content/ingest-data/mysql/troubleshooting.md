---
title: "Troubleshooting"
description: "Troubleshooting guides for MySQL source errors in Materialize"
menu:
  main:
    parent: "mysql"
    name: "Troubleshooting"
    identifier: "mysql-troubleshooting"
    weight: 80
---

This section contains troubleshooting guides for specific errors you may
encounter when using MySQL sources in Materialize. These guides focus on
errors that are unique to the MySQL replication workflow, including issues
with GTIDs, binlog management, and other CDC-specific scenarios.

For general data ingestion troubleshooting that applies to all source types, see
the main [Troubleshooting](/ingest-data/troubleshooting/) guide.

## Troubleshooting guides

| Guide | Description |
|-------|-------------|
| [Received out of order GTIDs](/ingest-data/mysql/received-out-of-order-gtids/) | Resolve errors when Materialize observes GTID events from MySQL in an order it cannot safely reconcile |
