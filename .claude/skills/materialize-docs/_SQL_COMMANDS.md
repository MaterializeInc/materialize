---
title: SQL Command Reference
description: Quick reference of all Materialize SQL commands
doc_type: reference
product_area: SQL
---

# SQL Command Reference

Quick reference of all SQL commands with one-line descriptions.

## Data Definition (DDL)

### Sources
- **`CREATE SOURCE`**: Define an external data source for ingestion — [docs](sql/create-source/index.md)
- **`ALTER SOURCE`**: Modify source configuration — [docs](sql/alter-source/index.md)
- **`DROP SOURCE`**: Remove a source — [docs](sql/drop-source/index.md)
- **`SHOW SOURCES`**: List sources — [docs](sql/show-sources/index.md)

### Views
- **`CREATE VIEW`**: Create a named query — [docs](sql/create-view/index.md)
- **`CREATE MATERIALIZED VIEW`**: Create an incrementally maintained view — [docs](sql/create-materialized-view/index.md)
- **`ALTER VIEW`**: Modify view properties — [docs](sql/alter-view/index.md)
- **`ALTER MATERIALIZED VIEW`**: Modify materialized view properties — [docs](sql/alter-materialized-view/index.md)
- **`DROP VIEW`**: Remove a view — [docs](sql/drop-view/index.md)
- **`DROP MATERIALIZED VIEW`**: Remove a materialized view — [docs](sql/drop-materialized-view/index.md)

### Indexes
- **`CREATE INDEX`**: Create an index for fast lookups — [docs](sql/create-index/index.md)
- **`ALTER INDEX`**: Modify index properties — [docs](sql/alter-index/index.md)
- **`DROP INDEX`**: Remove an index — [docs](sql/drop-index/index.md)

### Sinks
- **`CREATE SINK`**: Define an output destination — [docs](sql/create-sink/index.md)
- **`ALTER SINK`**: Modify sink configuration — [docs](sql/alter-sink/index.md)
- **`DROP SINK`**: Remove a sink — [docs](sql/drop-sink/index.md)

### Tables
- **`CREATE TABLE`**: Create a table for manual data entry — [docs](sql/create-table/index.md)
- **`ALTER TABLE`**: Modify table properties — [docs](sql/alter-table/index.md)
- **`DROP TABLE`**: Remove a table — [docs](sql/drop-table/index.md)

### Clusters
- **`CREATE CLUSTER`**: Create compute resources — [docs](sql/create-cluster/index.md)
- **`ALTER CLUSTER`**: Modify cluster configuration — [docs](sql/alter-cluster/index.md)
- **`DROP CLUSTER`**: Remove a cluster — [docs](sql/drop-cluster/index.md)

### Connections
- **`CREATE CONNECTION`**: Store external system credentials — [docs](sql/create-connection/index.md)
- **`ALTER CONNECTION`**: Modify connection settings — [docs](sql/alter-connection/index.md)
- **`DROP CONNECTION`**: Remove a connection — [docs](sql/drop-connection/index.md)

### Schemas & Databases
- **`CREATE DATABASE`**: Create a database — [docs](sql/create-database/index.md)
- **`CREATE SCHEMA`**: Create a schema — [docs](sql/create-schema/index.md)
- **`DROP DATABASE`**: Remove a database — [docs](sql/drop-database/index.md)
- **`DROP SCHEMA`**: Remove a schema — [docs](sql/drop-schema/index.md)

## Data Manipulation (DML)

- **`SELECT`**: Query data — [docs](sql/select/index.md)
- **`INSERT`**: Add rows to a table — [docs](sql/insert/index.md)
- **`UPDATE`**: Modify existing rows — [docs](sql/update/index.md)
- **`DELETE`**: Remove rows — [docs](sql/delete/index.md)
- **`COPY FROM`**: Bulk load data — [docs](sql/copy-from/index.md)
- **`COPY TO`**: Bulk export data — [docs](sql/copy-to/index.md)
- **`SUBSCRIBE`**: Stream changes from a relation — [docs](sql/subscribe/index.md)

## Query Introspection

- **`EXPLAIN PLAN`**: Show query execution plan — [docs](sql/explain-plan/index.md)
- **`EXPLAIN TIMESTAMP`**: Show timestamp selection — [docs](sql/explain-timestamp/index.md)
- **`EXPLAIN ANALYZE`**: Execute and profile a query — [docs](sql/explain-analyze/index.md)

## Access Control (RBAC)

- **`CREATE ROLE`**: Create a database role — [docs](sql/create-role/index.md)
- **`ALTER ROLE`**: Modify role properties — [docs](sql/alter-role/index.md)
- **`DROP ROLE`**: Remove a role — [docs](sql/drop-role/index.md)
- **`GRANT`**: Grant privileges — [docs](sql/grant-privilege/index.md)
- **`REVOKE`**: Remove privileges — [docs](sql/revoke-privilege/index.md)

## Session Management

- **`SET`**: Set session variables — [docs](sql/set/index.md)
- **`SHOW`**: Display session or object information — [docs](sql/show/index.md)
- **`BEGIN`**: Start a transaction — [docs](sql/begin/index.md)
- **`COMMIT`**: Commit a transaction — [docs](sql/commit/index.md)
