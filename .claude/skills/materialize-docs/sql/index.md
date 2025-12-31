---
title: SQL Commands Reference
description: Complete reference for all SQL commands in Materialize
doc_type: reference
product_area: SQL
audience: developer
status: stable
complexity: beginner
keywords:
  - SQL commands
  - DDL
  - DML
  - RBAC
  - queries
canonical_url: https://materialize.com/docs/sql/
---

# SQL Commands Reference

## Purpose
This is the main index for all SQL commands in Materialize. Each command has its own detailed documentation page.

If you're looking for a specific SQL command, use the categorized lists below to find the right documentation.

## Data Definition (DDL) — Create Objects

### Sources
- [CREATE SOURCE](create-source/index.md) — Connect to external data sources
- [ALTER SOURCE](alter-source/index.md) — Modify source configuration
- [DROP SOURCE](drop-source/index.md) — Remove a source

### Views and Materialized Views
- [CREATE VIEW](create-view/index.md) — Create a named query
- [CREATE MATERIALIZED VIEW](create-materialized-view/index.md) — Create an incrementally maintained view
- [ALTER VIEW](alter-view/index.md) — Modify view properties
- [ALTER MATERIALIZED VIEW](alter-materialized-view/index.md) — Modify materialized view properties
- [DROP VIEW](drop-view/index.md) — Remove a view
- [DROP MATERIALIZED VIEW](drop-materialized-view/index.md) — Remove a materialized view

### Indexes
- [CREATE INDEX](create-index/index.md) — Create an index for fast lookups
- [ALTER INDEX](alter-index/index.md) — Modify index properties
- [DROP INDEX](drop-index/index.md) — Remove an index

### Sinks
- [CREATE SINK](create-sink/index.md) — Define an output destination
- [ALTER SINK](alter-sink/index.md) — Modify sink configuration
- [DROP SINK](drop-sink/index.md) — Remove a sink

### Tables
- [CREATE TABLE](create-table/index.md) — Create a table
- [ALTER TABLE](alter-table/index.md) — Modify table properties
- [DROP TABLE](drop-table/index.md) — Remove a table

### Clusters
- [CREATE CLUSTER](create-cluster/index.md) — Create compute resources
- [CREATE CLUSTER REPLICA](create-cluster-replica/index.md) — Add replicas to a cluster
- [ALTER CLUSTER](alter-cluster/index.md) — Modify cluster configuration
- [ALTER CLUSTER REPLICA](alter-cluster-replica/index.md) — Modify replica settings
- [DROP CLUSTER](drop-cluster/index.md) — Remove a cluster
- [DROP CLUSTER REPLICA](drop-cluster-replica/index.md) — Remove a cluster replica

### Connections and Secrets
- [CREATE CONNECTION](create-connection/index.md) — Store external system credentials
- [ALTER CONNECTION](alter-connection/index.md) — Modify connection settings
- [DROP CONNECTION](drop-connection/index.md) — Remove a connection
- [VALIDATE CONNECTION](validate-connection/index.md) — Test a connection
- [CREATE SECRET](create-secret/index.md) — Store sensitive credentials
- [ALTER SECRET](alter-secret/index.md) — Modify secret values
- [DROP SECRET](drop-secret/index.md) — Remove a secret

### Schemas and Databases
- [CREATE DATABASE](create-database/index.md) — Create a database
- [CREATE SCHEMA](create-schema/index.md) — Create a schema
- [ALTER DATABASE](alter-database/index.md) — Modify database properties
- [ALTER SCHEMA](alter-schema/index.md) — Modify schema properties
- [DROP DATABASE](drop-database/index.md) — Remove a database
- [DROP SCHEMA](drop-schema/index.md) — Remove a schema

### Types
- [CREATE TYPE](create-type/index.md) — Create a custom type
- [ALTER TYPE](alter-type/index.md) — Modify type properties
- [DROP TYPE](drop-type/index.md) — Remove a custom type

### Network Policies (Cloud)
- [CREATE NETWORK POLICY](create-network-policy/index.md) — Define network access rules
- [ALTER NETWORK POLICY](alter-network-policy/index.md) — Modify network policy
- [DROP NETWORK POLICY](drop-network-policy/index.md) — Remove network policy

## Data Manipulation (DML)

- [SELECT](select/index.md) — Query data from relations
- [INSERT](insert/index.md) — Add rows to a table
- [UPDATE](update/index.md) — Modify existing rows
- [DELETE](delete/index.md) — Remove rows
- [COPY FROM](copy-from/index.md) — Bulk load data from files
- [COPY TO](copy-to/index.md) — Bulk export data to files
- [SUBSCRIBE](subscribe/index.md) — Stream changes from a relation
- [TABLE](table/index.md) — Simple table query
- [VALUES](values/index.md) — Construct rows from values

## Access Control (RBAC)

### Roles
- [CREATE ROLE](create-role/index.md) — Create a database role
- [ALTER ROLE](alter-role/index.md) — Modify role properties
- [DROP ROLE](drop-role/index.md) — Remove a role
- [DROP USER](drop-user/index.md) — Remove a user (alias for DROP ROLE)

### Privileges
- [GRANT (privilege)](grant-privilege/index.md) — Grant privileges on objects
- [REVOKE (privilege)](revoke-privilege/index.md) — Remove privileges
- [GRANT (role)](grant-role/index.md) — Grant role membership
- [REVOKE (role)](revoke-role/index.md) — Remove role membership
- [ALTER DEFAULT PRIVILEGES](alter-default-privileges/index.md) — Set default privileges

### Ownership
- [DROP OWNED](drop-owned/index.md) — Drop objects owned by a role
- [REASSIGN OWNED](reassign-owned/index.md) — Change object ownership

## Query Introspection

- [EXPLAIN PLAN](explain-plan/index.md) — Show query execution plan
- [EXPLAIN TIMESTAMP](explain-timestamp/index.md) — Show timestamp selection
- [EXPLAIN ANALYZE](explain-analyze/index.md) — Execute and profile a query
- [EXPLAIN SCHEMA](explain-schema/index.md) — Show output schema
- [EXPLAIN FILTER PUSHDOWN](explain-filter-pushdown/index.md) — Show filter optimization

## Object Introspection (SHOW)

### Databases and Schemas
- [SHOW DATABASES](show-databases/index.md) — List databases
- [SHOW SCHEMAS](show-schemas/index.md) — List schemas

### Relations
- [SHOW SOURCES](show-sources/index.md) — List sources
- [SHOW SUBSOURCES](show-subsources/index.md) — List subsources
- [SHOW VIEWS](show-views/index.md) — List views
- [SHOW MATERIALIZED VIEWS](show-materialized-views/index.md) — List materialized views
- [SHOW TABLES](show-tables/index.md) — List tables
- [SHOW INDEXES](show-indexes/index.md) — List indexes
- [SHOW SINKS](show-sinks/index.md) — List sinks
- [SHOW COLUMNS](show-columns/index.md) — List columns in a relation

### Infrastructure
- [SHOW CLUSTERS](show-clusters/index.md) — List clusters
- [SHOW CLUSTER REPLICAS](show-cluster-replicas/index.md) — List cluster replicas
- [SHOW CONNECTIONS](show-connections/index.md) — List connections
- [SHOW SECRETS](show-secrets/index.md) — List secrets
- [SHOW NETWORK POLICIES](show-network-policies/index.md) — List network policies

### Access Control
- [SHOW ROLES](show-roles/index.md) — List roles
- [SHOW ROLE MEMBERSHIP](show-role-membership/index.md) — Show role hierarchy
- [SHOW PRIVILEGES](show-privileges/index.md) — Show granted privileges
- [SHOW DEFAULT PRIVILEGES](show-default-privileges/index.md) — Show default privileges

### Other
- [SHOW TYPES](show-types/index.md) — List custom types
- [SHOW OBJECTS](show-objects/index.md) — List all objects

### SHOW CREATE
- [SHOW CREATE SOURCE](show-create-source/index.md)
- [SHOW CREATE VIEW](show-create-view/index.md)
- [SHOW CREATE MATERIALIZED VIEW](show-create-materialized-view/index.md)
- [SHOW CREATE TABLE](show-create-table/index.md)
- [SHOW CREATE INDEX](show-create-index/index.md)
- [SHOW CREATE SINK](show-create-sink/index.md)
- [SHOW CREATE CLUSTER](show-create-cluster/index.md)
- [SHOW CREATE CONNECTION](show-create-connection/index.md)
- [SHOW CREATE TYPE](show-create-type/index.md)

## Session and Transaction Management

- [SET](set/index.md) — Set session variables
- [SHOW](show/index.md) — Show session or system variables
- [RESET](reset/index.md) — Reset session variables
- [BEGIN](begin/index.md) — Start a transaction
- [COMMIT](commit/index.md) — Commit a transaction
- [ROLLBACK](rollback/index.md) — Abort a transaction

## System Configuration

- [ALTER SYSTEM SET](alter-system-set/index.md) — Set system configuration
- [ALTER SYSTEM RESET](alter-system-reset/index.md) — Reset system configuration
- [COMMENT ON](comment-on/index.md) — Add comments to objects

## Prepared Statements

- [PREPARE](prepare/index.md) — Create a prepared statement
- [EXECUTE](execute/index.md) — Execute a prepared statement
- [DEALLOCATE](deallocate/index.md) — Remove a prepared statement

## Cursors

- [DECLARE](declare/index.md) — Declare a cursor
- [FETCH](fetch/index.md) — Retrieve rows from a cursor
- [CLOSE](close/index.md) — Close a cursor
- [DISCARD](discard/index.md) — Discard session state

## Reference Topics

- [Namespaces](namespaces/index.md) — How objects are organized
- [Identifiers](identifiers/index.md) — Naming rules for objects
- [System Clusters](system-clusters/index.md) — Built-in system clusters
- [System Catalog](system-catalog/index.md) — Metadata tables
- [Data Types](types/index.md) — Supported data types
- [Functions](functions/index.md) — Built-in functions

## Key Takeaways

- Use `CREATE SOURCE` to ingest data from external systems (Kafka, PostgreSQL, MySQL, webhooks)
- Use `CREATE MATERIALIZED VIEW` for incrementally maintained query results
- Use `CREATE INDEX` to enable fast point lookups on views
- Use `CREATE SINK` to export data to external systems
- All objects are organized in databases and schemas (see [Namespaces](namespaces/index.md))
