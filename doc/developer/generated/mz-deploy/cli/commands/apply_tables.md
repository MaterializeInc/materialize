---
source: src/mz-deploy/src/cli/commands/apply_tables.rs
revision: d50441fcbe
---

# mz-deploy::cli::commands::apply_tables

Apply tables command - create tables that don't exist in the database.

The `plan` function filters the project for `CREATE TABLE` and `CREATE TABLE FROM SOURCE` statements, checks which already exist in the catalog, then calls `client.validation().validate_source_references` on the set of tables to create before preparing schemas or executing DDL. This ensures each table's upstream reference is resolvable before any schema creation is attempted.
