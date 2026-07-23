---
title: "SQL commands"
description: "SQL commands reference."
disable_list: true
menu:
  main:
    identifier: "commands"
    parent: "sql"
    weight: 100
aliases:
  - /sql/alter-owner/
  - /self-managed/v25.2/sql/
  - /self-managed/v25.2/sql/alter-system-set/
  - /self-managed/v25.2/sql/alter-system-reset/
  - /self-managed/v25.2/sql/alter-swap/
  - /self-managed/v25.2/sql/alter-role/
  - /self-managed/v25.2/sql/alter-owner/
  - /self-managed/v25.2/sql/alter-rename/
  - /self-managed/v25.2/sql/create-materialized-view/
  - /self-managed/v25.2/sql/create-cluster/
  - /self-managed/v25.2/sql/create-connection/
  - /self-managed/v25.2/sql/create-role/
  - /self-managed/v25.2/sql/create-sink/
  - /self-managed/v25.2/sql/create-sink/kafka/
  - /self-managed/v25.2/sql/set/
  - /self-managed/v25.2/sql/show/
  - /self-managed/v25.2/sql/show-roles/
  - /self-managed/v25.2/sql/appendix-cluster-sizes/
  - /self-managed/v25.2/sql/system-clusters/
  - /self-managed/v25.2/sql/system-catalog/
  - /self-managed/v25.2/sql/system-catalog/mz_catalog/
  - /self-managed/v25.2/sql/system-catalog/pg_catalog/
  - /self-managed/v25.2/sql/system-catalog/mz_internal/
  - /self-managed/v25.2/sql/system-catalog/mz_introspection/
  - /self-managed/v25.2/sql/system-catalog/information_schema/
---

## Create/Alter/Drop Objects

{{< sql-commands-table-by-label label="object" group_by="object" >}}

## Create/Read/Update/Delete Data

The following commands perform CRUD operations on materialized views, views,
sources, and tables:

{{< yaml-table data="sql_commands_crud" noHeader=true >}}

## RBAC

Commands to manage roles and privileges and owners:

{{< yaml-table data="sql_commands_rbac" noHeader=true >}}

## Query Introspection (`Explain`)

{{< yaml-list data="sql_commands_all" label="explain" numColumns="1" >}}

## Object Introspection (`SHOW`) { #show }

{{< yaml-list data="sql_commands_all" label="show" numColumns="3" >}}

## Session

Commands related with session state and configurations:

{{< yaml-list data="sql_commands_all" label="session" numColumns="1" >}}

## Validations

{{< yaml-list data="sql_commands_all" label="other" >}}

## Prepared Statements

{{< yaml-list data="sql_commands_all" label="prepared statements" >}}
