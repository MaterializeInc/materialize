---
title: "SQL commands"
description: "SQL commands reference."
disable_list: true
menu:
  main:
    identifier: "commands"
    parent: "reference"
    weight: 100

---

## Create/Alter/Drop Objects

{{< sql-commands-table-by-label label="object" group_by="object" >}}

## Create/Read/Update/Delete Data

The following commands perform CRUD operations on materialized views, views,
sources, and tables:

{{< yaml-table data="sql_commands_crud" noHeader=true >}}

## RBAC

Commands to manage roles and privileges:

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
