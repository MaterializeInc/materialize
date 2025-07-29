---
title: "Foreign data wrapper (FDW) "
description: "Use FDW to access Materialize"
menu:
  main:
    parent: "integrations"
    weight: 30
---

Materialize can be used as a remote server in a PostgreSQL foreign data wrapper (FDW). This allows you to query any object in Materialize as foreign tables from a PostgreSQL-compatible database. These objects appear as part of the local schema, making them accessible over an existing Postgres connection without requiring changes to application logic or tooling.

## Prerequisite

{{< include-md file="shared-content/fdw-setup-prereq.md" >}}

## Setup FDW in PostgreSQL

{{< include-md file="shared-content/fdw-setup-postgres.md" >}}
