---
title: "Foreign data wrapper (FDW) "
description: "Use FDW to access Materialize"
menu:
  main:
    parent: "integrations"
    weight: 30
---

Materialize's support for PostgreSQL Foreign Data Wrappers (FDWs) allows users
to query remote data as if it were local.

## Prerequisite

{{< include-md file="shared-content/fdw-setup-prereq.md" >}}

## Setup FDW in PostgreSQL

{{< include-md file="shared-content/fdw-setup-postgres.md" >}}
