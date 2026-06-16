---
title: "Manage Materialize"
description: ""
disable_list: true
menu:
  main:
    identifier: "manage"
    weight: 50
---

This section contains various resources for managing Materialize.

## Operational guides

| Guide | Description |
|-------|-------------|
| [Operational guidelines](/manage/operational-guidelines/) | General operational guidelines |
| [Monitoring and alerting](/manage/monitor/) | Guides to set up monitoring and alerting |
| [Disaster Recovery](/manage/disaster-recovery/) | Disaster recovery strategies for Materialize Cloud |

## Tools for managing Materialize

| Tool | Best for | Scope | Zero-downtime deployments |
|------|----------|-------|--------------------------|
| **Plain SQL / psql scripts** | Manual execution. No dependency tracking, no diff, no rollback. Where most teams start. | All object types | No |
| [**mz-deploy**](/manage/mz-deploy/) | SQL-native, git-based workflow with offline type-checking, unit tests, and staged deployments. | All object types | Yes |
| **[dbt](/manage/dbt/)** | Teams already invested in dbt. | Views & materialized views only | Yes (via `dbt-materialize` adapter macros) |
| **[Terraform](/manage/terraform/)** | Teams managing Materialize alongside other cloud infrastructure. | All object types | No |

## Usage and billing

| Guide | Description |
|-------|-------------|
| [**Usage & billing**](/administration/billing/) | Understand the billing model of Materialize |
