---
title: "Infrastructure as Code"
description: "Manage Materialize infrastructure and data models using infrastructure-as-code tools like dbt and Terraform."
menu:
  main:
    parent: "integrations"
    weight: 90
    name: "Infrastructure as Code"
---

Materialize supports infrastructure-as-code tools that help you version control,
automate, and manage your data infrastructure:

{{< yaml-table data="infrastructure_as_code" >}}

## dbt

[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for
data transformation ("the T in ELT"). It combines the accessibility of SQL with
software engineering best practices, allowing you to not only build reliable
data pipelines, but also document, test and version-control them.

The [`dbt-materialize`](/manage/dbt/) adapter allows you to use dbt to manage
views, materialized views, and indexes in Materialize. This is ideal for teams
that want to version control their data model and use dbt's testing and
documentation features.

[Get started with dbt and Materialize](/manage/dbt/get-started/)

## Terraform

[Terraform](https://www.terraform.io/) is an infrastructure-as-code tool that
allows you to manage your resources in a declarative configuration language.
The [Materialize Terraform provider](/manage/terraform/) helps you safely and
predictably provision and manage connections, sources, clusters, and other
database objects.

Terraform is ideal for teams that want to manage infrastructure components like
clusters, connections, secrets, and cloud resources alongside their other
infrastructure.

[Get started with Terraform and Materialize](/manage/terraform/get-started/)

## When to use which tool

| Use case | Recommended tool |
|----------|-----------------|
| Manage views, materialized views, and indexes | dbt |
| Orchestrate data model deployments | dbt |
| Blue/green deployments for data models | dbt |
| Manage clusters and cluster sizing | Terraform |
| Manage connections and secrets | Terraform |
| Manage cloud resources (PrivateLink, networking) | Terraform |
| RBAC and access control | Terraform |

Many teams use **both tools together**: Terraform for infrastructure and
configuration, and dbt for data modeling. See the [dbt documentation](/manage/dbt/)
and [Terraform documentation](/manage/terraform/) for more details.
