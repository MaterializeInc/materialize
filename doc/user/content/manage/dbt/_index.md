---
title: "Use dbt to manage Materialize"
description: "How to use dbt and Materialize to transform streaming data in real time."
disable_list: true
aliases:
  - /guides/dbt/
  - /third-party/dbt/
  - /integrations/dbt/
menu:
  main:
    parent: manage
    weight: 20
    identifier: "manage-dbt"
    name: "Manage with dbt"

---

[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for
data transformation ("the T in ELT"). It combines the accessibility of SQL with
software engineering best practices, allowing you to not only build reliable
data pipelines, but also document, test and version-control them.

Setting up a dbt project with Materialize is similar to setting it up with any
other database that requires a non-native adapter.

{{< note >}}
The `dbt-materialize` adapter can only be used with **dbt Core**. Making the
adapter available in dbt Cloud depends on prioritization by dbt Labs. If you
require dbt Cloud support, please [reach out to the dbt Labs team](https://www.getdbt.com/community/join-the-community/).
{{</ note >}}


## Available guides

{{< multilinkbox >}}
{{< linkbox title="To get started" >}}
[Get started with dbt and Materialize](./get-started/)
{{</ linkbox >}}
{{< linkbox title="Development guidelines" >}}
[Development guidelines](./development-workflows)
{{</ linkbox >}}
{{< linkbox title="Deployment" >}}
- [Blue-green deployment guide](/manage/dbt/blue-green-deployments/)

- [Slim deployment guide](/manage/dbt/slim-deployments/)

{{</ linkbox >}}
{{</ multilinkbox >}}


## See also

As a tool primarily meant to manage your data model, the `dbt-materialize`
adapter does not expose all Materialize objects types. If there is a **clear
separation** between data modeling and **infrastructure management ownership**
in your team, and you want to manage objects like
[clusters](/concepts/clusters/), [connections](/sql/create-connection/), or
[secrets](/sql/create-secret/) as code, we recommend using the [Materialize
Terraform provider](/manage/terraform/) as a complementary deployment tool.
