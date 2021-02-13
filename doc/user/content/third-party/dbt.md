---
title: "Using dbt"
description: "Get details about using Materialize with dbt"
menu:
  main:
    parent: 'third-party'
---

You can use [dbt] to easily create views and materialized views in your `materialized`
instance. To do so, all you need to do is install our [`dbt-materialize`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/README.md)
adapter.

## What's missing?

{{< warning >}}
Materialize does not offer production-level support for dbt.
{{< /warning >}}

The [`dbt-materialize`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/README.md)
adapter is still a work in progress and is not yet suitable for production use-cases. You can check its limitations
and track our progress [here](https://github.com/MaterializeInc/materialize/issues/5462).

[dbt]: https://www.getdbt.com/
