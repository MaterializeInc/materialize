---
title: "Using dbt"
description: "Get details about using Materialize with dbt"
menu:
  main:
    parent: 'third-party'
---

You can use [dbt] to easily create views and materialized views in your `materialized`
instance. To do so, all you need to do is install our [dbt-materialize] adapter.

*Note*: The [dbt-materialize] adapter is still a work in progress and is not yet suitable
for production use-cases. You can check its limitations and track our progress
[here](https://github.com/MaterializeInc/materialize/issues/5462).

## Limitations

The [dbt-materialize] adapter requires a few things to be considered ready for production
use-cases. To track this progress, check [here](https://github.com/MaterializeInc/materialize/issues/5462).

[dbt]: https://www.getdbt.com/
[dbt-materialize]: https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/README.md
