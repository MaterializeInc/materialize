---
title: "Using Metabase"
description: "Get details about using Materialize with Metabase"
menu:
  main:
    parent: 'third-party'
---

You can use [Metabase] to create business intelligence dashboards using the
real-time data streams in your Materialize instance. To get started, check out
our [`metabase-materialize-driver`](https://github.com/MaterializeInc/metabase-materialize-driver).

## What's missing?

{{< warning >}}
Materialize does not offer production-level support for Metabase.
{{< /warning >}}

Our [`metabase-materialize-driver`](https://github.com/MaterializeInc/metabase-materialize-driver)
uses a forked version of pgjdbc to connect to Materialize. While this should not have an affect
for anyone using the driver, we still aim to connect to Metabase cleanly in the future. Track
our progress in un-forking the pgjdbc code [here](https://github.com/MaterializeInc/materialize/issues/3727).

[Metabase]: https://www.metabase.com/
