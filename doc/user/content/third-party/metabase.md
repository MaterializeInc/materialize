---
title: "Using Metabase"
description: "Get details about using Materialize with Metabase"
menu:
  main:
    parent: 'third-party'
---

You can use [Metabase] to create business intelligence dashboards using the
real-time data streams in your Materialize instance. To get started, check out
our [metabase-materialize-driver].

## Limitations

Our [metabase-materialize-driver] uses a forked version of pgjdbc to work. This
should not affect users trying to use the driver, only those who wish to make updates.
Track our progress in un-forking the pgjdbc code [here](https://github.com/MaterializeInc/materialize/issues/3727).


[Metabase]: https://www.metabase.com/
[metabase-materialize-driver]: https://github.com/MaterializeInc/metabase-materialize-driver
