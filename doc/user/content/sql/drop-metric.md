---
title: "DROP METRIC"
description: "`DROP METRIC` removes a Prometheus metric created with `CREATE METRIC`."
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`DROP METRIC` removes a metric created with [`CREATE METRIC`](../create-metric).
The metric's API is left in place; if the API has no other metrics, it
continues to serve an empty exposition.

## Syntax

```mzsql
DROP METRIC [IF EXISTS] <metric_name> [, ...] [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named metric does not exist.
_metric&lowbar;name_ | The metric to drop.
**CASCADE** | Optional. Permitted for symmetry with other `DROP` statements. Metrics do not have dependents today, so `CASCADE` and `RESTRICT` behave identically.
**RESTRICT** | Optional. _(Default)_

## Examples

```mzsql
DROP METRIC materialize.public.orders_open;
```

After dropping the metric, scraping its API no longer emits samples for
`orders_open`.

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/drop-metric" %}}

## Related pages

- [`CREATE METRIC`](../create-metric)
- [`DROP API`](../drop-api)
- [`DROP OWNED`](../drop-owned)
