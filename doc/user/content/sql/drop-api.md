---
title: "DROP API"
description: "`DROP API` removes a Prometheus scrape endpoint created with `CREATE API`."
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`DROP API` removes an API created with [`CREATE API`](../create-api). If any
[metrics](../create-metric) depend on the API, you must drop them first or
use the `CASCADE` option.

## Syntax

```mzsql
DROP API [IF EXISTS] <api_name> [, ...] [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named API does not exist.
_api&lowbar;name_ | The API to drop.
**CASCADE** | Optional. Also drop every metric bound to the API.
**RESTRICT** | Optional. Refuse to drop the API if any metric depends on it. _(Default)_

## Examples

Drop an API with no dependent metrics:

```mzsql
DROP API materialize.public.app_metrics;
```

Drop an API together with its metrics:

```mzsql
DROP API materialize.public.app_metrics CASCADE;
```

After a successful `DROP API`, subsequent requests to
`/api/metrics/custom/<database>/<schema>/<api_name>` return `404`.

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/drop-api" %}}

## Related pages

- [`CREATE API`](../create-api)
- [`DROP METRIC`](../drop-metric)
- [`DROP OWNED`](../drop-owned)
