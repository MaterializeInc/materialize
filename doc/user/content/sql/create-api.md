---
title: "CREATE API"
description: "`CREATE API` exposes a Prometheus scrape endpoint that serves metrics defined with `CREATE METRIC`."
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`CREATE API` defines a Prometheus scrape endpoint backed by views in
Materialize. After creating the API, any [`CREATE METRIC`](../create-metric)
in the same API is exposed at
`GET /metrics/custom/<database>/<schema>/<api_name>` on every HTTP listener
whose configuration enables the custom-metrics route.

## Syntax

{{% include-syntax file="examples/create_api" example="syntax" %}}

## Details

### Endpoint exposure

Whether `/metrics/custom/...` is reachable on a given HTTP listener is a
deployment-time decision controlled by the listener's configuration, not by
the `CREATE API` statement. APIs are not bound to a specific listener; on
listeners where the route is enabled, every API the requesting role can see
is reachable.

### Scraping

Each request to `/metrics/custom/<database>/<schema>/<api_name>` re-evaluates
every metric in the API on the API's cluster. Empty exposures (an API with no
metrics, or metrics whose backing relation is empty) return `200 OK` with no
samples.

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-api" %}}

### Scrape-time privileges

`CREATE API` and `CREATE METRIC` only check the privileges of the role
creating those objects. When `/metrics/custom/<database>/<schema>/<api_name>`
is scraped, Materialize evaluates each metric's source relation as the role
that authenticated the HTTP request. That role must hold:

{{% include-headless "/headless/sql-command-privileges/scrape-api" %}}

On a listener with `authenticator_kind = "None"`, unauthenticated requests
run as `anonymous_http_user`. Grant the required privileges to that role
(or to `PUBLIC`) if you intend to expose the API without authentication.

## Examples

Create a relation to expose, an API, then a metric backed by that relation:

```mzsql
-- A table (or source) holding the raw data.
CREATE TABLE orders (id int, status text);
INSERT INTO orders VALUES
  (1, 'open'), (2, 'open'), (3, 'closed');

-- A view that aggregates it into one row per label value.
CREATE MATERIALIZED VIEW orders_by_status AS
  SELECT status, count(*) AS count
  FROM orders
  GROUP BY status;

CREATE API materialize.public.app_metrics
  FORMAT PROMETHEUS
  IN CLUSTER quickstart;

CREATE METRIC materialize.public.orders_open
  IN API materialize.public.app_metrics AS (
    TYPE 'gauge',
    HELP 'Number of open orders by status',
    VALUES FROM materialize.public.orders_by_status,
    VALUE COLUMN 'count'
  );
```

Scrape it:

```
curl https://<region-host>/metrics/custom/materialize/public/app_metrics
```

## Related pages

- [`CREATE METRIC`](../create-metric)
- [`DROP API`](../drop-api)
