---
title: "CREATE METRIC"
description: "`CREATE METRIC` exposes a relation as a Prometheus metric on an API created with `CREATE API`."
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`CREATE METRIC` binds a relation in Materialize to a Prometheus metric on an
existing [`API`](../create-api). Each row of the relation becomes a sample;
the value column supplies the numeric value, and the remaining columns
supply Prometheus label values.

## Syntax

{{% include-syntax file="examples/create_metric" example="syntax" %}}

## Details

### Sampling

When the API is scraped, Materialize evaluates the metric's source relation
on the API's cluster and produces one Prometheus sample per row. The same
metric can produce arbitrarily many samples, distinguished by their label
values.

A relation that produces no rows produces no samples. The metric's `# HELP`
and `# TYPE` lines are still emitted.

### Metric types

`TYPE 'gauge'` is the only supported metric type today. Counters and
histograms are not yet supported.

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-metric" %}}

## Examples

Expose a materialized view of order counts as a Prometheus gauge. The metric
needs a relation to read and an [`API`](../create-api) to attach to, so create
those first:

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

-- The API that exposes the metric.
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

A scrape of the API now returns one `orders_open{status="..."}` sample per
row of `orders_by_status`:

```nofmt
# HELP orders_open Number of open orders by status
# TYPE orders_open gauge
orders_open{status="open"} 2
orders_open{status="closed"} 1
```

## Related pages

- [`CREATE API`](../create-api)
- [`DROP METRIC`](../drop-metric)
