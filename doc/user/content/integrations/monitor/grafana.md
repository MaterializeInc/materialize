---
title: "Monitor Materialize with Grafana"
description: "Use Grafana to monitor Materialize."
menu:
  main:
    parent: ""
    name: 
---

Materialize exposes a system catalog that contains metadata about your running
Materialize instances. The system catalog consists of four schemas available
in all databases, which contain tables, and views that expose
different types of metadata. 

This metadata can help you track and monitor your Materialize usage in tools
like Grafana.

For this guide, you'll setup a Grafana integration and create a dashboard to
monitor cluster performance, credit consumption, and memory usage.

We'll also share some helpful queries you can use in your observability tools to

## Setup

Before you get started make sure you have the following:

- Docker

This integration quick start uses the Prometheus SQL exporter Docker image to
pass data to and from your Materialize instance and to a Grafana instance.
Clone the [example repository](https://github.com/MaterializeInc/demos/tree/main/integrations/prometheus-sql-exporter).

Rename the `config.yml.example` file to `config.yml`. Open the `config.yml` file in your text editor and look for the `connections`
settings.

This `connections` setting monitors your Materialize cluster replica metrics for
CPU, memory, credit usage, and replica status. There are also queries to monitor
source and sink usage.

```yaml
...
connections:
- "postgres://<USER>:<PASSWORD>@<HOST>:<PORT>/materialize?sslmode=require"
...
```
You can copy this string from the [Connect page](https://console.materialize.com) in your Materialize region.

> You need to escape the special `@` character in your user email for a
> successful connection. For example, `name@email.com`, use `name%40email.com`.

Create a new app password on the **Connect** page and paste it into the
`<PASSWORD>` section of the connection command.

Update the second `connections` setting near the end of the file. This
connection queries your Materialize storage usage and reports on objects using
storage resources.

## Deploy the SQL exporter and Grafana and Prometheus

In the current directory, run Docker to deploy the SQL exporter and the Grafana
and Prometheus agents. The SQL exporter tool uses the connection string you created in a
previous step to connect with your Materialize instance. 

```shell
$ docker compose up -d
```

The SQL exporter container runs a service that executes queries against your
database and then exports the metrics in a format that most monitoring services
can consume.

The Grafana container runs an agent to display your dashboard locally. The `docker-compose.yaml` file deploys Grafana with the OpenMetrics specification to allow it to parse the metrics returned by the SQL exporter.

## Review the Grafana dashboard

Now that the containers are running, navigate to your Grafana dashboard at
http://localhost:3000

Your dashboard has been preconfigured with the queries in the `config.yml` file.

## Helpful queries

This dashboard returns a lot of useful information about your Materialize
resources, but there are some more metrics in the system catalog that you may
find useful.

In your `config.yml` file, you can add additional metrics to the SQL exporter.
To display specific information about a table, add  a new query to the
configuration.

```sql
queries:
- name: "total orders"
   help: "Total Orders"
   values:
   - "count"
   query:  |
           SELECT count(*) FROM orders
```

To add more complex queries, you can use the template above and edit the query
and values you expect the query to return.


Most monitoring queries reference the `mz_internal` schema. For more information, visit the [`mz_internal` reference documentation](https://materialize.com/docs/sql/system-catalog/mz_internal/).

Monitor operator speed across workers:

```sql
SELECT mdo.id, mdo.name, mse.elapsed_ns
FROM mz_internal.mz_scheduling_elapsed AS mse,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mse.id = mdo.id
ORDER BY elapsed_ns DESC;
```

Monitor operator speed by worker:

```sql
SELECT mdo.id, mdo.name, mse.worker_id, mse.elapsed_ns
FROM mz_internal.mz_scheduling_elapsed_per_worker AS mse,
     mz_internal.mz_dataflow_operators AS mdo
WHERE mse.id = mdo.id
ORDER BY elapsed_ns DESC;
```

Monitor active `SUBSCRIBE` dataflows:


```sql
SELECT count(1) FROM (
    SELECT id
    FROM mz_internal.mz_dataflows
    WHERE substring(name, 0, 20) = 'Dataflow: subscribe'
    GROUP BY id
);
```
