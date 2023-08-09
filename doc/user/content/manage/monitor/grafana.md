---
title: "Monitor Materialize with Grafana"
description: "Use Grafana to monitor Materialize."
menu:
  main:
    parent: "monitor"
---

This guide walks you through the steps to monitor the performance and health of
your Materialize region with  Grafana integration.


## Before you begin 

For this guide, you will need to configure and run the following additional
services:

* A Prometheus SQL Exporter
* A Grafana instance

## Step 1. Set up a Prometheus SQL Exporter

To export metrics from Materialize and expose them to Grafana, you need to
configure a Prometheus SQL Exporter. This service will run SQL queries against
Materialize and export the metrics to a Prometheus endpoint.

We recommend using
[`justwatchcom/sql_exporter`](https://github.com/justwatchcom/sql_exporter),
which has been tried and tested in production environments.

1. In the host that will run the Prometheus SQL Exporter, create a configuration
   file called `config.yml` for the Exporter configuration.
   
   **Tip:** Use [the sample
   `config.yml`](https://github.com/MaterializeInc/demos/blob/main/integrations/grafana/local/config.yml.example) as guidance to bootstrap your monitoring with key Materialize metrics and indicators.
   
1. In the configuration file, define the connection to your Materialize region
   under `connections` using the credentials provided in the [Materialize
   console](https://console.materialize.com)
   
   {{< note >}}
   You need to escape the special `@` character in your user email for a successful connection.
   For example, instead of `name@email.com`, use `name%40email.com`.
   {{</ note >}}
   
   **Filename:** config.yml
   ```yaml
   ---
   jobs:
   - name: "materialize"
     interval: '1m'
     connections:
     - "postgres://<USER>:<PASSWORD>@<HOST>:<PORT>/materialize?application_name=mz_grafana_integration&sslmode=require"
     ...
  ```
  
  To specify different configurations for different sets of metrics, like a
  different `interval`, use additional jobs with a dedicated connection.
  
  
  ```yaml
   ---
   jobs:
   - name: "materialize"
     interval: '1h'
     connections:
     - "postgres://<USER>:<PASSWORD>@<HOST>:<PORT>/materialize?application_name=mz_grafana_integration&sslmode=require"
     ...
  ```

1. In the configuration file, configure the queries that the Prometheus SQL
   Exporter will run.
   
  For example, the query below will measure the memory usage of your
  replicas.
  
   ```yaml
   ...
   queries:
    # Prefixed with sql_ and used as the metric name.
    - name: "replica_memory_usage"
        # Required option of the Prometheus default registry. Currently NOT
        # used by the Prometheus server.
        help: "Replica memory usage"
        # Array of columns used as additional labels. All lables should
        # be of type text.
        labels:
        - "replica_name"
        - "cluster_id"
        # Array of columns used as metric values. All values should be
        # of type float.
        values:
        - "memory_percent"
        # The SQL query that is run unalterted for each job.
        query:  |
                SELECT
                   name::text AS replica_name,
                   cluster_id::text AS cluster_id,
                   memory_percent::float AS memory_percent
                FROM mz_cluster_replicas r
                JOIN mz_internal.mz_cluster_replica_utilization u ON r.id=u.replica_id;
   ```
   
1. After adding your queries, follow the instructions in the [`sql_exporter` repository](https://github.com/justwatchcom/sql_exporter#getting-started)to run the service using the configuration file from the previous step.

## Step 2. Set up a Grafana instance

To visualize the metrics available in the Prometheus SQL Exporter, you must use
Grafana with the OpenMetrics specification to parse the data.

1. Follow the [instructions to install and run Grafana withe
   OpenMetrics](https://grafana.com/blog/2022/05/10/how-to-collect-prometheus-metrics-with-the-opentelemetry-collector-and-grafana/)


## Step 3. Build a monitoring dashboard


