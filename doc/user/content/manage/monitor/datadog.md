---
title: "Datadog using Prometheus SQL Exporter"
description: "How to monitor the performance and overall health of your Materialize region using Datadog."
menu:
  main:
    parent: "monitor"
    weight: 5
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Datadog](https://www.datadoghq.com/).

## Before you begin

To make Materialize metadata available to Datadog, you must configure and run
the following additional services:

* A Prometheus SQL Exporter.
* A Datadog Agent configured with an [OpenMetrics check](https://docs.datadoghq.com/integrations/openmetrics/).

{{< note >}}
In the future, we plan to support a native Datadog integration that
continually reports metrics via the Datadog API.
{{< /note >}}

## Step 1. Set up a Prometheus SQL Exporter


To export metrics from Materialize and expose them in a format that Datadog can
consume, you need to configure and run a Prometheus SQL Exporter. This service
will run SQL queries against Materialize at specified intervals, and export the
resulting metrics to a Prometheus endpoint.

We recommend using [`justwatchcom/sql_exporter`](https://github.com/justwatchcom/sql_exporter),
which has been tried and tested in production environments.

1. In the host that will run the Prometheus SQL Exporter, create a configuration
   file (`config.yml`) to hold the Exporter configuration.

   {{< tip >}}
   You can use [this sample
   `config.yml.example`](https://github.com/MaterializeIncLabs/materialize-monitoring/blob/main/sql_exporter/config.yml)
   as guidance to bootstrap your monitoring with some key Materialize metrics
   and indicators.
   {{</ tip >}}


1. In the configuration file, define the connection to your Materialize region
   under `connections` using the credentials provided in the [Materialize Console](/console/).

   {{< note >}}
   You must escape the special `@` character in `USER` for a successful
   connection. Example: instead of `name@email.com`, use `name%40email.com`.
   {{</ note >}}

   **Filename:** config.yml
   ```yaml
   ---
   jobs:
   - name: "materialize"
     # Interval between the runs of the job
     interval: '1m'
     # Materialize connection string
     connections:
     - "postgres://<USER>:<PASSWORD>@<HOST>:6875/materialize?application_name=mz_datadog_integration&sslmode=require"
     ...
   ```

   To specify different configurations for different sets of metrics, like a
   different `interval`, use additional jobs with a dedicated connection.

   ```yaml
   ...
   - name: "materialize"
     interval: '1h'
     connections:
     - "postgres://<USER>:<PASSWORD>@<HOST>:6875/materialize?application_name=mz_datadog_integration&sslmode=require"
     ...
   ```

1. Then, configure the `queries` that the Prometheus SQL Exporter should run at the specified `interval`. Take [these considerations](#considerations) into account when exporting metrics from Materialize.

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

1. Once you are done with the Prometheus SQL Exporter configuration,
   follow the intructions in the [`sql_exporter` repository](https://github.com/justwatchcom/sql_exporter#getting-started)
   to run the service using the configuration file from the previous step.

## Step 2. Set up a Datadog Agent

To scrape the metrics available in the Prometheus SQL Exporter endpoint, you
must then set up a [Datadog Agent](https://docs.datadoghq.com/agent/) check
configured to scrape the OpenMetrics format.

1. Follow the [instructions to install and run a Datadog Agent](https://docs.datadoghq.com/agent/)
   in your host.

1. To configure an [OpenMetrics check](https://docs.datadoghq.com/integrations/openmetrics/)
   for the Datadog Agent installed in the previous step, edit the
   `openmetrics.d/conf.yaml` file at the root of the installation directory.

   **Filename**: openmetrics.d/conf.yaml
   ```yaml
   init_config:
       timeout: 50
   instances:
     - openmetrics_endpoint: <SQL_EXPORTER_HOST>/metrics/
       # The namespace to prepend to all metrics.
       namespace: "materialize"
       metrics: [.*]
   ```

  **Tip:** see [this sample](https://github.com/MaterializeInc/demos/blob/main/integrations/datadog/datadog/conf.d/openmetrics.yaml)
  for all available configuration options.

For more details on how to configure, run and troubleshoot Datadog Agents, see the [Datadog documentation](https://docs.datadoghq.com/getting_started/agent/).

## Step 3. Build a monitoring dashboard

With the Prometheus SQL Exporter running SQL queries againt your Materialize
region and exporting the results as metrics, and the Datadog Agent routing
these metrics to your Datadog account, you're ready to build a monitoring
dashboard!

**Tip:** use [this sample](https://github.com/MaterializeInc/demos/blob/main/integrations/datadog/dashboard.json)
to bootstrap a new dashboard with the key Materialize metrics and indicators
defined in the sample `config.yml`.

1. **Log in** to your Datadog account.

1. Navigate to **Dashboards**, and select **New Dashboard**.

1. To use the sample dashboard, navigate to ⚙️ in the upper right corner, and
   select **Import dashboard JSON**. Copy and paste the contents of the provided
   sample `.json` file.

    <br>

    <img width="1728" alt="Template Datadog monitoring dashboard" src="https://user-images.githubusercontent.com/11491779/216036715-9a4b4db7-8f93-4b6a-ac21-f7eb5a01d151.png">

## Considerations

Before adding a custom query, make sure to consider the following:

1. The label set cannot repeat across rows within the results of the same query.
2. Columns must not contain `NULL` values.
3. Value columns must be of type `float`.
4. The Datadog agent is subject to a limit of 2000 metrics.
5. Queries can impact cluster performance.
