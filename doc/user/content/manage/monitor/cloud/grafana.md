---
title: "Grafana"
description: "How to monitor the performance and overall health of your Materialize region using Grafana."
menu:
  main:
    parent: "monitor-cloud"
    weight: 10
---

This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Grafana](https://grafana.com/).

## Before you begin

To make Materialize metadata available to Grafana, you must configure and run
the following additional services:

* A Prometheus SQL Exporter.
* A metrics scraper: [Grafana Agent](https://grafana.com/docs/agent/latest/?pg=oss-agent) for Grafana Cloud users, and [Prometheus](https://prometheus.io/download/) for self-hosted Grafana.

## Step 1. Set up a Prometheus SQL Exporter

{{< note >}}
As a best practice, we strongly recommend using [service accounts](/security/users-service-accounts/create-service-accounts)
to connect external applications, like Grafana, to Materialize.
{{</ note >}}

To export metrics from Materialize and expose them in a format that Grafana can
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

2. In the configuration file, define the connection to your Materialize region
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
     - "postgres://<USER>:<PASSWORD>@<HOST>:6875/materialize?application_name=mz_Grafana_integration&sslmode=require"
     ...
   ```

   To specify different configurations for different sets of metrics, like a
   different `interval`, use additional jobs with a dedicated connection.

   ```yaml
   ...
   - name: "materialize"
     interval: '1h'
     connections:
     - "postgres://<USER>:<PASSWORD>@<HOST>:6875/materialize?application_name=mz_Grafana_integration&sslmode=require"
     ...
   ```

3. Then, configure the `queries` that the Prometheus SQL Exporter should run at the specified `interval`. Take [these considerations](#considerations) into account when exporting metrics from Materialize.

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

4. Once you are done with the Prometheus SQL Exporter configuration,
   follow the intructions in the [`sql_exporter` repository](https://github.com/justwatchcom/sql_exporter#getting-started)
   to run the service using the configuration file from the previous step.

## Step 2. Set up a metrics scraper

To scrape the metrics available in the Prometheus SQL Exporter endpoint, you
must then set up a [Grafana Agent](https://grafana.com/docs/agent/latest/?pg=oss-agent) for Grafana cloud, or [Prometheus](https://prometheus.io/download/) for the self-hosted version:

{{< tabs >}}
{{< tab "Grafana Cloud">}}

1. Follow the [instructions to install and run a Grafana Agent](https://grafana.com/docs/agent/latest/static/set-up/install/)
   in your host.

2. To configure a [Prometheus scrape](https://grafana.com/docs/grafana-cloud/monitor-infrastructure/metrics/metrics-prometheus/)
   for the Grafana Agent installed in the previous step, create and edit the [agent configuration file.](https://grafana.com/docs/agent/latest/static/configuration/create-config-file/)

   **Filename:** agent.yaml
   ```yaml
      ...
      scrape_configs:
         - job_name: node
         static_configs:
         - targets: ['<EXPORTER_HOST>:9237']
      remote_write:
         - url: <REMOTE_WRITE_URL>
         basic_auth:
            username: <USERNAME>
            password: <PASSWORD>
   ```

   **Tip:** see [this sample](https://github.com/MaterializeInc/demos/blob/main/integrations/grafana/cloud/agent.yaml)
   for all available configuration options.

   For more details on how to configure, run and troubleshoot Grafana Agents, see the [Grafana documentation](https://grafana.com/docs/agent/latest/).

   <br/>
   <details><summary>Video for generating configuration values for the first time.</summary>

   ![Gif](https://github.com/MaterializeInc/demos/assets/11491779/e512a95f-e3c6-433d-bc8f-6f5138b08115)

   </details>

{{< /tab >}}
{{< tab "Self-hosted Grafana">}}
1. Follow the [instructions to install and run Prometheus](https://prometheus.io/docs/prometheus/latest/installation/)
   in your host.

2. To configure a [Prometheus scrape](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config), edit the
   `prometheus.yml` file as follows:

   **Filename**: prometheus.yml
   ```yaml
      ...
      - job_name: sql_exporter
         scrape_interval: 15s
         static_configs:
            - targets: ['<EXPORTER_HOST>:9237']
            labels:
               instance: sql_exporter
   ```

     **Tip:** see [this sample](https://github.com/MaterializeInc/demos/blob/main/integrations/grafana/local/prometheus.yml) for all available configuration options.

3. Follow [the instructions](https://grafana.com/docs/grafana/latest/datasources/prometheus/) to add **Prometheus** as a new data source in Grafana.

   **Tip:** see [this sample](https://github.com/MaterializeInc/demos/blob/main/integrations/grafana/local/misc/datasources/prometheus.yml) for a Prometheus data source configuration.


For more details on how to configure, run and troubleshoot Prometheus, see the [Prometheus documentation](https://prometheus.io/docs/introduction/overview/).
{{< /tab >}}
{{< /tabs >}}

## Step 3. Build a monitoring dashboard

With the Prometheus SQL Exporter running SQL queries againt your Materialize
region and exporting the results as metrics, and a scraper routing
these metrics to Grafana, you're ready to build a monitoring
dashboard!

**Tip:** use [this sample](https://github.com/MaterializeInc/demos/blob/main/integrations/grafana/local/misc/dashboards/dashboard.json)
to bootstrap a new dashboard with the key Materialize metrics and indicators
defined in the sample `config.yml`.

1. **Go to** Grafana.

2. Navigate to **Dashboards**, click **New** and select the option **Import**.

3. To use the sample dashboard, copy and paste the contents of the provided
   sample `.json` file in the **Import via panel json** text field, click **Load** and then **Import**.

    <br>

    <img width="1728" alt="Template Grafana monitoring dashboard" src="https://github.com/joacoc/materialize/assets/11491779/500fdc03-546c-4f56-b2c3-dc4e92e04328">

## Considerations

Before adding a custom query, make sure to consider the following:

1. The label set cannot repeat across rows within the results of the same query.
2. Columns must not contain `NULL` values.
3. Value columns must be of type `float`.
4. Queries can impact cluster performance.
