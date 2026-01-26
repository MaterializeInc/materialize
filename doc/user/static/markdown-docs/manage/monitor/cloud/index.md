# Cloud

Monitoring and alerting for Materialize Cloud



This section covers monitoring and alerting for Materialize Cloud.

### Monitoring

You can monitor the performance and overall health of your Materialize region.
To help you get started, the following guides are available:

- [Datadog](/manage/monitor/cloud/datadog/)

- [Grafana](/manage/monitor/cloud/grafana/)

### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/cloud/alerting/).



---

## Alerting


After setting up a monitoring tool, it is important to configure alert rules. Alert rules send a notification when a metric surpasses a threshold. This will help you prevent operational incidents.

This page describes which metrics and thresholds to build as a starting point. For more details on how to set up alert rules in Datadog or Grafana, refer to:

 * [Datadog monitors](https://docs.datadoghq.com/monitors/)
 * [Grafana alerts](https://grafana.com/docs/grafana/latest/alerting/fundamentals/)

## Thresholds

Alert rules tend to have two threshold levels, and we are going to define them as follows:
 * Warning: represents a call to attention to a symptom with high chances to develop into an issue.
 * Alert: represents an active issue that requires immediate action.

For each threshold level, use the following table as a guide to set up your own alert rules:

Metric | Warning | Alert | Description
-- | -- | -- | --
Memory utilization | 80% | 90% | Average [memory utilization](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_cluster_replica_utilization), (defined using heap_percent) for a cluster in the last *15 minutes*.
Source status | - | On Change | Source status change in the last *1 minute*.
Cluster status | - | On Change | Cluster replica status change in the last *1 minute*.
Freshness | > 5s | > 1m | Average [lag behind an input](/sql/system-catalog/mz_internal/#mz_materialization_lag) in the last *15 minutes*.

> **Note:** Customers on legacy cluster sizes should still monitor their Memory usage. Please [contact support](/support/) for questions.
>


### Custom Thresholds

For the following table, replace the two variables, _X_ and _Y_, by your organization and use case:

Metric | Warning | Alert | Description
-- | -- | -- | --
Latency | Avg > X | Avg > Y | Average latency in the last *15 minutes*. Where X and Y are the expected latencies in milliseconds.
Credits | Consumption rate increase by X% | Consumption rate increase by Y% | Average credit consumption in the last *60 minutes*.

## Maintenance window

Materialize has a release and a maintenance window almost every week at a defined [schedule](/releases/schedule/#cloud-upgrade-schedule).

During an upgrade, clients may experience brief connection interruptions, but the service otherwise remains fully available. Alerts may get triggered during this brief period of time. For this case, you can configure your monitoring tool to avoid unnecessary alerts as follows:

* [Datadog downtimes](https://docs.datadoghq.com/monitors/downtimes/)
* [Grafana mute timings](https://grafana.com/docs/grafana/latest/alerting/manage-notifications/mute-timings/)

## Status Page

All performance‑impacting incidents are communicated on our [status page](https://status.materialize.com/), which provides current and historical incident details and subscription options for notifications. Reviewing this page is the quickest way to confirm if a known issue is affecting your database.


---

## Datadog


This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Datadog](https://www.datadoghq.com/).

## Before you begin

To make Materialize metadata available to Datadog, you must configure and run
the following additional services:

* A Prometheus SQL Exporter.
* A Datadog Agent configured with an [OpenMetrics check](https://docs.datadoghq.com/integrations/openmetrics/).

## Step 1. Set up a Prometheus SQL Exporter

> **Note:** As a best practice, we strongly recommend using [service accounts](/security/users-service-accounts/create-service-accounts)
> to connect external applications, like Datadog, to Materialize.
>


To export metrics from Materialize and expose them in a format that Datadog can
consume, you need to configure and run a Prometheus SQL Exporter. This service
will run SQL queries against Materialize at specified intervals, and export the
resulting metrics to a Prometheus endpoint.

We recommend using [`justwatchcom/sql_exporter`](https://github.com/justwatchcom/sql_exporter),
which has been tried and tested in production environments.

1. In the host that will run the Prometheus SQL Exporter, create a configuration
   file (`config.yml`) to hold the Exporter configuration.

   > **Tip:** You can use [this sample
>    `config.yml.example`](https://github.com/MaterializeIncLabs/materialize-monitoring/blob/main/sql_exporter/config.yml)
>    as guidance to bootstrap your monitoring with some key Materialize metrics
>    and indicators.
>



1. In the configuration file, define the connection to your Materialize region
   under `connections` using the credentials provided in the [Materialize Console](/console/).

   > **Note:** You must escape the special `@` character in `USER` for a successful
>    connection. Example: instead of `name@email.com`, use `name%40email.com`.
>


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


---

## Grafana


This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Grafana](https://grafana.com/).

## Before you begin

To make Materialize metadata available to Grafana, you must configure and run
the following additional services:

* A Prometheus SQL Exporter.
* A metrics scraper: [Grafana Agent](https://grafana.com/docs/agent/latest/?pg=oss-agent) for Grafana Cloud users, and [Prometheus](https://prometheus.io/download/) for self-hosted Grafana.

## Step 1. Set up a Prometheus SQL Exporter

> **Note:** As a best practice, we strongly recommend using [service accounts](/security/users-service-accounts/create-service-accounts)
> to connect external applications, like Grafana, to Materialize.
>


To export metrics from Materialize and expose them in a format that Grafana can
consume, you need to configure and run a Prometheus SQL Exporter. This service
will run SQL queries against Materialize at specified intervals, and export the
resulting metrics to a Prometheus endpoint.

We recommend using [`justwatchcom/sql_exporter`](https://github.com/justwatchcom/sql_exporter),
which has been tried and tested in production environments.

1. In the host that will run the Prometheus SQL Exporter, create a configuration
   file (`config.yml`) to hold the Exporter configuration.

   > **Tip:** You can use [this sample
>    `config.yml.example`](https://github.com/MaterializeIncLabs/materialize-monitoring/blob/main/sql_exporter/config.yml)
>    as guidance to bootstrap your monitoring with some key Materialize metrics
>    and indicators.
>


2. In the configuration file, define the connection to your Materialize region
   under `connections` using the credentials provided in the [Materialize Console](/console/).

   > **Note:** You must escape the special `@` character in `USER` for a successful
>    connection. Example: instead of `name@email.com`, use `name%40email.com`.
>


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


**Grafana Cloud:**

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


**Self-hosted Grafana:**
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
