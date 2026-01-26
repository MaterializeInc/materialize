# Self-Managed

Monitor the performance of your Self-Managed Materialize region with Datadog and Grafana.



This section covers monitoring and alerting for Self-Managed Materialize.

### Monitoring

You can monitor the performance and overall health of your Self-Manaed
Materialize.

To help you get started, the following guides are available:

- [Grafana using Prometheus](/manage/monitor/self-managed/prometheus/)

- [Datadog using Prometheus SQL Exporter](/manage/monitor/self-managed/datadog/)


### Alerting

After setting up a monitoring tool, you can configure alert rules. Alert rules
send a notification when a metric surpasses a threshold. This will help you
prevent operational incidents. For alert rules guidelines, see
[Alerting](/manage/monitor/self-managed/alerting/).



---

## Alerting


After setting up a monitoring tool, it is important to configure alert rules. Alert rules send a notification when a metric surpasses a threshold. This will help you prevent operational incidents.

This page describes which metrics and thresholds to build as a starting point. For more details on how to set up alert rules in Datadog or Grafana, refer to:

 * [Datadog monitors](https://docs.datadoghq.com/monitors/)
 * [Grafana alerts](https://grafana.com/docs/grafana/latest/alerting/fundamentals/)

## Thresholds

Alert rules tend to have two threshold levels, and we are going to define them as follows:
 * **Warning:** represents a call to attention to a symptom with high chances to develop into an issue.
 * **Alert:** represents an active issue that requires immediate action.

For each threshold level, use the following table as a guide to set up your own alert rules:

Metric | Warning | Alert | Description
-- | -- | -- | --
CPU | 85% | 100% | Average CPU usage for a cluster in the last *15 minutes*.
Memory | 80% | 90% | Average memory usage for a cluster in the last *15 minutes*.
Source status | - | On Change | Source status change in the last *1 minute*.
Cluster status | - | On Change | Cluster replica status change in the last *1 minute*.
Freshness | > 5s | > 1m | Average [lag behind an input](/sql/system-catalog/mz_internal/#mz_materialization_lag) in the last *15 minutes*.

### Custom Thresholds

For the following table, replace the two variables, _X_ and _Y_, by your organization and use case:

Metric | Warning | Alert | Description
-- | -- | -- | --
Latency | Avg > X | Avg > Y | Average latency in the last *15 minutes*. Where X and Y are the expected latencies in milliseconds.
Credits | Consumption rate increase by X% | Consumption rate increase by Y% | Average credit consumption in the last *60 minutes*.


---

## Datadog using Prometheus SQL Exporter


This guide walks you through the steps required to monitor the performance and
overall health of your Materialize region using [Datadog](https://www.datadoghq.com/).

## Before you begin

To make Materialize metadata available to Datadog, you must configure and run
the following additional services:

* A Prometheus SQL Exporter.
* A Datadog Agent configured with an [OpenMetrics check](https://docs.datadoghq.com/integrations/openmetrics/).


## Step 1. Set up a Prometheus SQL Exporter


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

## Grafana using Prometheus


> **Warning:** The metrics scraped are unstable and may change across releases.
>


This guide walks you through the steps required to monitor the performance and
overall health of your Materialize instance using Prometheus and Grafana.

## Before you begin

Ensure you have:

- A self-managed instance of Materialize installed with helm values `observability.enabled=true`, `observability.podMetrics.enabled=true`, and `prometheus.scrapeAnnotations.enabled=true`
- [Helm](https://helm.sh/docs/intro/install/) version 3.2.0+ installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed and configured

> **Important:** This guide assumes you have administrative access to your Kubernetes cluster and the necessary permissions to install Prometheus.
>


## 1. Download our Prometheus scrape configurations (`prometheus.yml`)
  Download the Prometheus scrape configurations that we'll use to configure Prometheus to collect metrics from Materialize:
  ```shell
  curl -o prometheus_scrape_configs.yml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/main/doc/user/data/self_managed/monitoring/prometheus.yml
  ```




## 2. Install Prometheus to your Kubernetes cluster

  > **Note:** This guide uses the [prometheus-community](https://github.com/prometheus-community/helm-charts) Helm chart to install Prometheus.
>



1. Download the prometheus-community default chart values (`values.yaml`):
   ```bash
   curl -O https://raw.githubusercontent.com/prometheus-community/helm-charts/refs/heads/main/charts/prometheus/values.yaml
   ```

2. Within `values.yaml`, replace `serverFiles > prometheus.yml > scrape_configs` with our scrape configurations (`prometheus_scrape_configs.yml`).

3. Install the operator with the updated `values.yaml`:
   ```bash
   kubectl create namespace prometheus
   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
   helm repo update
   helm install --namespace prometheus prometheus prometheus-community/prometheus \
   --values values.yaml
   ```

## 3. Optional. Visualize through Grafana

1. Install the Grafana helm chart following [this guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/helm/).


2.  Set up port forwarding to access the Grafana UI:
    ```bash
    MZ_POD_GRAFANA=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=grafana -o custom-columns="NAME:.metadata.name" --no-headers)
    kubectl port-forward pod/$MZ_POD_GRAFANA 3000:3000 -n monitoring
    ```

    > **Warning:** The port forwarding method is for testing purposes only. For production environments, configure an ingress controller to securely expose the Grafana UI.
>


3. Open the Grafana UI on [http://localhost:3000](http://localhost:3000) in a browser.

4. Add a Prometheus data source. In the Grafana UI, under **Connection > Data sources**,
   - Click **Add data source** and select **prometheus**
   - In the Connection section, set **Prometheus server URL** to `http://<prometheus server name>.<namespace>.svc.cluster.local:<port>`(e.g. `http://prometheus-server.prometheus.svc.cluster.local:80`).

    ![Image of Materialize Console login screen with mz_system user](/images/self-managed/grafana-prometheus-datasource-setup.png)

4. Download the following dashboards:
    ### Environment overview dashboard
    An overview of the state of different objects in your environment.

    ```bash
    # environment_overview_dashboard.json
    curl -O https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/self-managed-docs/v25.2/doc/user/data/monitoring/grafana_dashboards/environment_overview_dashboard.json
    ```
    ### Freshness overview dashboard
    An overview of how out of date objects in your environment are.
     ```bash
     # freshness_overview_dashboard.json
    curl -O https://raw.githubusercontent.com/MaterializeInc/materialize/refs/heads/self-managed-docs/v25.2/doc/user/data/monitoring/grafana_dashboards/freshness_overview_dashboard.json
    ```

5. [Import the dashboards using the Prometheus data source](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/import-dashboards/#importing-a-dashboard)

    ![Image of Materialize Console login screen with mz_system user](/images/self-managed/grafana-monitoring-success.png)
