# Observing Materialize

This document covers how to peer into the internals of a Materialize instance to understand what
it's doing and how it's performing. The intended audience for this document are developers working
on Materialize and others interested in improving the performance of materialized.

## Sources of Metrics

### Prometheus Metrics

Operational metrics can be scraped by Prometheus from port `6875` under the `/metrics` path. For
example, if you are running a materialized instance on your local machine, you can run:

    curl http://localhost:6875/metrics

And you will receive something like the following:

    # HELP mz_arrangement_maintenance_active_info Whether or not maintenance is currently occuring
    # TYPE mz_arrangement_maintenance_active_info gauge
    mz_arrangement_maintenance_active_info{worker_id="0"} 0
    mz_arrangement_maintenance_active_info{worker_id="1"} 0
    mz_arrangement_maintenance_active_info{worker_id="10"} 0
    mz_arrangement_maintenance_active_info{worker_id="11"} 0

The metrics are generated directly by code in materialized using the our [own
fork](https://github.com/MaterializeInc/rust-prometheus/commits/master) of
[rust-prometheus](https://github.com/tikv/rust-prometheus).

#### Prometheus metrics from SQL

All of materialized's prometheus metrics are being imported into materialize at
a 1-second resolution and retained for 5 minutes, where you can inspect them and
their changes over time with SQL commands.

The tables where you can find them are:

- `mz_catalog.mz_metrics` - counter and gauge values,
- `mz_catalog.mz_metric_histograms` - histogram distributions, and
- `mz_catalog.mz_metrics_meta` - data types and help texts of materialized's
  prometheus metrics.

### CI MZ SQL Exporter

For operational metrics backed by SQL queries of materialized internal data structures, we use
[sql_exporter](https://github.com/free/sql_exporter) to run SQL queries and turn them into the
Prometheus metrics structure. If you're running the `ci_mz_sql_exporter` locally (in chbench, you
can do `mzcompose up -d prometheus_sql_exporter_mz` to start it), you can query its metrics by
running:

    curl http://localhost:6875/metrics

And you will receive something like the following:

    # HELP mz_perf_arrangement_records How many records each arrangement has
    # TYPE mz_perf_arrangement_records gauge
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="0"} 0
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="1"} 0
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="10"} 0
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="11"} 0
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="12"} 0
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="13"} 0
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="14"} 0
    mz_perf_arrangement_records{name="<unknown>",operator="215",worker="15"} 0

Be careful when scraping these metrics, as they are expensive to generate and involve running a
bunch of ad hoc queries against Materialize.

### Diagnostic Queries

Another method for gathering performance metrics is to run diagnostic queries directly using psql.
For examples, see the [diagnosting using sql
guide](https://materialize.com/docs/ops/troubleshooting/).

### Materialize Log File

Our stdout logs don't currently emit any performance metrics, but they do emit events such as new
sources being discovered, configuration of Kafka sources, timestamping method for sources.

Our Docker image is configured to dump log messages to stderr and not a file within the container.

### Host Level Metrics

Within our AMIs, we configure [Prometheus Node
Exporter](https://github.com/prometheus/node_exporter) to gather host level metrics. The metrics
can then be scraped by Prometheus.
