---
title: "Monitoring"
description: "Find details about monitoring your Materialize instances"
---

_This page is a work in progress and will have more detail in the coming months.
If you have specific questions, feel free to [file a GitHub
issue](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md)._

Materialize supports integration with monitoring tools using HTTP endpoints.

## Quick Monitoring Dashboards

Materialize provides a recommended grafana dashboard and an all-inclusive docker image
preconfigured to run it as [`materialize/simple-monitor`](simplemon-hub).

The only configuration required to get started with the docker image is the
`MATERIALIZED_URL=<host>:<port>` environment variable.

While simple-monitor is provided as a convenience and should not be relied on for
production monitoring, if you would like to persist metrics across restarts of the
container you can mount a docker volume onto `/prometheus`.

## Health check

Materialize supports a minimal health check endpoint at `<materialized
host>/status`.

## Prometheus

Materialize exposes [Prometheus](https://prometheus.io/) metrics at the default
path, `<materialized host>/metrics`.

Materialize broadly publishes the following types of data there:

- Materialize-specific data with a `mz_*` prefix. For example,
  `rate(mz_responses_sent_total[10s])` will show you the number of responses
  averaged over 10 second windows.
- Standard process metrics with a `process_*` prefix. For exmple, `process_cpu`.

## Grafana

Materialize provides a [recommended dashboard][dashboard-json] that you can [import into
Grafana][graf-import]. It relies on you having configured prometheus to scrape
materialized.

[simplemon-hub]: https://hub.docker.com/repository/docker/materialize/materialized
[dashboard-json]: ../../../misc/monitoring/simple-monitor/user/conf/grafana/dashboards/overview.json
[graf-import]: https://grafana.com/docs/grafana/latest/reference/export_import/#importing-a-dashboard
