---
title: "Monitoring"
description: "Find details about your running Materialize instances"
aliases:
  - /monitoring
menu:
  main:
    parent: operations
---

_This page is a work in progress and will have more detail in the coming months.
If you have specific questions, feel free to [file a GitHub
issue](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md)._

Materialize supports integration with monitoring tools using both the
[Prometheus](#prometheus) format and via [SQL interface](#system-catalog-sql-interface)
that provide more information about internal state.

This page provides an overview of the tools available to diagnose a running materialized
instance. You can use the same features that are used by these tools to integrate
Materialize with other observability infrastructure.

## Quick monitoring dashboard

{{< warning >}}
The monitoring dashboard is provided on a best-effort basis. It relies on
Materialize's unstable [Prometheus metrics](#prometheus) and occasionally lags
behind changes to these metrics.

For best results, use only the latest version of the dashboard with the latest
version of Materialize.
{{< /warning >}}

Materialize provides a recommended Grafana dashboard and an all-inclusive Docker image
preconfigured to run it as [`materialize/dashboard`][simplemon-hub].

The only configuration required to get started with the Docker image is the
`MATERIALIZED_URL=<host>:<port>` environment variable.

As an example, if you are running Materialize in a cloud instance at the IP address
`172.16.0.0`, you can get a dashboard by running this command and
opening <http://localhost:3000> in your web browser:

```shell
$ docker run -d -p 3000:3000  -e MATERIALIZED_URL=172.16.0.0:6875 materialize/dashboard
#               expose ports  ______point it at materialize______
```

To instead run the dashboard on the machine on which you are running
Materialize, see the [Observing local Materialize](#observing-local-materialize)
section below.

The dashboard Docker image bundles Prometheus and Grafana together to make
getting insight into Materialize's performance easy. It is not particularly
configurable, and in particular is not designed to handle large metric volumes or long
uptimes. It will start truncating metrics history after about 1GB of storage, which
corresponds to about 3 days of data with the very fine-grained metrics collected inside
the container.

So, while the dashboard is provided as a convenience and should not be relied on for
production monitoring, if you would like to persist metrics across restarts of the
container you can mount a Docker volume onto `/prometheus`:

```console
$ docker run -d \
    -v /tmp/prom-data:/prometheus -u "$(id -u):$(id -g)" \
    -p 3000:3000 -e MATERIALIZED_URL=172.16.0.0:6875 \
    materialize/dashboard
```

### Observing local Materialize

Using the dashboard to observe a Materialize instance running on the same
machine as the dashboard is complicated by Docker. The solution depends upon
your host platform.

#### Inside Docker Compose or Kubernetes

Local schedulers like Docker Compose (which we use for our demos) or Kubernetes will
typically expose running containers to each other using their service name as a public
DNS hostname, but _only_ within the network that they are running in.

The easiest way to use the dashboard inside a scheduler is to tell the scheduler to run
it. Check out the [example configuration for Docker Compose][dc-example].

#### On macOS, with Materialize running outside of Docker

The problem with this is that `localhost` inside of Docker does not, on Docker for Mac,
refer to the macOS network. So instead you must use `host.docker.internal`:

```
docker run -p 3000:3000 -e MATERIALIZED_URL=host.docker.internal:6875 materialize/dashboard
```

#### On Linux, with Materialize running outside of Docker

Docker containers use a different network than their host by default, but that is easy to
override by using the `--network host` option. Using the host network means that ports will be
allocated from the host, so the `-p` flag is no longer necessary:

```
docker run --network host -e MATERIALIZED_URL=localhost:6875 materialize/dashboard
```

[simplemon-hub]: https://hub.docker.com/repository/docker/materialize/dashboard
[dashboard-json]: https://github.com/MaterializeInc/materialize/blob/main/misc/monitoring/dashboard/conf/grafana/dashboards/overview.json
[graf-import]: https://grafana.com/docs/grafana/latest/reference/export_import/#importing-a-dashboard
[dc-example]: https://github.com/MaterializeInc/materialize/blob/d793b112758c840c1240eefdd56ca6f7e4f484cf/demo/billing/mzcompose.yml#L60-L70

## Health check

{{< warning >}}
The health check is not part of Materialize's stable interface.
Backwards-incompatible changes to the health check may be made at any time.
{{< /warning >}}

Materialize supports a minimal HTTP health check endpoint at `http://<materialized
host>:6875/status`.

## Memory usage visualization

{{< warning >}}
The memory usage visualization is not part of Materialize's stable interface.
Backwards-incompatible changes to the visualization may be made at any time.
{{< /warning >}}

Materialize exposes an interactive, web-based memory usage visualization at
`http://<materialized host>:6875/memory` to aid in diagnosing unexpected memory
consumption. The visualization can display a diagram of the operators in each
running dataflow overlaid with the number of rows stored by each operator.

## Prometheus

{{< warning >}}
The Prometheus metrics are not part of Materialize's stable interface.
Backwards-incompatible changes to the exposed metrics may be made at any time.
{{< /warning >}}

Materialize exposes [Prometheus](https://prometheus.io/) metrics at the default
path, `<materialized host>/metrics`.

Materialize broadly publishes the following types of data there:

- Materialize-specific data with a `mz_*` prefix. For example,
  `rate(mz_responses_sent_total[10s])` will show you the number of responses
  averaged over 10 second windows.
- Standard process metrics with a `process_*` prefix. For exmple, `process_cpu`.

## System Catalog SQL Interface

The `mz_catalog` SQL interface provides a variety of ways to introspect Materialize. An
introduction to this catalog is available as part of our [SQL
documentation](/sql/system-catalog).

A user guide for debugging a running `materialized` using the system catalog is available
in the form of a walkthrough of useful [diagnostic queries](/ops/diagnosing-using-sql).

## Grafana

Materialize provides a [recommended dashboard][dashboard-json] that you can [import into
Grafana][graf-import]. It relies on you having configured Prometheus to scrape
Materialize.

## Datadog

Materialize metrics can be sent to Datadog via the
[OpenMetrics agent check](https://docs.datadoghq.com/integrations/openmetrics/),
which is bundled with recent versions of the Datadog agent.

Simply add the following configuration parameters to
`openmetrics.d/conf.yaml`:

Configuration parameter | Value
------------------------|------
`prometheus_url`        | `http://<materialized host>/metrics`
`namespace`             | Your choice
`metrics`               | `[mz*]` to select all metrics, or a list of specific metrics
