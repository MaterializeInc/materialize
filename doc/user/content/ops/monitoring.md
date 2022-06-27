---
title: "Monitoring"
description: "Find details about your running Materialize instances"
aliases:
  - /monitoring
menu:
  main:
    parent: ops
    weight: 50
---

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

Since the dashboard runs inside a Docker container, using it to observe a Materialize
instance running on the same machine is complicated by Docker networking isolation. The
exact steps required to expose `materialized` to the dashboard process depends on exactly
how you are running `materialized`.

{{< tabs >}}

{{< tab "Docker" >}}

Local schedulers like Docker Compose (which we use for our demos) or Kubernetes will
typically expose running containers to each other using the container's service name as a
public DNS hostname, but _only_ within the network that the containers are running in.

The easiest way to use the dashboard inside a scheduler is to tell the scheduler to run
it. Check out the [example configuration for Docker Compose][dc-example].

[dc-example]: https://github.com/MaterializeInc/materialize/blob/d793b112758c840c1240eefdd56ca6f7e4f484cf/demo/billing/mzcompose.yml#L60-L70

{{</ tab >}}
{{< tab "macOS" >}}

When our dashboard container is running inside of Docker via Docker for Mac and
`materialized` is running on the host macOS system the `localhost` inside of the
container does not refer to the macOS host machine's network.

You must use `host.docker.internal` to refer to the host system:

```
docker run -p 3000:3000 -e MATERIALIZED_URL=host.docker.internal:6875 materialize/dashboard
```

{{</ tab >}}
{{< tab "Linux" >}}

When our dashboard container is running inside of a Docker container on a Linux machine
we must override one of the isolation policies that Docker creates. Docker configures
containers to use a different network than their host by default, but that is easy to
override by using the `--network host` option:

```
docker run --network host -e MATERIALIZED_URL=localhost:6875 materialize/dashboard
```

Using the host network means that ports will be allocated from the host, so the `-p` flag
is no longer necessary, with the above command the dashboard will be available on at
`http://localhost:3000`.

{{</ tab >}}
{{</ tabs >}}

[simplemon-hub]: https://hub.docker.com/repository/docker/materialize/dashboard
[dashboard-json]: https://github.com/MaterializeInc/materialize/blob/v0.26/misc/monitoring/dashboard/conf/grafana/dashboards/overview.json
[graf-import]: https://grafana.com/docs/grafana/latest/reference/export_import/#importing-a-dashboard

## Health check

{{< warning >}}
The health check is not part of Materialize's stable interface.
Backwards-incompatible changes to the health check may be made at any time.
{{< /warning >}}

Materialize supports a basic HTTP health check at `http://<materialized_hostname>:6875/status`.

The health check returns HTTP status code 200 as long as Materialize has enough resources to respond
to HTTP requests. It does not otherwise assess the state of the system.

Use this endpoint to integrate Materialize with monitoring and orchestration tools that support HTTP health
checks, like [Kubernetes liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
or [AWS load balancer health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/target-group-health-checks.html).

To perform health checks that assess other metrics, consider using the [Prometheus metrics endpoint](#prometheus).


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

{{% monitoring/prometheus-details %}}

{{% monitoring/system-catalog-sql-interface %}}

{{% monitoring/grafana %}}

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

## Logging

Materialize periodically emits messages to its [log file](/cli/#log-filter).
These log messages serve several purposes:

  * To alert operators to critical issues
  * To record system status changes
  * To provide developers with visibility into the system's execution when
    troubleshooting issues

We recommend that you monitor for messages at the [`WARN` or `ERROR`
levels](#levels). Every message at either of these levels indicates an issue
that must be investigated and resolved.

### Message format

Each log message is a single line with the following format:

```
<timestamp> <level> <module>: [<tag>]... <body>
```

For example, Materialize emits the following log message when a table named `t`
is created:

```
2021-04-08T04:12:25.927738Z  INFO coord::catalog: create table materialize.public.t (u1)
```

The timestamp is always in UTC and formatted according to ISO 8601.

The log level is one of the five levels described in the next section,
formatted with all uppercase letters.

The module path reflects the log message's location in Materialize's source
code. Module paths change frequently from release to release and are not part of
Materialize's stable interface.

The tags, if included, further categorize the message. Tags are surrounded by
square brackets. The currently used tags are:

* `[customer-data]`: the message includes clear-text contents of data in the system
* `[deprecation]`: a feature in use will be removed or changed in a future release

The body is unstructured text intended for human consumption. It may include
embedded newlines.

### Levels

Every log message is associated with a level that indicates the severity of the
message. The levels are, in decreasing order of severity, `ERROR`, `WARN`,
`INFO`, `DEBUG`, and `TRACE`.

Log levels are used in the [`--log-filter` command-line option](/cli/#log-filter)
to determine which log messages to emit.

Messages at each level must meet the indicated standard:

* **`ERROR`**: Reports an error that has caused data corruption, data loss, or
  unavailability. You should page on-call staff immediately about these errors.

  Examples:

  * Authentication with an external system (e.g., Amazon S3) has failed.
  * A source ingested the deletion of a record that does not exist, causing a
    "negative multiplicity."

* **`WARN`**: Reports an issue that may lead to data corruption, data loss, or
  unavailability. It is reasonable to check for `WARN`-level messages once per
  day during normal business hours.

  Examples:

  * A network request (e.g., downloading an object from Amazon S3) has failed
    several times, but a retry is in progress.

* **`INFO`**: Reports normal system status changes. Messages at this level may
  be of interest to operators, but do not typically require attention.

  Examples:

  * A view was created.
  * A view was dropped.

* **`DEBUG`**: Provides information that may help when troubleshooting issues.
  Messages at this level are primarily of interest to Materialize engineers.

  Examples:

  * An HTTP request was routed through a proxy specified by the `http_proxy`
    environment variable.
  * An S3 object downloaded by an S3 source had an invalid `Content-Encoding`
    header that was ignored, but the object was nonetheless decoded
    successfully.

* **`TRACE`**: Like `DEBUG`, but the information meets a lower standard of
  relevance or importance.

  Enabling `TRACE` logs can generate multiple gigabytes of log messages per
  hour. We recommend that you only enable this level in development or at the
  direction of a Materialize engineer.

  Examples:

  * A Kafka source consumed a message.
  * A SQL client issued a command.
