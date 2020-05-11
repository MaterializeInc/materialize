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
preconfigured to run it as [`materialize/dashboard`][simplemon-hub].

The only configuration required to get started with the docker image is the
`MATERIALIZED_URL=<host>:<port>` environment variable.

As an example, if you are running `materialized` in a cloud instance at the IP address
172.16.0.0 (see "Observing Local materialized" below if you are running materialized on
the same host as the dashboard), you could get a dashboard by running this command and
opening `http://localhost:3000` in your web browser:

```console
#               expose ports  ______point it at materialize______
$ docker run -d -p 3000:3000  -e MATERIALIZED_URL=172.16.0.0:6875 materialize/dashboard
```

The `materialize/dashboard` docker image bundles Prometheus and Grafana together to make
getting insight into `materialized`s performance easy. It is not particularly
configurable, and in particular is not designed to handle large metric volumes or long
uptimes. It will start truncating metrics history after about 1GB of storage, which
corresponds to about 3 days of data with the very fine-grained metrics collected inside
the container.

So, while the dashboard is provided as a convenience and should not be relied on for
production monitoring, if you would like to persist metrics across restarts of the
container you can mount a docker volume onto `/prometheus`:

```console
$ docker run -d \
    -v /tmp/prom-data:/prometheus -u "$(id -u):$(id -g)" \
    -p 3000:3000 -e MATERIALIZED_URL=172.16.0.0:6875 \
    materialize/dashboard
```

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

# Other Setups

Even if you aren't running materialized at web scale, you can still use our web-scale
tools to observe it.

## Observing Local materialized

### Inside docker-compose or kubernetes

Local schedulers like `docker-compose` (which we use for our demos) or kubernetes will
typically expose running containers to each other using their service name as a public
DNS hostname, but _only_ within the network that they are running in.

The easiest way to use the dashboard inside a scheduler is to tell the scheduler to run
it. [Here is an example][dc-example] of configuring docker-compose to run the dashboard.

### On MacOS, with materialized running outside of Docker

The problem with this is that `localhost` inside of docker cannot, on Docker for Mac,
refer to the mac network. So instead you must use `host.docker.internal`:

```
docker run -p 3000:3000 -e MATERIALIZED_URL=host.docker.internal:6875 materialize/dashboard
```

### On Linux, with materialized running outside of docker

Docker containers use a different network than their host by default, but that is easy to
get around using the `--network` flag. Using the host network means that ports will be
allocated from the host, so the `-p` flag is no longer necessary:

```
docker run --network host -e MATERIALIZED_URL=local:6875 materialize/dashboard
```

[simplemon-hub]: https://hub.docker.com/repository/docker/materialize/dashboard
[dashboard-json]: https://github.com/MaterializeInc/materialize/blob/master/misc/monitoring/dashboard/conf/grafana/dashboards/overview.json
[graf-import]: https://grafana.com/docs/grafana/latest/reference/export_import/#importing-a-dashboard
[dc-example]: https://github.com/MaterializeInc/materialize/blob/d793b112758c840c1240eefdd56ca6f7e4f484cf/demo/billing/mzcompose.yml#L60-L70
