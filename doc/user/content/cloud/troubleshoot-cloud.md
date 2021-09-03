---
title: "Troubleshoot Cloud"
description: "Troubleshoot problems with Materialize Cloud."
menu:
  main:
    parent: "cloud"
    weight:
---

{{< cloud-notice >}}

We're working on other monitoring tools, but for now there are a few tools you can use for troubleshooting issues with Materialize Cloud:

* Logs
* Catalog-only mode
* Metrics integrations

## Logs

Materialize periodically emits messages to its [log file](/cli/#log-filter). You can view these logs in two ways:

* On the [Deployments](https://cloud.materialize.com/deployments) page, click the dots icon for a deployment and select **View logs**.
* Double-click on a deployment to view the deployment details and go to the **Logs** tab.

These log messages serve several purposes:

  * To alert operators to critical issues
  * To record system status changes
  * To provide developers with visibility into the system's execution when
    troubleshooting issues

We recommend that you monitor for messages at the [`WARN` or `ERROR`
levels](#levels). Every message at either of these levels indicates an issue
that must be investigated and resolved.

For more information, see [Monitoring: Logging](/ops/monitoring/#logging)

## Catalog-only mode

The system catalog contains metadata about a Materialize instance. If a deployment is unable to start, you will see the option to launch it in catalog-only mode. This launches Materialize without data and allows you to turn on specified indexes one at a time so that you can identify which dataflow is causing issues.

For more information, see [System Catalog](/sql/system-catalog).

## Third-party metrics tools

Materialize supports integrations with [Datadog](/ops/monitoring/#datadog), [Grafana](/ops/monitoring/#grafana), and [Prometheus](/ops/monitoring/#prometheus).

## Related topics

- [Monitoring](/ops/monitoring)
- [System Catalog](/sql/system-catalog)
