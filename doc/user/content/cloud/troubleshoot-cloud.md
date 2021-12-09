---
title: "Troubleshoot Cloud"
description: "Troubleshoot problems with Materialize Cloud."
menu:
  main:
    parent: "cloud"
---

{{< cloud-notice >}}


We're working on other monitoring tools, but for now there are a few tools you can use for troubleshooting issues with Materialize Cloud.

## Third-party monitoring tools

Materialize supports integrations with [Grafana](/ops/monitoring/#grafana) and [Prometheus](/ops/monitoring/#prometheus). For more information, see [Monitor Cloud].

## Status check

You can check on the status of Materialize systems at [https://status.materialize.com](https://status.materialize.com). You can also sign up at the page to be notified of any incidents through email, text, Slack, or Atom or RSS feed.

## Metrics

The Metrics card shows memory and CPU utilization for different ranges of time, and offers access to logs.

## Logs

Materialize periodically emits messages to its [log file](/cli/#log-filter). You can view these logs in the Metrics card on the [Deployments](https://cloud.materialize.com/deployments) page. Click **View logs** beneath the utilization graph, then click **Download** if you want to save the logs for review.

These log messages serve several purposes:

  * To alert operators to critical issues
  * To record system status changes
  * To provide developers with visibility into the system's execution when
    troubleshooting issues

We recommend that you monitor for messages at the [`WARN` or `ERROR`
levels](/ops/monitoring/#levels). Every message at either of these levels indicates an issue
that must be investigated and resolved.

For more information, see [Monitoring: Logging](/ops/monitoring/#logging)

## Disabling user indexes

{{< warning >}}
This feature is primarily meant for advanced administrators of Materialize.
{{< /warning >}}

If your Cloud deployment unexpectedly consumes all CPU or memory, you can troubleshoot by restarting with
[indexes][api-indexes] on user-created objects disabled.

To do so:

1. Go to the Deployment details page and click **Edit**.
1. Open the Advanced settings section and click **Disable User indexes**, then click **Save**.

   The deployment will restart automatically.

{{% troubleshooting/disable-user-indexes %}}

## Related topics

- [Monitor Cloud]
- [System Catalog](/sql/system-catalog)

[Monitor Cloud]:../../cloud/monitor-cloud
