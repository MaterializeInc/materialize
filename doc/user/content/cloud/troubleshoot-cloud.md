---
title: "Troubleshoot Cloud"
description: "Troubleshoot problems with Materialize Cloud."
menu:
  main:
    parent: "cloud"
    weight:
---

{{< cloud-notice >}}


We're working on other monitoring tools, but for now there are a few tools you can use for troubleshooting issues with Materialize Cloud.

## Status check

You can check on the status of Materialize systems at [https://status.materialize.com](https://status.materialize.com). You can also sign up at the page to be notified of any incidents through email, text, Slack, or Atom or RSS feed.

## Logs

Materialize periodically emits messages to its [log file](/cli/#log-filter). You can view these logs in the [Deployments](https://cloud.materialize.com/deployments) page, click on the deployment card and select **View logs** in the bottom right corner.

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
[indexes][api-indexes] on user-created objects disabled.  The option appears on the Deployment Details page.

{{% troubleshooting/disable-user-indexes %}}

## Third-party monitoring tools

Materialize supports integrations with [Grafana](/ops/monitoring/#grafana) and [Prometheus](/ops/monitoring/#prometheus). For more information, see [Monitor Cloud].

## Related topics

- [Monitor Cloud]
- [System Catalog](/sql/system-catalog)

[Monitor Cloud]:../../cloud/monitor-cloud
