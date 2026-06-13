---
title: "Essential metrics"
description: "The Prometheus metrics Materialize recommends building dashboards and alerts on."
disable_list: true
# TODO (SangJunBak): Unhide once ready to publish
# menu:
#   main:
#     parent: "monitor"
#     identifier: "monitor-essential-metrics"
#     weight: 19
---

This page lists the essential Prometheus metrics exposed by Materialize: the
ones we recommend building dashboards and alerts on. This list may evolve as
we add observability for new features and refine what's most useful.

The metrics are grouped by the component of Materialize they describe. A
grouping is shown only when it has at least one metric. For the complete list
of metrics Materialize exposes, see [Appendix:
Metrics](/manage/monitor/appendix-metrics/).

{{< metrics-table visibility="public" tag="environment" heading="Environment-level metrics" description="Metrics for the control plane: client connections, availability, and the catalog." >}}

{{< metrics-table visibility="public" tag="compute" heading="Compute metrics" description="Metrics for compute objects, such as indexes and materialized views, running on [clusters](/concepts/clusters/) and their replicas." >}}

{{< metrics-table visibility="public" tag="source" heading="Source metrics" description="Metrics for data ingestion from external systems." >}}

{{< metrics-table visibility="public" tag="sink" heading="Sink metrics" description="Metrics for data output to external systems." >}}

{{< metrics-table visibility="public" untagged="true" heading="Uncategorized metrics" description="Essential metrics that haven't yet been assigned to one of the groupings above." >}}
