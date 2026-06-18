---
title: "Essential metrics"
description: "The Prometheus metrics Materialize recommends building dashboards and alerts on."
disable_toc: true
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
we add observability for new features and refine what's most useful. A `*` in
a metric name denotes a family of metrics whose name is completed at runtime
(for example, `mz_persist_*_bytes`).

For the complete list of metrics Materialize exposes, including internal metrics
that are subject to change, see [Appendix:
Metrics](/manage/monitor/appendix-metrics/).

{{< metrics-table visibility="public" >}}
