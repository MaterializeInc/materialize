---
title: "Appendix: Metrics"
description: "Reference list of the Prometheus metrics exposed by Materialize."
disable_toc: true
disable_list: true
menu:
  main:
    parent: "monitor"
    name: "Appendix: metrics"
    identifier: "monitor-appendix-metrics"
    weight: 20
aliases:
  - /manage/monitor/appendix-metrics/
---

This page lists the Prometheus metrics exposed by Materialize, along with their
descriptions. A `*` in a metric name denotes a family of metrics
whose name is completed at runtime (for example, `mz_persist_*_bytes`).

{{< metrics-table >}}
