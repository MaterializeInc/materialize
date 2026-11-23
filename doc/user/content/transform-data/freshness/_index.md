---
title: "Freshness"
description: "Guides for tuning, monitoring, and troubleshooting data freshness in Materialize."
disable_list: true
menu:
  main:
    parent: transform-data
    weight: 84
    identifier: freshness
    name: "Freshness"
---

[Freshness](/fundamentals/concepts/reaction-time/#freshness) measures the time
from when a change occurs in an upstream system to when it becomes visible in
the results of a query. These guides can help you tune, track, and diagnose it:

- [Fine tune for sub-second freshness](/transform-data/freshness/subsecond-freshness/)
- [Monitor freshness](/transform-data/freshness/monitor-freshness/)
- [Troubleshoot freshness](/transform-data/freshness/troubleshoot-freshness/)
