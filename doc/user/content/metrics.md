---
title: "Metrics"
description: "The system catalog stores metadata about your Materialize instance."
menu:
  main:
    parent: 'reference'
    weight: 170
---

{{< version-added v0.5.0 />}}

Materialize exposes a system catalog that contains metadata about the running
Materialize instance.

{{< warning >}}
The system tables are not part of Materialize's stable interface.
Backwards-incompatible changes to these tables may be made at any time.
{{< /warning >}}
