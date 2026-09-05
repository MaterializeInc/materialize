---
title: "MaterializeDebug CRD Field Descriptions"
description: "Reference page on MaterializeDebug CRD Fields"
menu:
  main:
    parent: "sm-deployments"
    identifier: "materialize-debug-crd-field-descriptions"
    weight: 67
---

A `MaterializeDebug` resource runs an in-cluster debug collector for one
Materialize instance. The collector periodically snapshots the instance's
diagnostics into a ring buffer, from which [`mz-debug
self-managed`](/integrations/mz-debug/self-managed/) downloads them.

The operator creates one per instance when `debugCollector.enabled` is set in
its Helm values. To run a collector for a single instance instead, create a
resource in the instance's namespace with `materializeName` set; every other
field is optional.

{{% self-managed/materialize-debug-crd-descriptions-v1alpha1 %}}
