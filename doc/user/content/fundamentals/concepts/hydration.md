---
title: Hydration
description: "Learn about hydration in Materialize: reconstructing an object's in-memory state by reading from the storage layer."
menu:
  main:
    parent: concepts
    weight: 80
    identifier: 'concepts-hydration'
aliases:
  - /concepts/hydration/
---

{{% include-from-yaml data="hydration-details" name="definition" %}}

## When hydration occurs

{{% include-from-yaml data="hydration-details" name="triggers" %}}

For when hydration occurs for each object type, see [Objects and
hydration](#objects-and-hydration).

## Objects and hydration

{{% include-from-yaml data="hydration-details" name="per-replica" %}}

The objects on the affected replicas hydrate as described in the following
table.

{{% yaml-table data="hydration-objects-table" %}}

## Reducing hydration memory

Hydration primarily impacts memory usage, and its speed scales with cluster
size. For strategies to reduce hydration memory, speed hydration up, or avoid
triggering it, see [Optimize hydration
requirements](/clusters/optimize-hydration-requirements/).

## Related pages

- [Optimize hydration requirements](/clusters/optimize-hydration-requirements/)
- [Snapshotting](/fundamentals/concepts/snapshotting/)
- [Clusters](/fundamentals/concepts/clusters/)
- [Sources](/fundamentals/concepts/sources/)
- [Troubleshooting](/serve-results/troubleshooting/#hydrating-objects)
- [Updating materialized views](/transform-data/updating-materialized-views/)
