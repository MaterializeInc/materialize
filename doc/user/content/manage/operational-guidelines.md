---
title: "Operational guidelines"
description: "General guidelines for production"
menu:
  main:
    parent: "manage"
    weight: 4
    name: "Operational guidelines"
    identifier: "operational-guidelines"
---

The following provides some general guidelines for production.

## Clusters

### Production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

### Three-tier architecture

{{% best-practices/architecture/three-tier %}}

#### Alternatives

If a three-tier architecture is infeasible or unnecessary due to low volume or a
non-production setup, a two cluster or a single cluster architecture may
suffice.

See [Appendix: Alternative cluster
architectures](/manage/appendix-alternative-cluster-architectures/) for details.

## Sources

### Scheduling

{{% best-practices/ingest-data/scheduling %}}

### Separate cluster(s) for sources

In production, if possible, use a dedicated cluster for
[sources](/concepts/sources/); i.e., avoid putting sources on the same cluster
that hosts compute objects, sinks, and/or serves queries.

{{% best-practices/architecture/upsert-source %}}

See also [Production cluster architecture](#three-tier-architecture).

## Sinks

### Separate sinks from sources

To allow for [blue/green deployment](/manage/dbt/blue-green-deployments/), avoid
putting sinks on the same cluster that hosts sources .

See also [Cluster architecture](#three-tier-architecture).

## Snapshotting and hydration considerations

- For upsert sources, snapshotting is a resource-intensive operation that can
  require a significant amount of CPU and memory.

- During hydration (both initial and subsequent rehydrations), materialized
  views require memory proportional to both the input and output. When
  estimating required resources, consider both the hydration cost and the
  steady-state cost.

- During sink creation (initial hydration), sinks need to load an entire
  snapshot of the data in memory.

## Role-based access control (RBAC)

{{< tabs >}}

{{< tab "Cloud" >}}

### Cloud

{{% yaml-sections data="rbac/recommendations-cloud"
heading-field="recommendation" heading-level=5 %}}

{{< /tab >}}

{{< tab "Self-Managed" >}}

### Self-Managed

{{% yaml-sections data="rbac/recommendations-sm"
heading-field="recommendation" heading-level=5 %}}
{{< /tab >}}

{{< /tabs >}}
