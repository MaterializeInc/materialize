---
title: "Operational guidelines"
description: "General guidelines for production"
menu:
  main:
    parent: "manage"
    weight: 4
    name: "Operational guidelines"
---

The following provides some general guidelines for production.

## Clusters

### Production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

### Cluster architecture

{{% best-practices/architecture/three-tier %}}

#### Upsert source consideration

{{% best-practices/architecture/upsert-source %}}

#### Alternatives

Alternatively, if a three-tier architecture is not feasible or unnecessary due
to low volume or a non-production setup, a two cluster or a single cluster
architecture may suffice.

{{<tabs>}}
{{< tab "Two cluster architecture" >}}

{{< best-practices/architecture/two-cluster >}}

{{</ tab >}}

{{< tab "Single cluster architecture" >}}

{{< best-practices/architecture/one-cluster >}}

{{</ tab >}}

{{</ tabs >}}

## Sources

### Scheduling

{{% best-practices/ingest-data/scheduling %}}

### Separate cluster(s) for sources

In production,

- If possible, use a dedicated cluster for [sources](/concepts/sources/); i.e.,
  avoid putting sources on the same cluster that hosts compute objects, sinks,
  and/or serves queries.

- Separate upsert sources from other sources. Upsert sources have higher
  resource requirements (since, for upsert sources, Materialize maintains each
  key and the key's last value as well as performs deduplication). As such, if
  possible, use a separate source cluster for upsert sources.

See also [Cluster architecture](#cluster-architecture).

## Sinks

### Separate sinks from sources

Avoid putting sinks on the same cluster that hosts sources to allow for
[blue/green deployment](/manage/dbt/development-workflows/#bluegreen-deployments).

See also [Cluster architecture](#cluster-architecture).

## Snapshotting and hydration considerations

- For upsert sources, snapshotting is a resource-intensive operation that can
  require a significant amount of CPU and memory.

- During hydration (both initial and subsequent rehydrations), materialized
  views require memory proportional to both the input and output. When
  estimating required resources, consider both the hydration cost and the
  steady-state cost.

- During initial hydration, sinks need to load an entire snapshot of the data in
  memory.
