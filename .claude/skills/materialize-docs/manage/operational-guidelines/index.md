---
audience: developer
canonical_url: https://materialize.com/docs/manage/operational-guidelines/
complexity: intermediate
description: General guidelines for production
doc_type: howto
keywords:
- Operational guidelines
product_area: Operations
status: stable
title: Operational guidelines
---

# Operational guidelines

## Purpose
General guidelines for production

Follow the steps below to complete this task.


General guidelines for production


The following provides some general guidelines for production.

## Clusters

This section covers clusters.

### Production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

### Three-tier architecture

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->

#### Alternatives

If a three-tier architecture is infeasible or unnecessary due to low volume or a
non-production setup, a two cluster or a single cluster architecture may
suffice.

See [Appendix: Alternative cluster
architectures](/manage/appendix-alternative-cluster-architectures/) for details.

## Sources

This section covers sources.

### Scheduling

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->

### Separate cluster(s) for sources

In production, if possible, use a dedicated cluster for
[sources](/concepts/sources/); i.e., avoid putting sources on the same cluster
that hosts compute objects, sinks, and/or serves queries.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See best practices documentation --> --> -->

See also [Production cluster architecture](#three-tier-architecture).

## Sinks

This section covers sinks.

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

This section covers role-based access control (rbac).

#### Cloud

### Cloud

<!-- Unresolved shortcode: {{% yaml-sections data="rbac/recommendations-cloud... -->

#### Self-Managed

### Self-Managed

<!-- Unresolved shortcode: {{% yaml-sections data="rbac/recommendations-sm"
h... -->