---
title: "Materialize Cloud DR characteristics"
description: "Reference on various failure modes and recovery characteristics of Materializ Cloud."
menu:
  main:
    parent: "disaster-recovery"
    weight: 15
---

The following provides  various failure mode impact and recovery
characteristics for Materialize.

## System Environment

Materialize system components provide core features like database consensus,
durable storage, network access and security, and compute provisioning. These
components are deployed with high available configurations or with automated
recovery mechanisms. However, they can still experience outages due to failures
in underlying providers.

{{< yaml-table data="disaster_recovery/system_environment" >}}

{{< annotation type="Recommendation(s)" >}}
- Use privatelink when possible and configure to use multiple AZs.

- If you are concerned about multi-AZ outages, consider [duplicate Materialize
  environment in second region strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience)
{{</ annotation>}}

## Database environment

### `environmentd`

The `environmentd` runs on a single node in a single AZ. `environmentd`
has no data; as such, the RPO is `N/A`.

The component has the following failure characteristics:

{{< yaml-table data="disaster_recovery/environmentd" >}}
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

{{< annotation type="Key point(s)" >}}

- If `environmentd` becomes unavailable, RTO is non-zero.

- If `environmentd` becomes unavailable, its RTO affects the RTO of the clusters
  as you cannot access data while `environmentd` is unavailable.

{{</ annotation >}}

### Clusters

{{< yaml-table data="disaster_recovery/cluster" >}}
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

{{< annotation type="Key point(s)" >}}

- Cluster RTO can be affected if the environmentd is down (seconds to minutes).

- For regional failover strategy, you can use a [duplicate Materialize
  environment
  strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience).

{{</ annotation >}}

## Materialize data corruption/operations error

{{< yaml-table data="disaster_recovery/mz_operations" >}}
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

## End-user error

{{< yaml-table data="disaster_recovery/human" >}}
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

{{< annotation type="Key point(s)" >}}

- You can use [RBAC](/manage/access-control/rbac/) to reduce the risk of
  accidentally dropping sources (and other objects) in Materialize.

{{</ annotation >}}

## See also

- [Disaster recovery (DR) strategies](/manage/disaster-recovery/)
