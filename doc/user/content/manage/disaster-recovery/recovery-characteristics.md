---
title: "Materialize DR characteristics"
description: "Reference on various failure modes and recovery characteristics of Materialize."
menu:
  main:
    parent: "disaster-recovery"
    weight: 15
---

The following provides  various failure mode impact and recovery
characteristics for Materialize, specifically with regards to

- RPO (Recovery Point Objective)

- RTO (Recovery Time Objective)

  - With [replication factor (RF) 1](/concepts/clusters/#fault-tolerance)

  - With [replication factor (RF) 2](/concepts/clusters/#fault-tolerance)

  {{< annotation type="Replication factor and availability zones" >}}

  {{< include-md file="shared-content/multi-replica-az.md" >}}

  {{</ annotation >}}

## System Environment

{{< yaml-table data="disaster_recovery/system_environment_nested" >}}

{{< annotation type="Key point(s)" >}}

- The egress gateway runs on a single machine on a single Availability Zones
  (AZ); other components run on multiple Availability Zones (AZs).

- For regional failover strategy, you can use a [duplicate Materialize
  environment
  strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience).

{{</ annotation >}}

## Database environment

### `environmentd`

{{< yaml-table data="disaster_recovery/environmentd_nested" >}}

{{< annotation type="Key point(s)" >}}

- If `environmentd` becomes unavailable, RTO is non-zero.

- If `environmentd` becomes unavailable, its RTO affects the RTO of the clusters
  as you cannot access data while `environmentd` is unavailable.

{{</ annotation >}}

### Clusters

{{< yaml-table data="disaster_recovery/cluster" >}}


{{< annotation type="Key point(s)" >}}

- Cluster RTO can be affected if the environmentd is down (seconds to minutes).

- For regional failover strategy, you can use a [duplicate Materialize
  environment
  strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience).

{{</ annotation >}}

## Materialize data corruption/operations error

{{< yaml-table data="disaster_recovery/mz_operations" >}}

## End-user error

{{< yaml-table data="disaster_recovery/human" >}}

{{< annotation type="Key point(s)" >}}

- You can use [RBAC](/manage/access-control/rbac/) to reduce the risk of
  accidentally dropping sources (and other objects) in Materialize.

{{</ annotation >}}

## See also

- [Disaster recovery (DR) strategies](/manage/disaster-recovery/)
