---
audience: developer
canonical_url: https://materialize.com/docs/manage/disaster-recovery/recovery-characteristics/
complexity: intermediate
description: Reference on various failure modes and recovery characteristics of Materialize
  Cloud.
doc_type: reference
keywords:
- Materialize Cloud DR characteristics
- 'Recommendation(s):'
- 'Key point(s):'
product_area: Operations
status: stable
title: Materialize Cloud DR characteristics
---

# Materialize Cloud DR characteristics

## Purpose
Reference on various failure modes and recovery characteristics of Materialize Cloud.

If you need to understand the syntax and options for this command, you're in the right place.


Reference on various failure modes and recovery characteristics of Materialize Cloud.



The following provides  various failure mode impact and recovery
characteristics for Materialize.

## System Environment

Materialize system components provide core features like database consensus,
durable storage, network access and security, and compute provisioning. These
components are deployed with high available configurations or with automated
recovery mechanisms. However, they can still experience outages due to failures
in underlying providers.

<!-- Dynamic table: disaster_recovery/system_environment - see original docs -->

> **Recommendation(s):** - Use privatelink when possible and configure to use multiple AZs.

- If you are concerned about multi-AZ outages, consider [duplicate Materialize
  environment in second region strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience)

## Database environment

This section covers database environment.

### `environmentd`

The `environmentd` runs on a single node in a single AZ. `environmentd`
has no data; as such, the RPO is `N/A`.

The component has the following failure characteristics:

<!-- Dynamic table: disaster_recovery/environmentd - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - If `environmentd` becomes unavailable, RTO is non-zero.

- If `environmentd` becomes unavailable, its RTO affects the RTO of the clusters
  as you cannot access data while `environmentd` is unavailable.

### Clusters

<!-- Dynamic table: disaster_recovery/cluster - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - Cluster RTO can be affected if the environmentd is down (seconds to minutes).

- For regional failover strategy, you can use a [duplicate Materialize
  environment
  strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience).

## Materialize data corruption/operations error

<!-- Dynamic table: disaster_recovery/mz_operations - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

## End-user error

<!-- Dynamic table: disaster_recovery/human - see original docs -->
<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - You can use [RBAC](/security/access-control/) to reduce the risk of
  accidentally dropping sources (and other objects) in Materialize.

## See also

- [Disaster recovery (DR) strategies](/manage/disaster-recovery/)

