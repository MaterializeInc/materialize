# Materialize Cloud DR characteristics
Reference on various failure modes and recovery characteristics of Materialize Cloud.
The following provides  various failure mode impact and recovery
characteristics for Materialize.

## System Environment

Materialize system components provide core features like database consensus,
durable storage, network access and security, and compute provisioning. These
components are deployed with high available configurations or with automated
recovery mechanisms. However, they can still experience outages due to failures
in underlying providers.


| Failure Type | Impact |
| --- | --- |
| <strong>Single Availability Zone (AZ)</strong> | <ul> <li> <p>Connection issues using single-AZ Privatelink and sources/sinks.</p> </li> <li> <p>Brief <code>pgwire</code> and <code>https</code> connection drops as network rebalances.</p> </li> </ul>  |
| <strong>Two Availability Zones</strong> | <ul> <li>Temporary issues with cluster provisioning.</li> <li>Temporary issues with Console access.</li> </ul>  |
| <strong>Three or More Availability Zones</strong> | <ul> <li> <p>Partial to no access to the database.</p> </li> <li> <p>May require point-in-time recovery (PITR) of environments.</p> </li> </ul>  |
| <strong>Single Region System Resources</strong> | There are metadata resources running in HA in <strong>us-east-1</strong>. An outage in <strong>us-east-1</strong> may result in issues viewing the console for other regions. This does <strong>not</strong> affect database access, up-time, or performance. |


> **Recommendation(s):** - Use privatelink when possible and configure to use multiple AZs. - If you are concerned about multi-AZ outages, consider [duplicate Materialize environment in second region strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience)


## Database environment

### `environmentd`

The `environmentd` runs on a single node in a single AZ. `environmentd`
has no data; as such, the RPO is `N/A`.

The component has the following failure characteristics:


| Failure Type | RPO | RTO (RF1 - single AZ) | RTO (RF2 - multiple AZs) |
| --- | --- | --- | --- |
| <strong>Machine failure</strong> | N/A | Time to launch on new machine (~seconds to minutes). | N/A |
| <strong>Single AZ failure</strong> | N/A | Time to launch new instance in a new AZ. | N/A |

<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - If `environmentd` becomes unavailable, RTO is non-zero. - If `environmentd` becomes unavailable, its RTO affects the RTO of the clusters as you cannot access data while `environmentd` is unavailable.


### Clusters


| Failure Type | RPO | RTO (RF1 - single AZ) | RTO (RF2 - multiple AZs) |
| --- | --- | --- | --- |
| <strong>Machine failure</strong> |  | <p>Time to spin up new machine + possible rehydration time, depending on the objects on the machine:</p> <ul> <li>If non-upsert sources, no rehydration time(i.e., does not require rehydration).</li> <li>If upsert sources, rehydration time.</li> <li>If sinks, no rehydration time (i.e., does not require rehydration).</li> <li>If compute, rehydration time.</li> <li>If serving, rehydration time.</li> </ul> <p>Additionally, there may be some time to catch up with changes that may have occurred during the downtime.</p> <p>To reduce rehydration time, scale up the cluster.</p>  | <p>Can be:</p> <ul> <li> <p>0 if only compute and serving objects are on the machine.</p> </li> <li> <p>Time to spin up new machine if sources or sinks are on the machine.</p> </li> </ul> <p>In addition, cluster RTO is affected if the <a href="#environmentd" ><code>environmentd</code> is down</a> (seconds to minutes).</p>  |
| <strong>Single AZ failure</strong> |  | <p><em>For managed clusters</em></p> <p>Time to spin up new machine + possible rehydration time, depending on the objects on the machine:</p> <ul> <li>If non-upsert sources, no rehydration time(i.e., does not require rehydration).</li> <li>If upsert sources, rehydration time.</li> <li>If sinks, no rehydration time (i.e., does not require rehydration).</li> <li>If compute, rehydration time.</li> <li>If serving, rehydration time.</li> </ul> <p>Additionally, there may be some time to catch up with changes that may have occurred during the downtime.</p> <p>To reduce rehydration time, you can scale up the cluster.</p> <p>During downtime, single AZ PrivateLinks are impacted.</p>  | <p>Can be:</p> <ul> <li> <p>0 if only compute and serving objects are on the machine.</p> </li> <li> <p>Time to spin up new machine if sources or sinks are on the machine.</p> </li> </ul> <p>In addition, cluster RTO is affected if the <a href="#environmentd" ><code>environmentd</code> is down</a> (seconds to minutes).</p>  |
| <strong>Regional failure (or 2 AZs failures)</strong> | At most, 1 hour (time since last backup, based on hourly backups).<br> | ~1 hour (time to check pointers). | High/Significant. Consider using a <a href="/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience" >regional failover strategy</a>. |

<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - Cluster RTO can be affected if the environmentd is down (seconds to minutes). - For regional failover strategy, you can use a [duplicate Materialize environment strategy](/manage/disaster-recovery/#level-3-a-duplicate-materialize-environment-inter-region-resilience).


## Materialize data corruption/operations error


| Failure Type | RPO | RTO (RF1/RF2) |
| --- | --- | --- |
| <strong>Non-data corruption errors</strong> | Maximum 1 hour (time since last backup, based on hourly backups). | Case specific |
| <strong>Data corruption errors</strong> | High/Significant. RPO is dictated by upstream system. | Case specific |

<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

## End-user error


| Failure Type | RPO | RTO (RF1/RF2) |
| --- | --- | --- |
| <strong>Accidental source drop (and dependent objects)</strong> | <p>Same as upstream source system. Source will need to be recreated in Materialize.</p> <p>Consider using <a href="/security/access-control/" >RBAC</a> to reduce the risk of accidentally dropping sources.</p>  | <p>Time to recreate the source and snapshot + time to recreate the dependent objects and rehydrate.</p> <p>Consider using <a href="/security/access-control/" >RBAC</a> to reduce the risk of accidentally dropping sources.</p>  |
| <strong>Accidental materialized view/index drop</strong> | 0 | Time to rehydrate. |

<span class="caption">
RPO (Recovery Point Objective) • RTO (Recovery Time Objective) • RF (Replication
Factor)
</span>

> **Key point(s):** - You can use [RBAC](/security/access-control/) to reduce the risk of accidentally dropping sources (and other objects) in Materialize.


## See also

- [Disaster recovery (DR) strategies](/manage/disaster-recovery/)
