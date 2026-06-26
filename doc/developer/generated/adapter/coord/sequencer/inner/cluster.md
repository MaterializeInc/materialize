---
source: src/adapter/src/coord/sequencer/inner/cluster.rs
revision: 277b33e9c0
---

# adapter::coord::sequencer::inner::cluster

Implements sequencing for cluster-related DDL: `CREATE CLUSTER`, `CREATE CLUSTER REPLICA`, `DROP CLUSTER`, `DROP CLUSTER REPLICA`, `ALTER CLUSTER`, and `ALTER CLUSTER REPLICA RENAME`.
Handles managed and unmanaged replica configurations, validates size parameters against the allowed replica-size map, and drives the scheduling-policy state after cluster alteration. Managed cluster variants carry `auto_scaling_strategy`, `reconfiguration`, and `burst` fields (all `None` by default) in addition to the existing scheduling and optimizer-override fields. The durable `ReplicaLocation::Managed` record stores a list of availability zones (`availability_zones: Vec<String>`) rather than an optional single zone; user-pinned `AVAILABILITY ZONE` clauses produce a one-element list, and the cluster's AZ pool is stamped in at provisioning time.
Replica IDs are pre-allocated out-of-band via `Catalog::allocate_replica_ids` before the catalog transaction, dispatching to user or system ID space based on the owning cluster's ID type. This mirrors how cluster and item IDs are allocated, so nothing allocates a replica ID in-apply.
Cluster and replica creation and deletion are applied through the catalog implications pipeline rather than directly in this module.
For `CREATE CLUSTER` (both managed and unmanaged variants), `scoped_overrides_create_op` is called with `ClusterEvalContext` and `ReplicaEvalContext` values built from plan data and pre-allocated replica IDs, and the resulting `Op::UpdateScopedSystemParameters` is folded into the same catalog transaction that creates the cluster. This makes the committed diff drive the replica-scoped controller push before `create_replica`, so the new cluster's optimizer and each new replica's render-frozen flags take effect before any dataflow renders, rather than falling back to environment-wide values until the next sync tick.
