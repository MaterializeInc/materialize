---
source: src/adapter/src/coord/sequencer/inner/cluster.rs
revision: 7258dad07f
---

# adapter::coord::sequencer::inner::cluster

Implements sequencing for cluster-related DDL: `CREATE CLUSTER`, `CREATE CLUSTER REPLICA`, `DROP CLUSTER`, `DROP CLUSTER REPLICA`, `ALTER CLUSTER`, and `ALTER CLUSTER REPLICA RENAME`.
Handles managed and unmanaged replica configurations, validates size parameters against the allowed replica-size map, and drives the scheduling-policy state after cluster alteration. Managed cluster variants carry `auto_scaling_strategy`, `reconfiguration`, and `burst` fields (all `None` by default) in addition to the existing scheduling and optimizer-override fields. The durable `ReplicaLocation::Managed` record stores a list of availability zones (`availability_zones: Vec<String>`) rather than an optional single zone; user-pinned `AVAILABILITY ZONE` clauses produce a one-element list, and the cluster's AZ pool is stamped in at provisioning time.
Replica IDs are pre-allocated out-of-band via `Catalog::allocate_replica_ids` before the catalog transaction, dispatching to user or system ID space based on the owning cluster's ID type. This mirrors how cluster and item IDs are allocated, so nothing allocates a replica ID in-apply.
Cluster and replica creation and deletion are applied through the catalog implications pipeline rather than directly in this module.
After the catalog transaction for `CREATE CLUSTER` (both managed and unmanaged variants), `resolve_scoped_for_new_objects` is called with the new cluster's id to evaluate and persist its cluster-coherent scoped overrides synchronously. This ensures the optimizer sees the correct feature flags on the first plan rather than falling back to environment-wide values until the next sync tick, because optimizer feature flags are baked into immutable dataflows.
