---
source: src/adapter/src/coord/sequencer/inner/cluster.rs
revision: 80f8711523
---

# adapter::coord::sequencer::inner::cluster

Implements sequencing for cluster-related DDL: `CREATE CLUSTER`, `CREATE CLUSTER REPLICA`, `DROP CLUSTER`, `DROP CLUSTER REPLICA`, `ALTER CLUSTER`, and `ALTER CLUSTER REPLICA RENAME`.
Handles managed and unmanaged replica configurations, validates size parameters against the allowed replica-size map, and drives the scheduling-policy state after cluster alteration. Managed cluster variants carry `auto_scaling_strategy`, `reconfiguration`, and `burst` fields (all `None` by default) in addition to the existing scheduling and optimizer-override fields. The durable `ReplicaLocation::Managed` record stores a list of availability zones (`availability_zones: Vec<String>`) rather than an optional single zone; user-pinned `AVAILABILITY ZONE` clauses produce a one-element list, and the cluster's AZ pool is stamped in at provisioning time.
Cluster and replica creation and deletion are applied through the catalog implications pipeline rather than directly in this module.
