---
source: src/adapter/src/coord/sequencer/inner/cluster.rs
revision: eb105c0ce8
---

# adapter::coord::sequencer::inner::cluster

Implements sequencing for cluster-related DDL: `CREATE CLUSTER`, `CREATE CLUSTER REPLICA`, `DROP CLUSTER`, `DROP CLUSTER REPLICA`, `ALTER CLUSTER`, and `ALTER CLUSTER REPLICA RENAME`.
Handles managed and unmanaged replica configurations, validates size parameters against the allowed replica-size map, and drives the scheduling-policy state after cluster alteration.
Cluster and replica creation and deletion are applied through the catalog implications pipeline rather than directly in this module.
