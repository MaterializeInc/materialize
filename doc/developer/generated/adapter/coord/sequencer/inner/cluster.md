---
source: src/adapter/src/coord/sequencer/inner/cluster.rs
revision: c58b2ebb27
---

# adapter::coord::sequencer::inner::cluster

Implements sequencing for cluster-related DDL: `CREATE CLUSTER`, `CREATE CLUSTER REPLICA`, `DROP CLUSTER`, `DROP CLUSTER REPLICA`, `ALTER CLUSTER`, and `ALTER CLUSTER REPLICA RENAME`.
Handles managed and unmanaged replica configurations, validates size parameters against the allowed replica-size map, and drives the scheduling-policy state after cluster creation or alteration.
