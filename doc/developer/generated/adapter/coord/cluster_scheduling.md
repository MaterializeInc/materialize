---
source: src/adapter/src/coord/cluster_scheduling.rs
revision: 8598d82c1c
---

# adapter::coord::cluster_scheduling

Implements automated cluster scheduling policies: currently the `REFRESH HYDRATION TIME ESTIMATE` policy, which keeps track of per-cluster hydration time estimates and uses them to schedule cluster replica creation and teardown.
`check_scheduling_policies` is called on a timer from the coordinator's main loop and emits `AlterCluster` operations when scheduling decisions change.
When `ENABLE_CLUSTER_CONTROLLER` is on and the cluster is user-owned, `handle_scheduling_decisions` defers its replication-factor write for any cluster that has a graceful reconfiguration in flight, because the controller owns that transition. The deferral is skipped when the gate is off (the legacy path retains the record as cancelled instead).
