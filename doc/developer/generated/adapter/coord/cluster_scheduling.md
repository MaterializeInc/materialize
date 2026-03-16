---
source: src/adapter/src/coord/cluster_scheduling.rs
revision: c58b2ebb27
---

# adapter::coord::cluster_scheduling

Implements automated cluster scheduling policies: currently the `REFRESH HYDRATION TIME ESTIMATE` policy, which keeps track of per-cluster hydration time estimates and uses them to schedule cluster replica creation and teardown.
`check_scheduling_policies` is called on a timer from the coordinator's main loop and emits `AlterCluster` operations when scheduling decisions change.
