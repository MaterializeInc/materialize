---
source: src/adapter/src/coord/cluster_scheduling.rs
revision: 6eeaca032b
---

# adapter::coord::cluster_scheduling

Implements the legacy automated cluster scheduling policy: `SCHEDULE = ON REFRESH`, which keeps track of per-cluster hydration time estimates and uses them to schedule cluster replica creation and teardown.
Both `check_scheduling_policies` and `handle_scheduling_decisions` are no-ops when `ENABLE_CLUSTER_CONTROLLER` (default on) is set: the controller's `OnRefreshStrategy` is then the sole authority over scheduled clusters, so the legacy policy must not also toggle their replication factor.
When the gate is off, `check_scheduling_policies` is called on a timer from the coordinator's main loop and sends `Message::SchedulingDecisions` to drive `handle_scheduling_decisions`. `handle_scheduling_decisions` sums decisions across all policies per cluster and turns replicas on or off as needed, but skips any cluster that has a pending (graceful-reconfiguration) replica, because that reconfiguration owns the replica set until it finalizes. When turning a cluster off it drops replicas by id rather than by altering the replication factor, so it can retire replicas left behind by the controller after a gate-off. When the factor is already 0 but owned replicas exist (controller handoff scenario), it adopts one replica and retires any surplus in the same transaction.
