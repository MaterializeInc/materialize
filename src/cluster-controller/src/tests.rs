// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Boundary and kernel tests.
//!
//! The headline boundary is the [`ClusterControllerCtx`] seam: we drive the
//! controller against a fake implementation and assert the reconcile loop is a
//! no-op for a steady cluster and that a compare-and-append conflict is rejected
//! and recovered. A handful of pure-kernel tests cover the multiset union/diff.

use std::collections::BTreeMap;

use async_trait::async_trait;
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_repr::Timestamp;

use crate::ClusterController;
use crate::ctx::{
    ApplyOutcome, AvailabilityZones, ClusterControllerCtx, ClusterState, Decision, ObservedReplica,
    ReplicaShape, StateWrite,
};
use crate::strategy::{DesiredReplica, Strategy};

fn cluster(n: u64) -> ClusterId {
    ClusterId::user(n).expect("valid user cluster id")
}

fn replica(n: u64) -> ReplicaId {
    ReplicaId::User(n)
}

fn shape(size: &str) -> ReplicaShape {
    ReplicaShape {
        size: size.to_string(),
        availability_zones: AvailabilityZones(Vec::new()),
        logging: ComputeReplicaLogging::default(),
    }
}

fn observed(replica_id: ReplicaId, name: &str, size: &str) -> ObservedReplica {
    ObservedReplica {
        replica_id,
        name: name.to_string(),
        shape: shape(size),
    }
}

/// Builds a managed cluster state with the given realized size, replication
/// factor, and replicas. No reconfiguration or burst in flight.
fn state(
    cluster_id: ClusterId,
    size: &str,
    replication_factor: u32,
    replicas: Vec<ObservedReplica>,
) -> ClusterState {
    ClusterState {
        cluster_id,
        size: size.to_string(),
        replication_factor,
        availability_zones: Vec::new(),
        logging: ComputeReplicaLogging::default(),
        reconfiguration: None,
        burst: None,
        replicas,
    }
}

/// A fake [`ClusterControllerCtx`] over an in-memory map of cluster states. It
/// records the decisions each `apply` saw, and can be told to reject the next
/// `apply` once (simulating a compare-and-append conflict from a concurrent
/// `ALTER`).
struct FakeCtx {
    now: Timestamp,
    states: BTreeMap<ClusterId, ClusterState>,
    /// Each batch of decisions passed to `apply`, in order.
    applied: Vec<Vec<Decision>>,
    /// If set, the next `apply` is rejected and this count decremented.
    reject_next: usize,
}

impl FakeCtx {
    fn new(states: Vec<ClusterState>) -> Self {
        Self {
            now: Timestamp::from(1000u64),
            states: states.into_iter().map(|s| (s.cluster_id, s)).collect(),
            applied: Vec::new(),
            reject_next: 0,
        }
    }

    /// All create decisions across every applied batch.
    fn creates(&self) -> Vec<&Decision> {
        self.applied
            .iter()
            .flatten()
            .filter(|d| matches!(d, Decision::CreateReplica { .. }))
            .collect()
    }

    /// All drop decisions across every applied batch.
    fn drops(&self) -> Vec<&Decision> {
        self.applied
            .iter()
            .flatten()
            .filter(|d| matches!(d, Decision::DropReplica { .. }))
            .collect()
    }
}

#[async_trait]
impl ClusterControllerCtx for FakeCtx {
    fn now(&self) -> Timestamp {
        self.now
    }

    async fn cluster_states(&mut self, clusters: &[ClusterId]) -> Vec<ClusterState> {
        clusters
            .iter()
            .filter_map(|id| self.states.get(id).cloned())
            .collect()
    }

    async fn managed_cluster_ids(&mut self) -> Vec<ClusterId> {
        self.states.keys().copied().collect()
    }

    async fn apply(&mut self, decisions: Vec<Decision>) -> ApplyOutcome {
        if self.reject_next > 0 {
            self.reject_next -= 1;
            // A real apply rejects without recording any catalog change. We
            // still record the attempt so tests can assert what was tried.
            self.applied.push(decisions);
            return ApplyOutcome::Rejected;
        }
        // Mirror the real apply: a confirmed batch mutates the durable state, so
        // a subsequent tick reads the post-apply view (cut-overs visible, created
        // replicas present, dropped replicas gone).
        for decision in &decisions {
            self.apply_to_state(decision);
        }
        self.applied.push(decisions);
        ApplyOutcome::Applied
    }
}

impl FakeCtx {
    /// Fold one applied decision into the in-memory durable state, mirroring what
    /// the real catalog transaction would do.
    fn apply_to_state(&mut self, decision: &Decision) {
        match decision {
            Decision::UpdateClusterState {
                cluster_id, write, ..
            } => {
                let Some(state) = self.states.get_mut(cluster_id) else {
                    return;
                };
                // Exhaustive destructure (no `..`): keeps this fake mirror of the
                // adapter's `build_update_cluster_config_op` from silently
                // forgetting a field added to `StateWrite`.
                let StateWrite {
                    new_size,
                    new_replication_factor,
                    new_availability_zones,
                    new_logging,
                    reconfiguration,
                    burst,
                } = write;
                if let Some(size) = new_size {
                    state.size = size.clone();
                }
                if let Some(rf) = new_replication_factor {
                    state.replication_factor = *rf;
                }
                if let Some(azs) = new_availability_zones {
                    state.availability_zones = azs.clone();
                }
                if let Some(logging) = new_logging {
                    state.logging = logging.clone();
                }
                if let Some(reconfiguration) = reconfiguration {
                    state.reconfiguration = reconfiguration.clone();
                }
                if let Some(burst) = burst {
                    state.burst = burst.clone();
                }
            }
            Decision::CreateReplica {
                cluster_id,
                name,
                shape,
                ..
            } => {
                if let Some(state) = self.states.get_mut(cluster_id) {
                    let next_id = u64::cast_from(state.replicas.len()) + 1;
                    state.replicas.push(ObservedReplica {
                        replica_id: replica(next_id),
                        name: name.clone(),
                        shape: shape.clone(),
                    });
                }
            }
            Decision::DropReplica {
                cluster_id,
                replica_id,
                ..
            } => {
                if let Some(state) = self.states.get_mut(cluster_id) {
                    state.replicas.retain(|r| r.replica_id != *replica_id);
                }
            }
        }
    }
}

#[mz_ore::test(tokio::test)]
async fn steady_state_is_a_noop() {
    let c = cluster(1);
    let states = vec![state(
        c,
        "100cc",
        2,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ],
    )];
    let mut ctx = FakeCtx::new(states);

    ClusterController::new().reconcile(&mut ctx).await;

    // Desired (2 @ 100cc) equals actual, so no decision of any kind was applied.
    assert!(
        ctx.applied.is_empty(),
        "steady cluster should produce no decisions, got {:?}",
        ctx.applied
    );
}

#[mz_ore::test(tokio::test)]
async fn no_managed_clusters_is_a_noop() {
    let mut ctx = FakeCtx::new(vec![]);
    ClusterController::new().reconcile(&mut ctx).await;
    assert!(ctx.applied.is_empty());
}

#[mz_ore::test(tokio::test)]
async fn under_provisioned_baseline_creates() {
    let c = cluster(1);
    // replication_factor 3, but only one replica exists.
    let states = vec![state(
        c,
        "100cc",
        3,
        vec![observed(replica(1), "r0", "100cc")],
    )];
    let mut ctx = FakeCtx::new(states);

    ClusterController::new().reconcile(&mut ctx).await;

    let creates = ctx.creates();
    assert_eq!(creates.len(), 2, "should create the two missing replicas");
    assert!(ctx.drops().is_empty());
    // Fresh names avoid the in-use `r0`.
    for create in creates {
        if let Decision::CreateReplica { name, shape, .. } = create {
            assert_ne!(name, "r0");
            assert_eq!(shape.size, "100cc");
        }
    }
}

#[mz_ore::test(tokio::test)]
async fn over_provisioned_baseline_drops() {
    let c = cluster(1);
    // replication_factor 1, but two replicas exist.
    let states = vec![state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ],
    )];
    let mut ctx = FakeCtx::new(states);

    ClusterController::new().reconcile(&mut ctx).await;

    assert!(ctx.creates().is_empty());
    let drops = ctx.drops();
    assert_eq!(drops.len(), 1, "should drop the one excess replica");
}

#[mz_ore::test(tokio::test)]
async fn wrong_shape_replica_dropped() {
    let c = cluster(1);
    // The realized config is 200cc rf=1, but the single replica is at 100cc:
    // no strategy desires the 100cc shape, and a 200cc slot is unfilled.
    let states = vec![state(
        c,
        "200cc",
        1,
        vec![observed(replica(1), "r0", "100cc")],
    )];
    let mut ctx = FakeCtx::new(states);

    ClusterController::new().reconcile(&mut ctx).await;

    let creates = ctx.creates();
    let drops = ctx.drops();
    assert_eq!(creates.len(), 1);
    assert_eq!(drops.len(), 1);
    if let Decision::CreateReplica { shape, .. } = creates[0] {
        assert_eq!(shape.size, "200cc");
    }
}

// ----- A second, fake additive strategy, to exercise the union/diff. -----

/// Desires `count` replicas at `size`, regardless of state. Stands in for a
/// policy strategy (graceful/burst) for union/diff tests.
struct FixedStrategy {
    name: &'static str,
    size: String,
    count: u32,
}

impl Strategy for FixedStrategy {
    fn name(&self) -> &'static str {
        self.name
    }

    fn desired_replicas(&self, _state: &ClusterState, _now: Timestamp) -> Vec<DesiredReplica> {
        (0..self.count)
            .map(|_| DesiredReplica {
                shape: shape(&self.size),
            })
            .collect()
    }
}

fn controller_with(strategies: Vec<Box<dyn Strategy>>) -> ClusterController {
    // The kernel runs whatever strategies it holds; we construct one directly
    // for union/diff tests rather than going through `new()`.
    ClusterController { strategies }
}

#[mz_ore::test(tokio::test)]
async fn union_takes_max_not_sum_per_shape() {
    use crate::strategy::BaselineStrategy;

    let c = cluster(1);
    // Baseline desires 2 @ 100cc; the extra strategy also desires 1 @ 100cc.
    // The union is max(2, 1) = 2 @ 100cc, NOT 3. With two actual replicas the
    // result is a no-op.
    let states = vec![state(
        c,
        "100cc",
        2,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ],
    )];
    let mut ctx = FakeCtx::new(states);

    let controller = controller_with(vec![
        Box::new(BaselineStrategy),
        Box::new(FixedStrategy {
            name: "extra",
            size: "100cc".to_string(),
            count: 1,
        }),
    ]);
    controller.reconcile(&mut ctx).await;

    assert!(
        ctx.applied.is_empty(),
        "union of 2 and 1 at the same shape is 2, matching actual; got {:?}",
        ctx.applied
    );
}

#[mz_ore::test(tokio::test)]
async fn distinct_shapes_union_and_attribute() {
    use crate::strategy::{BASELINE_STRATEGY_NAME, BaselineStrategy};

    let c = cluster(1);
    // Baseline desires 2 @ 100cc; the extra strategy desires 1 @ 200cc. Actual
    // has the two 100cc replicas, so the controller creates one 200cc replica,
    // attributed to "extra" only.
    let states = vec![state(
        c,
        "100cc",
        2,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ],
    )];
    let mut ctx = FakeCtx::new(states);

    let controller = controller_with(vec![
        Box::new(BaselineStrategy),
        Box::new(FixedStrategy {
            name: "extra",
            size: "200cc".to_string(),
            count: 1,
        }),
    ]);
    controller.reconcile(&mut ctx).await;

    let creates = ctx.creates();
    assert_eq!(creates.len(), 1);
    assert!(ctx.drops().is_empty());
    if let Decision::CreateReplica { shape, reasons, .. } = creates[0] {
        assert_eq!(shape.size, "200cc");
        assert_eq!(reasons, &vec!["extra"]);
        assert!(!reasons.contains(&BASELINE_STRATEGY_NAME));
    }
}

#[mz_ore::test(tokio::test)]
async fn caa_conflict_is_rejected_and_recovered() {
    use crate::ctx::{
        AvailabilityZones, ExpectedClusterState, ReconfigurationRecord, ReconfigurationTarget,
    };

    // A strategy that mirrors the cluster's current replication factor into a
    // reconfiguration record's target, so phase 1 produces an UpdateClusterState
    // whose contents depend on the state it was derived from. This lets the test
    // distinguish a write recomputed against the pre-ALTER state from one
    // recomputed against the post-ALTER state.
    struct WritingStrategy;
    impl Strategy for WritingStrategy {
        fn name(&self) -> &'static str {
            "writing"
        }
        fn update_state(&self, state: &ClusterState, now: Timestamp) -> StateWrite {
            // Only write while no record exists, so once the write lands the
            // strategy stops contributing and the controller reaches a no-op.
            if state.reconfiguration.is_some() {
                return StateWrite::default();
            }
            StateWrite {
                reconfiguration: Some(Some(ReconfigurationRecord {
                    target: ReconfigurationTarget {
                        size: "200cc".to_string(),
                        replication_factor: state.replication_factor,
                        availability_zones: AvailabilityZones(Vec::new()),
                        logging: ComputeReplicaLogging::default(),
                    },
                    deadline: now,
                })),
                ..Default::default()
            }
        }
        fn desired_replicas(&self, state: &ClusterState, _now: Timestamp) -> Vec<DesiredReplica> {
            // Mirror the realized set so phase 2 is a no-op for a steady cluster:
            // this test is about the phase-1 write recomputation, not reshaping.
            let shape = state.realized_shape();
            (0..state.replication_factor)
                .map(|_| DesiredReplica {
                    shape: shape.clone(),
                })
                .collect()
        }
    }

    let c = cluster(1);
    // rf=1 with one replica.
    let states = vec![state(
        c,
        "100cc",
        1,
        vec![observed(replica(1), "r0", "100cc")],
    )];
    let mut ctx = FakeCtx::new(states);
    ctx.reject_next = 1;

    let controller = controller_with(vec![Box::new(WritingStrategy)]);
    controller.reconcile(&mut ctx).await;

    // The single applied batch is the rejected phase-1 write. Because the
    // cluster's phase-1 write was rejected, phase 2 was skipped for it, so no
    // create/drop was applied, and the rejection left the state untouched.
    assert_eq!(ctx.applied.len(), 1);
    assert!(matches!(
        ctx.applied[0][0],
        Decision::UpdateClusterState { .. }
    ));
    assert!(ctx.creates().is_empty());
    assert!(ctx.drops().is_empty());

    // The carried `expected` reflects the (pre-ALTER) state the write was
    // derived from: replication factor 1.
    if let Decision::UpdateClusterState {
        expected, write, ..
    } = &ctx.applied[0][0]
    {
        assert_eq!(
            expected,
            &ExpectedClusterState {
                size: "100cc".to_string(),
                replication_factor: 1,
                availability_zones: AvailabilityZones(Vec::new()),
                logging: ComputeReplicaLogging::default(),
                reconfiguration: None,
                burst: None,
            }
        );
        if let Some(Some(record)) = &write.reconfiguration {
            assert_eq!(record.target.replication_factor, 1);
        } else {
            panic!("expected a reconfiguration write");
        }
    }

    // Simulate the concurrent `ALTER` that caused the rejection: the durable
    // replication factor changed out from under the tick, and (as a real rf
    // `ALTER` would) the replica set changed with it, so the new state is
    // self-consistent and phase 2 stays a no-op.
    {
        let state = ctx.states.get_mut(&c).expect("cluster present");
        state.replication_factor = 2;
        state.replicas = vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ];
    }

    // Next tick, with no rejection, the controller recomputes from the NEW state
    // and the write applies. The recovered write must reflect the post-ALTER
    // replication factor (2), proving the controller did not reuse its stale
    // (rf=1) computation. Phase 2 is a no-op against the self-consistent state,
    // so the only new batch is the recovered write.
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), 2);
    if let Decision::UpdateClusterState {
        expected, write, ..
    } = &ctx.applied[1][0]
    {
        assert_eq!(expected.replication_factor, 2);
        match &write.reconfiguration {
            Some(Some(record)) => assert_eq!(record.target.replication_factor, 2),
            other => panic!("expected a reconfiguration write derived from rf=2, got {other:?}"),
        }
    } else {
        panic!("expected the recovered tick to apply an UpdateClusterState");
    }

    // The record is now durable; a third tick recomputes against it and reaches
    // a no-op (the strategy stops writing once the record exists, and phase 2
    // matches the realized set), so the controller converges.
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), 2, "converged: no further decisions");
}

#[mz_ore::test(tokio::test)]
async fn create_drop_is_caa_guarded_and_recovers() {
    use crate::ctx::{AvailabilityZones, ExpectedClusterState};

    let c = cluster(1);
    // Over-provisioned: rf=1 with two replicas, so the baseline-only controller
    // emits a phase-2 drop. There is no phase-1 write, so this exercises the
    // compare-and-append guard on the create/drop batch specifically.
    let states = vec![state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ],
    )];
    let mut ctx = FakeCtx::new(states);
    // Reject the (sole, phase-2) apply this tick.
    ctx.reject_next = 1;

    let controller = ClusterController::new();
    controller.reconcile(&mut ctx).await;

    // The drop carried the durable state it was derived from and was rejected;
    // because the fake records rejected batches but applies no catalog change,
    // both replicas remain.
    assert_eq!(ctx.applied.len(), 1);
    let drops = ctx.drops();
    assert_eq!(drops.len(), 1, "should have attempted one drop");
    if let Decision::DropReplica { expected, .. } = drops[0] {
        assert_eq!(
            expected,
            &ExpectedClusterState {
                size: "100cc".to_string(),
                replication_factor: 1,
                availability_zones: AvailabilityZones(Vec::new()),
                logging: ComputeReplicaLogging::default(),
                reconfiguration: None,
                burst: None,
            }
        );
    } else {
        panic!("expected a DropReplica decision");
    }
    assert_eq!(
        ctx.states[&c].replicas.len(),
        2,
        "rejected drop must not retire a replica"
    );

    // Next tick, with no rejection, the drop applies and the cluster converges to
    // the single desired replica.
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.drops().len(), 2, "the recovered tick re-emits the drop");
    assert_eq!(ctx.states[&c].replicas.len(), 1, "converged to rf=1");

    // A third tick is a no-op: desired equals actual.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "converged: no further decisions");
}

#[mz_ore::test]
fn replica_name_gen_is_one_based_and_avoids_used() {
    use crate::ReplicaNameGen;

    // Managed-replica names are 1-based: an empty cluster's first generated
    // name is `r1`.
    let mut name_gen = ReplicaNameGen::new(&[]);
    assert_eq!(name_gen.next_name(), "r1");
    assert_eq!(name_gen.next_name(), "r2");

    // Fresh names start past the highest observed `rNN` index and skip names
    // in use.
    let mut name_gen = ReplicaNameGen::new(&["r2", "custom"]);
    assert_eq!(name_gen.next_name(), "r3");
}
