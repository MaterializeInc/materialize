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

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_repr::Timestamp;

use crate::ClusterController;
use crate::ctx::{
    ApplyOutcome, ClusterControllerCtx, ClusterState, Decision, ObservedReplica, ReplicaShape,
    StateWrite,
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
        availability_zones: Vec::new(),
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
        hydrated_replicas: BTreeSet::new(),
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
    /// Replicas the fake reports as hydrated when the controller probes. A
    /// graceful test sets this to drive cut-over.
    hydrated: BTreeSet<ReplicaId>,
}

impl FakeCtx {
    fn new(states: Vec<ClusterState>) -> Self {
        Self {
            now: Timestamp::from(1000u64),
            states: states.into_iter().map(|s| (s.cluster_id, s)).collect(),
            applied: Vec::new(),
            reject_next: 0,
            hydrated: BTreeSet::new(),
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

    async fn hydrated_replicas(
        &mut self,
        _cluster_id: ClusterId,
        replicas: &[ReplicaId],
    ) -> BTreeSet<ReplicaId> {
        replicas
            .iter()
            .copied()
            .filter(|r| self.hydrated.contains(r))
            .collect()
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
                if let Some(size) = &write.new_size {
                    state.size = size.clone();
                }
                if let Some(rf) = write.new_replication_factor {
                    state.replication_factor = rf;
                }
                if let Some(azs) = &write.new_availability_zones {
                    state.availability_zones = azs.clone();
                }
                if let Some(logging) = &write.new_logging {
                    state.logging = logging.clone();
                }
                if let Some(reconfiguration) = &write.reconfiguration {
                    state.reconfiguration = reconfiguration.clone();
                }
                if let Some(burst) = &write.burst {
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
        ExpectedClusterState, OnTimeout, ReconfigurationRecord, ReconfigurationTarget,
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
                        availability_zones: Vec::new(),
                        logging: ComputeReplicaLogging::default(),
                    },
                    deadline: now,
                    on_timeout: OnTimeout::Rollback,
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
                availability_zones: Vec::new(),
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
    use crate::ctx::ExpectedClusterState;

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
                availability_zones: Vec::new(),
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

// ----- Graceful reconfiguration strategy. -----

use crate::ctx::{OnTimeout, ReconfigurationRecord, ReconfigurationTarget};
use crate::strategy::{GRACEFUL_RECONFIGURATION_STRATEGY_NAME, GracefulReconfigurationStrategy};

/// A reconfiguration record targeting `size` at `rf` with the given `deadline`,
/// the (default) `Rollback` timeout action, empty AZ list and default logging.
fn record(size: &str, rf: u32, deadline: u64) -> ReconfigurationRecord {
    record_on_timeout(size, rf, deadline, OnTimeout::Rollback)
}

/// As [`record`], with an explicit `on_timeout` action.
fn record_on_timeout(
    size: &str,
    rf: u32,
    deadline: u64,
    on_timeout: OnTimeout,
) -> ReconfigurationRecord {
    ReconfigurationRecord {
        target: ReconfigurationTarget {
            size: size.to_string(),
            replication_factor: rf,
            availability_zones: Vec::new(),
            logging: ComputeReplicaLogging::default(),
        },
        deadline: Timestamp::from(deadline),
        on_timeout,
    }
}

/// Convenience: a `ClusterState` with an in-flight reconfiguration and an
/// explicit hydrated-replica set.
fn reconfiguring_state(
    cluster_id: ClusterId,
    size: &str,
    rf: u32,
    replicas: Vec<ObservedReplica>,
    rec: ReconfigurationRecord,
    hydrated: BTreeSet<ReplicaId>,
) -> ClusterState {
    ClusterState {
        cluster_id,
        size: size.to_string(),
        replication_factor: rf,
        availability_zones: Vec::new(),
        logging: ComputeReplicaLogging::default(),
        reconfiguration: Some(rec),
        burst: None,
        replicas,
        hydrated_replicas: hydrated,
    }
}

#[mz_ore::test]
fn graceful_desires_target_while_in_flight() {
    // Realized 100cc rf=2; target 200cc rf=2; nothing hydrated; before deadline.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        2,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ],
        record("200cc", 2, 5000),
        BTreeSet::new(),
    );
    let now = Timestamp::from(1000u64);

    let g = GracefulReconfigurationStrategy;
    // No cut-over while not hydrated.
    assert!(g.update_state(&state, now).is_empty());
    // Desires the two target-shape replicas.
    let desired = g.desired_replicas(&state, now);
    assert_eq!(desired.len(), 2);
    assert!(desired.iter().all(|d| d.shape.size == "200cc"));
}

#[mz_ore::test]
fn graceful_cuts_over_when_target_hydrated() {
    // Both target replicas present and hydrated -> cut over, even before deadline.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        2,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
            observed(replica(3), "r2", "200cc"),
            observed(replica(4), "r3", "200cc"),
        ],
        record("200cc", 2, 5000),
        BTreeSet::from([replica(3), replica(4)]),
    );
    let now = Timestamp::from(1000u64);

    let g = GracefulReconfigurationStrategy;
    let write = g.update_state(&state, now);
    assert_eq!(write.new_size.as_deref(), Some("200cc"));
    assert_eq!(write.new_replication_factor, Some(2));
    // The record is cleared on cut-over.
    assert_eq!(write.reconfiguration, Some(None));
}

#[mz_ore::test]
fn graceful_partial_hydration_does_not_cut_over() {
    // Only one of the two target replicas is hydrated: not enough for HA, so no
    // cut-over and the target set is still desired.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        2,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
            observed(replica(3), "r2", "200cc"),
            observed(replica(4), "r3", "200cc"),
        ],
        record("200cc", 2, 5000),
        BTreeSet::from([replica(3)]),
    );
    let now = Timestamp::from(1000u64);

    let g = GracefulReconfigurationStrategy;
    assert!(g.update_state(&state, now).is_empty());
    assert_eq!(g.desired_replicas(&state, now).len(), 2);
}

#[mz_ore::test]
fn graceful_timeout_vs_hydrated_precedence() {
    // Past the deadline AND fully hydrated: success takes precedence over timeout,
    // so we still cut over and keep desiring the target until the cut-over lands.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "200cc"),
        ],
        record("200cc", 1, 1000),
        BTreeSet::from([replica(2)]),
    );
    let now = Timestamp::from(9999u64); // well past the deadline

    let g = GracefulReconfigurationStrategy;
    let write = g.update_state(&state, now);
    assert_eq!(
        write.new_size.as_deref(),
        Some("200cc"),
        "success cuts over"
    );
    // Still desired (awaiting the cut-over a rejected tick could not apply).
    assert_eq!(g.desired_replicas(&state, now).len(), 1);
}

#[mz_ore::test]
fn graceful_timeout_parks_and_drops_target() {
    // Past the deadline and NOT hydrated: park. No cut-over, and the strategy
    // ceases to contribute the target replicas, so the controller drops them.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "200cc"),
        ],
        record("200cc", 1, 1000),
        BTreeSet::new(),
    );
    let now = Timestamp::from(9999u64);

    let g = GracefulReconfigurationStrategy;
    assert!(
        g.update_state(&state, now).is_empty(),
        "no cut-over on timeout"
    );
    assert!(
        g.desired_replicas(&state, now).is_empty(),
        "parked: target replicas no longer desired"
    );
}

#[mz_ore::test]
fn graceful_commit_on_timeout_cuts_over_unhydrated() {
    // Past the deadline and NOT hydrated, but `on_timeout` is `Commit`: cut over to
    // the un-hydrated target anyway and clear the record. The target replicas stay
    // desired (they become the realized set at the cut-over), in contrast to the
    // `Rollback` default which drops them.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "200cc"),
        ],
        record_on_timeout("200cc", 1, 1000, OnTimeout::Commit),
        BTreeSet::new(),
    );
    let now = Timestamp::from(9999u64); // past the deadline, target un-hydrated

    let g = GracefulReconfigurationStrategy;
    let write = g.update_state(&state, now);
    assert_eq!(
        write.new_size.as_deref(),
        Some("200cc"),
        "commit-on-timeout cuts over the un-hydrated target"
    );
    assert_eq!(
        write.reconfiguration,
        Some(None),
        "commit-on-timeout clears the record"
    );
    assert_eq!(
        g.desired_replicas(&state, now).len(),
        1,
        "commit keeps desiring the target (it becomes the realized set)"
    );
}

#[mz_ore::test]
fn graceful_rollback_on_timeout_before_deadline_still_overlaps() {
    // Before the deadline, `Rollback` behaves exactly like the live overlap: the
    // target is desired and there is no cut-over. The action only takes effect once
    // the deadline passes un-hydrated (covered by graceful_timeout_parks_and_drops).
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![observed(replica(1), "r0", "100cc")],
        record_on_timeout("200cc", 1, 5000, OnTimeout::Rollback),
        BTreeSet::new(),
    );
    let now = Timestamp::from(1000u64); // before the deadline

    let g = GracefulReconfigurationStrategy;
    assert!(
        g.update_state(&state, now).is_empty(),
        "no cut-over before the deadline"
    );
    assert_eq!(
        g.desired_replicas(&state, now).len(),
        1,
        "target desired during the overlap regardless of on_timeout"
    );
}

#[mz_ore::test]
fn graceful_az_only_reconfiguration_is_a_shape_change() {
    // Same size, different AZ list: the target shape differs from the realized
    // shape, so the in-flight replica is at a distinct shape and is desired
    // separately from the baseline's realized-shape replica.
    let c = cluster(1);
    let mut state = ClusterState {
        cluster_id: c,
        size: "100cc".to_string(),
        replication_factor: 1,
        availability_zones: vec!["az1".to_string()],
        logging: ComputeReplicaLogging::default(),
        reconfiguration: Some(ReconfigurationRecord {
            target: ReconfigurationTarget {
                size: "100cc".to_string(),
                replication_factor: 1,
                availability_zones: vec!["az2".to_string()],
                logging: ComputeReplicaLogging::default(),
            },
            deadline: Timestamp::from(5000u64),
            on_timeout: OnTimeout::Rollback,
        }),
        burst: None,
        replicas: vec![ObservedReplica {
            replica_id: replica(1),
            name: "r0".to_string(),
            shape: ReplicaShape {
                size: "100cc".to_string(),
                availability_zones: vec!["az1".to_string()],
                logging: ComputeReplicaLogging::default(),
            },
        }],
        hydrated_replicas: BTreeSet::new(),
    };
    let now = Timestamp::from(1000u64);

    let g = GracefulReconfigurationStrategy;
    let desired = g.desired_replicas(&state, now);
    assert_eq!(desired.len(), 1);
    assert_eq!(desired[0].shape.availability_zones, vec!["az2".to_string()]);
    // The desired AZ shape does not match the realized replica's AZ shape.
    assert!(!desired[0].shape.matches(&state.replicas[0].shape));

    // Mark the realized replica hydrated: it is NOT a target replica (wrong AZ),
    // so this must not trigger a cut-over.
    state.hydrated_replicas.insert(replica(1));
    assert!(g.update_state(&state, now).is_empty());
}

#[mz_ore::test(tokio::test)]
async fn graceful_full_flow_overlap_then_cutover() {
    // End-to-end through the kernel: a 100cc rf=2 cluster reconfiguring to 200cc.
    // Tick 1 creates the two 200cc target replicas (overlap). Once they hydrate,
    // a later tick cuts over and the old 100cc replicas fall out.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        2,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "100cc"),
        ],
        record("200cc", 2, 9_000_000),
        BTreeSet::new(),
    );
    let mut ctx = FakeCtx::new(vec![state]);

    let controller = ClusterController::new();

    // Tick 1: overlap — create two 200cc replicas, no drops, no cut-over.
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.creates().len(), 2);
    assert!(ctx.drops().is_empty());
    assert_eq!(ctx.states[&c].size, "100cc", "realized config unchanged");
    assert_eq!(ctx.states[&c].replicas.len(), 4);

    // The target replicas are the two 200cc ones; mark them hydrated.
    let target_ids: BTreeSet<_> = ctx.states[&c]
        .replicas
        .iter()
        .filter(|r| r.shape.size == "200cc")
        .map(|r| r.replica_id)
        .collect();
    assert_eq!(target_ids.len(), 2);
    ctx.hydrated = target_ids.clone();

    // Tick 2: cut over (phase 1) then drop the old 100cc replicas (phase 2).
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert!(ctx.applied.len() > before);
    assert_eq!(ctx.states[&c].size, "200cc", "cut over to the target size");
    assert!(ctx.states[&c].reconfiguration.is_none(), "record cleared");
    assert_eq!(ctx.states[&c].replicas.len(), 2);
    assert!(
        ctx.states[&c]
            .replicas
            .iter()
            .all(|r| r.shape.size == "200cc"),
        "only the target replicas remain"
    );

    // The drops in this run are attributed to no strategy (none desired them).
    let drop_after_cutover = ctx
        .applied
        .iter()
        .flatten()
        .filter_map(|d| match d {
            Decision::DropReplica { reasons, .. } => Some(reasons),
            _ => None,
        })
        .next_back()
        .expect("a drop happened");
    assert!(drop_after_cutover.is_empty());

    // Tick 3: converged, no further decisions.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "converged");
}

#[mz_ore::test(tokio::test)]
async fn graceful_alter_back_clears_record_without_churn() {
    // ALTER-back/cancel: a reconfiguration whose target equals the realized
    // config. The target shape matches the existing replicas, so there is nothing
    // to create; once those replicas hydrate the cut-over just clears the record.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![observed(replica(1), "r0", "100cc")],
        record("100cc", 1, 9_000_000),
        BTreeSet::new(),
    );
    let mut ctx = FakeCtx::new(vec![state]);
    // The controller probes hydration through the ctx; the existing replica is
    // already hydrated.
    ctx.hydrated = BTreeSet::from([replica(1)]);
    let controller = ClusterController::new();

    controller.reconcile(&mut ctx).await;

    // No create, no drop — the realized replica already satisfies the target.
    assert!(ctx.creates().is_empty());
    assert!(ctx.drops().is_empty());
    // The cut-over clears the record (realized config is unchanged).
    assert!(ctx.states[&c].reconfiguration.is_none());
    assert_eq!(ctx.states[&c].size, "100cc");
    assert_eq!(ctx.states[&c].replicas.len(), 1);

    let _ = GRACEFUL_RECONFIGURATION_STRATEGY_NAME;
}

#[mz_ore::test(tokio::test)]
async fn graceful_rollback_at_timeout_drops_target_through_seam() {
    // End-to-end through the seam: a 100cc rf=1 cluster reconfiguring to 200cc
    // whose target never hydrated and whose deadline has passed, under the default
    // `Rollback` action. The realized 100cc replica and the in-flight 200cc target
    // replica both exist (an earlier overlap tick created the target). The tick
    // must drop the in-flight target via `apply`, revert to the pre-reconfiguration
    // set, and retain the record as a tombstone (no cut-over).
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "200cc"),
        ],
        record_on_timeout("200cc", 1, 1000, OnTimeout::Rollback),
        BTreeSet::new(),
    );
    let mut ctx = FakeCtx::new(vec![state]);
    // Past the deadline (1000), target un-hydrated.
    ctx.now = Timestamp::from(9999u64);
    let controller = ClusterController::new();

    controller.reconcile(&mut ctx).await;

    // No phase-1 write: `Rollback`-at-timeout does not cut over.
    assert!(
        !ctx.applied
            .iter()
            .flatten()
            .any(|d| matches!(d, Decision::UpdateClusterState { .. })),
        "rollback-at-timeout must not write durable state"
    );
    // Phase 2 drops the un-desired in-flight target replica.
    assert!(ctx.creates().is_empty());
    let drops = ctx.drops();
    assert_eq!(drops.len(), 1, "the in-flight 200cc target is dropped");
    if let Decision::DropReplica { replica_id, .. } = drops[0] {
        assert_eq!(
            *replica_id,
            replica(2),
            "the 200cc target replica is dropped"
        );
    } else {
        panic!("expected a DropReplica decision");
    }
    // Reverted to the pre-reconfiguration set; the record is retained as a
    // tombstone (its past deadline keeps the strategy parked).
    assert_eq!(ctx.states[&c].size, "100cc", "realized config unchanged");
    assert_eq!(ctx.states[&c].replicas.len(), 1);
    assert_eq!(ctx.states[&c].replicas[0].shape.size, "100cc");
    assert!(
        ctx.states[&c].reconfiguration.is_some(),
        "the record is retained as a tombstone under rollback"
    );

    // A second tick is a no-op: the parked record desires nothing past its
    // deadline, the realized set already matches, and there is nothing left to
    // drop.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "parked: converged to a no-op");
}

#[mz_ore::test(tokio::test)]
async fn graceful_commit_at_timeout_cuts_over_through_seam() {
    // End-to-end through the seam: the same timed-out, un-hydrated reconfiguration
    // but under `Commit`. The tick must cut the realized config over to the
    // un-hydrated 200cc target (phase 1) and then drop the now-undesired 100cc
    // replica (phase 2), clearing the record.
    let c = cluster(1);
    let state = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "200cc"),
        ],
        record_on_timeout("200cc", 1, 1000, OnTimeout::Commit),
        BTreeSet::new(),
    );
    let mut ctx = FakeCtx::new(vec![state]);
    // Past the deadline (1000), target un-hydrated.
    ctx.now = Timestamp::from(9999u64);
    let controller = ClusterController::new();

    controller.reconcile(&mut ctx).await;

    // Phase 1 cut the realized config over to the target and cleared the record.
    assert_eq!(ctx.states[&c].size, "200cc", "commit-at-timeout cuts over");
    assert!(
        ctx.states[&c].reconfiguration.is_none(),
        "commit-at-timeout clears the record"
    );
    assert!(
        ctx.applied
            .iter()
            .flatten()
            .any(|d| matches!(d, Decision::UpdateClusterState { .. })),
        "commit-at-timeout writes the cut-over"
    );
    // Phase 2 dropped the old 100cc replica; only the target shape remains.
    assert!(ctx.creates().is_empty());
    let drops = ctx.drops();
    assert_eq!(drops.len(), 1, "the old 100cc replica is dropped");
    assert_eq!(ctx.states[&c].replicas.len(), 1);
    assert_eq!(
        ctx.states[&c].replicas[0].shape.size, "200cc",
        "only the target replica remains"
    );

    // A second tick is a no-op: the realized set matches the cut-over config and
    // there is no record left to act on.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "converged after the cut-over");
}
