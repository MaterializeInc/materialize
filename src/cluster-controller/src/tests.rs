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
use std::time::Duration;

use async_trait::async_trait;
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_repr::Timestamp;

use crate::ClusterController;
use crate::ctx::{
    ApplyOutcome, AutoScalingPolicy, ClusterControllerCtx, ClusterSchedule, ClusterState, Decision,
    ObservedReplica, RefreshWindowInputs, ReplicaShape, StateWrite,
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

/// Builds a MANUAL managed cluster state with the given realized size,
/// replication factor, and replicas. No reconfiguration or burst in flight.
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
        schedule: ClusterSchedule::Manual,
        auto_scaling_policy: None,
        reconfiguration: None,
        burst: None,
        burst_enabled: true,
        default_burst_linger: Duration::ZERO,
        replicas,
        hydrated_replicas: BTreeSet::new(),
        refresh_window: None,
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
    /// If set, `apply` performs the real compare-and-append witness check: it
    /// re-reads each decision's target and rejects the batch if any carried
    /// `expected` no longer matches the stored durable state. Off by default so
    /// the blunt `reject_next` counter drives most tests; on for the seam tests
    /// that assert a specific witness *field* (e.g. `schedule`) actually guards a
    /// decision, mirroring the adapter's `cluster_state_matches`.
    witness_check: bool,
    /// A concurrent `ALTER` to splice in just before the first `apply`: each
    /// entry's `ClusterSchedule` is written onto the stored state, modeling an
    /// `ALTER ... SET (SCHEDULE = ...)` that lands after the controller's read
    /// but before its append. Combined with `witness_check`, this exercises the
    /// `schedule` field of the compare-and-append witness end-to-end.
    concurrent_schedule_alter: BTreeMap<ClusterId, ClusterSchedule>,
    /// As `concurrent_schedule_alter`, but for the autoscaling policy: each
    /// entry's value is written onto the stored state's `auto_scaling_policy`
    /// before the witness check, modeling an `ALTER ... SET/RESET (AUTO SCALING
    /// STRATEGY ...)` that lands mid-tick. Exercises the `auto_scaling_policy`
    /// field of the witness.
    concurrent_policy_alter: BTreeMap<ClusterId, Option<AutoScalingPolicy>>,
    /// Replicas the fake reports as hydrated when the controller probes. A
    /// graceful test sets this to drive cut-over.
    hydrated: BTreeSet<ReplicaId>,
    /// Refresh-window inputs the fake returns per cluster when the controller
    /// probes a scheduled cluster. An on-refresh test sets this to drive the
    /// window decision.
    refresh_window: BTreeMap<ClusterId, RefreshWindowInputs>,
}

impl FakeCtx {
    fn new(states: Vec<ClusterState>) -> Self {
        Self {
            now: Timestamp::from(1000u64),
            states: states.into_iter().map(|s| (s.cluster_id, s)).collect(),
            applied: Vec::new(),
            reject_next: 0,
            witness_check: false,
            concurrent_schedule_alter: BTreeMap::new(),
            concurrent_policy_alter: BTreeMap::new(),
            hydrated: BTreeSet::new(),
            refresh_window: BTreeMap::new(),
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

    async fn refresh_window_inputs(
        &mut self,
        cluster_id: ClusterId,
    ) -> Option<RefreshWindowInputs> {
        self.refresh_window.get(&cluster_id).cloned()
    }

    async fn apply(&mut self, decisions: Vec<Decision>) -> ApplyOutcome {
        if self.reject_next > 0 {
            self.reject_next -= 1;
            // A real apply rejects without recording any catalog change. We
            // still record the attempt so tests can assert what was tried.
            self.applied.push(decisions);
            return ApplyOutcome::Rejected;
        }
        // Splice in a concurrent `ALTER` that lands between the controller's read
        // and this append: rewrite the stored schedule before the witness check
        // runs, so a decision derived from the pre-`ALTER` view fails its guard.
        for (cluster_id, schedule) in std::mem::take(&mut self.concurrent_schedule_alter) {
            if let Some(state) = self.states.get_mut(&cluster_id) {
                state.schedule = schedule;
            }
        }
        for (cluster_id, policy) in std::mem::take(&mut self.concurrent_policy_alter) {
            if let Some(state) = self.states.get_mut(&cluster_id) {
                state.auto_scaling_policy = policy;
            }
        }
        if self.witness_check && !self.witness_holds(&decisions) {
            // A real apply rejects without recording any catalog change. We still
            // record the attempt so tests can assert what was tried.
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
    /// Whether every decision's carried `expected` still matches the durable
    /// state of its target cluster, mirroring the adapter's
    /// `cluster_state_matches`. A divergence (e.g. a concurrent `ALTER` that
    /// flipped the `schedule`) makes the whole batch fail its compare-and-append.
    fn witness_holds(&self, decisions: &[Decision]) -> bool {
        decisions.iter().all(|decision| {
            let (cluster_id, expected) = match decision {
                Decision::CreateReplica {
                    cluster_id,
                    expected,
                    ..
                }
                | Decision::DropReplica {
                    cluster_id,
                    expected,
                    ..
                }
                | Decision::UpdateClusterState {
                    cluster_id,
                    expected,
                    ..
                } => (cluster_id, expected),
            };
            self.states
                .get(cluster_id)
                .is_some_and(|state| &state.expected() == expected)
        })
    }

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
                schedule: ClusterSchedule::Manual,
                auto_scaling_policy: None,
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
                schedule: ClusterSchedule::Manual,
                auto_scaling_policy: None,
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
        schedule: ClusterSchedule::Manual,
        auto_scaling_policy: None,
        reconfiguration: Some(rec),
        burst: None,
        burst_enabled: true,
        default_burst_linger: Duration::ZERO,
        replicas,
        hydrated_replicas: hydrated,
        refresh_window: None,
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
        schedule: ClusterSchedule::Manual,
        auto_scaling_policy: None,
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
        burst_enabled: true,
        default_burst_linger: Duration::ZERO,
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
        refresh_window: None,
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

// ----- On-refresh scheduling strategy. -----

use mz_repr::refresh_schedule::RefreshSchedule;
use timely::progress::Antichain;

use crate::ctx::{ClusterSchedule as Sched, RefreshMvInfo};
use crate::strategy::{ON_REFRESH_STRATEGY_NAME, OnRefreshStrategy};

/// A scheduled (`ON REFRESH`) cluster state with the given realized size,
/// replication factor, replicas, and optional refresh-window inputs.
fn scheduled_state(
    cluster_id: ClusterId,
    size: &str,
    replication_factor: u32,
    hydration_time_estimate_ms: u64,
    replicas: Vec<ObservedReplica>,
    refresh_window: Option<RefreshWindowInputs>,
) -> ClusterState {
    ClusterState {
        cluster_id,
        size: size.to_string(),
        replication_factor,
        availability_zones: Vec::new(),
        logging: ComputeReplicaLogging::default(),
        schedule: Sched::Refresh {
            hydration_time_estimate: Duration::from_millis(hydration_time_estimate_ms),
        },
        auto_scaling_policy: None,
        reconfiguration: None,
        burst: None,
        burst_enabled: true,
        default_burst_linger: Duration::ZERO,
        replicas,
        hydrated_replicas: BTreeSet::new(),
        refresh_window,
    }
}

/// A `REFRESH AT` schedule with a single refresh time.
fn refresh_at(at: u64) -> RefreshSchedule {
    RefreshSchedule {
        everies: Vec::new(),
        ats: vec![Timestamp::from(at)],
    }
}

/// Refresh-window inputs: read ts, compaction estimate, and one MV with the given
/// write frontier and schedule. `Some(ts)` is a single-element write frontier
/// `[ts]`; `None` is the empty (sealed) frontier `[]`.
fn window_inputs(
    read_ts: u64,
    compaction_ms: u64,
    write_frontier: Option<u64>,
    schedule: RefreshSchedule,
) -> RefreshWindowInputs {
    let write_frontier = match write_frontier {
        Some(ts) => Antichain::from_elem(Timestamp::from(ts)),
        None => Antichain::new(),
    };
    RefreshWindowInputs {
        read_ts: Timestamp::from(read_ts),
        compaction_estimate: Duration::from_millis(compaction_ms),
        refresh_mvs: vec![RefreshMvInfo {
            write_frontier,
            refresh_schedule: schedule,
        }],
    }
}

#[mz_ore::test]
fn on_refresh_baseline_holds_nothing_on_scheduled() {
    // Even with a stale non-zero rf, the baseline contributes nothing to a
    // scheduled cluster — the on-refresh strategy is the sole contributor.
    let c = cluster(1);
    let state = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    let baseline = crate::strategy::BaselineStrategy;
    assert!(
        baseline
            .desired_replicas(&state, Timestamp::from(0u64))
            .is_empty(),
        "baseline must hold nothing on a scheduled cluster"
    );
}

#[mz_ore::test]
fn on_refresh_normalizes_replication_factor() {
    // A scheduled cluster carrying a stale non-zero rf is normalized to 0.
    let c = cluster(1);
    let state = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    let s = OnRefreshStrategy;
    let write = s.update_state(&state, Timestamp::from(0u64));
    assert_eq!(write.new_replication_factor, Some(0));

    // Already 0 (or MANUAL): no write, so steady ticks stay no-ops.
    let normalized = scheduled_state(c, "100cc", 0, 0, Vec::new(), None);
    assert!(
        s.update_state(&normalized, Timestamp::from(0u64))
            .is_empty()
    );
    let manual = state_(c, "100cc", 1);
    assert!(s.update_state(&manual, Timestamp::from(0u64)).is_empty());
}

/// A MANUAL cluster with no replicas, for the normalization no-op check.
fn state_(cluster_id: ClusterId, size: &str, rf: u32) -> ClusterState {
    state(cluster_id, size, rf, Vec::new())
}

#[mz_ore::test]
fn on_refresh_in_window_desires_one_replica() {
    // Read ts 100, MV write frontier 50 (strictly below the read ts), so the MV
    // still needs a refresh and the cluster is On. One replica at the realized
    // shape.
    let c = cluster(1);
    let inputs = window_inputs(100, 0, Some(50), refresh_at(1000));
    let state = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    let desired = s.desired_replicas(&state, Timestamp::from(0u64));
    assert_eq!(desired.len(), 1, "in-window cluster desires one replica");
    assert_eq!(desired[0].shape.size, "100cc");
}

#[mz_ore::test]
fn on_refresh_caught_up_at_read_ts_is_off() {
    // Frontier exactly at the read ts (and no hydration lead, no compaction
    // window): the MV is caught up, so the cluster is Off. The needs-refresh check
    // is strict (`frontier < read_ts + estimate`), matching the legacy scheduler.
    let c = cluster(1);
    let inputs = window_inputs(100, 0, Some(100), refresh_at(50));
    let state = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert!(
        s.desired_replicas(&state, Timestamp::from(0u64)).is_empty(),
        "a caught-up MV at the read ts leaves the cluster off"
    );
}

#[mz_ore::test]
fn on_refresh_out_of_window_desires_nothing() {
    // Read ts 100, MV write frontier 200 (already advanced past the read ts), no
    // hydration lead and no compaction window, so the MV needs no refresh: Off.
    let c = cluster(1);
    let inputs = window_inputs(100, 0, Some(200), refresh_at(50));
    let state = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert!(
        s.desired_replicas(&state, Timestamp::from(0u64)).is_empty(),
        "out-of-window cluster desires nothing"
    );
}

#[mz_ore::test]
fn on_refresh_empty_frontier_needs_no_refresh() {
    // An empty (sealed) write frontier `[]` is the "complete past every timestamp"
    // state: `Antichain::less_than` is `false` for every timestamp, so the MV never
    // reads as needing a refresh on that count — exactly as the legacy refresh
    // policy decides it with `Antichain::less_than`. The compaction window is also
    // closed here (read ts 1000 is well past the last `AT 200` plus the compaction
    // estimate), so the cluster is Off.
    //
    // This guards the empty/sealed-frontier arm of the window decision. A
    // single-input total-order MV's write frontier holds at most one element, so a
    // multi-element frontier is not reachable and is not exercised here; the
    // `Antichain` seam keeps the model faithful regardless.
    let c = cluster(1);
    let inputs = window_inputs(1000, 100, None, refresh_at(200));
    let state = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert!(
        s.desired_replicas(&state, Timestamp::from(0u64)).is_empty(),
        "an empty write frontier needs no refresh and leaves the cluster off"
    );
}

#[mz_ore::test]
fn on_refresh_hydration_estimate_opens_window_early() {
    // Write frontier 200 is past the read ts 100, so on its own the MV needs no
    // refresh. But a hydration-time estimate of 150 adjusts the read ts to 250,
    // which the frontier (200) is now below, so the cluster turns on early to
    // rehydrate ahead of the refresh.
    let c = cluster(1);
    let inputs = window_inputs(100, 0, Some(200), refresh_at(1000));
    let state = scheduled_state(c, "100cc", 0, 150, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert_eq!(
        s.desired_replicas(&state, Timestamp::from(0u64)).len(),
        1,
        "the hydration estimate opens the window early"
    );

    // With no estimate the same frontier leaves the cluster Off.
    let no_estimate = scheduled_state(
        c,
        "100cc",
        0,
        0,
        Vec::new(),
        Some(window_inputs(100, 0, Some(200), refresh_at(1000))),
    );
    assert!(
        s.desired_replicas(&no_estimate, Timestamp::from(0u64))
            .is_empty()
    );
}

#[mz_ore::test]
fn on_refresh_compaction_window_keeps_cluster_on() {
    // The MV's frontier (300) is past the read ts (250), so it needs no refresh.
    // But its previous refresh was recent: with frontier 300 rounded down past
    // the `AT 200` schedule, prev_refresh = 200, and 200 + compaction_estimate
    // (100) = 300 > read ts 250, so the cluster stays on for compaction.
    let c = cluster(1);
    let inputs = window_inputs(250, 100, Some(300), refresh_at(200));
    let state = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert_eq!(
        s.desired_replicas(&state, Timestamp::from(0u64)).len(),
        1,
        "the compaction window keeps the cluster on"
    );

    // A later read ts past the compaction window turns it off: the frontier (500)
    // needs no refresh at read ts 400, and prev_refresh 200 + compaction 100 = 300
    // is not > read ts 400.
    let past = scheduled_state(
        c,
        "100cc",
        0,
        0,
        Vec::new(),
        Some(window_inputs(400, 100, Some(500), refresh_at(200))),
    );
    assert!(s.desired_replicas(&past, Timestamp::from(0u64)).is_empty());
}

#[mz_ore::test(tokio::test)]
async fn on_refresh_creates_in_window_through_seam() {
    // End-to-end through the ctx seam: a scheduled cluster with a stale rf=1 and
    // no replicas, inside its refresh window. Phase 1 normalizes rf to 0; phase 2
    // creates the one in-window replica.
    let c = cluster(1);
    let state = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    let mut ctx = FakeCtx::new(vec![state]);
    ctx.refresh_window
        .insert(c, window_inputs(100, 0, Some(50), refresh_at(1000)));

    let controller = ClusterController::new();
    controller.reconcile(&mut ctx).await;

    assert_eq!(
        ctx.states[&c].replication_factor, 0,
        "rf normalized to 0 at runtime"
    );
    let creates = ctx.creates();
    assert_eq!(creates.len(), 1, "one in-window replica is created");
    if let Decision::CreateReplica { reasons, shape, .. } = creates[0] {
        assert!(reasons.contains(&ON_REFRESH_STRATEGY_NAME));
        assert_eq!(shape.size, "100cc");
    } else {
        panic!("expected a CreateReplica");
    }
    assert!(ctx.drops().is_empty());

    // A second tick converges: rf is 0 (no write), and the in-window replica
    // matches the on-refresh desire, so nothing changes.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "converged inside the window");
}

#[mz_ore::test(tokio::test)]
async fn on_refresh_drops_outside_window_through_seam() {
    // A scheduled cluster (already normalized rf=0) with a running replica, now
    // outside its refresh window: the on-refresh strategy desires nothing, so the
    // replica is dropped.
    let c = cluster(1);
    let state = scheduled_state(
        c,
        "100cc",
        0,
        0,
        vec![observed(replica(1), "r0", "100cc")],
        None,
    );
    let mut ctx = FakeCtx::new(vec![state]);
    ctx.refresh_window
        .insert(c, window_inputs(100, 0, Some(200), refresh_at(50)));

    let controller = ClusterController::new();
    controller.reconcile(&mut ctx).await;

    assert!(ctx.creates().is_empty());
    let drops = ctx.drops();
    assert_eq!(drops.len(), 1, "the out-of-window replica is dropped");
    assert!(ctx.states[&c].replicas.is_empty());
}

#[mz_ore::test(tokio::test)]
async fn on_refresh_schedule_alter_rejects_in_flight_decision() {
    // The `schedule` field of the compare-and-append witness is load-bearing: a
    // concurrent `ALTER ... SET (SCHEDULE = MANUAL)` that lands between the
    // controller's read and its append must reject the in-flight on-refresh drop,
    // so the on-refresh strategy never reshapes a cluster the user just handed
    // back to the baseline. With `witness_check` on, this exercises the real
    // per-decision compare (not the blunt `reject_next` counter), so dropping
    // `schedule` from `ExpectedClusterState` would make this test fail.
    let c = cluster(1);
    // Scheduled, already-normalized (rf=0), one running replica, outside its
    // window — so the on-refresh strategy emits a phase-2 drop and no phase-1
    // write. The drop carries `expected.schedule = Refresh`.
    let state = scheduled_state(
        c,
        "100cc",
        0,
        0,
        vec![observed(replica(1), "r0", "100cc")],
        None,
    );
    let mut ctx = FakeCtx::new(vec![state]);
    ctx.refresh_window
        .insert(c, window_inputs(100, 0, Some(200), refresh_at(50)));
    ctx.witness_check = true;
    // The `ALTER` flips only the schedule (rf, size, azs, logging unchanged), so
    // the rejection is attributable solely to the witness `schedule` field.
    ctx.concurrent_schedule_alter.insert(c, Sched::Manual);

    let controller = ClusterController::new();
    controller.reconcile(&mut ctx).await;

    // The drop was attempted but rejected by its compare-and-append guard, so the
    // replica the user's now-MANUAL cluster owns is left untouched.
    let drops = ctx.drops();
    assert_eq!(drops.len(), 1, "the drop was attempted");
    if let Decision::DropReplica { expected, .. } = drops[0] {
        assert_eq!(
            expected.schedule,
            Sched::Refresh {
                hydration_time_estimate: Duration::from_millis(0)
            },
            "the drop was derived from the pre-ALTER (Refresh) schedule"
        );
    } else {
        panic!("expected a DropReplica");
    }
    assert_eq!(
        ctx.states[&c].replicas.len(),
        1,
        "the rejected drop left the replica in place"
    );
    assert_eq!(ctx.states[&c].schedule, Sched::Manual);
}

#[mz_ore::test(tokio::test)]
async fn on_refresh_unchanged_schedule_passes_witness() {
    // The dual of the rejection test: with the witness check on but no concurrent
    // `ALTER`, the matching `schedule` lets the same out-of-window drop apply, so
    // the check is not vacuously rejecting.
    let c = cluster(1);
    let state = scheduled_state(
        c,
        "100cc",
        0,
        0,
        vec![observed(replica(1), "r0", "100cc")],
        None,
    );
    let mut ctx = FakeCtx::new(vec![state]);
    ctx.refresh_window
        .insert(c, window_inputs(100, 0, Some(200), refresh_at(50)));
    ctx.witness_check = true;

    let controller = ClusterController::new();
    controller.reconcile(&mut ctx).await;

    assert_eq!(ctx.drops().len(), 1, "the out-of-window replica is dropped");
    assert!(
        ctx.states[&c].replicas.is_empty(),
        "the drop applied under a matching witness"
    );
}

mod hydration_burst {
    use std::time::Duration;

    use mz_compute_types::config::ComputeReplicaLogging;
    use mz_repr::Timestamp;

    use super::{ObservedReplica, cluster, observed, replica, state};
    use crate::ctx::{
        AutoScalingPolicy, BurstRecord, ClusterState, OnHydrationPolicy, ReplicaShape, StateWrite,
    };
    use crate::strategy::{HYDRATION_BURST_STRATEGY_NAME, HydrationBurstStrategy, Strategy};

    /// A MANUAL cluster carrying an `ON HYDRATION` policy at `hydration_size` with
    /// the given linger, plus an optional in-flight burst record. `burst_enabled`
    /// is on and the default linger is zero, matching a steady environment.
    fn burst_state(
        size: &str,
        rf: u32,
        hydration_size: &str,
        linger: Duration,
        replicas: Vec<ObservedReplica>,
        burst: Option<BurstRecord>,
    ) -> ClusterState {
        let mut s = state(cluster(1), size, rf, replicas);
        s.auto_scaling_policy = Some(AutoScalingPolicy {
            on_hydration: Some(OnHydrationPolicy {
                hydration_size: hydration_size.to_string(),
                linger_duration: Some(linger),
            }),
        });
        s.burst = burst;
        s
    }

    fn record(burst_size: &str, linger: Duration, stamped: Option<u64>) -> BurstRecord {
        BurstRecord {
            burst_size: burst_size.to_string(),
            linger_duration: linger,
            steady_hydrated_at: stamped.map(Timestamp::from),
        }
    }

    fn now(ms: u64) -> Timestamp {
        Timestamp::from(ms)
    }

    #[mz_ore::test]
    fn burst_arms_when_steady_unhydrated() {
        // Policy set, cluster On, the one steady replica not hydrated, no record:
        // arm a burst at the hydration size.
        let s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let write = HydrationBurstStrategy.update_state(&s, now(1000));
        let burst = write
            .burst
            .expect("a burst record is written")
            .expect("present");
        assert_eq!(burst.burst_size, "400cc");
        assert_eq!(burst.linger_duration, Duration::from_millis(10));
        assert_eq!(burst.steady_hydrated_at, None);
    }

    #[mz_ore::test]
    fn burst_uses_default_linger_when_omitted() {
        // The policy omits LINGER DURATION, so the record takes the env default.
        let mut s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::ZERO,
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        s.auto_scaling_policy = Some(AutoScalingPolicy {
            on_hydration: Some(OnHydrationPolicy {
                hydration_size: "400cc".to_string(),
                linger_duration: None,
            }),
        });
        s.default_burst_linger = Duration::from_secs(42);
        let write = HydrationBurstStrategy.update_state(&s, now(1000));
        let burst = write.burst.expect("written").expect("present");
        assert_eq!(burst.linger_duration, Duration::from_secs(42));
    }

    #[mz_ore::test]
    fn burst_does_not_arm_when_steady_hydrated() {
        // The steady replica is already hydrated, so no burst is warranted: no
        // record is written.
        let mut s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        s.hydrated_replicas.insert(replica(1));
        assert!(
            HydrationBurstStrategy
                .update_state(&s, now(1000))
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn burst_desires_one_replica_at_hydration_size() {
        // With a record present, the strategy desires exactly one replica at the
        // burst size (with the cluster's AZ pool and logging).
        let s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(10), None)),
        );
        let desired = HydrationBurstStrategy.desired_replicas(&s, now(1000));
        assert_eq!(desired.len(), 1);
        assert_eq!(desired[0].shape.size, "400cc");
    }

    #[mz_ore::test]
    fn burst_no_record_desires_nothing() {
        let s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        assert!(
            HydrationBurstStrategy
                .desired_replicas(&s, now(1000))
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn burst_stamps_then_lingers_then_tears_down() {
        // Record present, steady replica hydrated for the first time: stamp the
        // linger start.
        let mut s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        s.hydrated_replicas.insert(replica(1));
        let write = HydrationBurstStrategy.update_state(&s, now(1000));
        let burst = write.burst.expect("stamped").expect("present");
        assert_eq!(burst.steady_hydrated_at, Some(now(1000)));

        // Stamped, linger not yet elapsed (now=1050, stamped=1000, linger=100): hold.
        let mut s = s.clone();
        s.burst = Some(record("400cc", Duration::from_millis(100), Some(1000)));
        assert!(
            HydrationBurstStrategy
                .update_state(&s, now(1050))
                .is_empty()
        );

        // Stamped, linger elapsed (now=1101 > 1000+100): tear down.
        let write = HydrationBurstStrategy.update_state(&s, now(1101));
        assert_eq!(write.burst, Some(None), "burst record cleared at teardown");
    }

    #[mz_ore::test]
    fn burst_re_arms_when_steady_unhydrates() {
        // Record present, previously stamped, but the steady set is no longer
        // hydrated: reset the stamp so the linger restarts after the next hydration.
        let s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), Some(1000))),
        );
        // hydrated_replicas is empty: the steady replica went un-hydrated.
        let write = HydrationBurstStrategy.update_state(&s, now(1050));
        let burst = write.burst.expect("rewritten").expect("present");
        assert_eq!(burst.steady_hydrated_at, None, "stamp reset on re-arm");
    }

    #[mz_ore::test]
    fn burst_tears_down_when_cluster_off() {
        // The cluster was turned off (rf=0). A burst is no longer warranted, so the
        // record is cleared regardless of linger.
        let mut s = burst_state(
            "100cc",
            0,
            "400cc",
            Duration::from_millis(100),
            Vec::new(),
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        s.replication_factor = 0;
        assert_eq!(
            HydrationBurstStrategy.update_state(&s, now(1000)).burst,
            Some(None)
        );
    }

    #[mz_ore::test]
    fn burst_tears_down_when_policy_removed() {
        // The policy was removed (the cluster no longer carries ON HYDRATION). The
        // stale record is cleared.
        let mut s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        s.auto_scaling_policy = None;
        assert_eq!(
            HydrationBurstStrategy.update_state(&s, now(1000)).burst,
            Some(None)
        );
    }

    #[mz_ore::test]
    fn burst_tears_down_on_size_change() {
        // The HYDRATION SIZE changed from the record's size: the stale record is
        // cleared (a fresh one at the new size is written on a later tick).
        let s = burst_state(
            "100cc",
            1,
            "800cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        assert_eq!(
            HydrationBurstStrategy.update_state(&s, now(1000)).burst,
            Some(None)
        );
    }

    #[mz_ore::test]
    fn burst_break_glass_disables_strategy() {
        // The break-glass flag is off: no burst is armed even when steady is
        // un-hydrated, and an existing record is torn down.
        let mut s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        s.burst_enabled = false;
        assert!(
            HydrationBurstStrategy
                .update_state(&s, now(1000))
                .is_empty()
        );

        s.burst = Some(record("400cc", Duration::from_millis(100), None));
        assert_eq!(
            HydrationBurstStrategy.update_state(&s, now(1000)).burst,
            Some(None)
        );
    }

    #[mz_ore::test]
    fn burst_no_policy_is_a_noop() {
        // A plain MANUAL cluster with no autoscaling policy: the strategy never
        // writes or desires anything.
        let s = state(
            cluster(1),
            "100cc",
            1,
            vec![observed(replica(1), "r0", "100cc")],
        );
        assert!(
            HydrationBurstStrategy
                .update_state(&s, now(1000))
                .is_empty()
        );
        assert!(
            HydrationBurstStrategy
                .desired_replicas(&s, now(1000))
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn burst_replica_shape_carries_az_and_logging() {
        // The burst replica differs from steady only in size: it carries the
        // cluster's AZ pool and logging.
        let mut s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            Vec::new(),
            Some(record("400cc", Duration::from_millis(10), None)),
        );
        s.availability_zones = vec!["az1".to_string(), "az2".to_string()];
        let desired = HydrationBurstStrategy.desired_replicas(&s, now(1000));
        let expected = ReplicaShape {
            size: "400cc".to_string(),
            availability_zones: s.availability_zones.clone(),
            logging: ComputeReplicaLogging::default(),
        };
        assert!(desired[0].shape.matches(&expected));
    }

    #[mz_ore::test]
    fn burst_strategy_name() {
        assert_eq!(HydrationBurstStrategy.name(), HYDRATION_BURST_STRATEGY_NAME);
    }

    #[mz_ore::test(tokio::test)]
    async fn burst_seam_creates_then_tears_down() {
        use super::FakeCtx;
        use crate::ClusterController;
        use crate::ctx::Decision;

        // A cluster with a 100cc steady replica that is not hydrated and an
        // ON HYDRATION (400cc) policy. The controller writes a burst record
        // (phase 1) and creates the 400cc burst replica (phase 2).
        let c = cluster(1);
        let s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let mut ctx = FakeCtx::new(vec![s]);
        let controller = ClusterController::new();
        controller.reconcile(&mut ctx).await;

        // The burst replica was created at the hydration size.
        let burst_creates: Vec<_> = ctx
            .creates()
            .into_iter()
            .filter(|d| matches!(d, Decision::CreateReplica { shape, .. } if shape.size == "400cc"))
            .collect();
        assert_eq!(burst_creates.len(), 1, "one 400cc burst replica created");
        assert!(
            matches!(&burst_creates[0], Decision::CreateReplica { reasons, .. } if reasons.contains(&HYDRATION_BURST_STRATEGY_NAME)),
            "the create is attributed to the burst strategy"
        );
        assert!(
            ctx.states[&c].burst.is_some(),
            "the burst record was written"
        );

        // Now mark the steady replica hydrated; with a zero linger the next tick
        // (advancing `now`) stamps and then tears down the burst.
        ctx.hydrated.insert(replica(1));
        ctx.now = Timestamp::from(2000u64);
        controller.reconcile(&mut ctx).await;
        // First post-hydration tick stamps `steady_hydrated_at`.
        ctx.now = Timestamp::from(3000u64);
        controller.reconcile(&mut ctx).await;
        assert!(
            ctx.states[&c].burst.is_none(),
            "the burst record is cleared after the (zero) linger elapses"
        );
        assert!(
            !ctx.states[&c]
                .replicas
                .iter()
                .any(|r| r.shape.size == "400cc"),
            "the burst replica is torn down"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn burst_policy_alter_rejects_in_flight_decision() {
        use super::FakeCtx;
        use crate::ClusterController;
        use crate::ctx::Decision;

        // The `auto_scaling_policy` field of the compare-and-append witness is
        // load-bearing: a concurrent `ALTER ... RESET (AUTO SCALING STRATEGY)`
        // that lands between the controller's read and its append must reject the
        // in-flight burst write, so the controller never arms a burst on a cluster
        // whose policy the user just cleared. With `witness_check` on, this
        // exercises the real per-decision compare, so dropping `auto_scaling_policy`
        // from `ExpectedClusterState` would make this test fail.
        let c = cluster(1);
        // Policy set, cluster On, the steady replica not hydrated, no record: the
        // strategy emits a phase-1 burst write carrying `expected.auto_scaling_policy`.
        let s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let mut ctx = FakeCtx::new(vec![s]);
        ctx.witness_check = true;
        // The `ALTER` clears only the policy (size, rf, azs, logging unchanged), so
        // the rejection is attributable solely to the witness `auto_scaling_policy`.
        ctx.concurrent_policy_alter.insert(c, None);

        let controller = ClusterController::new();
        controller.reconcile(&mut ctx).await;

        // The burst write was attempted but rejected by its compare-and-append
        // guard, so no burst record landed and no burst replica was created.
        let writes: Vec<_> = ctx
            .applied
            .iter()
            .flatten()
            .filter(|d| matches!(d, Decision::UpdateClusterState { .. }))
            .collect();
        assert_eq!(writes.len(), 1, "the burst write was attempted");
        assert!(
            ctx.states[&c].burst.is_none(),
            "the rejected write left no burst record"
        );
        assert!(
            !ctx.states[&c]
                .replicas
                .iter()
                .any(|r| r.shape.size == "400cc"),
            "no burst replica was created under the rejected witness"
        );
        assert!(
            ctx.states[&c].auto_scaling_policy.is_none(),
            "the user's policy reset stands"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn burst_unchanged_policy_passes_witness() {
        use super::FakeCtx;
        use crate::ClusterController;

        // The dual of the rejection test: with the witness check on but no
        // concurrent policy `ALTER`, the matching `auto_scaling_policy` lets the
        // same burst write apply, so the check is not vacuously rejecting.
        let c = cluster(1);
        let s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let mut ctx = FakeCtx::new(vec![s]);
        ctx.witness_check = true;

        let controller = ClusterController::new();
        controller.reconcile(&mut ctx).await;

        assert!(
            ctx.states[&c].burst.is_some(),
            "the burst record was written under a matching witness"
        );
        assert!(
            ctx.states[&c]
                .replicas
                .iter()
                .any(|r| r.shape.size == "400cc"),
            "the burst replica was created under a matching witness"
        );
    }

    #[mz_ore::test]
    fn burst_success_precedence_when_hydrated_with_record() {
        // Record present and steady hydrated, but already stamped within linger:
        // the record is held, not torn down, until linger elapses (a previous
        // arm/stamp test path). Here a fresh stamp happens because not yet stamped.
        let mut s = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(50),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(50), None)),
        );
        s.hydrated_replicas.insert(replica(1));
        let write: StateWrite = HydrationBurstStrategy.update_state(&s, now(2000));
        let burst = write.burst.expect("stamped").expect("present");
        assert_eq!(burst.steady_hydrated_at, Some(now(2000)));
    }
}
