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
use mz_adapter_types::dyncfgs::all_dyncfgs;
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_repr::Timestamp;

use crate::ClusterController;
use crate::ctx::{
    ApplyOutcome, AutoScalingPolicy, AvailabilityZones, BurstAudit, ClusterControllerCtx,
    ClusterState, Decision, ObservedReplica, ReconfigurationAudit, ReconfigurationStatus,
    ReplicaShape, StateWrite,
};
use crate::strategy::{ConfigSignals, DesiredReplica, LiveSignals, Strategy};

/// A controller over the crate's dyncfgs at their defaults: burst enabled (the
/// break-glass default) and a zero default linger, so seam tests observe burst
/// teardown as soon as the steady set hydrates.
fn controller() -> ClusterController {
    ClusterController::new(all_dyncfgs(ConfigSet::default()))
}

/// The config signals the kernel tests evaluate against, matching what
/// [`controller`]'s dyncfg defaults latch each tick.
fn config() -> ConfigSignals {
    ConfigSignals {
        burst_enabled: true,
        default_burst_linger: Duration::ZERO,
    }
}

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

/// Builds a managed cluster state with the given realized size,
/// replication factor, and replicas. No reconfiguration or burst in flight and
/// no autoscaling policy.
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
        auto_scaling_policy: None,
        reconfiguration: None,
        burst: None,
        replicas,
        reserved_replica_names: Vec::new(),
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
    /// If set, the next `apply` reports resource exhaustion (nothing is
    /// applied) and this count is decremented.
    exhaust_next: usize,
    /// If set, `apply` performs the real compare-and-append witness check: it
    /// re-reads each decision's target and rejects the batch if any carried
    /// `expected` no longer matches the stored durable state. Off by default so
    /// the blunt `reject_next` counter drives most tests; on for the seam tests
    /// that assert a specific witness *field* (e.g. `auto_scaling_policy`)
    /// actually guards a decision, mirroring the adapter's
    /// `cluster_state_matches`.
    witness_check: bool,
    /// A concurrent `ALTER` to splice in just before the first `apply`: each
    /// entry's value is written onto the stored state's `auto_scaling_policy`
    /// before the witness check, modeling an `ALTER ... SET/RESET (AUTO SCALING
    /// STRATEGY ...)` that lands after the controller's read but before its
    /// append. Combined with `witness_check`, this exercises the
    /// `auto_scaling_policy` field of the compare-and-append witness end-to-end.
    concurrent_policy_alter: BTreeMap<ClusterId, Option<AutoScalingPolicy>>,
    /// Replicas the fake reports as hydrated when the controller probes. A
    /// graceful test sets this to drive cut-over.
    hydrated: BTreeSet<ReplicaId>,
    /// How many times the controller probed hydration, for asserting that an
    /// object-less cluster is never probed.
    hydration_probes: usize,
    /// What the fake answers when the controller pulls the object-existence
    /// signal; an absent entry reads `false` (no objects). Held beside the
    /// states (like `hydrated`) rather than read from them, and
    /// `cluster_states` returns the state field at the adapter default
    /// (`false`), so the signal reaches the controller only through the
    /// `has_hydratable_objects` pull, keeping that pull load-bearing for
    /// the seam tests.
    has_hydratable_objects: BTreeMap<ClusterId, bool>,
}

impl FakeCtx {
    fn new(states: Vec<ClusterState>) -> Self {
        Self {
            now: Timestamp::from(1000u64),
            states: states.into_iter().map(|s| (s.cluster_id, s)).collect(),
            applied: Vec::new(),
            reject_next: 0,
            exhaust_next: 0,
            witness_check: false,
            concurrent_policy_alter: BTreeMap::new(),
            hydrated: BTreeSet::new(),
            hydration_probes: 0,
            has_hydratable_objects: BTreeMap::new(),
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
        self.hydration_probes += 1;
        replicas
            .iter()
            .copied()
            .filter(|r| self.hydrated.contains(r))
            .collect()
    }

    async fn has_hydratable_objects(&mut self, cluster_id: ClusterId) -> bool {
        self.has_hydratable_objects
            .get(&cluster_id)
            .copied()
            .unwrap_or(false)
    }

    async fn apply(&mut self, decisions: Vec<Decision>) -> ApplyOutcome {
        if self.reject_next > 0 {
            self.reject_next -= 1;
            // A real apply rejects without recording any catalog change. We
            // still record the attempt so tests can assert what was tried.
            self.applied.push(decisions);
            return ApplyOutcome::Rejected;
        }
        if self.exhaust_next > 0 {
            self.exhaust_next -= 1;
            // Like a rejection, an exhausted apply changes no durable state.
            self.applied.push(decisions);
            return ApplyOutcome::ResourceExhausted;
        }
        // Splice in a concurrent `ALTER` that lands between the controller's read
        // and this append: rewrite the stored policy before the witness check
        // runs, so a decision derived from the pre-`ALTER` view fails its guard.
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
    /// cleared the `auto_scaling_policy`) makes the whole batch fail its
    /// compare-and-append.
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
                    state.reconfiguration = reconfiguration.record.clone();
                }
                if let Some(burst) = burst {
                    state.burst = burst.record.clone();
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

    controller().reconcile(&mut ctx).await;

    // Desired (2 @ 100cc) equals actual, so no decision of any kind was applied.
    assert!(
        ctx.applied.is_empty(),
        "steady cluster should produce no decisions, got {:?}",
        ctx.applied
    );
}

#[mz_ore::test(tokio::test)]
async fn create_skips_reserved_replica_names() {
    // A non-owned (INTERNAL / BILLED AS) replica already occupies "r1": it is
    // reserved but not part of the controller-owned set, so the baseline's create
    // must skip that name rather than collide with it.
    let c = cluster(1);
    let mut s = state(c, "100cc", 1, vec![]);
    s.reserved_replica_names = vec!["r1".to_string()];
    let mut ctx = FakeCtx::new(vec![s]);

    controller().reconcile(&mut ctx).await;

    let created: Vec<&str> = ctx
        .applied
        .iter()
        .flatten()
        .filter_map(|d| match d {
            Decision::CreateReplica { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        created,
        vec!["r2"],
        "the created replica should skip the reserved name r1"
    );
}

#[mz_ore::test(tokio::test)]
async fn no_managed_clusters_is_a_noop() {
    let mut ctx = FakeCtx::new(vec![]);
    controller().reconcile(&mut ctx).await;
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

    controller().reconcile(&mut ctx).await;

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

    controller().reconcile(&mut ctx).await;

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

    controller().reconcile(&mut ctx).await;

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

    fn desired_replicas(
        &self,
        _state: &ClusterState,
        _signals: &LiveSignals,
        _config: &ConfigSignals,
        _now: Timestamp,
    ) -> Vec<DesiredReplica> {
        (0..self.count)
            .map(|_| DesiredReplica {
                shape: shape(&self.size),
            })
            .collect()
    }
}

fn controller_with(strategies: Vec<Box<dyn Strategy>>) -> ClusterController {
    // The kernel runs whatever strategies it holds. We construct one directly
    // for union/diff tests rather than going through `new()`.
    ClusterController {
        strategies,
        dyncfgs: all_dyncfgs(ConfigSet::default()),
    }
}

#[mz_ore::test(tokio::test)]
async fn union_takes_max_not_sum_per_shape() {
    use crate::strategy::BaselineStrategy;

    let c = cluster(1);
    // Baseline desires 2 @ 100cc. The extra strategy also desires 1 @ 100cc.
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
    // Baseline desires 2 @ 100cc. The extra strategy desires 1 @ 200cc. Actual
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
        AvailabilityZones, ExpectedClusterState, OnTimeout, ReconfigurationRecord,
        ReconfigurationStatus, ReconfigurationTarget, ReconfigurationWrite,
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
        fn update_state(
            &self,
            state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            now: Timestamp,
        ) -> StateWrite {
            // Only write while no record exists, so once the write lands the
            // strategy stops contributing and the controller reaches a no-op.
            if state.reconfiguration.is_some() {
                return StateWrite::default();
            }
            StateWrite {
                reconfiguration: Some(ReconfigurationWrite {
                    record: Some(ReconfigurationRecord {
                        target: ReconfigurationTarget {
                            size: "200cc".to_string(),
                            replication_factor: state.replication_factor,
                            availability_zones: AvailabilityZones(Vec::new()),
                            logging: ComputeReplicaLogging::default(),
                        },
                        deadline: now,
                        on_timeout: OnTimeout::Rollback,
                        status: ReconfigurationStatus::InProgress,
                    }),
                    audit: None,
                }),
                ..Default::default()
            }
        }
        fn desired_replicas(
            &self,
            state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> Vec<DesiredReplica> {
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
                auto_scaling_policy: None,
                reconfiguration: None,
                burst: None,
            }
        );
        if let Some(record) = write
            .reconfiguration
            .as_ref()
            .and_then(|w| w.record.as_ref())
        {
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
        match write
            .reconfiguration
            .as_ref()
            .and_then(|w| w.record.as_ref())
        {
            Some(record) => assert_eq!(record.target.replication_factor, 2),
            other => panic!("expected a reconfiguration write derived from rf=2, got {other:?}"),
        }
    } else {
        panic!("expected the recovered tick to apply an UpdateClusterState");
    }

    // The record is now durable. A third tick recomputes against it and reaches
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

    let controller = controller();
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

#[mz_ore::test(tokio::test)]
async fn rejection_is_isolated_per_cluster() {
    // Two clusters are each over-provisioned (rf=1 with two replicas), so the
    // baseline-only controller wants a phase-2 drop on each. We reject exactly
    // one apply. Under per-cluster apply that rejection is scoped to a single
    // cluster, so the other still converges this tick. A single batched apply
    // would have sunk both, leaving both over-provisioned. We assert on the
    // multiset of outcomes rather than which specific cluster lost the race, so
    // the test does not depend on the order the ctx reports clusters in.
    let c1 = cluster(1);
    let c2 = cluster(2);
    let over_provisioned = |c| {
        state(
            c,
            "100cc",
            1,
            vec![
                observed(replica(1), "r0", "100cc"),
                observed(replica(2), "r1", "100cc"),
            ],
        )
    };
    let mut ctx = FakeCtx::new(vec![over_provisioned(c1), over_provisioned(c2)]);
    ctx.reject_next = 1;

    controller().reconcile(&mut ctx).await;

    // One cluster's drop was rejected (still 2 replicas), the other converged to
    // rf=1. A batched apply would leave both at 2.
    let mut counts = [
        ctx.states[&c1].replicas.len(),
        ctx.states[&c2].replicas.len(),
    ];
    counts.sort_unstable();
    assert_eq!(
        counts,
        [1, 2],
        "exactly one cluster converges despite the other's rejection, got {counts:?}"
    );
}

#[mz_ore::test(tokio::test)]
async fn disjoint_state_writes_merge_into_one_apply() {
    // Two strategies write different `StateWrite` fields for the same cluster.
    // They must land together in a single phase-1 apply, not serialize one per
    // tick: the merge unions disjoint fields under one compare-and-append.
    struct WritesSize;
    impl Strategy for WritesSize {
        fn name(&self) -> &'static str {
            "writes-size"
        }
        fn update_state(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> StateWrite {
            StateWrite {
                new_size: Some("200cc".to_string()),
                ..Default::default()
            }
        }
        fn desired_replicas(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> Vec<DesiredReplica> {
            // No replica contribution: this test is about the phase-1 merge, and
            // an empty cluster keeps phase 2 a no-op so the only apply is phase 1.
            Vec::new()
        }
    }
    struct WritesReplicationFactor;
    impl Strategy for WritesReplicationFactor {
        fn name(&self) -> &'static str {
            "writes-rf"
        }
        fn update_state(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> StateWrite {
            StateWrite {
                new_replication_factor: Some(2),
                ..Default::default()
            }
        }
        fn desired_replicas(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> Vec<DesiredReplica> {
            Vec::new()
        }
    }

    let c = cluster(1);
    let mut ctx = FakeCtx::new(vec![state(c, "100cc", 1, Vec::new())]);
    let controller = controller_with(vec![
        Box::new(WritesSize),
        Box::new(WritesReplicationFactor),
    ]);
    controller.reconcile(&mut ctx).await;

    assert_eq!(
        ctx.applied.len(),
        1,
        "the two disjoint writes merge into a single apply, got {:?}",
        ctx.applied
    );
    match &ctx.applied[0][0] {
        Decision::UpdateClusterState { write, .. } => {
            assert_eq!(write.new_size.as_deref(), Some("200cc"));
            assert_eq!(write.new_replication_factor, Some(2));
        }
        other => panic!("expected one merged UpdateClusterState, got {other:?}"),
    }
}

#[mz_ore::test(tokio::test)]
#[should_panic(expected = "conflicting state writes")]
async fn conflicting_state_writes_trip_the_tripwire() {
    // Two strategies set the SAME field to different values. By design this
    // cannot happen (every `StateWrite` field is owned by one strategy), so the
    // merge treats it as an invariant violation and soft-panics, which is a hard
    // panic under the test harness's soft assertions.
    struct WantsLarge;
    impl Strategy for WantsLarge {
        fn name(&self) -> &'static str {
            "wants-large"
        }
        fn update_state(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> StateWrite {
            StateWrite {
                new_size: Some("400cc".to_string()),
                ..Default::default()
            }
        }
        fn desired_replicas(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> Vec<DesiredReplica> {
            Vec::new()
        }
    }
    struct WantsSmall;
    impl Strategy for WantsSmall {
        fn name(&self) -> &'static str {
            "wants-small"
        }
        fn update_state(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> StateWrite {
            StateWrite {
                new_size: Some("100cc".to_string()),
                ..Default::default()
            }
        }
        fn desired_replicas(
            &self,
            _state: &ClusterState,
            _signals: &LiveSignals,
            _config: &ConfigSignals,
            _now: Timestamp,
        ) -> Vec<DesiredReplica> {
            Vec::new()
        }
    }

    let c = cluster(1);
    let mut ctx = FakeCtx::new(vec![state(c, "200cc", 1, Vec::new())]);
    let controller = controller_with(vec![Box::new(WantsLarge), Box::new(WantsSmall)]);
    controller.reconcile(&mut ctx).await;
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
            availability_zones: AvailabilityZones(Vec::new()),
            logging: ComputeReplicaLogging::default(),
        },
        deadline: Timestamp::from(deadline),
        on_timeout,
        status: ReconfigurationStatus::InProgress,
    }
}

/// Convenience: a `ClusterState` with an in-flight reconfiguration, plus the
/// [`LiveSignals`] carrying an explicit hydrated-replica set.
fn reconfiguring_state(
    cluster_id: ClusterId,
    size: &str,
    rf: u32,
    replicas: Vec<ObservedReplica>,
    rec: ReconfigurationRecord,
    hydrated: BTreeSet<ReplicaId>,
) -> (ClusterState, LiveSignals) {
    let state = ClusterState {
        cluster_id,
        size: size.to_string(),
        replication_factor: rf,
        availability_zones: Vec::new(),
        logging: ComputeReplicaLogging::default(),
        auto_scaling_policy: None,
        reconfiguration: Some(rec),
        burst: None,
        replicas,
        reserved_replica_names: Vec::new(),
    };
    let signals = LiveSignals {
        hydrated_replicas: hydrated,
        ..Default::default()
    };
    (state, signals)
}

fn reconfiguration_status(state: &ClusterState) -> Option<ReconfigurationStatus> {
    state.reconfiguration.as_ref().map(|record| record.status)
}

fn written_reconfiguration_status(write: &StateWrite) -> Option<ReconfigurationStatus> {
    write
        .reconfiguration
        .as_ref()
        .and_then(|w| w.record.as_ref())
        .map(|record| record.status)
}

fn written_reconfiguration_audit(write: &StateWrite) -> Option<ReconfigurationAudit> {
    write.reconfiguration.as_ref().and_then(|w| w.audit)
}

fn written_burst_record(write: &StateWrite) -> Option<crate::ctx::BurstRecord> {
    write.burst.as_ref().and_then(|w| w.record.clone())
}

fn written_burst_audit(write: &StateWrite) -> Option<BurstAudit> {
    write.burst.as_ref().and_then(|w| w.audit)
}

#[mz_ore::test]
fn graceful_desires_target_while_in_flight() {
    // Realized 100cc rf=2, target 200cc rf=2, nothing hydrated, before deadline.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
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
    assert!(g.update_state(&state, &signals, &config(), now).is_empty());
    // Desires the two target-shape replicas.
    let desired = g.desired_replicas(&state, &signals, &config(), now);
    assert_eq!(desired.len(), 2);
    assert!(desired.iter().all(|d| d.shape.size == "200cc"));
}

#[mz_ore::test]
fn graceful_cuts_over_when_target_hydrated() {
    // Both target replicas present and hydrated -> cut over, even before deadline.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
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
    let write = g.update_state(&state, &signals, &config(), now);
    assert_eq!(write.new_size.as_deref(), Some("200cc"));
    assert_eq!(write.new_replication_factor, Some(2));
    // The record is retained with terminal status on cut-over, and the write
    // declares a hydrated (not forced) finalize.
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::Finalized)
    );
    assert_eq!(
        written_reconfiguration_audit(&write),
        Some(ReconfigurationAudit::Finalized { forced: false })
    );
}

#[mz_ore::test]
fn graceful_partial_hydration_does_not_cut_over() {
    // Only one of the two target replicas is hydrated: not enough for HA, so no
    // cut-over and the target set is still desired.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
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
    assert!(g.update_state(&state, &signals, &config(), now).is_empty());
    assert_eq!(
        g.desired_replicas(&state, &signals, &config(), now).len(),
        2
    );
}

#[mz_ore::test]
fn graceful_rf_zero_target_cuts_over_on_first_tick() {
    // A target with replication_factor 0 has no replicas to hydrate, so
    // `target_hydrated` is vacuously true and `update_state` finalizes on the
    // first tick, well before the deadline. The audit declares an unforced
    // (hydrated) finalize.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![observed(replica(1), "r0", "100cc")],
        record("200cc", 0, 9_000_000),
        BTreeSet::new(),
    );
    let now = Timestamp::from(1000u64); // well before the deadline

    let g = GracefulReconfigurationStrategy;
    let write = g.update_state(&state, &signals, &config(), now);
    assert_eq!(write.new_size.as_deref(), Some("200cc"));
    assert_eq!(write.new_replication_factor, Some(0));
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::Finalized),
        "an rf=0 target finalizes immediately"
    );
    assert_eq!(
        written_reconfiguration_audit(&write),
        Some(ReconfigurationAudit::Finalized { forced: false }),
        "the vacuously hydrated cut-over is not forced"
    );
}

#[mz_ore::test]
fn graceful_extra_unhydrated_same_shape_replica_does_not_block_cutover() {
    // Two replicas match the target shape but the record only asks for rf=1,
    // and only one of the two is hydrated. rf-many hydrated target replicas
    // are all the cut-over needs: the post-cut-over reconcile retires any
    // surplus target-shape replicas regardless, so waiting for a stray extra
    // to hydrate would only delay the cut-over.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![
            observed(replica(1), "r0", "100cc"),
            observed(replica(2), "r1", "200cc"),
            observed(replica(3), "r2", "200cc"),
        ],
        record("200cc", 1, 9_000_000),
        BTreeSet::from([replica(2)]),
    );
    let now = Timestamp::from(1000u64); // before the deadline

    let g = GracefulReconfigurationStrategy;
    let write = g.update_state(&state, &signals, &config(), now);
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::Finalized),
        "rf-many hydrated target replicas cut over despite an un-hydrated extra"
    );
    assert_eq!(
        written_reconfiguration_audit(&write),
        Some(ReconfigurationAudit::Finalized { forced: false }),
        "the cut-over is a genuine (un-forced) success"
    );
}

#[mz_ore::test]
fn graceful_timeout_vs_hydrated_precedence() {
    // Past the deadline AND fully hydrated: success takes precedence over timeout,
    // so we still cut over and keep desiring the target until the cut-over lands.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
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
    let write = g.update_state(&state, &signals, &config(), now);
    assert_eq!(
        write.new_size.as_deref(),
        Some("200cc"),
        "success cuts over"
    );
    // Still desired (awaiting the cut-over a rejected tick could not apply).
    assert_eq!(
        g.desired_replicas(&state, &signals, &config(), now).len(),
        1
    );
}

#[mz_ore::test]
fn graceful_timeout_marks_record_timed_out_and_drops_target() {
    // Past the deadline and NOT hydrated: abandon. `update_state` marks the
    // record timed out without touching the realized config, and the strategy
    // ceases to contribute the target replicas, so the controller drops them.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
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
    let write = g.update_state(&state, &signals, &config(), now);
    assert!(write.new_size.is_none(), "no cut-over on timeout");
    assert!(
        write.new_replication_factor.is_none()
            && write.new_availability_zones.is_none()
            && write.new_logging.is_none(),
        "the realized config is untouched"
    );
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::TimedOut),
        "the record is marked timed out"
    );
    assert_eq!(
        written_reconfiguration_audit(&write),
        Some(ReconfigurationAudit::TimedOut),
        "and the write declares the timeout"
    );

    // Even before the status write lands, `desired_replicas` stops desiring the
    // target. This matters when the deadline crosses between phase 1's and
    // phase 2's `now` reads within one tick: phase 1 saw the deadline unreached
    // and wrote nothing, phase 2 sees it reached and already drops the target,
    // so the rollback's replica drops stay prompt.
    assert!(
        g.desired_replicas(&state, &signals, &config(), now)
            .is_empty(),
        "timed out: target replicas no longer desired"
    );
}

#[mz_ore::test]
fn graceful_commit_on_timeout_cuts_over_unhydrated() {
    // Past the deadline and NOT hydrated, but `on_timeout` is `Commit`: cut over to
    // the un-hydrated target anyway and retain the record as finalized. The target
    // replicas stay desired (they become the realized set at the cut-over), in
    // contrast to the `Rollback` default which drops them.
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
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
    let write = g.update_state(&state, &signals, &config(), now);
    assert_eq!(
        write.new_size.as_deref(),
        Some("200cc"),
        "commit-on-timeout cuts over the un-hydrated target"
    );
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::Finalized),
        "commit-on-timeout marks the record finalized"
    );
    assert_eq!(
        written_reconfiguration_audit(&write),
        Some(ReconfigurationAudit::Finalized { forced: true }),
        "and declares the cut-over forced"
    );
    assert_eq!(
        g.desired_replicas(&state, &signals, &config(), now).len(),
        1,
        "commit keeps desiring the target (it becomes the realized set)"
    );
}

#[mz_ore::test]
fn graceful_deadline_fires_at_exact_timestamp() {
    // Exercise the exact deadline boundary. One tick before the deadline is the
    // control case.
    let c = cluster(1);
    let deadline = 1000u64;
    let g = GracefulReconfigurationStrategy;
    let mk = |on_timeout| {
        reconfiguring_state(
            c,
            "100cc",
            1,
            vec![observed(replica(1), "r0", "100cc")],
            record_on_timeout("200cc", 1, deadline, on_timeout),
            BTreeSet::new(),
        )
    };

    let (early, early_signals) = mk(OnTimeout::Commit);
    assert!(
        g.update_state(
            &early,
            &early_signals,
            &config(),
            Timestamp::from(deadline - 1)
        )
        .is_empty(),
        "no cut-over one tick before the deadline"
    );

    let (commit, commit_signals) = mk(OnTimeout::Commit);
    let write = g.update_state(
        &commit,
        &commit_signals,
        &config(),
        Timestamp::from(deadline),
    );
    assert_eq!(
        write.new_size.as_deref(),
        Some("200cc"),
        "commit cuts over at exactly the deadline"
    );
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::Finalized),
        "and marks the record finalized"
    );

    let (rollback, rollback_signals) = mk(OnTimeout::Rollback);
    let write = g.update_state(
        &rollback,
        &rollback_signals,
        &config(),
        Timestamp::from(deadline),
    );
    assert_eq!(
        write.new_size, None,
        "rollback does not advance the realized config at the deadline"
    );
    assert_eq!(
        written_reconfiguration_status(&write),
        Some(ReconfigurationStatus::TimedOut),
        "but marks the record timed out"
    );
    assert!(
        g.desired_replicas(
            &rollback,
            &rollback_signals,
            &config(),
            Timestamp::from(deadline)
        )
        .is_empty(),
        "rollback stops desiring the target at exactly the deadline"
    );
}

#[mz_ore::test]
fn graceful_rollback_on_timeout_before_deadline_still_overlaps() {
    // Before the deadline, `Rollback` behaves exactly like the live overlap: the
    // target is desired and there is no cut-over. The action only takes effect once
    // the deadline passes un-hydrated (covered by
    // graceful_timeout_marks_record_timed_out_and_drops_target).
    let c = cluster(1);
    let (state, signals) = reconfiguring_state(
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
        g.update_state(&state, &signals, &config(), now).is_empty(),
        "no cut-over before the deadline"
    );
    assert_eq!(
        g.desired_replicas(&state, &signals, &config(), now).len(),
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
    let state = ClusterState {
        cluster_id: c,
        size: "100cc".to_string(),
        replication_factor: 1,
        availability_zones: vec!["az1".to_string()],
        logging: ComputeReplicaLogging::default(),
        auto_scaling_policy: None,
        reconfiguration: Some(ReconfigurationRecord {
            target: ReconfigurationTarget {
                size: "100cc".to_string(),
                replication_factor: 1,
                availability_zones: AvailabilityZones(vec!["az2".to_string()]),
                logging: ComputeReplicaLogging::default(),
            },
            deadline: Timestamp::from(5000u64),
            on_timeout: OnTimeout::Rollback,
            status: ReconfigurationStatus::InProgress,
        }),
        burst: None,
        replicas: vec![ObservedReplica {
            replica_id: replica(1),
            name: "r0".to_string(),
            shape: ReplicaShape {
                size: "100cc".to_string(),
                availability_zones: AvailabilityZones(vec!["az1".to_string()]),
                logging: ComputeReplicaLogging::default(),
            },
        }],
        reserved_replica_names: Vec::new(),
    };
    let mut signals = LiveSignals::default();
    let now = Timestamp::from(1000u64);

    let g = GracefulReconfigurationStrategy;
    let desired = g.desired_replicas(&state, &signals, &config(), now);
    assert_eq!(desired.len(), 1);
    assert_eq!(
        desired[0].shape.availability_zones.0,
        vec!["az2".to_string()]
    );
    // The desired AZ shape does not match the realized replica's AZ shape.
    assert!(!desired[0].shape.matches(&state.replicas[0].shape));

    // Mark the realized replica hydrated: it is NOT a target replica (wrong AZ),
    // so this must not trigger a cut-over.
    signals.hydrated_replicas.insert(replica(1));
    assert!(g.update_state(&state, &signals, &config(), now).is_empty());
}

#[mz_ore::test(tokio::test)]
async fn graceful_full_flow_overlap_then_cutover() {
    // End-to-end through the kernel: a 100cc rf=2 cluster reconfiguring to 200cc.
    // Tick 1 creates the two 200cc target replicas (overlap). Once they hydrate,
    // a later tick cuts over and the old 100cc replicas fall out.
    let c = cluster(1);
    let (state, _signals) = reconfiguring_state(
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

    let controller = controller();

    // Tick 1: overlap, create two 200cc replicas, no drops, no cut-over.
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.creates().len(), 2);
    assert!(ctx.drops().is_empty());
    assert_eq!(ctx.states[&c].size, "100cc", "realized config unchanged");
    assert_eq!(ctx.states[&c].replicas.len(), 4);

    // The target replicas are the two 200cc ones. Mark them hydrated.
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
    assert_eq!(
        reconfiguration_status(&ctx.states[&c]),
        Some(ReconfigurationStatus::Finalized),
        "record finalized"
    );
    assert_eq!(ctx.states[&c].replicas.len(), 2);
    assert!(
        ctx.states[&c]
            .replicas
            .iter()
            .all(|r| r.shape.size == "200cc"),
        "only the target replicas remain"
    );

    // The old set was retired via explicit drop decisions (no strategy desired
    // the old shape after cut-over, and drops carry no per-strategy attribution).
    let dropped = ctx
        .applied
        .iter()
        .flatten()
        .any(|d| matches!(d, Decision::DropReplica { .. }));
    assert!(dropped, "a drop happened");

    // Tick 3: converged, no further decisions.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "converged");
}

#[mz_ore::test(tokio::test)]
async fn graceful_alter_back_finalizes_without_churn() {
    // An in-progress record whose target equals the realized config. The
    // adapter never writes this state itself: an ALTER back to the realized
    // shape cancels immediately (the record is written with status Cancelled).
    // It remains reachable via the v88->v89 migration backfill, which marks any
    // pre-upgrade record in-progress, including one that was an old-style
    // cancel-back. The target shape matches the existing replicas, so there is
    // nothing to create. Once those replicas hydrate the cut-over just marks
    // the record finalized.
    let c = cluster(1);
    let (state, _signals) = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![observed(replica(1), "r0", "100cc")],
        record("100cc", 1, 9_000_000),
        BTreeSet::new(),
    );
    let mut ctx = FakeCtx::new(vec![state]);
    // The controller probes hydration through the ctx. The existing replica is
    // already hydrated.
    ctx.hydrated = BTreeSet::from([replica(1)]);
    let controller = controller();

    controller.reconcile(&mut ctx).await;

    // No create, no drop. The realized replica already satisfies the target.
    assert!(ctx.creates().is_empty());
    assert!(ctx.drops().is_empty());
    // The cut-over finalizes the record (realized config is unchanged).
    assert_eq!(
        reconfiguration_status(&ctx.states[&c]),
        Some(ReconfigurationStatus::Finalized)
    );
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
    // set, and mark the record timed out without a cut-over.
    let c = cluster(1);
    let (state, _signals) = reconfiguring_state(
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
    let controller = controller();

    controller.reconcile(&mut ctx).await;

    // The only phase-1 write marks the record timed out: no cut-over (the
    // realized config fields are untouched).
    let state_writes: Vec<_> = ctx
        .applied
        .iter()
        .flatten()
        .filter_map(|d| match d {
            Decision::UpdateClusterState { write, .. } => Some(write),
            _ => None,
        })
        .collect();
    assert_eq!(
        state_writes.len(),
        1,
        "exactly one phase-1 write: mark the record timed out"
    );
    assert!(
        state_writes[0].new_size.is_none(),
        "rollback-at-timeout must not cut over"
    );
    assert_eq!(
        written_reconfiguration_status(state_writes[0]),
        Some(ReconfigurationStatus::TimedOut),
        "the record is marked timed out"
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
    // Reverted to the pre-reconfiguration set. The record is terminal, so the
    // strategy is disengaged and the baseline alone shapes the cluster.
    assert_eq!(ctx.states[&c].size, "100cc", "realized config unchanged");
    assert_eq!(ctx.states[&c].replicas.len(), 1);
    assert_eq!(ctx.states[&c].replicas[0].shape.size, "100cc");
    assert_eq!(
        reconfiguration_status(&ctx.states[&c]),
        Some(ReconfigurationStatus::TimedOut),
        "the record is marked timed out under rollback"
    );

    // A second tick is a no-op: no in-progress record means no strategy
    // engagement, the realized set already matches, and there is nothing left to
    // drop.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(
        ctx.applied.len(),
        before,
        "rolled back: converged to a no-op"
    );
}

#[mz_ore::test(tokio::test)]
async fn graceful_commit_at_timeout_cuts_over_through_seam() {
    // End-to-end through the seam: the same timed-out, un-hydrated reconfiguration
    // but under `Commit`. The tick must cut the realized config over to the
    // un-hydrated 200cc target (phase 1) and then drop the now-undesired 100cc
    // replica (phase 2), marking the record finalized.
    let c = cluster(1);
    let (state, _signals) = reconfiguring_state(
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
    let controller = controller();

    controller.reconcile(&mut ctx).await;

    // Phase 1 cut the realized config over to the target and finalized the record.
    assert_eq!(ctx.states[&c].size, "200cc", "commit-at-timeout cuts over");
    assert_eq!(
        reconfiguration_status(&ctx.states[&c]),
        Some(ReconfigurationStatus::Finalized),
        "commit-at-timeout finalizes the record"
    );
    assert!(
        ctx.applied
            .iter()
            .flatten()
            .any(|d| matches!(d, Decision::UpdateClusterState { .. })),
        "commit-at-timeout writes the cut-over"
    );
    // Phase 2 dropped the old 100cc replica. Only the target shape remains.
    assert!(ctx.creates().is_empty());
    let drops = ctx.drops();
    assert_eq!(drops.len(), 1, "the old 100cc replica is dropped");
    assert_eq!(ctx.states[&c].replicas.len(), 1);
    assert_eq!(
        ctx.states[&c].replicas[0].shape.size, "200cc",
        "only the target replica remains"
    );

    // A second tick is a no-op: the realized set matches the cut-over config and
    // there is no in-progress record left to act on.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "converged after the cut-over");
}

#[mz_ore::test(tokio::test)]
async fn resource_exhaustion_sheds_the_reconfiguration() {
    // A reconfiguration is in flight and the target replica does not exist yet,
    // so phase 2 emits a create. The apply reports resource exhaustion, and the
    // controller must shed the reconfiguration: a follow-up state write under
    // the same expected witness, which marks the record resource-exhausted without
    // touching the realized config or the existing replica.
    let c = cluster(1);
    let (state, _signals) = reconfiguring_state(
        c,
        "100cc",
        1,
        vec![observed(replica(1), "r0", "100cc")],
        // A deadline far past the fake's `now`, so the deadline machinery stays
        // out of the picture and phase 1 writes nothing.
        record("200cc", 1, 9999),
        BTreeSet::new(),
    );
    let expected = state.expected();
    let mut ctx = FakeCtx::new(vec![state]);
    ctx.exhaust_next = 1;
    let controller = controller();

    controller.reconcile(&mut ctx).await;

    // Two applies: the exhausted create batch, then the shed.
    assert_eq!(ctx.applied.len(), 2);
    assert!(
        matches!(ctx.applied[0][0], Decision::CreateReplica { .. }),
        "the exhausted batch was the target create"
    );
    let [shed] = &ctx.applied[1][..] else {
        panic!("the shed is a single decision, got {:?}", ctx.applied[1]);
    };
    let Decision::UpdateClusterState {
        cluster_id,
        expected: shed_expected,
        write,
    } = shed
    else {
        panic!("expected a state update decision, got {shed:?}");
    };
    assert_eq!(*cluster_id, c);
    assert_eq!(
        *shed_expected, expected,
        "the shed reuses the tick's expected witness"
    );

    assert_eq!(
        written_reconfiguration_status(write),
        Some(ReconfigurationStatus::ResourceExhausted),
        "the shed marks the record resource-exhausted"
    );
    assert_eq!(
        written_reconfiguration_audit(write),
        Some(ReconfigurationAudit::ResourceExhausted),
        "and declares the abort"
    );

    // The record is terminal, the realized config and replica set are untouched.
    assert_eq!(
        reconfiguration_status(&ctx.states[&c]),
        Some(ReconfigurationStatus::ResourceExhausted),
        "record marked resource-exhausted"
    );
    assert_eq!(ctx.states[&c].size, "100cc");
    assert_eq!(ctx.states[&c].replicas.len(), 1);

    // The next tick is a no-op: with the record terminal, the baseline alone
    // shapes the cluster and the existing replica satisfies it.
    let before = ctx.applied.len();
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.applied.len(), before, "stable after the shed");
}

#[mz_ore::test(tokio::test)]
async fn resource_exhaustion_without_transient_strategy_sheds_nothing() {
    // A baseline create (replication factor above the actual replica count) hits
    // resource exhaustion. There is no transient strategy to shed, so the
    // controller must not emit any follow-up decision. The cluster stays as it
    // is and the next tick retries the same create.
    let c = cluster(1);
    let states = vec![state(
        c,
        "100cc",
        2,
        vec![observed(replica(1), "r0", "100cc")],
    )];
    let mut ctx = FakeCtx::new(states);
    ctx.exhaust_next = 1;
    let controller = controller();

    controller.reconcile(&mut ctx).await;

    // One apply only: the exhausted create, with no shed after it.
    assert_eq!(ctx.applied.len(), 1);
    assert!(matches!(ctx.applied[0][0], Decision::CreateReplica { .. }));
    assert_eq!(ctx.states[&c].replicas.len(), 1, "nothing was applied");
}

mod hydration_burst {
    use std::time::Duration;

    use mz_compute_types::config::ComputeReplicaLogging;
    use mz_repr::Timestamp;

    use super::{
        ObservedReplica, cluster, config, controller, observed, replica, state,
        written_burst_audit, written_burst_record,
    };
    use crate::ctx::{
        AutoScalingPolicy, AvailabilityZones, BurstAudit, BurstFinishCause, BurstRecord,
        BurstWrite, ClusterState, OnHydrationPolicy, ReplicaShape,
    };
    use crate::strategy::{
        ConfigSignals, HYDRATION_BURST_STRATEGY_NAME, HydrationBurstStrategy, LiveSignals, Strategy,
    };

    /// A MANUAL cluster carrying an `ON HYDRATION` policy at `hydration_size` with
    /// the given linger, plus an optional in-flight burst record. Evaluate against
    /// [`config`], which enables burst with a zero default linger.
    fn burst_state(
        size: &str,
        rf: u32,
        hydration_size: &str,
        linger: Duration,
        replicas: Vec<ObservedReplica>,
        burst: Option<BurstRecord>,
    ) -> (ClusterState, LiveSignals) {
        let mut s = state(cluster(1), size, rf, replicas);
        s.auto_scaling_policy = Some(AutoScalingPolicy {
            on_hydration: Some(OnHydrationPolicy {
                hydration_size: hydration_size.to_string(),
                linger_duration: Some(linger),
            }),
        });
        s.burst = burst;
        // Most burst cases have something to hydrate; tests for the object
        // gate flip this off explicitly.
        let signals = LiveSignals {
            has_hydratable_objects: true,
            ..Default::default()
        };
        (s, signals)
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
        let (s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let write = HydrationBurstStrategy.update_state(&s, &signals, &config(), now(1000));
        let burst = written_burst_record(&write).expect("a burst record is written");
        assert_eq!(burst.burst_size, "400cc");
        assert_eq!(burst.linger_duration, Duration::from_millis(10));
        assert_eq!(burst.steady_hydrated_at, None);
        assert_eq!(
            written_burst_audit(&write),
            Some(BurstAudit::Started),
            "arming a burst declares a started transition"
        );
    }

    #[mz_ore::test]
    fn burst_uses_default_linger_when_omitted() {
        // The policy omits LINGER DURATION, so the record takes the env default.
        let (mut s, signals) = burst_state(
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
        let config = ConfigSignals {
            default_burst_linger: Duration::from_secs(42),
            ..config()
        };
        let write = HydrationBurstStrategy.update_state(&s, &signals, &config, now(1000));
        let burst = written_burst_record(&write).expect("written");
        assert_eq!(burst.linger_duration, Duration::from_secs(42));
    }

    #[mz_ore::test]
    fn burst_does_not_arm_when_steady_hydrated() {
        // The steady replica is already hydrated, so no burst is warranted: no
        // record is written.
        let (s, mut signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        signals.hydrated_replicas.insert(replica(1));
        assert!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn burst_does_not_arm_without_objects() {
        // Zero hydratable objects: a burst is never warranted, no matter what
        // the steady set looks like. Neither an un-hydrated steady replica, an
        // absent one, nor a hydrated one arms a burst.
        let base = |replicas| {
            let (s, mut signals) = burst_state(
                "100cc",
                1,
                "400cc",
                Duration::from_millis(10),
                replicas,
                None,
            );
            signals.has_hydratable_objects = false;
            (s, signals)
        };

        // Steady replica present but un-hydrated.
        let (s, signals) = base(vec![observed(replica(1), "r0", "100cc")]);
        assert!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .is_empty()
        );
        // Steady replica absent entirely (a brand-new cluster's first ticks).
        let (s, signals) = base(Vec::new());
        assert!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .is_empty()
        );
        // Steady replica reporting hydrated despite zero user objects (its
        // log dataflows hydrated).
        let (s, mut signals) = base(vec![observed(replica(1), "r0", "100cc")]);
        signals.hydrated_replicas.insert(replica(1));
        assert!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn burst_arms_with_absent_steady_replica() {
        // A cluster On (rf 1) with objects but no steady replica observed at all
        // (still provisioning, or crashed) reads as un-hydrated, so a burst
        // arms. Correct: the objects are not served and a burst replica can pick
        // them up. The kernel cannot distinguish this from a present-but-
        // unreporting replica: both are absence from `hydrated_replicas`, which
        // `burst_arms_when_steady_unhydrated` covers.
        let (s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            Vec::new(),
            None,
        );
        let write = HydrationBurstStrategy.update_state(&s, &signals, &config(), now(1000));
        assert!(
            write.burst.is_some(),
            "burst arms with no steady replica at all"
        );
    }

    #[mz_ore::test]
    fn burst_desires_one_replica_at_hydration_size() {
        // With a record present, the strategy desires exactly one replica at the
        // burst size (with the cluster's AZ pool and logging).
        let (s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(10), None)),
        );
        let desired = HydrationBurstStrategy.desired_replicas(&s, &signals, &config(), now(1000));
        assert_eq!(desired.len(), 1);
        assert_eq!(desired[0].shape.size, "400cc");
    }

    #[mz_ore::test]
    fn burst_no_record_desires_nothing() {
        let (s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        assert!(
            HydrationBurstStrategy
                .desired_replicas(&s, &signals, &config(), now(1000))
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn burst_stamps_then_lingers_then_tears_down() {
        // Record present, steady replica hydrated for the first time: stamp the
        // linger start.
        let (s, mut signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        signals.hydrated_replicas.insert(replica(1));
        let write = HydrationBurstStrategy.update_state(&s, &signals, &config(), now(1000));
        let burst = written_burst_record(&write).expect("stamped");
        assert_eq!(burst.steady_hydrated_at, Some(now(1000)));
        assert_eq!(
            written_burst_audit(&write),
            None,
            "the linger stamp is bookkeeping, not a lifecycle transition"
        );

        // Stamped, linger not yet elapsed (now=1050, stamped=1000, linger=100): hold.
        let mut s = s.clone();
        s.burst = Some(record("400cc", Duration::from_millis(100), Some(1000)));
        assert!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1050))
                .is_empty()
        );

        // Stamped, linger elapsed (now=1101 > 1000+100): tear down.
        let write = HydrationBurstStrategy.update_state(&s, &signals, &config(), now(1101));
        assert_eq!(
            write.burst,
            Some(BurstWrite {
                record: None,
                audit: Some(BurstAudit::Finished {
                    cause: BurstFinishCause::LingerElapsed,
                }),
            }),
            "burst record cleared at teardown, declaring the elapsed linger"
        );
    }

    #[mz_ore::test]
    fn burst_re_arms_when_steady_unhydrates() {
        // Record present, previously stamped, but the steady set is no longer
        // hydrated: reset the stamp so the linger restarts after the next hydration.
        let (s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), Some(1000))),
        );
        // hydrated_replicas is empty: the steady replica went un-hydrated.
        let write = HydrationBurstStrategy.update_state(&s, &signals, &config(), now(1050));
        let burst = written_burst_record(&write).expect("rewritten");
        assert_eq!(burst.steady_hydrated_at, None, "stamp reset on re-arm");
        assert_eq!(
            written_burst_audit(&write),
            None,
            "the stamp reset is bookkeeping, not a lifecycle transition"
        );
    }

    #[mz_ore::test]
    fn burst_tears_down_when_cluster_off() {
        // The cluster was turned off (rf=0). A burst is no longer warranted, so the
        // record is cleared regardless of linger.
        let (mut s, signals) = burst_state(
            "100cc",
            0,
            "400cc",
            Duration::from_millis(100),
            Vec::new(),
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        s.replication_factor = 0;
        assert_eq!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .burst,
            Some(BurstWrite {
                record: None,
                audit: Some(BurstAudit::Finished {
                    cause: BurstFinishCause::NoLongerWarranted,
                }),
            })
        );
    }

    #[mz_ore::test]
    fn burst_tears_down_when_policy_removed() {
        // The policy was removed (the cluster no longer carries ON HYDRATION). The
        // stale record is cleared.
        let (mut s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        s.auto_scaling_policy = None;
        assert_eq!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .burst,
            Some(BurstWrite {
                record: None,
                audit: Some(BurstAudit::Finished {
                    cause: BurstFinishCause::NoLongerWarranted,
                }),
            })
        );
    }

    #[mz_ore::test]
    fn burst_tears_down_on_size_change() {
        // The HYDRATION SIZE changed from the record's size: the stale record is
        // cleared (a fresh one at the new size is written on a later tick).
        let (s, signals) = burst_state(
            "100cc",
            1,
            "800cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(100), None)),
        );
        assert_eq!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .burst,
            Some(BurstWrite {
                record: None,
                audit: Some(BurstAudit::Finished {
                    cause: BurstFinishCause::NoLongerWarranted,
                }),
            })
        );
    }

    #[mz_ore::test]
    fn burst_break_glass_disables_strategy() {
        // The break-glass flag is off: no burst is armed even when steady is
        // un-hydrated, and an existing record is torn down.
        let (mut s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(100),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let config = ConfigSignals {
            burst_enabled: false,
            ..config()
        };
        assert!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config, now(1000))
                .is_empty()
        );

        s.burst = Some(record("400cc", Duration::from_millis(100), None));
        assert_eq!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config, now(1000))
                .burst,
            Some(BurstWrite {
                record: None,
                audit: Some(BurstAudit::Finished {
                    cause: BurstFinishCause::NoLongerWarranted,
                }),
            })
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
        let signals = LiveSignals::default();
        assert!(
            HydrationBurstStrategy
                .update_state(&s, &signals, &config(), now(1000))
                .is_empty()
        );
        assert!(
            HydrationBurstStrategy
                .desired_replicas(&s, &signals, &config(), now(1000))
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn burst_replica_shape_carries_az_and_logging() {
        // The burst replica differs from steady only in size: it carries the
        // cluster's AZ pool and logging.
        let (mut s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            Vec::new(),
            Some(record("400cc", Duration::from_millis(10), None)),
        );
        s.availability_zones = vec!["az1".to_string(), "az2".to_string()];
        let desired = HydrationBurstStrategy.desired_replicas(&s, &signals, &config(), now(1000));
        let expected = ReplicaShape {
            size: "400cc".to_string(),
            availability_zones: AvailabilityZones(s.availability_zones.clone()),
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
        use crate::ctx::Decision;

        // A cluster with a 100cc steady replica that is not hydrated and an
        // ON HYDRATION (400cc) policy. The controller writes a burst record
        // (phase 1) and creates the 400cc burst replica (phase 2).
        let c = cluster(1);
        let (s, _signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let mut ctx = FakeCtx::new(vec![s]);
        ctx.has_hydratable_objects.insert(c, true);
        let controller = controller();
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
    async fn burst_empty_cluster_never_arms_through_seam() {
        use super::FakeCtx;

        // A strategy-carrying cluster with zero hydratable objects never arms,
        // tick after tick, neither while its steady replica has not yet
        // registered (reports un-hydrated) nor once it reports hydrated (its
        // log dataflows). This is the brand-new-cluster case: no burst at
        // creation.
        let c = cluster(1);
        let (s, _signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        // No `has_hydratable_objects` entry on the ctx: the pull reports no
        // objects.
        let mut ctx = FakeCtx::new(vec![s]);
        let controller = controller();

        // Ticks with the steady replica not reporting hydrated (booting).
        controller.reconcile(&mut ctx).await;
        ctx.now = Timestamp::from(2000u64);
        controller.reconcile(&mut ctx).await;
        // Ticks once it reports.
        ctx.hydrated.insert(replica(1));
        ctx.now = Timestamp::from(3000u64);
        controller.reconcile(&mut ctx).await;

        assert!(
            ctx.applied.is_empty(),
            "an object-less cluster produces no decisions at all, got {:?}",
            ctx.applied
        );
        assert!(ctx.states[&c].burst.is_none(), "no burst record ever");
        // NOTE: the cluster IS probed for hydration each tick. The strategy
        // requests hydration and object existence independently (pure over
        // durable state, it cannot condition one request on the other's
        // result), and the object gate then blocks the arm. The probes are
        // in-memory controller state, the accepted cost of pure requests.
        assert_eq!(
            ctx.hydration_probes, 3,
            "one hydration probe per tick while the policy is active"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn burst_arms_when_first_object_lands_through_seam() {
        use super::FakeCtx;
        use crate::ctx::Decision;

        // The dual of the never-arms test: the same cluster arms as soon as its
        // first hydratable object exists while the steady set is un-hydrated.
        let c = cluster(1);
        let (s, _signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let mut ctx = FakeCtx::new(vec![s]);
        let controller = controller();

        controller.reconcile(&mut ctx).await;
        assert!(ctx.applied.is_empty(), "no objects: nothing happens");

        // The first object lands (e.g. CREATE INDEX); the steady replica has
        // not hydrated it. The controller only learns this through its
        // `has_hydratable_objects` pull.
        ctx.has_hydratable_objects.insert(c, true);
        ctx.now = Timestamp::from(2000u64);
        controller.reconcile(&mut ctx).await;

        assert!(
            ctx.states[&c].burst.is_some(),
            "the burst record was written once an object existed"
        );
        let burst_creates: Vec<_> = ctx
            .creates()
            .into_iter()
            .filter(|d| matches!(d, Decision::CreateReplica { shape, .. } if shape.size == "400cc"))
            .collect();
        assert_eq!(burst_creates.len(), 1, "one 400cc burst replica created");
    }

    #[mz_ore::test(tokio::test)]
    async fn burst_policy_alter_rejects_in_flight_decision() {
        use super::FakeCtx;
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
        let (s, _signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let mut ctx = FakeCtx::new(vec![s]);
        ctx.has_hydratable_objects.insert(c, true);
        ctx.witness_check = true;
        // The `ALTER` clears only the policy (size, rf, azs, logging unchanged), so
        // the rejection is attributable solely to the witness `auto_scaling_policy`.
        ctx.concurrent_policy_alter.insert(c, None);

        let controller = controller();
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

        // The dual of the rejection test: with the witness check on but no
        // concurrent policy `ALTER`, the matching `auto_scaling_policy` lets the
        // same burst write apply, so the check is not vacuously rejecting.
        let c = cluster(1);
        let (s, _signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(0),
            vec![observed(replica(1), "r0", "100cc")],
            None,
        );
        let mut ctx = FakeCtx::new(vec![s]);
        ctx.has_hydratable_objects.insert(c, true);
        ctx.witness_check = true;

        let controller = controller();
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
}
