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
use mz_repr::{GlobalId, Timestamp};

use crate::ClusterController;
use crate::ctx::{
    ApplyOutcome, AutoScalingPolicy, AvailabilityZones, BurstAudit, ClusterControllerCtx,
    ClusterSchedule, ClusterState, Decision, ObservedReplica, ReconfigurationAudit,
    ReconfigurationStatus, RefreshWindowInputs, ReplicaShape, StateWrite,
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
        arrangement_compression: false,
    }
}

fn observed(replica_id: ReplicaId, name: &str, size: &str) -> ObservedReplica {
    ObservedReplica {
        replica_id,
        name: name.to_string(),
        shape: Some(shape(size)),
        internal: false,
        billed_as: false,
        pending: false,
    }
}

/// A replica the controller does not own (e.g. INTERNAL or BILLED AS).
fn foreign(replica_id: ReplicaId, name: &str, size: &str) -> ObservedReplica {
    ObservedReplica {
        replica_id,
        name: name.to_string(),
        shape: Some(shape(size)),
        internal: true,
        billed_as: false,
        pending: false,
    }
}

/// Builds a MANUAL managed cluster state with the given realized size,
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
        arrangement_compression: false,
        schedule: ClusterSchedule::Manual,
        auto_scaling_policy: None,
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
    /// As `concurrent_policy_alter`, but for the schedule: each entry's
    /// `ClusterSchedule` is written onto the stored state, modeling an
    /// `ALTER ... SET (SCHEDULE = ...)` that lands mid-tick. Exercises the
    /// `schedule` field of the witness.
    concurrent_schedule_alter: BTreeMap<ClusterId, ClusterSchedule>,
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
            exhaust_next: 0,
            witness_check: false,
            concurrent_policy_alter: BTreeMap::new(),
            concurrent_schedule_alter: BTreeMap::new(),
            hydrated: BTreeSet::new(),
            hydration_probes: 0,
            has_hydratable_objects: BTreeMap::new(),
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
        if self.exhaust_next > 0 {
            self.exhaust_next -= 1;
            // Like a rejection, an exhausted apply changes no durable state.
            self.applied.push(decisions);
            return ApplyOutcome::ResourceExhausted;
        }
        // Splice in a concurrent `ALTER` that lands between the controller's read
        // and this append: rewrite the stored config before the witness check
        // runs, so a decision derived from the pre-`ALTER` view fails its guard.
        for (cluster_id, policy) in std::mem::take(&mut self.concurrent_policy_alter) {
            if let Some(state) = self.states.get_mut(&cluster_id) {
                state.auto_scaling_policy = policy;
            }
        }
        for (cluster_id, schedule) in std::mem::take(&mut self.concurrent_schedule_alter) {
            if let Some(state) = self.states.get_mut(&cluster_id) {
                state.schedule = schedule;
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
                    new_arrangement_compression,
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
                if let Some(arrangement_compression) = new_arrangement_compression {
                    state.arrangement_compression = *arrangement_compression;
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
                        shape: Some(shape.clone()),
                        internal: false,
                        billed_as: false,
                        pending: false,
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
async fn create_skips_foreign_replica_names() {
    // A non-owned (INTERNAL) replica already occupies "r1": it is invisible to
    // the desired/actual diff (neither counted nor dropped), but its name is
    // taken, so the baseline's create must skip it rather than collide.
    let c = cluster(1);
    let s = state(c, "100cc", 1, vec![foreign(replica(9), "r1", "100cc")]);
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
        "the created replica should skip the occupied name r1"
    );
    assert!(
        !ctx.applied
            .iter()
            .flatten()
            .any(|d| matches!(d, Decision::DropReplica { .. })),
        "the foreign replica must not be dropped as excess"
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
                audit_detail: None,
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
    if let Decision::CreateReplica {
        shape,
        reasons,
        audit_detail,
        ..
    } = creates[0]
    {
        assert_eq!(shape.size, "200cc");
        assert_eq!(reasons, &vec!["extra"]);
        assert!(!reasons.contains(&BASELINE_STRATEGY_NAME));
        assert!(
            audit_detail.is_none(),
            "only on-refresh creates carry a window decision"
        );
    }
}

#[mz_ore::test]
fn shared_shape_merge_keeps_audit_detail() {
    use crate::ctx::{AuditDetail, RefreshWindowDecision};
    use crate::strategy::ON_REFRESH_STRATEGY_NAME;

    // Two strategies desire the same shape, one slot with a window decision and
    // one without (the order strategies run in puts the detail-less slot
    // first). The merged create keeps the detail, so a shape shared with
    // another contributor still explains its on-refresh side.
    let c = cluster(1);
    let (state, _signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), None);
    let detail = AuditDetail::OnRefresh(RefreshWindowDecision {
        objects_needing_refresh: vec![GlobalId::User(7)],
        objects_needing_compaction: Vec::new(),
        hydration_time_estimate: Duration::ZERO,
    });
    let contributions: Vec<(&'static str, Vec<DesiredReplica>)> = vec![
        (
            "extra",
            vec![DesiredReplica {
                shape: shape("100cc"),
                audit_detail: None,
            }],
        ),
        (
            ON_REFRESH_STRATEGY_NAME,
            vec![DesiredReplica {
                shape: shape("100cc"),
                audit_detail: Some(detail.clone()),
            }],
        ),
    ];
    let decisions = crate::reconcile_replicas(&state, &contributions);
    assert_eq!(decisions.len(), 1);
    if let Decision::CreateReplica {
        reasons,
        audit_detail,
        ..
    } = &decisions[0]
    {
        assert_eq!(reasons, &vec!["extra", ON_REFRESH_STRATEGY_NAME]);
        assert_eq!(audit_detail.as_ref(), Some(&detail));
    } else {
        panic!("expected a CreateReplica");
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
                            arrangement_compression: false,
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
                    audit_detail: None,
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
                arrangement_compression: false,
                schedule: ClusterSchedule::Manual,
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
                arrangement_compression: false,
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
            arrangement_compression: false,
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
        arrangement_compression: false,
        schedule: ClusterSchedule::Manual,
        auto_scaling_policy: None,
        reconfiguration: Some(rec),
        burst: None,
        replicas,
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
        arrangement_compression: false,
        schedule: ClusterSchedule::Manual,
        auto_scaling_policy: None,
        reconfiguration: Some(ReconfigurationRecord {
            target: ReconfigurationTarget {
                size: "100cc".to_string(),
                replication_factor: 1,
                availability_zones: AvailabilityZones(vec!["az2".to_string()]),
                logging: ComputeReplicaLogging::default(),
                arrangement_compression: false,
            },
            deadline: Timestamp::from(5000u64),
            on_timeout: OnTimeout::Rollback,
            status: ReconfigurationStatus::InProgress,
        }),
        burst: None,
        replicas: vec![ObservedReplica {
            replica_id: replica(1),
            name: "r0".to_string(),
            shape: Some(ReplicaShape {
                size: "100cc".to_string(),
                availability_zones: AvailabilityZones(vec!["az1".to_string()]),
                logging: ComputeReplicaLogging::default(),
                arrangement_compression: false,
            }),
            internal: false,
            billed_as: false,
            pending: false,
        }],
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
    assert!(
        !desired[0]
            .shape
            .matches(state.replicas[0].shape.as_ref().unwrap())
    );

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
        .filter(|r| r.shape.as_ref().is_some_and(|s| s.size == "200cc"))
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
            .all(|r| r.shape.as_ref().is_some_and(|s| s.size == "200cc")),
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
    assert_eq!(
        ctx.states[&c].replicas[0].shape.as_ref().unwrap().size,
        "100cc"
    );
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
        ctx.states[&c].replicas[0].shape.as_ref().unwrap().size,
        "200cc",
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

// ----- On-refresh scheduling strategy. -----

use mz_repr::refresh_schedule::RefreshSchedule;
use timely::progress::Antichain;

use crate::ctx::{AuditDetail, ClusterSchedule as Sched, RefreshMvInfo, RefreshWindowDecision};
use crate::strategy::{ON_REFRESH_STRATEGY_NAME, OnRefreshStrategy};

/// Unwrap an `AuditDetail` into the on-refresh window decision behind it.
fn window_decision(detail: &Option<AuditDetail>) -> &RefreshWindowDecision {
    let Some(AuditDetail::OnRefresh(decision)) = detail else {
        panic!("expected an on-refresh window decision");
    };
    decision
}

/// A scheduled (`ON REFRESH`) cluster state with the given realized size,
/// replication factor, replicas, and optional refresh-window inputs.
fn scheduled_state(
    cluster_id: ClusterId,
    size: &str,
    replication_factor: u32,
    hydration_time_estimate_ms: u64,
    replicas: Vec<ObservedReplica>,
    refresh_window: Option<RefreshWindowInputs>,
) -> (ClusterState, LiveSignals) {
    let state = ClusterState {
        cluster_id,
        size: size.to_string(),
        replication_factor,
        availability_zones: Vec::new(),
        logging: ComputeReplicaLogging::default(),
        arrangement_compression: false,
        schedule: Sched::Refresh {
            hydration_time_estimate: Duration::from_millis(hydration_time_estimate_ms),
        },
        auto_scaling_policy: None,
        reconfiguration: None,
        burst: None,
        replicas,
    };
    let signals = LiveSignals {
        refresh_window,
        ..Default::default()
    };
    (state, signals)
}

/// A `REFRESH AT` schedule with a single refresh time.
fn refresh_at(at: u64) -> RefreshSchedule {
    RefreshSchedule {
        everies: Vec::new(),
        ats: vec![Timestamp::from(at)],
    }
}

/// Refresh-window inputs: read ts, compaction estimate, and one MV (id `u1`)
/// with the given write frontier and schedule. `Some(ts)` is a single-element
/// write frontier `[ts]`; `None` is the empty (sealed) frontier `[]`.
fn window_inputs(
    read_ts: u64,
    compaction_ms: u64,
    write_frontier: Option<u64>,
    schedule: RefreshSchedule,
) -> RefreshWindowInputs {
    RefreshWindowInputs {
        read_ts: Timestamp::from(read_ts),
        compaction_estimate: Duration::from_millis(compaction_ms),
        refresh_mvs: vec![refresh_mv(1, write_frontier, schedule)],
    }
}

/// One REFRESH MV with id `u<id>`, the given write frontier, and schedule.
fn refresh_mv(id: u64, write_frontier: Option<u64>, schedule: RefreshSchedule) -> RefreshMvInfo {
    let write_frontier = match write_frontier {
        Some(ts) => Antichain::from_elem(Timestamp::from(ts)),
        None => Antichain::new(),
    };
    RefreshMvInfo {
        id: GlobalId::User(id),
        write_frontier,
        refresh_schedule: schedule,
    }
}

#[mz_ore::test]
fn on_refresh_baseline_holds_nothing_on_scheduled() {
    // Even with a stale non-zero rf, the baseline contributes nothing to a
    // scheduled cluster. The on-refresh strategy is the sole contributor.
    let c = cluster(1);
    let (state, signals) = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    let baseline = crate::strategy::BaselineStrategy;
    assert!(
        baseline
            .desired_replicas(&state, &signals, &config(), Timestamp::from(0u64))
            .is_empty(),
        "baseline must hold nothing on a scheduled cluster"
    );
}

#[mz_ore::test]
fn on_refresh_normalizes_replication_factor() {
    // A scheduled cluster carrying a stale non-zero rf is normalized to 0.
    let c = cluster(1);
    let (state, signals) = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    let s = OnRefreshStrategy;
    let write = s.update_state(&state, &signals, &config(), Timestamp::from(0u64));
    assert_eq!(write.new_replication_factor, Some(0));

    // Already 0 (or MANUAL): no write, so steady ticks stay no-ops.
    let (normalized, normalized_signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), None);
    assert!(
        s.update_state(
            &normalized,
            &normalized_signals,
            &config(),
            Timestamp::from(0u64)
        )
        .is_empty()
    );
    let manual = state_(c, "100cc", 1);
    assert!(
        s.update_state(
            &manual,
            &LiveSignals::default(),
            &config(),
            Timestamp::from(0u64)
        )
        .is_empty()
    );
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
    let (state, signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    let desired = s.desired_replicas(&state, &signals, &config(), Timestamp::from(0u64));
    assert_eq!(desired.len(), 1, "in-window cluster desires one replica");
    assert_eq!(desired[0].shape.size, "100cc");

    // The slot carries the window decision: the refresh-due MV explains the
    // open window, and there is no compaction reason.
    let detail = window_decision(&desired[0].audit_detail);
    assert_eq!(detail.objects_needing_refresh, vec![GlobalId::User(1)]);
    assert!(detail.objects_needing_compaction.is_empty());
    assert_eq!(detail.hydration_time_estimate, Duration::ZERO);
}

#[mz_ore::test]
fn on_refresh_window_decision_lists_due_mvs() {
    // Two MVs: u1's frontier (50) is below the read ts (100), u2's (200) is
    // past it. Only u1 appears in the window decision's refresh list. The
    // lists name exactly the MVs that explain the open window.
    let c = cluster(1);
    let inputs = RefreshWindowInputs {
        read_ts: Timestamp::from(100u64),
        compaction_estimate: Duration::ZERO,
        refresh_mvs: vec![
            refresh_mv(1, Some(50), refresh_at(1000)),
            refresh_mv(2, Some(200), refresh_at(1000)),
        ],
    };
    let (state, signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    let desired = s.desired_replicas(&state, &signals, &config(), Timestamp::from(0u64));
    assert_eq!(desired.len(), 1);
    let detail = window_decision(&desired[0].audit_detail);
    assert_eq!(detail.objects_needing_refresh, vec![GlobalId::User(1)]);
    assert!(detail.objects_needing_compaction.is_empty());
}

#[mz_ore::test]
fn on_refresh_caught_up_at_read_ts_is_off() {
    // Frontier exactly at the read ts (and no hydration lead, no compaction
    // window): the MV is caught up, so the cluster is Off. The needs-refresh check
    // is strict (`frontier < read_ts + estimate`), matching the legacy scheduler.
    let c = cluster(1);
    let inputs = window_inputs(100, 0, Some(100), refresh_at(50));
    let (state, signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert!(
        s.desired_replicas(&state, &signals, &config(), Timestamp::from(0u64))
            .is_empty(),
        "a caught-up MV at the read ts leaves the cluster off"
    );
}

#[mz_ore::test]
fn on_refresh_empty_frontier_needs_no_refresh() {
    // An empty (sealed) write frontier `[]` is the "complete past every timestamp"
    // state: `Antichain::less_than` is `false` for every timestamp, so the MV never
    // reads as needing a refresh on that count, exactly as the legacy refresh
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
    let (state, signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert!(
        s.desired_replicas(&state, &signals, &config(), Timestamp::from(0u64))
            .is_empty(),
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
    let (state, signals) = scheduled_state(c, "100cc", 0, 150, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    assert_eq!(
        s.desired_replicas(&state, &signals, &config(), Timestamp::from(0u64))
            .len(),
        1,
        "the hydration estimate opens the window early"
    );

    // With no estimate the same frontier leaves the cluster Off.
    let (no_estimate, no_estimate_signals) = scheduled_state(
        c,
        "100cc",
        0,
        0,
        Vec::new(),
        Some(window_inputs(100, 0, Some(200), refresh_at(1000))),
    );
    assert!(
        s.desired_replicas(
            &no_estimate,
            &no_estimate_signals,
            &config(),
            Timestamp::from(0u64)
        )
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
    let (state, signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    let desired = s.desired_replicas(&state, &signals, &config(), Timestamp::from(0u64));
    assert_eq!(
        desired.len(),
        1,
        "the compaction window keeps the cluster on"
    );

    // The window decision attributes the open window to compaction, not a
    // pending refresh.
    let detail = window_decision(&desired[0].audit_detail);
    assert!(detail.objects_needing_refresh.is_empty());
    assert_eq!(detail.objects_needing_compaction, vec![GlobalId::User(1)]);

    // A later read ts past the compaction window turns it off: the frontier (500)
    // needs no refresh at read ts 400, and prev_refresh 200 + compaction 100 = 300
    // is not > read ts 400.
    let (past, past_signals) = scheduled_state(
        c,
        "100cc",
        0,
        0,
        Vec::new(),
        Some(window_inputs(400, 100, Some(500), refresh_at(200))),
    );
    assert!(
        s.desired_replicas(&past, &past_signals, &config(), Timestamp::from(0u64))
            .is_empty()
    );
}

#[mz_ore::test(tokio::test)]
async fn on_refresh_creates_in_window_through_seam() {
    // End-to-end through the ctx seam: a scheduled cluster with a stale rf=1 and
    // no replicas, inside its refresh window. Phase 1 normalizes rf to 0; phase 2
    // creates the one in-window replica.
    let c = cluster(1);
    let (state, _signals) = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    let mut ctx = FakeCtx::new(vec![state]);
    ctx.refresh_window
        .insert(c, window_inputs(100, 0, Some(50), refresh_at(1000)));

    let controller = controller();
    controller.reconcile(&mut ctx).await;

    assert_eq!(
        ctx.states[&c].replication_factor, 0,
        "rf normalized to 0 at runtime"
    );
    let creates = ctx.creates();
    assert_eq!(creates.len(), 1, "one in-window replica is created");
    if let Decision::CreateReplica {
        reasons,
        shape,
        audit_detail,
        ..
    } = creates[0]
    {
        assert!(reasons.contains(&ON_REFRESH_STRATEGY_NAME));
        assert_eq!(shape.size, "100cc");
        // The create carries the window decision through the kernel for the
        // audit log's `scheduling_policies` detail.
        let detail = window_decision(audit_detail);
        assert_eq!(detail.objects_needing_refresh, vec![GlobalId::User(1)]);
        assert!(detail.objects_needing_compaction.is_empty());
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
    // window, so the on-refresh strategy emits a phase-2 drop and no phase-1
    // write. The drop carries `expected.schedule = Refresh`.
    let (state, _signals) = scheduled_state(
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

    let controller = controller();
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
    let (state, _signals) = scheduled_state(
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

    let controller = controller();
    controller.reconcile(&mut ctx).await;

    assert_eq!(ctx.drops().len(), 1, "the out-of-window replica is dropped");
    assert!(
        ctx.states[&c].replicas.is_empty(),
        "the drop applied under a matching witness"
    );
}

/// A `REFRESH EVERY` schedule with the given interval in milliseconds, aligned
/// to timestamp 0.
fn refresh_every(interval_ms: u64) -> RefreshSchedule {
    RefreshSchedule {
        everies: vec![mz_repr::refresh_schedule::RefreshEvery {
            interval: Duration::from_millis(interval_ms),
            aligned_to: Timestamp::from(0u64),
        }],
        ats: Vec::new(),
    }
}

#[mz_ore::test]
fn on_refresh_compaction_window_with_refresh_every() {
    // An EVERY schedule exercises the everies arm of the previous-refresh
    // computation. The MV's frontier (500) sits at the next refresh of an
    // `EVERY 100ms` schedule, so it needs no refresh at read ts 450, but
    // rounding the frontier down puts the previous refresh at 400, and
    // 400 + compaction_estimate (100) = 500 > read ts 450 keeps the cluster on.
    let c = cluster(1);
    let inputs = window_inputs(450, 100, Some(500), refresh_every(100));
    let (state, signals) = scheduled_state(c, "100cc", 0, 0, Vec::new(), Some(inputs));
    let s = OnRefreshStrategy;
    let desired = s.desired_replicas(&state, &signals, &config(), Timestamp::from(0u64));
    assert_eq!(
        desired.len(),
        1,
        "the compaction window keeps the cluster on"
    );
    let detail = window_decision(&desired[0].audit_detail);
    assert!(detail.objects_needing_refresh.is_empty());
    assert_eq!(detail.objects_needing_compaction, vec![GlobalId::User(1)]);

    // A smaller estimate closes the window: 400 + 10 = 410 is not > 450.
    let (closed, closed_signals) = scheduled_state(
        c,
        "100cc",
        0,
        0,
        Vec::new(),
        Some(window_inputs(450, 10, Some(500), refresh_every(100))),
    );
    assert!(
        s.desired_replicas(&closed, &closed_signals, &config(), Timestamp::from(0u64))
            .is_empty()
    );

    // An EVERY MV with an empty (sealed) frontier has no wall-clock handle on
    // its last refresh (`last_refresh` is `None` for a periodic schedule), so no
    // compaction time is scheduled even under a huge estimate.
    let (sealed, sealed_signals) = scheduled_state(
        c,
        "100cc",
        0,
        0,
        Vec::new(),
        Some(window_inputs(450, 100_000, None, refresh_every(100))),
    );
    assert!(
        s.desired_replicas(&sealed, &sealed_signals, &config(), Timestamp::from(0u64))
            .is_empty()
    );
}

#[mz_ore::test]
fn on_refresh_skips_rf_normalization_while_reconfiguring() {
    // While a reconfiguration record is in progress the graceful strategy owns
    // `new_replication_factor`, so the on-refresh normalization defers to keep
    // the field single-writer. (The sequencer never writes a record for a
    // scheduled cluster, so the state is reachable only for a record written
    // before the cluster acquired its schedule.)
    let c = cluster(1);
    let (mut state, signals) = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    state.reconfiguration = Some(record("200cc", 2, 5000));
    let s = OnRefreshStrategy;
    assert!(
        s.update_state(&state, &signals, &config(), Timestamp::from(0u64))
            .is_empty(),
        "normalization defers to an in-progress reconfiguration"
    );

    // A settled record no longer owns the field: normalization resumes.
    let mut settled = record("200cc", 2, 5000);
    settled.status = ReconfigurationStatus::Finalized;
    state.reconfiguration = Some(settled);
    let write = s.update_state(&state, &signals, &config(), Timestamp::from(0u64));
    assert_eq!(write.new_replication_factor, Some(0));
}

#[mz_ore::test(tokio::test)]
async fn on_refresh_graceful_record_settles_then_normalizes() {
    // A scheduled cluster carrying an in-progress reconfiguration record. The
    // sequencer refuses both a schedule change mid-record and a new record on a
    // scheduled cluster, so this state only arises for a record written before
    // the cluster acquired its schedule (pre-upgrade catalog state). The
    // graceful strategy owns the record to settlement: its cut-over writes the
    // target (including rf 2) alone, without contending with the on-refresh
    // normalization (a dual write of `new_replication_factor` would trip the
    // merge tripwire's soft panic and fail this test). The next tick sees the
    // record settled and normalizes rf back to 0.
    let c = cluster(1);
    let (mut state, _signals) = scheduled_state(c, "100cc", 1, 0, Vec::new(), None);
    // The deadline (500) already passed at the fake's now (1000) under COMMIT,
    // so the first tick cuts over without waiting for hydration.
    state.reconfiguration = Some(record_on_timeout("200cc", 2, 500, OnTimeout::Commit));
    let mut ctx = FakeCtx::new(vec![state]);

    let controller = controller();
    controller.reconcile(&mut ctx).await;

    // The cut-over landed alone: the realized config advanced to the target and
    // the record settled, with rf briefly at the target's value.
    assert_eq!(ctx.states[&c].size, "200cc");
    assert_eq!(ctx.states[&c].replication_factor, 2);
    assert_eq!(
        reconfiguration_status(&ctx.states[&c]),
        Some(ReconfigurationStatus::Finalized)
    );

    // The next tick normalizes the scheduled cluster's rf back to 0.
    controller.reconcile(&mut ctx).await;
    assert_eq!(ctx.states[&c].replication_factor, 0);
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
        // With a record present, the strategy desires exactly one replica that
        // differs from steady only in size: the burst size, with the cluster's
        // AZ pool and logging.
        let (mut s, signals) = burst_state(
            "100cc",
            1,
            "400cc",
            Duration::from_millis(10),
            vec![observed(replica(1), "r0", "100cc")],
            Some(record("400cc", Duration::from_millis(10), None)),
        );
        s.availability_zones = vec!["az1".to_string(), "az2".to_string()];
        let desired = HydrationBurstStrategy.desired_replicas(&s, &signals, &config(), now(1000));
        assert_eq!(desired.len(), 1);
        let expected = ReplicaShape {
            size: "400cc".to_string(),
            availability_zones: AvailabilityZones(s.availability_zones.clone()),
            logging: ComputeReplicaLogging::default(),
            arrangement_compression: false,
        };
        assert!(desired[0].shape.matches(&expected));
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
            matches!(
                &burst_creates[0],
                Decision::CreateReplica {
                    audit_detail: None,
                    ..
                }
            ),
            "only on-refresh creates carry a window decision"
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
                .any(|r| r.shape.as_ref().is_some_and(|s| s.size == "400cc")),
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
                .any(|r| r.shape.as_ref().is_some_and(|s| s.size == "400cc")),
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
                .any(|r| r.shape.as_ref().is_some_and(|s| s.size == "400cc")),
            "the burst replica was created under a matching witness"
        );
    }
}
