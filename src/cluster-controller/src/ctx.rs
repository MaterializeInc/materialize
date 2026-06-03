// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The boundary between the controller and its environment.
//!
//! [`ClusterControllerCtx`] is the single, strategy-agnostic seam through which
//! the controller pulls the signals a tick examines and applies the catalog
//! mutations it derives. The signals in are primitive and carry no per-strategy
//! state; the decisions out are primitive catalog mutations plus per-tick audit
//! attribution. A create carries the names of the strategies that desired it,
//! and the on-refresh window decision when that strategy did; the environment
//! turns these into audit events. The controller crate knows nothing about the
//! Coordinator — the Coordinator implements this trait — which is what makes
//! the controller testable against a fake implementation and extractable later
//! without touching controller code.
//!
//! The interface is **pull-based**: a tick fetches only the signals it actually
//! examines (no eager all-clusters-all-replicas snapshot is pushed in), and the
//! controller drives what is fetched. Read methods are batched so a separate-task
//! deployment can bound its round-trips to the Coordinator.

use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, Timestamp};
use timely::progress::Antichain;

/// The config shape of a replica: the dimensions a reconfiguration changes and
/// that the reconcile kernel matches desired slots against actual replicas by.
///
/// Two replicas with equal shape are interchangeable for the purpose of
/// satisfying a desired slot. `availability_zones` is the provisioned AZ pool
/// (order-insensitive — compared as a set), so an `AVAILABILITY ZONES`
/// divergence is a shape difference.
#[derive(Clone, Debug)]
pub struct ReplicaShape {
    pub size: String,
    pub availability_zones: Vec<String>,
    pub logging: ComputeReplicaLogging,
}

impl ReplicaShape {
    /// Whether two shapes are interchangeable. `availability_zones` is compared
    /// as a set so provisioning order does not matter.
    pub fn matches(&self, other: &ReplicaShape) -> bool {
        self.size == other.size
            && self.logging == other.logging
            && self.availability_zones.iter().collect::<BTreeSet<_>>()
                == other.availability_zones.iter().collect::<BTreeSet<_>>()
    }
}

/// A replica that actually exists on a cluster, as observed through the ctx.
#[derive(Clone, Debug)]
pub struct ObservedReplica {
    pub replica_id: ReplicaId,
    pub name: String,
    pub shape: ReplicaShape,
}

/// An in-flight graceful reconfiguration record, mirrored from durable state.
///
/// Opaque to the kernel; the graceful strategy interprets it. Present here so a
/// tick can read it as part of the cluster state and so the compare-and-append
/// guard can carry it as the `expected` value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationRecord {
    pub target: ReconfigurationTarget,
    pub deadline: Timestamp,
    /// What to do once the deadline passes with the target not yet hydrated.
    pub on_timeout: OnTimeout,
}

/// The action a graceful reconfiguration applies once its `deadline` passes with
/// the target replicas not yet hydrated. Success always takes precedence: a
/// hydrated target cuts over regardless of this. The controller's own mirror of
/// the durable `mz_sql::plan::OnTimeoutAction`, kept here so the pure crate need
/// not depend on the SQL layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnTimeout {
    /// Cut over to the (not-yet-hydrated) target anyway and clear the record.
    Commit,
    /// Drop the target replica set, reverting to the pre-reconfiguration shape,
    /// and retain the record as a tombstone.
    Rollback,
}

/// The full config shape a reconfiguration is moving the cluster to. Distinct
/// from [`ReplicaShape`] because it additionally carries `replication_factor`
/// (a cluster-level, not replica-level, dimension).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationTarget {
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: Vec<String>,
    pub logging: ComputeReplicaLogging,
}

impl ReconfigurationTarget {
    /// The per-replica shape of the target: everything but `replication_factor`.
    pub fn shape(&self) -> ReplicaShape {
        ReplicaShape {
            size: self.size.clone(),
            availability_zones: self.availability_zones.clone(),
            logging: self.logging.clone(),
        }
    }
}

/// An active hydration-burst record, mirrored from durable state. Opaque to the
/// kernel; the burst strategy interprets it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BurstRecord {
    pub burst_size: String,
    pub linger_duration: Duration,
    pub steady_hydrated_at: Option<Timestamp>,
}

/// The user-configured autoscaling policy of a managed cluster, mirrored from
/// durable state. The controller's own mirror of `mz_sql::plan::AutoScalingStrategy`,
/// kept here so the pure crate need not depend on the SQL layer.
///
/// Extensible: future strategies are additional optional sub-policies. v1 carries
/// only the `ON HYDRATION` burst policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoScalingPolicy {
    pub on_hydration: Option<OnHydrationPolicy>,
}

/// The `ON HYDRATION` burst sub-policy: while a steady replica is not yet hydrated,
/// run one extra replica at `hydration_size` to accelerate hydration, lingering for
/// `linger_duration` after the steady set hydrates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnHydrationPolicy {
    pub hydration_size: String,
    /// `None` falls back to the system default linger when the burst strategy
    /// writes its record.
    pub linger_duration: Option<Duration>,
}

/// A managed cluster's scheduling policy, mirrored from durable state. The
/// controller's own mirror of `mz_sql::plan::ClusterSchedule`, kept here so the
/// pure crate need not depend on the SQL layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterSchedule {
    /// The cluster is user-managed: the implicit baseline owns the replica set
    /// and `replication_factor` is the user's capacity knob.
    Manual,
    /// The cluster is scheduled `ON REFRESH`: the controller owns the replica
    /// set, `replication_factor` is held at `0`, and the on-refresh strategy is
    /// the sole contributor of replicas. `hydration_time_estimate` is how far
    /// ahead of a refresh the cluster should turn on so it can rehydrate before
    /// the refresh time.
    Refresh { hydration_time_estimate: Duration },
}

/// One REFRESH materialized view bound to a scheduled cluster, as the on-refresh
/// strategy needs to see it.
///
/// `write_frontier` is the MV's storage write frontier, carried with full
/// fidelity as the `Antichain` the storage controller reports. The strategy
/// compares it against the read timestamp (`less_than`) to decide whether the MV
/// still needs a refresh. For the compaction window it reads the frontier's lone
/// element via `as_option` to find the previous refresh time, falling back to the
/// schedule's last refresh on the empty/sealed frontier `[]`, mirroring the
/// legacy refresh policy. The frontier of a single-input total-order MV holds at
/// most one element.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshMvInfo {
    /// The MV's writes-`GlobalId`: the identity the window decision records in
    /// [`RefreshWindowDecision`] so the audit log can say which MVs kept the
    /// cluster on.
    pub id: GlobalId,
    pub write_frontier: Antichain<Timestamp>,
    pub refresh_schedule: RefreshSchedule,
}

/// The on-refresh strategy's per-tick window decision: which bound REFRESH MVs
/// keep a scheduled cluster on, and why. The window is open iff either list is
/// non-empty, so an open window always has an explanation.
///
/// Carried as the `audit_detail` on the create decisions the open window
/// produces; the environment converts it to the audit log's
/// `scheduling_policies` detail. Plain ids and durations so the controller crate
/// stays free of audit-log vocabulary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshWindowDecision {
    /// MVs whose write frontier has not yet passed the (hydration-adjusted) read
    /// timestamp: a refresh is due or imminent.
    pub objects_needing_refresh: Vec<GlobalId>,
    /// MVs estimated to still need Persist compaction after their last refresh.
    pub objects_needing_compaction: Vec<GlobalId>,
    /// The cluster's `HYDRATION TIME ESTIMATE` the refresh window was widened by.
    pub hydration_time_estimate: Duration,
}

impl RefreshWindowDecision {
    /// Whether the refresh window is open: some MV still needs a refresh or
    /// compaction time.
    pub fn window_open(&self) -> bool {
        !self.objects_needing_refresh.is_empty() || !self.objects_needing_compaction.is_empty()
    }
}

/// The live signals the on-refresh strategy reads to decide whether a scheduled
/// cluster is inside a refresh window: the current read timestamp, the
/// Persist-compaction time estimate, and the bound REFRESH MVs' frontiers and
/// schedules.
///
/// Pulled on demand only for scheduled clusters; a MANUAL cluster carries `None`
/// and is never probed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshWindowInputs {
    /// The local oracle read timestamp the window decision is taken against.
    pub read_ts: Timestamp,
    /// How long after a refresh an MV is estimated to still need Persist
    /// compaction, which also keeps the cluster on.
    pub compaction_estimate: Duration,
    /// The REFRESH MVs bound to the cluster.
    pub refresh_mvs: Vec<RefreshMvInfo>,
}

/// The durable state of a single managed cluster plus its observed replicas, as
/// pulled through the ctx for one reconcile tick.
///
/// This is the input every strategy reads. Unmanaged clusters are not
/// controller-owned and are not represented here.
#[derive(Clone, Debug)]
pub struct ClusterState {
    pub cluster_id: ClusterId,
    /// The realized config the cluster is currently serving. The implicit
    /// baseline desires `replication_factor` replicas at this shape.
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: Vec<String>,
    pub logging: ComputeReplicaLogging,
    /// The cluster's scheduling policy. Drives whether the implicit baseline owns
    /// the replica set (MANUAL) or the on-refresh strategy does (REFRESH).
    pub schedule: ClusterSchedule,
    /// The user-configured autoscaling policy, if any. Drives the hydration-burst
    /// strategy.
    pub auto_scaling_policy: Option<AutoScalingPolicy>,
    /// In-flight graceful reconfiguration, if any.
    pub reconfiguration: Option<ReconfigurationRecord>,
    /// In-flight hydration burst, if any.
    pub burst: Option<BurstRecord>,
    /// Whether the hydration-burst strategy is enabled environment-wide (the
    /// break-glass flag). A **config signal**, not durable cluster state, so it is
    /// excluded from [`ClusterState::expected`]: the controller derives it fresh
    /// each tick and a flip does not need to reject an in-flight decision.
    pub burst_enabled: bool,
    /// The system-default burst linger duration, written into a new `burst` record
    /// when the policy's `linger_duration` is omitted. A config signal, excluded
    /// from the witness for the same reason as `burst_enabled`.
    pub default_burst_linger: Duration,
    /// The replicas that actually exist on the cluster.
    pub replicas: Vec<ObservedReplica>,
    /// The refresh-window live signals, populated only for scheduled clusters
    /// (pulled on demand via [`ClusterControllerCtx::refresh_window_inputs`]);
    /// `None` for MANUAL clusters, which are never probed.
    pub refresh_window: Option<RefreshWindowInputs>,
    /// The replicas observed this tick to have *all* current collections on the
    /// cluster hydrated. A **live signal**, not durable state, so it is excluded
    /// from [`ClusterState::expected`] (the compare-and-append witness).
    ///
    /// Populated only for clusters where a strategy needs it (pulled on demand);
    /// empty for steady clusters the controller never probes.
    pub hydrated_replicas: BTreeSet<ReplicaId>,
}

impl ClusterState {
    /// The shape the implicit baseline desires: the realized config.
    pub fn realized_shape(&self) -> ReplicaShape {
        ReplicaShape {
            size: self.size.clone(),
            availability_zones: self.availability_zones.clone(),
            logging: self.logging.clone(),
        }
    }

    /// The compare-and-append witness for decisions derived from this state: the
    /// durable fields a concurrent `ALTER` could change out from under a tick.
    pub fn expected(&self) -> ExpectedClusterState {
        ExpectedClusterState {
            size: self.size.clone(),
            replication_factor: self.replication_factor,
            availability_zones: self.availability_zones.clone(),
            logging: self.logging.clone(),
            schedule: self.schedule,
            auto_scaling_policy: self.auto_scaling_policy.clone(),
            reconfiguration: self.reconfiguration.clone(),
            burst: self.burst.clone(),
        }
    }
}

/// The durable cluster state a [`Decision`] was derived from. The apply path
/// re-reads it and rejects the batch if it no longer holds (compare-and-append),
/// so a user `ALTER` that lands mid-tick cannot have a stale controller decision
/// clobber it; the controller recomputes from the new state next tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedClusterState {
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: Vec<String>,
    pub logging: ComputeReplicaLogging,
    /// The scheduling policy. A concurrent `ALTER ... SET (SCHEDULE = ...)` flips
    /// which strategy owns the replica set, so it is part of the witness: a
    /// scheduled→MANUAL change must reject an in-flight on-refresh decision.
    pub schedule: ClusterSchedule,
    /// The autoscaling policy. A concurrent `ALTER ... SET/RESET (AUTO SCALING
    /// STRATEGY ...)` changes whether (and at what size) a burst is warranted, so
    /// it is part of the witness: disabling burst, or changing its `HYDRATION
    /// SIZE`, must reject an in-flight burst decision.
    pub auto_scaling_policy: Option<AutoScalingPolicy>,
    pub reconfiguration: Option<ReconfigurationRecord>,
    pub burst: Option<BurstRecord>,
}

/// A durable state mutation a strategy's `update_state` asks for: cut over the
/// realized config to a target and/or write or clear the reconfiguration/burst
/// records. The reconcile kernel pairs it with the [`ExpectedClusterState`] it
/// was derived from for the compare-and-append guard.
///
/// As of the baseline-only controller no strategy produces these; the type
/// exists so the kernel and apply path are shaped for the strategies that
/// follow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StateWrite {
    /// New realized config to cut over to. `None` leaves it unchanged.
    pub new_size: Option<String>,
    pub new_replication_factor: Option<u32>,
    pub new_availability_zones: Option<Vec<String>>,
    pub new_logging: Option<ComputeReplicaLogging>,
    /// Write (`Some(Some(_))`), clear (`Some(None)`), or leave unchanged
    /// (`None`) the reconfiguration record.
    pub reconfiguration: Option<Option<ReconfigurationRecord>>,
    /// Write, clear, or leave unchanged the burst record, as above.
    pub burst: Option<Option<BurstRecord>>,
}

impl StateWrite {
    /// Whether this write would actually mutate any durable field.
    pub fn is_empty(&self) -> bool {
        self.new_size.is_none()
            && self.new_replication_factor.is_none()
            && self.new_availability_zones.is_none()
            && self.new_logging.is_none()
            && self.reconfiguration.is_none()
            && self.burst.is_none()
    }
}

/// A single command the controller emits for the environment to transact. The
/// apply path interprets these and turns them into catalog operations.
///
/// Every variant carries the [`ExpectedClusterState`] the decision was derived
/// from. The apply path re-reads each target cluster and rejects the whole batch
/// if any state has since diverged (compare-and-append), so a user `ALTER` that
/// lands mid-tick cannot let a stale create or drop reshape the replica set
/// against the new config; the controller recomputes from the new state next
/// tick.
#[derive(Clone, Debug)]
pub enum Decision {
    /// Create a replica of the given shape under a deterministic fresh name.
    /// `reasons` records which strategies desired it (for audit attribution);
    /// `audit_detail` carries the on-refresh window decision when that strategy
    /// desired the shape, for the richer `scheduling_policies` audit detail.
    CreateReplica {
        cluster_id: ClusterId,
        name: String,
        shape: ReplicaShape,
        reasons: Vec<&'static str>,
        audit_detail: Option<RefreshWindowDecision>,
        expected: ExpectedClusterState,
    },
    /// Drop a specific existing replica. A drop happens exactly when no
    /// strategy desires the replica, so it carries no strategy attribution;
    /// the apply path audits every controller drop with the uniform `retired`
    /// reason.
    DropReplica {
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        expected: ExpectedClusterState,
    },
    /// Apply a durable state write under a compare-and-append guard against
    /// `expected`.
    UpdateClusterState {
        cluster_id: ClusterId,
        expected: ExpectedClusterState,
        write: StateWrite,
    },
}

/// The outcome of applying one tick's batch of [`Decision`]s.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Every decision in the batch was transacted.
    Applied,
    /// At least one decision failed its compare-and-append guard. The whole
    /// batch is rejected; the controller recomputes next tick.
    Rejected,
}

/// The strategy-agnostic pull/apply interface between the controller and its
/// environment.
///
/// The controller depends on exactly this trait. Reads are batched and pulled
/// on demand; the single write applies a tick's batch under a compare-and-append
/// guard. Implementations marshal these to wherever the live signals live (for
/// v1, the Coordinator's catalog and compute/storage controllers, reached over a
/// channel from the controller's own task — hence the `Send` bound).
#[async_trait]
pub trait ClusterControllerCtx: Send {
    /// Current wall-clock time, as the controller's strategies should see it.
    fn now(&self) -> Timestamp;

    /// A consistent durable view of the given managed clusters and their
    /// replicas. Clusters that do not exist or are unmanaged are omitted from
    /// the result.
    async fn cluster_states(&mut self, clusters: &[ClusterId]) -> Vec<ClusterState>;

    /// The ids of all managed clusters the controller owns this tick.
    async fn managed_cluster_ids(&mut self) -> Vec<ClusterId>;

    /// Of `replicas` on `cluster`, which have *all* current (non-transient)
    /// collections on the cluster hydrated. The returned set is a subset of
    /// `replicas`.
    ///
    /// Pulled on demand: a tick asks only about the replicas it examines (the
    /// graceful strategy probes a cluster's replicas only while a
    /// `reconfiguration` is in flight). The baseline-only controller never calls
    /// this; the first hydration-dependent strategy (graceful reconfiguration)
    /// does.
    async fn hydrated_replicas(
        &mut self,
        cluster_id: ClusterId,
        replicas: &[ReplicaId],
    ) -> BTreeSet<ReplicaId>;

    /// The refresh-window live signals for one scheduled cluster: the read
    /// timestamp, the compaction estimate, and the bound REFRESH MVs' write
    /// frontiers and schedules. Returns `None` for a cluster that is not
    /// scheduled `ON REFRESH` (the on-refresh strategy never asks about a MANUAL
    /// cluster).
    ///
    /// Pulled on demand the same way as [`Self::hydrated_replicas`]: the
    /// controller probes a cluster only when the on-refresh strategy needs the
    /// signal (i.e. the cluster is scheduled), so a steady MANUAL cluster never
    /// pays for it.
    async fn refresh_window_inputs(&mut self, cluster_id: ClusterId)
    -> Option<RefreshWindowInputs>;

    /// Apply a tick's batch of decisions under their compare-and-append guards.
    /// Each decision carries the [`ExpectedClusterState`] it was derived from;
    /// the implementation re-reads every target cluster and, if any has since
    /// diverged, returns [`ApplyOutcome::Rejected`] without transacting anything.
    /// Otherwise the batch's catalog operations are transacted together.
    async fn apply(&mut self, decisions: Vec<Decision>) -> ApplyOutcome;
}
