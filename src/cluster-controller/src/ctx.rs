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
//! [`ClusterControllerCtx`] is the single, strategy-agnostic boundary through which
//! the controller pulls the signals a tick examines and applies the catalog
//! mutations it derives. The signals in are primitive and carry no per-strategy
//! state; the decisions out are primitive catalog mutations plus per-tick audit
//! attribution. A create carries the names of the strategies that desired it,
//! and the on-refresh window decision when that strategy did; the environment
//! turns these into audit events. The controller crate knows nothing about the
//! Coordinator. The Coordinator implements this trait, which is what makes
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

// The compare-and-append witness types, and the replica shape they pair with,
// live in `mz-adapter-types` so the catalog transaction that applies a
// decision can share them without depending on this crate. They are part of the
// ctx vocabulary, so re-export them here.
pub use mz_adapter_types::cluster_state::{
    AvailabilityZones, BurstRecord, ClusterSchedule, ExpectedClusterState, OnTimeout,
    ReconfigurationRecord, ReconfigurationTarget, ReplicaShape,
};

/// A replica that actually exists on a cluster, as observed through the ctx.
#[derive(Clone, Debug)]
pub struct ObservedReplica {
    pub replica_id: ReplicaId,
    pub name: String,
    pub shape: ReplicaShape,
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
///
/// The `size`, `replication_factor`, `availability_zones`, and `logging` fields
/// together are the realized config the cluster is currently serving. The
/// implicit baseline desires `replication_factor` replicas at that shape.
#[derive(Clone, Debug)]
pub struct ClusterState {
    pub cluster_id: ClusterId,
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: Vec<String>,
    pub logging: ComputeReplicaLogging,
    /// The cluster's scheduling policy. Drives whether the implicit baseline owns
    /// the replica set (MANUAL) or the on-refresh strategy does (REFRESH).
    pub schedule: ClusterSchedule,
    /// In-flight graceful reconfiguration, if any.
    pub reconfiguration: Option<ReconfigurationRecord>,
    /// In-flight hydration burst, if any.
    pub burst: Option<BurstRecord>,
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
            availability_zones: AvailabilityZones(self.availability_zones.clone()),
            logging: self.logging.clone(),
        }
    }

    /// The compare-and-append witness for decisions derived from this state: the
    /// durable fields a concurrent `ALTER` could change out from under a tick.
    pub fn expected(&self) -> ExpectedClusterState {
        ExpectedClusterState {
            size: self.size.clone(),
            replication_factor: self.replication_factor,
            availability_zones: AvailabilityZones(self.availability_zones.clone()),
            logging: self.logging.clone(),
            schedule: self.schedule,
            reconfiguration: self.reconfiguration.clone(),
            burst: self.burst.clone(),
        }
    }
}

/// A durable state mutation a strategy's `update_state` asks for: cut over the
/// realized config to a target and/or write or clear the reconfiguration/burst
/// records. The reconcile kernel pairs it with the [`ExpectedClusterState`] it
/// was derived from for the compare-and-append guard.
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
        // Exhaustive destructure (no `..`): a field added to `StateWrite` is a
        // compile error here until it's accounted for.
        let StateWrite {
            new_size,
            new_replication_factor,
            new_availability_zones,
            new_logging,
            reconfiguration,
            burst,
        } = self;
        new_size.is_none()
            && new_replication_factor.is_none()
            && new_availability_zones.is_none()
            && new_logging.is_none()
            && reconfiguration.is_none()
            && burst.is_none()
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
/// channel from the controller's own task, hence the `Send` bound).
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
    /// Pulled on demand: a tick asks only about the replicas it examines. The
    /// controller probes a cluster's replicas only when a hydration-dependent
    /// strategy needs the signal this tick: the graceful strategy while a
    /// `reconfiguration` is in flight, the hydration-burst strategy while a burst
    /// is warranted or in flight.
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
