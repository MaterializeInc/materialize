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
//! mutations it derives. It carries primitive signals in and primitive catalog
//! mutations out; it has no per-strategy state or vocabulary. The controller
//! crate knows nothing about the Coordinator. The Coordinator implements this
//! trait, which is what makes the controller testable against a fake
//! implementation and extractable later without touching controller code.
//!
//! The interface is **pull-based**: a tick fetches only the signals it actually
//! examines (no eager all-clusters-all-replicas snapshot is pushed in), and the
//! controller drives what is fetched. Read methods are batched so a separate-task
//! deployment can bound its round-trips to the Coordinator.

use std::collections::BTreeSet;

use async_trait::async_trait;
use mz_compute_types::config::ComputeReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::Timestamp;

// The compare-and-append witness types, and the replica shape they pair with,
// live in `mz-adapter-types` so the catalog transaction that applies a
// decision can share them without depending on this crate. They are part of the
// ctx vocabulary, so re-export them here.
pub use mz_adapter_types::cluster_state::{
    AvailabilityZones, BurstRecord, ExpectedClusterState, OnTimeout, ReconfigurationRecord,
    ReconfigurationTarget, ReplicaShape,
};

/// A replica that actually exists on a cluster, as observed through the ctx.
#[derive(Clone, Debug)]
pub struct ObservedReplica {
    pub replica_id: ReplicaId,
    pub name: String,
    pub shape: ReplicaShape,
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
    /// In-flight graceful reconfiguration, if any.
    pub reconfiguration: Option<ReconfigurationRecord>,
    /// In-flight hydration burst, if any.
    pub burst: Option<BurstRecord>,
    /// The replicas that actually exist on the cluster.
    pub replicas: Vec<ObservedReplica>,
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
    /// `reasons` records which strategies desired it (for audit attribution).
    CreateReplica {
        cluster_id: ClusterId,
        name: String,
        shape: ReplicaShape,
        reasons: Vec<&'static str>,
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
    /// Pulled on demand: a tick asks only about the replicas it examines (the
    /// graceful strategy probes a cluster's replicas only while a
    /// `reconfiguration` is in flight). The baseline-only controller never calls
    /// this; the first hydration-dependent strategy (graceful reconfiguration)
    /// does.
    ///
    /// This is the first of the live-signal reads the seam will grow. The
    /// remaining ones, collection write frontiers and a read timestamp, which
    /// the refresh strategy needs to decide whether a cluster is inside a refresh
    /// window, are deliberately *not* declared here. Their signatures are
    /// dictated by that consumer (e.g. an `Antichain<Timestamp>` frontier type),
    /// and declaring them speculatively would both fix the wrong shape and pull
    /// an otherwise-unused frontier dependency into this pure crate. They land
    /// with their first consumer, backed the same pull-on-demand way as this one.
    async fn hydrated_replicas(
        &mut self,
        cluster_id: ClusterId,
        replicas: &[ReplicaId],
    ) -> BTreeSet<ReplicaId>;

    /// Apply a tick's batch of decisions under their compare-and-append guards.
    /// Each decision carries the [`ExpectedClusterState`] it was derived from;
    /// the implementation re-reads every target cluster and, if any has since
    /// diverged, returns [`ApplyOutcome::Rejected`] without transacting anything.
    /// Otherwise the batch's catalog operations are transacted together.
    async fn apply(&mut self, decisions: Vec<Decision>) -> ApplyOutcome;
}
