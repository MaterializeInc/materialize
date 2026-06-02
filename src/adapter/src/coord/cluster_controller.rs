// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Driver and glue for the [`mz_cluster_controller`] reconciler.
//!
//! The controller crate is pure: it knows nothing about the Coordinator. This
//! module is the half of the [`ClusterControllerCtx`] seam that does: it runs
//! the controller as a **separate task** and implements the ctx by marshaling
//! each pull/apply to the Coordinator over the internal command channel, because
//! the catalog and the live compute/storage signals are reachable only from the
//! coordinator loop. Pulls are batched so the round-trip count per tick is
//! bounded.
//!
//! Everything here is gated by [`ENABLE_CLUSTER_CONTROLLER`] (default off). With
//! the gate off the task does not tick, so the legacy scheduling and graceful
//! paths remain the sole writers of the replica set. With the gate on the
//! controller owns the *user* managed-cluster replica set; the legacy entry
//! points no-op. (System/builtin clusters are never controller-owned — the
//! catalog's bootstrap migration owns their replicas.)

use std::sync::Arc;
use std::time::Duration;

use mz_adapter_types::dyncfgs::{CLUSTER_CONTROLLER_TICK_INTERVAL, ENABLE_CLUSTER_CONTROLLER};
use mz_catalog::memory::objects::{ClusterConfig, ClusterVariant, ClusterVariantManaged};
use mz_cluster_controller::ClusterController;
use mz_cluster_controller::ctx::{
    ApplyOutcome, ClusterControllerCtx, ClusterState, Decision, ExpectedClusterState,
    ObservedReplica, ReconfigurationRecord, ReconfigurationTarget, ReplicaShape, StateWrite,
};
use mz_compute_types::config::ComputeReplicaConfig;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::task::spawn;
use mz_repr::{GlobalId, Timestamp};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::catalog::{DropObjectInfo, Op, ReplicaCreateDropReason};
use crate::coord::{Coordinator, Message};
use crate::error::AdapterError;

/// A request the controller task marshals to the Coordinator to satisfy one
/// [`ClusterControllerCtx`] call. Each variant carries a oneshot for the reply.
///
/// The reads are batched (`now` is folded into `ClusterStates` so a tick's two
/// reads cost two round-trips, not more). The hydration read exists for the
/// strategies that follow; the baseline-only controller never sends it.
#[derive(Debug)]
pub enum ClusterControllerRequest {
    /// The ids of all *user* managed clusters the controller owns this tick.
    /// System/builtin clusters are excluded — their replica set is owned by the
    /// catalog's bootstrap migration, not the controller.
    ManagedClusterIds { tx: oneshot::Sender<Vec<ClusterId>> },
    /// A consistent durable view of the given clusters and their replicas, plus
    /// the current time.
    ClusterStates {
        clusters: Vec<ClusterId>,
        tx: oneshot::Sender<(Vec<ClusterState>, Timestamp)>,
    },
    /// Whether `collections` are hydrated on all of `replicas` of `cluster`.
    CollectionsHydrated {
        cluster_id: ClusterId,
        replicas: Vec<ReplicaId>,
        collections: Vec<GlobalId>,
        tx: oneshot::Sender<bool>,
    },
    /// Apply a tick's batch of decisions under their compare-and-append guards.
    Apply {
        decisions: Vec<Decision>,
        tx: oneshot::Sender<ApplyOutcome>,
    },
    /// The current configured reconcile cadence. Read once per tick so a runtime
    /// change to `cluster_controller_tick_interval` takes effect without a
    /// restart.
    TickInterval { tx: oneshot::Sender<Duration> },
}

/// The controller-task side of the seam: a [`ClusterControllerCtx`] that
/// marshals every call to the Coordinator over `internal_cmd_tx`.
struct CoordCtx {
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Latched `now` from the most recent batched read, returned by
    /// [`ClusterControllerCtx::now`] so a strategy and the kernel see a single
    /// consistent time per phase.
    now: Timestamp,
}

impl CoordCtx {
    /// Send a request and await its reply. Returns `None` if the Coordinator has
    /// gone away (shutdown), which the caller treats as "nothing to do".
    async fn request<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<T>) -> ClusterControllerRequest,
    ) -> Option<T> {
        let (tx, rx) = oneshot::channel();
        if self
            .internal_cmd_tx
            .send(Message::ClusterControllerRequest(make(tx)))
            .is_err()
        {
            return None;
        }
        rx.await.ok()
    }
}

#[async_trait::async_trait]
impl ClusterControllerCtx for CoordCtx {
    fn now(&self) -> Timestamp {
        self.now
    }

    async fn managed_cluster_ids(&mut self) -> Vec<ClusterId> {
        self.request(|tx| ClusterControllerRequest::ManagedClusterIds { tx })
            .await
            .unwrap_or_default()
    }

    async fn cluster_states(&mut self, clusters: &[ClusterId]) -> Vec<ClusterState> {
        let clusters = clusters.to_vec();
        match self
            .request(|tx| ClusterControllerRequest::ClusterStates { clusters, tx })
            .await
        {
            Some((states, now)) => {
                self.now = now;
                states
            }
            None => Vec::new(),
        }
    }

    async fn collections_hydrated_on_replicas(
        &mut self,
        cluster_id: ClusterId,
        replicas: &[ReplicaId],
        collections: &[GlobalId],
    ) -> bool {
        let replicas = replicas.to_vec();
        let collections = collections.to_vec();
        self.request(|tx| ClusterControllerRequest::CollectionsHydrated {
            cluster_id,
            replicas,
            collections,
            tx,
        })
        .await
        .unwrap_or(false)
    }

    async fn apply(&mut self, decisions: Vec<Decision>) -> ApplyOutcome {
        self.request(|tx| ClusterControllerRequest::Apply { decisions, tx })
            .await
            // A lost reply means shutdown; treat as rejected so we make no
            // further claims about the catalog state.
            .unwrap_or(ApplyOutcome::Rejected)
    }
}

impl Coordinator {
    /// Spawn the cluster controller task.
    ///
    /// The task ticks at [`CLUSTER_CONTROLLER_TICK_INTERVAL`] and reconciles when
    /// [`ENABLE_CLUSTER_CONTROLLER`] is on; while the gate is off it ticks but
    /// each tick is an early no-op. Both the gate and the interval are re-read
    /// each tick (the interval via a [`ClusterControllerRequest::TickInterval`]
    /// round-trip), so a runtime change to either takes effect without a restart.
    /// It owns the controller and a [`CoordCtx`] that marshals back to this
    /// Coordinator.
    ///
    /// The interval is the fallback cadence: `cluster_controller_kick` cuts the
    /// sleep short after a catalog transaction changes durable cluster state. A
    /// kick only wakes the task — the tick still pulls fresh state through the
    /// coordinator loop — and the controller's own applies re-kick at the cost
    /// of one no-op tick.
    pub(crate) fn spawn_cluster_controller_task(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let kick = Arc::clone(&self.cluster_controller_kick);

        spawn(|| "cluster_controller", async move {
            let controller = ClusterController::new();
            let mut ctx = CoordCtx {
                internal_cmd_tx,
                now: Timestamp::MIN,
            };

            loop {
                // Re-read the cadence each tick so a runtime change takes effect.
                // A lost reply means the Coordinator is gone; stop ticking.
                let Some(interval) = ctx
                    .request(|tx| ClusterControllerRequest::TickInterval { tx })
                    .await
                else {
                    break;
                };
                tokio::select! {
                    _ = tokio::time::sleep(interval.max(Duration::from_millis(1))) => {}
                    _ = kick.notified() => {}
                }

                if ctx.internal_cmd_tx.is_closed() {
                    // Coordinator gone; stop ticking.
                    break;
                }
                controller.reconcile(&mut ctx).await;
            }
        });
    }

    /// Handle one [`ClusterControllerRequest`] on the coordinator loop.
    ///
    /// The controller is inactive when the gate is off, or while the deployment
    /// is in read-only mode (a 0dt upgrade, where it must not write the catalog).
    /// When inactive, reads report no managed clusters (so the controller finds
    /// nothing to reconcile) and applies are rejected: the task still wakes each
    /// tick and sends one `ManagedClusterIds` request, but that request
    /// early-returns here and no catalog state is read or written, so the legacy
    /// paths remain the sole writers of the replica set. The task keeps ticking,
    /// so the controller reactivates on its own once the deployment promotes out
    /// of read-only mode.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn handle_cluster_controller_request(
        &mut self,
        request: ClusterControllerRequest,
    ) {
        let active = ENABLE_CLUSTER_CONTROLLER.get(self.catalog().system_config().dyncfgs())
            && !self.controller.read_only();

        match request {
            ClusterControllerRequest::ManagedClusterIds { tx } => {
                let ids = if active {
                    self.catalog()
                        .clusters()
                        // Only *user* managed clusters. System/builtin clusters
                        // (mz_system, mz_catalog_server, …) are also managed, but
                        // their replica set is owned by the bootstrap migration
                        // (`add_new_remove_old_builtin_cluster_replicas_migration`),
                        // which holds exactly the `BUILTIN_CLUSTER_REPLICAS`-defined
                        // replicas regardless of the cluster's `replication_factor`.
                        // Letting the controller own them too would make two writers
                        // of one replica set: the baseline would, for example, add a
                        // replica to reach a builtin cluster's `replication_factor`,
                        // which the bootstrap migration then tears down on the next
                        // open. The legacy scheduler likewise only ever acted on user
                        // clusters.
                        .filter(|c| c.is_managed() && c.id.is_user())
                        .map(|c| c.id)
                        .collect()
                } else {
                    Vec::new()
                };
                let _ = tx.send(ids);
            }
            ClusterControllerRequest::ClusterStates { clusters, tx } => {
                let now = Timestamp::from(self.now());
                // Only ever asked about clusters the controller is reconciling
                // this tick, which the inactive `ManagedClusterIds` gate above
                // makes empty — so no guard is needed here.
                let states: Vec<_> = clusters
                    .into_iter()
                    .filter_map(|id| self.observe_cluster_state(id))
                    .collect();
                let _ = tx.send((states, now));
            }
            ClusterControllerRequest::CollectionsHydrated { tx, .. } => {
                // Baseline-only controller never queries hydration; PR 3 backs
                // this with the compute/storage controllers.
                let _ = tx.send(false);
            }
            ClusterControllerRequest::Apply { decisions, tx } => {
                let outcome = if active {
                    self.apply_cluster_decisions(decisions).await
                } else {
                    ApplyOutcome::Rejected
                };
                let _ = tx.send(outcome);
            }
            ClusterControllerRequest::TickInterval { tx } => {
                let interval =
                    CLUSTER_CONTROLLER_TICK_INTERVAL.get(self.catalog().system_config().dyncfgs());
                let _ = tx.send(interval);
            }
        }
    }

    /// Build the controller's view of one managed cluster from the catalog.
    /// Returns `None` for a missing or unmanaged cluster.
    fn observe_cluster_state(&self, cluster_id: ClusterId) -> Option<ClusterState> {
        let cluster = self.catalog().try_get_cluster(cluster_id)?;
        let ClusterVariant::Managed(managed) = &cluster.config.variant else {
            return None;
        };
        let ClusterVariantManaged {
            size,
            availability_zones,
            logging,
            replication_factor,
            optimizer_feature_overrides: _,
            schedule: _,
            auto_scaling_strategy: _,
            reconfiguration,
            burst,
        } = managed;

        let replicas = cluster
            .replicas()
            .filter_map(|replica| {
                // INTERNAL / BILLED AS replicas are manually managed and live
                // outside the controller's replication-factor domain: a user can
                // attach one to any managed cluster, and the legacy scheduler and
                // reconfiguration paths never create or drop them. Exclude them
                // from the observed set so the controller neither counts one
                // toward a desired shape (letting it stand in for a managed
                // replica) nor drops it as excess.
                if replica.config.location.internal()
                    || replica.config.location.billed_as().is_some()
                {
                    return None;
                }
                let shape = replica_shape(&replica.config)?;
                Some(ObservedReplica {
                    replica_id: replica.replica_id,
                    name: replica.name.clone(),
                    shape,
                })
            })
            .collect();

        Some(ClusterState {
            cluster_id,
            size: size.clone(),
            replication_factor: *replication_factor,
            availability_zones: availability_zones.clone(),
            logging: logging.clone(),
            reconfiguration: reconfiguration.as_ref().map(reconfiguration_record),
            burst: burst.as_ref().map(burst_record),
            replicas,
        })
    }

    /// Apply one batch of decisions.
    ///
    /// The kernel calls this once per tick phase: a phase-1 batch is all
    /// `UpdateClusterState`, a phase-2 batch is all create/drop. Either batch may
    /// in principle be mixed; this handles both.
    ///
    /// Every decision — create, drop, or state write — carries the durable state
    /// it was derived from; the **compare-and-append guard** re-reads each target
    /// cluster and rejects the *whole* batch if any state has since diverged
    /// (e.g. a user `ALTER` landed mid-tick), so a stale create or drop can never
    /// reshape the replica set against the config the `ALTER` has since
    /// established (in particular, a stale drop cannot retire a replica the
    /// `ALTER` has just made desired). On rejection nothing is applied and the
    /// controller recomputes next tick. When the guards hold, the batch's ops are
    /// transacted together, so they commit atomically.
    async fn apply_cluster_decisions(&mut self, decisions: Vec<Decision>) -> ApplyOutcome {
        // Phase 0: compare-and-append guard. Every decision carries the durable
        // state it was derived from; if any target cluster has since diverged the
        // whole batch is stale.
        for decision in &decisions {
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
                } => (*cluster_id, expected),
            };
            if !self.cluster_state_matches(cluster_id, expected) {
                return ApplyOutcome::Rejected;
            }
        }

        let mut ops = Vec::new();
        let mut drops = Vec::new();
        for decision in decisions {
            match decision {
                Decision::UpdateClusterState {
                    cluster_id, write, ..
                } => match self.build_update_cluster_config_op(cluster_id, &write) {
                    Some(op) => ops.push(op),
                    None => {
                        // The cluster vanished between the guard and here; the
                        // batch is no longer coherent.
                        return ApplyOutcome::Rejected;
                    }
                },
                Decision::CreateReplica {
                    cluster_id,
                    name,
                    shape,
                    ..
                } => match self.build_create_replica_op(cluster_id, name, &shape) {
                    Ok(Some(op)) => ops.push(op),
                    Ok(None) => return ApplyOutcome::Rejected,
                    Err(err) => {
                        warn!(%cluster_id, "cluster controller could not build replica create: {err}");
                        return ApplyOutcome::Rejected;
                    }
                },
                Decision::DropReplica {
                    cluster_id,
                    replica_id,
                    ..
                } => {
                    drops.push(DropObjectInfo::ClusterReplica((
                        cluster_id,
                        replica_id,
                        ReplicaCreateDropReason::Retired,
                    )));
                }
            }
        }
        if !drops.is_empty() {
            ops.push(Op::DropObjects(drops));
        }

        if ops.is_empty() {
            return ApplyOutcome::Applied;
        }

        match self.catalog_transact(None, ops).await {
            Ok(()) => ApplyOutcome::Applied,
            Err(AdapterError::ReadOnly) => {
                // The controller is quiesced while read-only (see
                // `handle_cluster_controller_request`), so this is normally
                // unreachable; if reached it's expected and not actionable, not
                // a failure to surface.
                debug!("cluster controller apply skipped in read-only mode");
                ApplyOutcome::Rejected
            }
            Err(err) => {
                warn!("cluster controller apply failed: {err}");
                ApplyOutcome::Rejected
            }
        }
    }

    /// Build an [`Op::UpdateClusterConfig`] that applies `write`'s deltas to the
    /// cluster's current in-memory config, or `None` if the cluster is gone or
    /// unmanaged. The write was guard-checked against the same state, so this is
    /// the realized cut-over / record write.
    fn build_update_cluster_config_op(
        &self,
        cluster_id: ClusterId,
        write: &StateWrite,
    ) -> Option<Op> {
        let cluster = self.catalog().try_get_cluster(cluster_id)?;
        let mut config = cluster.config.clone();
        let ClusterConfig {
            variant: ClusterVariant::Managed(managed),
            ..
        } = &mut config
        else {
            return None;
        };
        if let Some(size) = &write.new_size {
            managed.size = size.clone();
        }
        if let Some(rf) = write.new_replication_factor {
            managed.replication_factor = rf;
        }
        if let Some(azs) = &write.new_availability_zones {
            managed.availability_zones = azs.clone();
        }
        if let Some(logging) = &write.new_logging {
            managed.logging = logging.clone();
        }
        if let Some(reconfiguration) = &write.reconfiguration {
            managed.reconfiguration = reconfiguration.as_ref().map(memory_reconfiguration);
        }
        if let Some(burst) = &write.burst {
            managed.burst = burst.as_ref().map(memory_burst);
        }
        Some(Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name.clone(),
            config,
        })
    }

    /// Build an [`Op::CreateClusterReplica`] for a desired replica `shape` on
    /// `cluster_id`. Returns `Ok(None)` if the cluster is gone or unmanaged.
    fn build_create_replica_op(
        &self,
        cluster_id: ClusterId,
        name: String,
        shape: &ReplicaShape,
    ) -> Result<Option<Op>, mz_catalog::memory::error::Error> {
        let Some(cluster) = self.catalog().try_get_cluster(cluster_id) else {
            return Ok(None);
        };
        if !cluster.is_managed() {
            return Ok(None);
        }
        let owner_id = cluster.owner_id;

        let location = mz_catalog::durable::ReplicaLocation::Managed {
            // Concretized from the cluster config below; left empty here.
            availability_zones: Vec::new(),
            billed_as: None,
            internal: false,
            size: shape.size.clone(),
            pending: false,
        };
        let azs: Option<&[String]> = if shape.availability_zones.is_empty() {
            None
        } else {
            Some(&shape.availability_zones)
        };
        let location = self.catalog().concretize_replica_location(
            location,
            &self
                .catalog()
                .get_role_allowed_cluster_sizes(&Some(owner_id)),
            azs,
        )?;

        let config = mz_controller::clusters::ReplicaConfig {
            location,
            compute: ComputeReplicaConfig {
                logging: shape.logging.clone(),
            },
        };

        Ok(Some(Op::CreateClusterReplica {
            cluster_id,
            name,
            config,
            owner_id,
            // See the note on the drop reason above.
            reason: ReplicaCreateDropReason::Manual,
        }))
    }

    /// Whether `cluster_id`'s current durable state still matches `expected`.
    fn cluster_state_matches(
        &self,
        cluster_id: ClusterId,
        expected: &ExpectedClusterState,
    ) -> bool {
        let Some(cluster) = self.catalog().try_get_cluster(cluster_id) else {
            return false;
        };
        let ClusterVariant::Managed(managed) = &cluster.config.variant else {
            return false;
        };
        managed.size == expected.size
            && managed.replication_factor == expected.replication_factor
            && managed.availability_zones == expected.availability_zones
            && managed.logging == expected.logging
            && managed.reconfiguration.as_ref().map(reconfiguration_record)
                == expected.reconfiguration
            && managed.burst.as_ref().map(burst_record) == expected.burst
    }
}

/// Map an in-memory replica config to a [`ReplicaShape`], or `None` for an
/// unmanaged replica (which the controller does not own).
fn replica_shape(config: &mz_controller::clusters::ReplicaConfig) -> Option<ReplicaShape> {
    use mz_controller::clusters::ReplicaLocation;
    let ReplicaLocation::Managed(managed) = &config.location else {
        return None;
    };
    Some(ReplicaShape {
        size: managed.size.clone(),
        availability_zones: managed.availability_zones.clone(),
        logging: config.compute.logging.clone(),
    })
}

fn reconfiguration_record(
    record: &mz_catalog::memory::objects::ReconfigurationState,
) -> ReconfigurationRecord {
    ReconfigurationRecord {
        target: ReconfigurationTarget {
            size: record.target.size.clone(),
            replication_factor: record.target.replication_factor,
            availability_zones: record.target.availability_zones.clone(),
            logging: record.target.logging.clone(),
        },
        deadline: record.deadline,
    }
}

fn burst_record(
    record: &mz_catalog::memory::objects::BurstState,
) -> mz_cluster_controller::ctx::BurstRecord {
    mz_cluster_controller::ctx::BurstRecord {
        burst_size: record.burst_size.clone(),
        linger_duration: record.linger_duration,
        steady_hydrated_at: record.steady_hydrated_at,
    }
}

fn memory_reconfiguration(
    record: &ReconfigurationRecord,
) -> mz_catalog::memory::objects::ReconfigurationState {
    mz_catalog::memory::objects::ReconfigurationState {
        target: mz_catalog::memory::objects::ReconfigurationTarget {
            size: record.target.size.clone(),
            replication_factor: record.target.replication_factor,
            availability_zones: record.target.availability_zones.clone(),
            logging: record.target.logging.clone(),
        },
        deadline: record.deadline,
        // The baseline controller writes no reconfiguration records, so this
        // mapper is never exercised; default to the conservative ROLLBACK.
        on_timeout: mz_sql::plan::OnTimeoutAction::Rollback,
    }
}

fn memory_burst(
    record: &mz_cluster_controller::ctx::BurstRecord,
) -> mz_catalog::memory::objects::BurstState {
    mz_catalog::memory::objects::BurstState {
        burst_size: record.burst_size.clone(),
        linger_duration: record.linger_duration,
        steady_hydrated_at: record.steady_hydrated_at,
    }
}
