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
//! module is the half of the [`ClusterControllerCtx`] boundary that does: it runs
//! the controller as a **separate task** and implements the ctx by marshaling
//! each pull/apply to the Coordinator over the internal command channel, because
//! the catalog and the live compute/storage signals are reachable only from the
//! coordinator loop. The two whole-tick reads are batched; the per-cluster live
//! signals are pulled on demand, so a tick's round-trips scale with the number of
//! managed clusters that need a live signal, not with a constant.
//!
//! Everything here is gated by [`ENABLE_CLUSTER_CONTROLLER`] (default off). With
//! the gate off the task does not tick, so the legacy scheduling and graceful
//! paths remain the sole writers of the replica set. With the gate on the
//! controller owns the *user* managed-cluster replica set; the legacy entry
//! points no-op. (System/builtin clusters are never controller-owned. The
//! catalog's bootstrap migration owns their replicas.)

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use mz_adapter_types::dyncfgs::{CLUSTER_CONTROLLER_TICK_INTERVAL, ENABLE_CLUSTER_CONTROLLER};
use mz_catalog::memory::objects::{ClusterConfig, ClusterVariant};
use mz_cluster_controller::ClusterController;
use mz_cluster_controller::ctx::{
    ApplyOutcome, AvailabilityZones, ClusterControllerCtx, ClusterState, Decision,
    ExpectedClusterState, ObservedReplica, OnTimeout, ReconfigurationRecord, ReconfigurationStatus,
    ReconfigurationTarget, ReplicaShape, StateWrite,
};
use mz_cluster_controller::strategy::{
    GRACEFUL_RECONFIGURATION_STRATEGY_NAME, HYDRATION_BURST_STRATEGY_NAME,
};
use mz_compute_types::config::ComputeReplicaConfig;
use mz_controller::clusters::ClusterStatus;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::task::spawn;
use mz_repr::Timestamp;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::catalog::{DropObjectInfo, Op, ReplicaCreateDropReason};
use crate::coord::{ClusterReplicaStatuses, Coordinator, Message};
use crate::error::AdapterError;

/// A request the controller task marshals to the Coordinator to satisfy one
/// [`ClusterControllerCtx`] call. Each variant carries a oneshot for the reply.
///
/// `ManagedClusterIds` and `ClusterStates` are the per-tick batched reads. The
/// `ClusterStates` reply also carries `now`. `HydratedReplicas` is a
/// per-cluster live signal a strategy pulls on demand.
#[derive(Debug)]
pub enum ClusterControllerRequest {
    /// The ids of all *user* managed clusters the controller owns this tick.
    /// System/builtin clusters are excluded. Their replica set is owned by the
    /// catalog's bootstrap migration, not the controller.
    ManagedClusterIds { tx: oneshot::Sender<Vec<ClusterId>> },
    /// A consistent durable view of the given clusters and their replicas, plus
    /// the current time.
    ClusterStates {
        clusters: Vec<ClusterId>,
        tx: oneshot::Sender<(Vec<ClusterState>, Timestamp)>,
    },
    /// Of `replicas` on `cluster`, which are online and have all current
    /// collections hydrated.
    HydratedReplicas {
        cluster_id: ClusterId,
        replicas: Vec<ReplicaId>,
        tx: oneshot::Sender<BTreeSet<ReplicaId>>,
    },
    /// Whether the cluster has any hydratable (dataflow-backed) objects bound to
    /// it.
    HasHydratableObjects {
        cluster_id: ClusterId,
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

struct ReplicaHydrationCheck {
    replica_id: ReplicaId,
    compute_hydrated: oneshot::Receiver<bool>,
}

/// The controller-task side of the boundary: a [`ClusterControllerCtx`] that
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

    async fn hydrated_replicas(
        &mut self,
        cluster_id: ClusterId,
        replicas: &[ReplicaId],
    ) -> BTreeSet<ReplicaId> {
        let replicas = replicas.to_vec();
        self.request(|tx| ClusterControllerRequest::HydratedReplicas {
            cluster_id,
            replicas,
            tx,
        })
        .await
        .unwrap_or_default()
    }

    async fn has_hydratable_objects(&mut self, cluster_id: ClusterId) -> bool {
        self.request(|tx| ClusterControllerRequest::HasHydratableObjects { cluster_id, tx })
            .await
            // A lost reply means shutdown; "no objects" arms nothing, which is
            // the safe answer.
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
    /// The interval is the fallback cadence: `reconcile_now` cuts the
    /// sleep short after a catalog transaction changes durable cluster state.
    /// The notification only wakes the task. The tick still pulls fresh state
    /// through the coordinator loop, and the controller's own applies wake it
    /// again at the cost of one no-op tick.
    pub(crate) fn spawn_cluster_controller_task(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let reconcile_now = Arc::clone(&self.reconcile_now);
        // A shared handle: dyncfg updates land in the same underlying values, so
        // the controller task sees flag flips without any push.
        let dyncfgs = self.catalog().system_config().dyncfgs().clone();

        spawn(|| "cluster_controller", async move {
            let controller = ClusterController::new(dyncfgs);
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
                    _ = reconcile_now.notified() => {}
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
                // makes empty, so no guard is needed here.
                let states: Vec<_> = clusters
                    .into_iter()
                    .filter_map(|id| self.observe_cluster_state(id))
                    .collect();
                let _ = tx.send((states, now));
            }
            ClusterControllerRequest::HydratedReplicas {
                cluster_id,
                replicas,
                tx,
            } => {
                let checks = self.start_hydration_checks(cluster_id, replicas);
                // Start the controller calls on the coordinator loop, then wait
                // for compute's replies off-loop. The compute check can wait on
                // the compute instance task.
                spawn(|| "cluster_controller_hydration_probe", async move {
                    let mut hydrated = BTreeSet::new();
                    for check in checks {
                        if check.compute_hydrated.await.unwrap_or(false) {
                            hydrated.insert(check.replica_id);
                        }
                    }
                    let _ = tx.send(hydrated);
                });
            }
            ClusterControllerRequest::HasHydratableObjects { cluster_id, tx } => {
                let _ = tx.send(self.cluster_has_hydratable_objects(cluster_id));
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
        // The witness fields come from the same projection the compare-and-append
        // check uses, so the state a decision is derived from and the state the
        // apply path checks against cannot drift.
        let expected = crate::catalog::cluster_state::project_expected(managed);

        // All replicas, with the raw traits the controller's ownership test
        // (`ObservedReplica::owned_shape`) classifies on.
        let replicas = cluster
            .replicas()
            .map(|replica| ObservedReplica {
                replica_id: replica.replica_id,
                name: replica.name.clone(),
                shape: replica_shape(&replica.config),
                internal: replica.config.location.internal(),
                billed_as: replica.config.location.billed_as().is_some(),
                pending: replica.config.location.pending(),
            })
            .collect();

        Some(ClusterState {
            cluster_id,
            size: expected.size,
            replication_factor: expected.replication_factor,
            availability_zones: expected.availability_zones.0,
            logging: expected.logging,
            arrangement_compression: expected.arrangement_compression,
            auto_scaling_policy: expected.auto_scaling_policy,
            reconfiguration: expected.reconfiguration,
            burst: expected.burst,
            replicas,
        })
    }

    /// Whether the cluster has any hydratable objects bound to it, backing the
    /// controller's [`ClusterControllerCtx::has_hydratable_objects`] pull (see
    /// the trait method for the approximation contract and why mismatches with
    /// the hydration check are self-healing).
    fn cluster_has_hydratable_objects(&self, cluster_id: ClusterId) -> bool {
        let Some(cluster) = self.catalog().try_get_cluster(cluster_id) else {
            return false;
        };
        cluster
            .bound_objects
            .iter()
            .any(|id| self.catalog().get_entry(id).item().is_hydratable())
    }

    /// Starts per-replica hydration checks for `cluster_id`.
    ///
    /// Returns only checks for replicas whose processes are all online, that
    /// are already storage-hydrated, and that are known to the compute
    /// controller. The compute receiver completes off the coordinator loop.
    fn start_hydration_checks(
        &self,
        cluster_id: ClusterId,
        replicas: Vec<ReplicaId>,
    ) -> Vec<ReplicaHydrationCheck> {
        use mz_catalog::memory::objects::CatalogItem;

        // Materialized views pinned to a replica (via `IN CLUSTER ... REPLICA`)
        // are only ever installed on that replica, so any other replica can
        // never report them hydrated. Collect the cluster's pinned MVs once,
        // then exclude the ones pinned elsewhere from each replica's hydration
        // check. Otherwise a graceful reconfiguration's cut-over to a fresh
        // replica set would wait forever for the new replicas to hydrate an MV
        // bound to a replica being replaced, then roll back at the deadline
        // (leaving the old replica, and the targeted MV, in place). Indexes
        // cannot be replica-pinned, so MVs are the only case.
        let pinned_mvs: Vec<(ReplicaId, mz_repr::GlobalId)> = self
            .catalog()
            .try_get_cluster(cluster_id)
            .into_iter()
            .flat_map(|cluster| cluster.bound_objects.iter())
            .filter_map(|id| match self.catalog().get_entry(id).item() {
                CatalogItem::MaterializedView(mv) => mv
                    .target_replica
                    .map(|target| (target, mv.global_id_writes())),
                _ => None,
            })
            .collect();

        let mut checks = Vec::new();
        for replica_id in replicas {
            // Skip replicas that are not online. We wait for a replica to be
            // online even when it has no objects that need hydration on it
            // (e.g. a single-replica source).
            let status = self
                .cluster_replica_statuses
                .try_get_cluster_replica_statuses(cluster_id, replica_id)
                .map(ClusterReplicaStatuses::cluster_replica_status);
            if !matches!(status, Some(ClusterStatus::Online)) {
                continue;
            }
            let exclude: BTreeSet<mz_repr::GlobalId> = pinned_mvs
                .iter()
                .filter(|(target, _)| *target != replica_id)
                .map(|(_, id)| *id)
                .collect();
            let compute_fut = match self.controller.compute.collections_hydrated_for_replicas(
                cluster_id,
                vec![replica_id],
                exclude.clone(),
            ) {
                Ok(fut) => fut,
                // The replica is not known to the compute controller. Treat it
                // as not hydrated.
                Err(_) => continue,
            };
            let storage_hydrated = match self.controller.storage.collections_hydrated_on_replicas(
                Some(vec![replica_id]),
                &cluster_id,
                &exclude,
            ) {
                Ok(hydrated) => hydrated,
                Err(_) => continue,
            };
            if storage_hydrated {
                checks.push(ReplicaHydrationCheck {
                    replica_id,
                    compute_hydrated: compute_fut,
                });
            }
        }
        checks
    }

    /// Apply one batch of decisions under their compare-and-append guards.
    ///
    /// The kernel calls this once per tick phase: a phase-1 batch is all
    /// `UpdateClusterState`, a phase-2 batch is all create/drop. Either batch may
    /// in principle be mixed; this handles both. The work is staged across four
    /// steps: collect the per-cluster guards, pre-allocate the ids the creates
    /// need, build the mutation ops, then commit ops and guards in one
    /// transaction (see [`Self::commit_with_checks`] for why the guard holds).
    /// Any step that finds the batch incoherent rejects it, and the controller
    /// recomputes next tick.
    async fn apply_cluster_decisions(&mut self, decisions: Vec<Decision>) -> ApplyOutcome {
        let checks = Self::partition_checks(&decisions);

        // Pre-allocate replica ids before the apply transaction (each allocation
        // is its own durable commit, so it cannot happen inside the transaction).
        let Some(replica_ids) = self.allocate_replica_ids_for_creates(&decisions).await else {
            return ApplyOutcome::Rejected;
        };

        let Some(mutations) = self.build_mutation_ops(decisions, replica_ids) else {
            return ApplyOutcome::Rejected;
        };
        if mutations.is_empty() {
            // Nothing to apply, so the checks guard nothing. Skip the transaction
            // rather than commit a check-only batch, which would still cost a
            // durable round-trip.
            return ApplyOutcome::Applied;
        }

        self.commit_with_checks(checks, mutations).await
    }

    /// The compare-and-append guards for a decision batch: one
    /// `(cluster_id, expected)` per distinct cluster, in first-seen order. All
    /// of a cluster's decisions in a tick come from one snapshot, so they share
    /// one `expected` witness and one guard covers them all.
    fn partition_checks(decisions: &[Decision]) -> Vec<(ClusterId, ExpectedClusterState)> {
        let mut checks: Vec<(ClusterId, ExpectedClusterState)> = Vec::new();
        let mut seen_clusters = BTreeSet::new();
        for decision in decisions {
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
            if seen_clusters.insert(cluster_id) {
                checks.push((cluster_id, expected.clone()));
            } else {
                debug_assert!(
                    checks
                        .iter()
                        .any(|(c, e)| *c == cluster_id && e == expected),
                    "decisions for a cluster in one tick must share one expected witness",
                );
            }
        }
        checks
    }

    /// Pre-allocate one replica id per `CreateReplica` decision, in the order the
    /// creates appear (which is the order [`Self::build_mutation_ops`] consumes
    /// them). Returns `None` if any allocation fails, which rejects the batch.
    ///
    /// `Op::CreateClusterReplica` carries a pre-allocated id, so we allocate
    /// out-of-band here, before the apply transaction. Each allocation commits
    /// durably, so we take a fresh write ts per allocation: two commits must not
    /// share a timestamp.
    async fn allocate_replica_ids_for_creates(
        &mut self,
        decisions: &[Decision],
    ) -> Option<Vec<ReplicaId>> {
        let mut replica_ids = Vec::new();
        for decision in decisions {
            let Decision::CreateReplica { cluster_id, .. } = decision else {
                continue;
            };
            let id_ts = self.get_catalog_write_ts().await;
            match self
                .catalog()
                .allocate_replica_ids(*cluster_id, 1, id_ts)
                .await
            {
                Ok(ids) => {
                    replica_ids.push(ids.into_iter().next().expect("allocated one replica id"))
                }
                Err(err) => {
                    warn!(%cluster_id, "cluster controller could not allocate replica id: {err}");
                    return None;
                }
            }
        }
        Some(replica_ids)
    }

    /// Turn a decision batch into the catalog mutation ops to transact, consuming
    /// the `replica_ids` pre-allocated for the creates (one per `CreateReplica`,
    /// in order). Returns `None` if a target cluster has vanished or gone
    /// unmanaged, which makes the batch incoherent and rejects it.
    fn build_mutation_ops(
        &self,
        decisions: Vec<Decision>,
        replica_ids: Vec<ReplicaId>,
    ) -> Option<Vec<Op>> {
        let mut replica_ids = replica_ids.into_iter();
        let mut mutations = Vec::new();
        let mut drops = Vec::new();
        for decision in decisions {
            match decision {
                Decision::UpdateClusterState {
                    cluster_id, write, ..
                } => match self.build_update_cluster_config_op(cluster_id, &write) {
                    Some(op) => mutations.push(op),
                    // The cluster vanished. The batch is no longer coherent.
                    None => return None,
                },
                Decision::CreateReplica {
                    cluster_id,
                    name,
                    shape,
                    reasons,
                    ..
                } => {
                    let replica_id = replica_ids.next().expect("one pre-allocated id per create");
                    let reason = reason_from_strategies(&reasons);
                    match self.build_create_replica_op(cluster_id, replica_id, name, &shape, reason)
                    {
                        Ok(Some(op)) => mutations.push(op),
                        Ok(None) => return None,
                        Err(err) => {
                            warn!(%cluster_id, "cluster controller could not build replica create: {err}");
                            return None;
                        }
                    }
                }
                Decision::DropReplica {
                    cluster_id,
                    replica_id,
                    ..
                } => {
                    // The replica may have vanished since the decisions were
                    // derived (a user DDL landed between the tick's read and
                    // this apply). The in-transaction witness check would
                    // reject such a stale batch, but resource-limit validation
                    // runs before the transaction and panics on a missing
                    // replica, so reject the batch here instead.
                    if self
                        .catalog()
                        .try_get_cluster_replica(cluster_id, replica_id)
                        .is_none()
                    {
                        return None;
                    }
                    drops.push(DropObjectInfo::ClusterReplica((
                        cluster_id,
                        replica_id,
                        ReplicaCreateDropReason::Retired,
                    )));
                }
            }
        }
        if !drops.is_empty() {
            mutations.push(Op::DropObjects(drops));
        }
        Some(mutations)
    }

    /// Prepend the per-cluster compare-and-append `checks` to `mutations` and
    /// transact them together.
    ///
    /// The checks run inside the transaction, before any mutation, so they cannot
    /// be separated from the commit they guard. A cluster whose durable state has
    /// diverged from what the decisions were derived from (e.g. a user `ALTER`
    /// landed mid-tick) aborts the whole batch, so a stale create or drop can
    /// never reshape the replica set against the config the `ALTER` has since
    /// established (in particular, a stale drop cannot retire a replica the
    /// `ALTER` has just made desired). On rejection nothing is applied.
    async fn commit_with_checks(
        &mut self,
        checks: Vec<(ClusterId, ExpectedClusterState)>,
        mutations: Vec<Op>,
    ) -> ApplyOutcome {
        let mut ops: Vec<Op> = checks
            .into_iter()
            .map(|(cluster_id, expected)| Op::CheckClusterState {
                cluster_id,
                expected,
            })
            .collect();
        ops.extend(mutations);

        match self.catalog_transact(None, ops).await {
            Ok(()) => ApplyOutcome::Applied,
            Err(AdapterError::ClusterStateChanged { .. }) => {
                // A concurrent `ALTER` moved a cluster's durable state out from
                // under the decisions. Expected, so the controller recomputes
                // next tick.
                ApplyOutcome::Rejected
            }
            Err(AdapterError::ReadOnly) => {
                // The controller is quiesced while read-only (see
                // `handle_cluster_controller_request`), so this is normally
                // unreachable; if reached it's expected and not actionable, not
                // a failure to surface.
                debug!("cluster controller apply skipped in read-only mode");
                ApplyOutcome::Rejected
            }
            Err(AdapterError::ResourceExhaustion { .. }) => {
                // The batch cannot fit the resource budget. Report the fact and
                // leave the reaction (what, if anything, to shed) to the kernel.
                debug!("cluster controller apply exceeded the resource budget");
                ApplyOutcome::ResourceExhausted
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
        // Exhaustive destructure of the source (no `..`): a field added to
        // `StateWrite` is a compile error here until it's overlaid onto the
        // managed config. We cannot destructure `managed` itself. It carries
        // fields the controller does not model (`workload_class`,
        // `optimizer_feature_overrides`) that this overlay must leave untouched.
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
            managed.size = size.clone();
        }
        if let Some(rf) = new_replication_factor {
            managed.replication_factor = *rf;
        }
        if let Some(azs) = new_availability_zones {
            managed.availability_zones = azs.clone();
        }
        if let Some(logging) = new_logging {
            managed.logging = logging.clone();
        }
        if let Some(arrangement_compression) = new_arrangement_compression {
            managed.arrangement_compression = *arrangement_compression;
        }
        if let Some(reconfiguration) = reconfiguration {
            managed.reconfiguration = reconfiguration.record.as_ref().map(memory_reconfiguration);
        }
        if let Some(burst) = burst {
            managed.burst = burst.record.as_ref().map(memory_burst);
        }
        // The audit intents travel with the write, declared by the strategy at
        // the decision point. We pass them through untouched so the events are
        // emitted in the same catalog transaction as the state they describe.
        let reconfiguration_audit = write.reconfiguration.as_ref().and_then(|w| w.audit);
        let burst_audit = write.burst.as_ref().and_then(|w| w.audit);
        Some(Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name.clone(),
            config,
            reconfiguration_audit,
            burst_audit,
        })
    }

    /// Build an [`Op::CreateClusterReplica`] for a desired replica `shape` on
    /// `cluster_id` with the pre-allocated `replica_id`, attributed to `reason`.
    /// Returns `Ok(None)` if the cluster is gone or unmanaged.
    fn build_create_replica_op(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        name: String,
        shape: &ReplicaShape,
        reason: ReplicaCreateDropReason,
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
        let azs: Option<&[String]> = if shape.availability_zones.0.is_empty() {
            None
        } else {
            Some(&shape.availability_zones.0)
        };
        let location = self.catalog().concretize_replica_location(
            location,
            &self
                .catalog()
                .get_role_allowed_cluster_sizes(&Some(owner_id)),
            azs,
            false,
        )?;

        let config = mz_controller::clusters::ReplicaConfig {
            location,
            compute: ComputeReplicaConfig {
                logging: shape.logging.clone(),
                arrangement_compression: shape.arrangement_compression,
            },
        };

        Ok(Some(Op::CreateClusterReplica {
            cluster_id,
            replica_id,
            name,
            config,
            owner_id,
            reason,
        }))
    }
}

/// Map a create decision's strategy-attribution to the audit reason carried on
/// the create event. Graceful wins over burst when both desired a shape (their
/// shapes differ in practice, so this is a stable tie-break); a baseline-held
/// replica is `Manual`, the tag for replicas the user's own config calls for.
///
/// Drops never come through here: a drop happens exactly when no strategy
/// desires the replica, so it carries no attribution and is uniformly audited
/// [`ReplicaCreateDropReason::Retired`].
fn reason_from_strategies(reasons: &[&'static str]) -> ReplicaCreateDropReason {
    if reasons.contains(&GRACEFUL_RECONFIGURATION_STRATEGY_NAME) {
        ReplicaCreateDropReason::GracefulReconfiguration
    } else if reasons.contains(&HYDRATION_BURST_STRATEGY_NAME) {
        ReplicaCreateDropReason::HydrationBurst
    } else {
        ReplicaCreateDropReason::Manual
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
        availability_zones: AvailabilityZones(managed.availability_zones.clone()),
        logging: config.compute.logging.clone(),
        arrangement_compression: config.compute.arrangement_compression,
    })
}

fn on_timeout_from_controller(action: OnTimeout) -> mz_sql::plan::OnTimeoutAction {
    match action {
        OnTimeout::Commit => mz_sql::plan::OnTimeoutAction::Commit,
        OnTimeout::Rollback => mz_sql::plan::OnTimeoutAction::Rollback,
    }
}

fn memory_reconfiguration(
    record: &ReconfigurationRecord,
) -> mz_catalog::memory::objects::ReconfigurationState {
    // Destructure the source (no `..`): a field added to the controller type is a
    // compile error here until it's carried across. The target is the same.
    let ReconfigurationRecord {
        target,
        deadline,
        on_timeout,
        status,
    } = record;
    let ReconfigurationTarget {
        size,
        replication_factor,
        availability_zones,
        logging,
        arrangement_compression,
    } = target;
    mz_catalog::memory::objects::ReconfigurationState {
        target: mz_catalog::memory::objects::ReconfigurationTarget {
            size: size.clone(),
            replication_factor: *replication_factor,
            availability_zones: availability_zones.0.clone(),
            logging: logging.clone(),
            arrangement_compression: *arrangement_compression,
        },
        deadline: *deadline,
        on_timeout: on_timeout_from_controller(*on_timeout),
        status: status_from_controller(*status),
    }
}

fn status_from_controller(
    status: ReconfigurationStatus,
) -> mz_catalog::memory::objects::ReconfigurationStatus {
    match status {
        ReconfigurationStatus::InProgress => {
            mz_catalog::memory::objects::ReconfigurationStatus::InProgress
        }
        ReconfigurationStatus::Finalized => {
            mz_catalog::memory::objects::ReconfigurationStatus::Finalized
        }
        ReconfigurationStatus::TimedOut => {
            mz_catalog::memory::objects::ReconfigurationStatus::TimedOut
        }
        ReconfigurationStatus::Cancelled => {
            mz_catalog::memory::objects::ReconfigurationStatus::Cancelled
        }
        ReconfigurationStatus::ResourceExhausted => {
            mz_catalog::memory::objects::ReconfigurationStatus::ResourceExhausted
        }
    }
}

fn memory_burst(
    record: &mz_cluster_controller::ctx::BurstRecord,
) -> mz_catalog::memory::objects::BurstState {
    // Destructure the source (no `..`): a field added to the controller type is a
    // compile error here until it's carried across.
    let mz_cluster_controller::ctx::BurstRecord {
        burst_size,
        linger_duration,
        steady_hydrated_at,
    } = record;
    mz_catalog::memory::objects::BurstState {
        burst_size: burst_size.clone(),
        linger_duration: *linger_duration,
        steady_hydrated_at: *steady_hydrated_at,
    }
}

#[cfg(test)]
mod tests {
    use mz_cluster_controller::strategy::BASELINE_STRATEGY_NAME;

    use super::*;

    #[mz_ore::test]
    fn test_reason_from_strategies() {
        use ReplicaCreateDropReason as Reason;

        // Each strategy maps to its own reason; the baseline (or no
        // attribution) is `Manual`.
        assert!(matches!(
            reason_from_strategies(&[BASELINE_STRATEGY_NAME]),
            Reason::Manual
        ));
        assert!(matches!(reason_from_strategies(&[]), Reason::Manual));
        assert!(matches!(
            reason_from_strategies(&[GRACEFUL_RECONFIGURATION_STRATEGY_NAME]),
            Reason::GracefulReconfiguration
        ));
        assert!(matches!(
            reason_from_strategies(&[HYDRATION_BURST_STRATEGY_NAME]),
            Reason::HydrationBurst
        ));

        // Graceful wins the tie-break when several strategies desired the
        // shape, and any strategy attribution beats the baseline's `Manual`.
        assert!(matches!(
            reason_from_strategies(&[
                BASELINE_STRATEGY_NAME,
                HYDRATION_BURST_STRATEGY_NAME,
                GRACEFUL_RECONFIGURATION_STRATEGY_NAME,
            ]),
            Reason::GracefulReconfiguration
        ));
        assert!(matches!(
            reason_from_strategies(&[BASELINE_STRATEGY_NAME, HYDRATION_BURST_STRATEGY_NAME]),
            Reason::HydrationBurst
        ));
    }
}
