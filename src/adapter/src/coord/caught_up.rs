// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for checking whether clusters/collections are caught up during a 0dt
//! deployment.
//!
//! During a zero-downtime upgrade the new `environmentd` boots read-only and
//! reports "ready to promote" once its clusters have caught up with the leader
//! generation. [`Coordinator::maybe_check_caught_up`] runs that check on an
//! interval (see `with_0dt_deployment_caught_up_check_interval`). We call one
//! such run a "tick", and the term is used throughout this module.
//!
//! A point-in-time caught-up check is not enough on its own: a crash- or
//! OOM-looping replica can momentarily look hydrated and caught-up, and cutting
//! over right then drops us straight into a crashing replica. On top of the
//! per-tick caught-up classification we therefore run a stability gate. A
//! cluster must be caught-up now and have all replicas healthy for a
//! configurable period, capped by their leader counterparts' healthy-run ages
//! at the incoming runs' starts. Any disruption
//! (a replica not `Online`, a status flap between ticks, or a replica restart)
//! resets the streak, so a crash-looping replica never accumulates the required
//! stable time. [`ReplicaStabilityState`] holds the per-replica gate state
//! across ticks. Orchestrator health timestamps reconstruct the initial streak
//! after environmentd restarts, without requiring collection hydration reports
//! to have arrived throughout that period.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice as _;
use futures::StreamExt;
use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_CAUGHT_UP_LEADER_HYDRATION_CHECK, ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK,
    ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK, WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG,
    WITH_0DT_CAUGHT_UP_CHECK_CUTOFF, WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD,
};
use mz_catalog::builtin::{
    MZ_CLUSTER_REPLICA_FRONTIERS, MZ_CLUSTER_REPLICA_STATUS_HISTORY, MZ_COMPUTE_HYDRATION_TIMES,
};
use mz_catalog::memory::objects::Cluster;
use mz_compute_client::controller::CollectionReadiness;
use mz_controller::clusters::{ClusterStatus, ProcessId};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::OfflineReason;
use mz_ore::channel::trigger::Trigger;
use mz_ore::now::EpochMillis;
use mz_repr::{GlobalId, Row, Timestamp};
use timely::progress::{Antichain, Timestamp as _};

use crate::coord::{ClusterReplicaStatuses, Coordinator};

/// Context needed to check whether clusters/collections are caught up.
#[derive(Debug)]
pub struct CaughtUpCheckContext {
    /// A trigger that signals that all clusters/collections have been caught
    /// up.
    pub trigger: Trigger,
    /// Collections to exclude from the caught up check.
    ///
    /// When a caught up check is performed as part of a 0dt upgrade, it makes sense to exclude
    /// collections of newly added builtin objects, as these might not hydrate in read-only mode.
    pub exclude_collections: BTreeSet<GlobalId>,
    /// Per-cluster state for the stability gate, retained across checks.
    ///
    /// Only genuinely caught-up clusters have an entry. When recreating an
    /// entry, the orchestrator supplies the beginning of the healthy run.
    pub cluster_stability: BTreeMap<ClusterId, BTreeMap<ReplicaId, ReplicaStabilityState>>,
    /// The catalog's leader generation. Generation numbers need not be consecutive.
    pub leader_generation: u64,
    /// Current leader process health anchors, reconstructed by the service watch.
    /// Missing or offline processes cannot justify a shorter stability period.
    pub leader_health: BTreeMap<ReplicaId, BTreeMap<ProcessId, Option<EpochMillis>>>,
}

/// How a cluster relates to the 0dt caught-up check on a given tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClusterCaughtUpStatus {
    /// Genuinely hydrated and within lag. Subject to the stability gate.
    CaughtUp,
    /// Excluded by the existing checks (no replicas, or hopelessly behind with
    /// only crash/OOM-looping replicas). Does not block readiness and is not
    /// health-gated, so we keep ignoring clusters that are already unhealthy in
    /// the leader environment.
    Ignored,
    /// Not yet caught up. Blocks readiness.
    NotCaughtUp,
}

/// Per-replica state for the stability gate, retained across caught-up checks.
///
/// The gate requires a cluster to be caught-up now and fully healthy for a
/// configurable period before we report it ready. A point-in-time check isn't
/// enough: a crash-looping replica can momentarily look hydrated and healthy, so
/// we'd cut over right into a crash. We therefore track health over time here.
#[derive(Debug, Default, Clone)]
pub struct ReplicaStabilityState {
    /// Beginning of the uninterrupted healthy streak. Seeded from orchestrator
    /// timestamps when available, otherwise measured with environmentd's clock.
    /// This assumes bounded clock skew between environmentd and the orchestrator.
    stable_since: Option<EpochMillis>,
    /// Max replica-process status-change time observed on the previous tick.
    ///
    /// Used to detect status transitions that happened and resolved between two
    /// ticks (a fast flap we'd otherwise miss by only sampling the current
    /// status). This is an orchestrator-supplied timestamp (`process.time`), not
    /// a locally measured one, which is why it's a `DateTime` and not an
    /// `Instant`. We only ever compare these orchestrator times against each
    /// other, so orchestrator/environmentd clock skew doesn't matter.
    last_status_change: Option<DateTime<Utc>>,
    /// Restart count per replica process observed on the previous tick.
    ///
    /// Any difference from this tick resets the streak: an increased count means
    /// a restart, a decreased one means the process was recreated, and an added
    /// or removed key means replica/process churn. Restart counts survive gaps
    /// in the orchestrator watch, so they catch restarts the status stream can
    /// drop. We track them per process rather than as a cluster-wide sum so that
    /// offsetting changes across processes can't cancel out and hide a restart.
    last_restart_counts: Option<BTreeMap<ProcessId, u64>>,
}

/// A point-in-time view of a replica's health, derived from the
/// in-memory mirror of orchestrator-reported replica statuses.
#[derive(Debug, Clone)]
struct ReplicaHealthSnapshot {
    /// True iff every process is `Online`. The gate requires this for every
    /// replica, not just one replica per cluster.
    all_healthy: bool,
    /// Latest healthy-run start across all processes. Unknown if any process
    /// lacks a reconstructible anchor.
    healthy_since: Option<EpochMillis>,
    /// Max status-change time across this replica's processes.
    max_status_change: Option<DateTime<Utc>>,
    /// Restart count per replica process.
    ///
    /// Kept per process rather than summed: restart counts are not monotonic (a
    /// recreated process resets to zero), so a cluster-wide sum could cancel
    /// offsetting changes across processes and hide a restart. Comparing the
    /// whole map between ticks also catches replica/process churn.
    restart_counts: BTreeMap<ProcessId, u64>,
}

impl ReplicaHealthSnapshot {
    /// A replica's healthy run starts when its last process becomes healthy.
    /// Require evidence for the complete process set, not just the first report.
    fn leader_healthy_since(
        &self,
        reports: Option<&BTreeMap<ProcessId, Option<EpochMillis>>>,
    ) -> Option<EpochMillis> {
        let reports = reports?;
        if self.restart_counts.is_empty() || reports.len() != self.restart_counts.len() {
            return None;
        }
        self.restart_counts.keys().try_fold(0u64, |since, process| {
            Some(since.max((*reports.get(process)?)?))
        })
    }
}

/// Why a caught-up replica is being held back by the stability gate on a given
/// tick.
///
/// Only ever set when the cluster is not yet ready, so there is no "stable"
/// variant. Recorded so we can log the cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StabilityBlocker {
    /// Not all replicas are currently `Online`.
    NotHealthy,
    /// A status change happened and resolved between two ticks.
    StatusFlapped,
    /// A replica process restarted between two ticks.
    Restarted,
    /// Currently caught-up and healthy, but the streak hasn't reached the
    /// required period yet.
    WithinPeriod,
}

/// Outcome of folding one health snapshot into a [`ReplicaStabilityState`].
#[derive(Debug, Clone, Copy)]
struct StabilityObservation {
    /// Whether the replica has now been continuously healthy for
    /// at least the required period.
    ready: bool,
    /// How long the current uninterrupted streak has lasted, in milliseconds.
    /// `None` when the replica is not currently in a streak (this tick reset it).
    stable_for_ms: Option<u64>,
    /// Why the replica is being held back, for logging. `None` once it's ready.
    blocked_by: Option<StabilityBlocker>,
}

impl ReplicaStabilityState {
    /// Folds in the latest health snapshot and returns an observation: whether
    /// the replica has now been continuously healthy for at least
    /// `period_ms`, how long the current streak has lasted, and (when not ready)
    /// what is holding it back.
    ///
    /// A replica is "good" on a tick only if all its processes are currently
    /// healthy and nothing changed since the previous tick (no status flap, no
    /// restart). Any disruption resets the streak, so a crash-looping replica can
    /// never accumulate the required stable time.
    fn observe(
        &mut self,
        snapshot: &ReplicaHealthSnapshot,
        now: EpochMillis,
        period_ms: u64,
    ) -> StabilityObservation {
        // Bootstrap installs placeholder Offline statuses before the watch has
        // reported. Wait for a healthy snapshot before establishing the initial
        // baseline, rather than mistaking controller reconstruction for a flap.
        if self.last_restart_counts.is_none() && !snapshot.all_healthy {
            return StabilityObservation {
                ready: false,
                stable_for_ms: None,
                blocked_by: Some(StabilityBlocker::NotHealthy),
            };
        }
        // NOTE: We don't assume orchestrator status events arrive in order or
        // that every process of a cluster reports within the same tick. The
        // snapshot reflects whatever the in-memory mirror holds right now, and
        // the three checks below are deliberately redundant so no single one has
        // to be reliable on its own:
        //
        //   - `all_healthy` is a point-in-time check, independent of ordering.
        //   - a change in the per-process `restart_counts` is the durable signal:
        //     k8s reports restart counts and they survive gaps in the orchestrator
        //     watch, so they catch restarts the status stream drops. We compare
        //     the whole map, never a cluster-wide sum: restart counts are not
        //     monotonic (a recreated process resets to zero), so a sum could
        //     cancel offsetting changes across processes and hide a restart.
        //   - `max_status_change` advancing is a best-effort flap detector. A
        //     cluster-wide max is enough here, unlike the restart counts, because
        //     any status change stamps `process.time` at ~now, so a flap pushes
        //     the max past the previous tick's value. It can still miss a flap if
        //     an out-of-order event reports an older time, which is why the
        //     restart counts are the belt-and-suspenders.
        //
        // Status event times are only compared against each other. Health-run
        // timestamps below are compared with environmentd's wall clock.
        let status_flapped = match (self.last_status_change, snapshot.max_status_change) {
            (Some(prev), Some(cur)) => cur > prev,
            _ => false,
        };
        let restarted = self
            .last_restart_counts
            .as_ref()
            .is_some_and(|prev| prev != &snapshot.restart_counts);

        let good = snapshot.all_healthy && !status_flapped && !restarted;

        self.stable_since = if good {
            let anchor = snapshot.healthy_since.map(|since| since.min(now));
            let since = self.stable_since.unwrap_or_else(|| {
                // Only seed from historical health on the first observation.
                // A locally observed disruption must not be erased by a stale
                // orchestrator anchor on a subsequent tick.
                if self.last_restart_counts.is_none() {
                    anchor.unwrap_or(now)
                } else {
                    now
                }
            });
            Some(since.max(anchor.unwrap_or(since)))
        } else {
            None
        };
        self.last_status_change = snapshot.max_status_change;
        self.last_restart_counts = Some(snapshot.restart_counts.clone());

        let stable_for_ms = self.stable_since.map(|since| now.saturating_sub(since));
        let ready = stable_for_ms.is_some_and(|elapsed| elapsed >= period_ms);

        let blocked_by = if ready {
            None
        } else if !snapshot.all_healthy {
            Some(StabilityBlocker::NotHealthy)
        } else if status_flapped {
            Some(StabilityBlocker::StatusFlapped)
        } else if restarted {
            Some(StabilityBlocker::Restarted)
        } else {
            Some(StabilityBlocker::WithinPeriod)
        };

        StabilityObservation {
            ready,
            stable_for_ms,
            blocked_by,
        }
    }
}

impl Coordinator {
    /// Checks that all clusters/collections are caught up. If so, this will
    /// trigger `self.caught_up_check.trigger`.
    ///
    /// This method is a no-op when the trigger has already been fired.
    pub async fn maybe_check_caught_up(&mut self) {
        fail::fail_point!("0dt_caught_up_check", |_| ());
        if self.caught_up_check.is_none() {
            return;
        }

        let replica_frontier_item_id = self
            .catalog()
            .resolve_builtin_storage_collection(&MZ_CLUSTER_REPLICA_FRONTIERS);
        let replica_frontier_gid = self
            .catalog()
            .get_entry(&replica_frontier_item_id)
            .latest_global_id();

        // `snapshot_latest` requires that the collection consolidates to a
        // set. `mz_cluster_replica_frontiers` is a controller-managed builtin
        // written with ±1 diffs, so it satisfies that invariant.
        //
        // NOTE: these are the leader's frontiers only because we read the leader's shard.
        // `validate_migration_steps` forbids migrating `mz_cluster_replica_frontiers` for this
        // reason, so a declared migration can't reach here. A test forcing replacement across all
        // builtins bypasses that guard, hands us a shard we write ourselves, and the lag check
        // below then compares this deployment against itself.
        let live_frontiers = self
            .controller
            .storage_collections
            .snapshot_latest(replica_frontier_gid)
            .await
            .expect("can't read mz_cluster_replica_frontiers");

        let live_frontiers = live_frontiers
            .into_iter()
            .map(|row| {
                let mut iter = row.into_iter();

                let id: GlobalId = iter
                    .next()
                    .expect("missing object id")
                    .unwrap_str()
                    .parse()
                    .expect("cannot parse id");
                let replica_id = iter
                    .next()
                    .expect("missing replica id")
                    .unwrap_str()
                    .to_string();
                let maybe_upper_ts = iter.next().expect("missing upper_ts");
                // The timestamp has a total order, so there can be at
                // most one entry in the upper frontier, which is this
                // timestamp here. And NULL encodes the empty upper
                // frontier.
                let upper_frontier = if maybe_upper_ts.is_null() {
                    Antichain::new()
                } else {
                    let upper_ts = maybe_upper_ts.unwrap_mz_timestamp();
                    Antichain::from_elem(upper_ts)
                };

                (id, replica_id, upper_frontier)
            })
            .collect_vec();

        let leader_unhydrated = if ENABLE_0DT_CAUGHT_UP_LEADER_HYDRATION_CHECK
            .get(self.catalog().system_config().dyncfgs())
        {
            let item_id = self
                .catalog()
                .resolve_builtin_storage_collection(&MZ_COMPUTE_HYDRATION_TIMES);
            let id = self.catalog().get_entry(&item_id).latest_global_id();
            // Like the frontiers above, this must be the leader's shard. The
            // migration guard forbids replacing this source. Its controller
            // maintains a set, as required by snapshot_latest.
            match self
                .controller
                .storage_collections
                .snapshot_latest(id)
                .await
            {
                Ok(rows) => leader_unhydrated_collections(&live_frontiers, rows),
                Err(error) => {
                    tracing::warn!(%error, "cannot read leader hydration; requiring local hydration");
                    BTreeSet::new()
                }
            }
        } else {
            BTreeSet::new()
        };

        // We care about each collection being hydrated on _some_
        // replica. We don't check that at least one replica has all
        // collections of that cluster hydrated.
        let live_collection_frontiers: BTreeMap<_, _> = live_frontiers
            .into_iter()
            .map(|(oid, _replica_id, upper_ts)| (oid, upper_ts))
            .into_grouping_map()
            .fold(
                Antichain::from_elem(Timestamp::minimum()),
                |mut acc, _key, upper| {
                    acc.join_assign(&upper);
                    acc
                },
            )
            .into_iter()
            .collect();

        tracing::debug!(?live_collection_frontiers, "checking re-hydration status");

        let allowed_lag =
            WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG.get(self.catalog().system_config().dyncfgs());
        let allowed_lag: u64 = allowed_lag
            .as_millis()
            .try_into()
            .expect("must fit into u64");

        let cutoff = WITH_0DT_CAUGHT_UP_CHECK_CUTOFF.get(self.catalog().system_config().dyncfgs());
        let cutoff: u64 = cutoff.as_millis().try_into().expect("must fit into u64");

        let now = self.now();

        // Something might go wrong with querying the status collection, so we
        // have an emergency flag for disabling it.
        let replica_status_check_enabled =
            ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK.get(self.catalog().system_config().dyncfgs());

        // Analyze replica statuses to detect crash-looping or OOM-looping replicas
        let problematic_replicas = if replica_status_check_enabled {
            self.analyze_replica_looping(now).await
        } else {
            BTreeSet::new()
        };

        let stability_check_enabled =
            ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK.get(self.catalog().system_config().dyncfgs());
        let stability_period =
            WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD.get(self.catalog().system_config().dyncfgs());
        // Cap rather than panic on an absurdly large configured duration. A
        // period of u64::MAX milliseconds means "effectively never auto-ready",
        // which is the safe, conservative outcome: we won't cut over on our own,
        // and an operator can still force it via skip-catchup.
        let stability_period_ms = u64::try_from(stability_period.as_millis()).unwrap_or(u64::MAX);

        // We clone the exclude set so we don't hold a borrow of `caught_up_check`
        // across the classification, which lets us update the per-cluster
        // stability state on it (mutably) afterwards.
        let exclude_collections = self
            .caught_up_check
            .as_ref()
            .expect("known to exist")
            .exclude_collections
            .clone();

        let classification = self
            .classify_clusters(
                allowed_lag.into(),
                cutoff.into(),
                now.into(),
                &live_collection_frontiers,
                &leader_unhydrated,
                &exclude_collections,
                &problematic_replicas,
            )
            .await;

        // Read the health snapshots for genuinely caught-up clusters now, while we
        // only hold a shared borrow of `self`. We update the stability state in a
        // separate, mutable pass below.
        let health: BTreeMap<ClusterId, BTreeMap<ReplicaId, ReplicaHealthSnapshot>> =
            classification
                .iter()
                .filter(|(_, status)| **status == ClusterCaughtUpStatus::CaughtUp)
                .map(|(&cluster_id, _)| (cluster_id, self.cluster_health(cluster_id)))
                .collect();

        let ctx = self.caught_up_check.as_mut().expect("known to exist");

        // Reconstruct health from the orchestrator when a cluster becomes
        // caught-up again. Loss of collection readiness is not a replica crash.
        ctx.cluster_stability.retain(|cluster_id, _| {
            classification.get(cluster_id) == Some(&ClusterCaughtUpStatus::CaughtUp)
        });

        let mut all_ready = true;
        for (&cluster_id, status) in &classification {
            match status {
                ClusterCaughtUpStatus::Ignored => {}
                ClusterCaughtUpStatus::NotCaughtUp => {
                    all_ready = false;
                }
                ClusterCaughtUpStatus::CaughtUp => {
                    // Break-glass: when disabled, a caught-up cluster is
                    // immediately ready, with no replica-health requirement,
                    // i.e. the behavior from before this gate existed. We keep it
                    // as a config-level, fleet-wide revert. Operators can already
                    // force a single cutover via skip-catchup/promote, but this
                    // flag restores prior auto-cutover behavior across all
                    // environments without per-deploy manual intervention or a
                    // code release, mirroring
                    // `enable_0dt_caught_up_replica_status_check`.
                    if !stability_check_enabled {
                        continue;
                    }
                    let replicas = health.get(&cluster_id).expect("computed above");
                    if replicas.is_empty() {
                        all_ready = false;
                    }
                    let states = ctx.cluster_stability.entry(cluster_id).or_default();
                    states.retain(|id, _| replicas.contains_key(id));
                    for (&replica_id, snapshot) in replicas {
                        let required_period_ms = replica_stability_period(
                            stability_period_ms,
                            now,
                            snapshot.healthy_since,
                            snapshot.leader_healthy_since(ctx.leader_health.get(&replica_id)),
                        );
                        let observation = states.entry(replica_id).or_default().observe(
                            snapshot,
                            now,
                            required_period_ms,
                        );
                        if observation.ready {
                            continue;
                        }
                        all_ready = false;
                        tracing::info!(
                            %cluster_id,
                            %replica_id,
                            reason = ?observation.blocked_by,
                            all_healthy = snapshot.all_healthy,
                            stable_for_ms = ?observation.stable_for_ms,
                            required_period_ms,
                            max_status_change = ?snapshot.max_status_change,
                            // Summed only for a readable log line. The gate
                            // compares the per-process map, not this total.
                            restart_total = snapshot.restart_counts.values().sum::<u64>(),
                            "cluster is caught up but not yet stable for the required period"
                        );
                    }
                }
            }
        }

        tracing::info!(%all_ready, "checked caught-up status of clusters");

        if all_ready {
            let ctx = self.caught_up_check.take().expect("known to exist");
            ctx.trigger.fire();
        }
    }

    /// Reads the current health of a cluster's replicas from the in-memory
    /// mirror of orchestrator-reported statuses.
    ///
    /// A cluster with no replica status entries (e.g. a freshly created cluster
    /// whose statuses haven't been initialized) is reported as not healthy.
    fn cluster_health(&self, cluster_id: ClusterId) -> BTreeMap<ReplicaId, ReplicaHealthSnapshot> {
        let Some(replicas) = self
            .cluster_replica_statuses
            .try_get_cluster_statuses(cluster_id)
            .filter(|replicas| !replicas.is_empty())
        else {
            // A cluster with no replica statuses is treated as not healthy.
            return BTreeMap::new();
        };

        let mut health = BTreeMap::new();
        for (replica_id, processes) in replicas {
            let all_healthy = !processes.is_empty()
                && ClusterReplicaStatuses::cluster_replica_status(processes)
                    == ClusterStatus::Online;
            let mut healthy_since = Some(0);
            let mut max_status_change = None;
            let mut restart_counts = BTreeMap::new();
            for (process_id, process) in processes {
                healthy_since =
                    healthy_since
                        .zip(process.healthy_since)
                        .and_then(|(since, time)| {
                            u64::try_from(time.timestamp_millis())
                                .ok()
                                .map(|time| since.max(time))
                        });
                max_status_change = max_status_change.max(Some(process.time));
                restart_counts.insert(*process_id, process.restart_count);
            }
            health.insert(
                *replica_id,
                ReplicaHealthSnapshot {
                    all_healthy,
                    healthy_since,
                    max_status_change,
                    restart_counts,
                },
            );
        }
        health
    }

    /// Classifies every cluster for the caught-up check.
    ///
    /// Informally, a cluster is considered caught-up if it is at least as healthy as its
    /// counterpart in the leader environment. To determine that, we use the following rules:
    ///
    ///  (1) A cluster is caught-up if all non-transient, non-excluded collections installed on it
    ///      are either caught-up or ignored.
    ///  (2) A collection is caught-up when it is (a) hydrated, or explicitly unhydrated on
    ///      every hosting leader replica, and (b) its write frontier is within
    ///      `allowed_lag` of the "live" frontier, the collection's frontier reported by the leader
    ///      environment.
    ///  (3) A collection is ignored if its "live" frontier is behind `now` by more than `cutoff`.
    ///      Such a collection is unhealthy in the leader environment, so we don't care about its
    ///      health in the read-only environment either.
    ///  (4) On a cluster that is crash-looping, all collections are ignored.
    ///
    /// A cluster that is caught-up only because it has no replicas, or because it is hopelessly
    /// behind with only crash/OOM-looping replicas (rule 4), is reported as
    /// [`ClusterCaughtUpStatus::Ignored`] rather than [`ClusterCaughtUpStatus::CaughtUp`]. The
    /// caller does not health-gate ignored clusters, so we keep ignoring clusters that are already
    /// unhealthy in the leader environment.
    async fn classify_clusters(
        &self,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        leader_unhydrated: &BTreeSet<GlobalId>,
        exclude_collections: &BTreeSet<GlobalId>,
        problematic_replicas: &BTreeSet<ReplicaId>,
    ) -> BTreeMap<ClusterId, ClusterCaughtUpStatus> {
        let mut result = BTreeMap::new();
        for cluster in self.catalog().clusters() {
            let status = self
                .collections_caught_up(
                    cluster,
                    allowed_lag.clone(),
                    cutoff.clone(),
                    now.clone(),
                    live_frontiers,
                    leader_unhydrated,
                    exclude_collections,
                    problematic_replicas,
                )
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(
                        "unexpected error while checking if cluster {} caught up: {e:#}",
                        cluster.id
                    );
                    ClusterCaughtUpStatus::NotCaughtUp
                });

            if status == ClusterCaughtUpStatus::NotCaughtUp {
                // We log all non-caught-up clusters instead of breaking out early.
                tracing::info!("cluster {} is not caught up", cluster.id);
            }

            result.insert(cluster.id, status);
        }

        result
    }

    /// Classifies the given cluster for the caught-up check.
    ///
    /// See [`Coordinator::classify_clusters`] for details.
    async fn collections_caught_up(
        &self,
        cluster: &Cluster,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        leader_unhydrated: &BTreeSet<GlobalId>,
        exclude_collections: &BTreeSet<GlobalId>,
        problematic_replicas: &BTreeSet<ReplicaId>,
    ) -> Result<ClusterCaughtUpStatus, anyhow::Error> {
        if cluster.replicas().next().is_none() {
            return Ok(ClusterCaughtUpStatus::Ignored);
        }

        // Check if all replicas in this cluster are crash/OOM-looping. As long
        // as there is at least one healthy replica, the cluster is okay-ish.
        let cluster_has_only_problematic_replicas = cluster
            .replicas()
            .all(|replica| problematic_replicas.contains(&replica.replica_id));

        enum CollectionType {
            Storage,
            Compute,
        }

        let mut all_caught_up = true;

        let storage_frontiers = self
            .controller
            .storage
            .active_ingestion_exports(cluster.id)
            .copied()
            .filter(|id| !id.is_transient() && !exclude_collections.contains(id))
            .map(|id| {
                let (_read_frontier, write_frontier) =
                    self.controller.storage.collection_frontiers(id)?;
                Ok::<_, anyhow::Error>((id, write_frontier, CollectionType::Storage))
            });

        let compute_frontiers = self
            .controller
            .compute
            .collection_ids(cluster.id)?
            .filter(|id| !id.is_transient() && !exclude_collections.contains(id))
            .map(|id| {
                let write_frontier = self
                    .controller
                    .compute
                    .collection_frontiers(id, Some(cluster.id))?
                    .write_frontier
                    .to_owned();
                Ok((id, write_frontier, CollectionType::Compute))
            });

        for res in itertools::chain(storage_frontiers, compute_frontiers) {
            let (id, write_frontier, collection_type) = res?;
            let live_write_frontier = match live_frontiers.get(&id) {
                Some(frontier) => frontier,
                None => {
                    // No live frontier to compare against, either because the collection didn't
                    // exist on the leader or because the leader hosts it as something
                    // `mz_cluster_replica_frontiers` doesn't track. A table→MV conversion is the
                    // latter: it keeps the table's `GlobalId`, still a table on the leader, so the
                    // new MV lands here instead of the strong path below.
                    //
                    // Require hydration, not just a write frontier past the minimum. A fresh MV's
                    // sink reaches frontier 1 after one batch, which would otherwise look caught
                    // up mid-hydration and bring back the cut-over spike this gate prevents.
                    let collection_hydrated = match collection_type {
                        CollectionType::Compute => {
                            self.controller
                                .compute
                                .collection_hydrated(cluster.id, id)
                                .await?
                        }
                        CollectionType::Storage => {
                            self.controller.storage.collection_hydrated(id)?
                        }
                    };

                    // Also require the frontier to be within the allowed lag, the bound the
                    // live-frontier path applies, with `now` standing in for the missing live
                    // frontier. Hydration is one-shot: a collection that hydrated and then stalled
                    // would otherwise satisfy this branch forever.
                    //
                    // NOTE: there is deliberately no `cutoff` escape hatch here. A frontier frozen
                    // at the minimum is exactly what this gate must catch, so a collection stuck
                    // here blocks promotion until `with_0dt_deployment_max_wait` elapses.
                    let readiness = CollectionReadiness::classify(
                        collection_hydrated,
                        &write_frontier,
                        Some((&Antichain::from_elem(now), allowed_lag)),
                    );

                    tracing::info!(
                        ?write_frontier,
                        ?readiness,
                        ?allowed_lag,
                        ?now,
                        "collection {id} not in live frontiers"
                    );
                    if write_frontier.less_equal(&Timestamp::minimum())
                        || readiness != CollectionReadiness::Ready
                    {
                        all_caught_up = false;
                    }
                    continue;
                }
            };

            // We can't do comparisons and subtractions, so we bump up the live
            // write frontier by the cutoff, and then compare that against
            // `now`.
            let live_write_frontier_plus_cutoff = live_write_frontier
                .iter()
                .map(|t| t.step_forward_by(&cutoff));
            let live_write_frontier_plus_cutoff =
                Antichain::from_iter(live_write_frontier_plus_cutoff);

            let beyond_all_hope = live_write_frontier_plus_cutoff.less_equal(&now);

            if beyond_all_hope && cluster_has_only_problematic_replicas {
                tracing::info!(
                    ?live_write_frontier,
                    ?cutoff,
                    ?now,
                    "live write frontier of collection {id} is too far behind 'now'"
                );
                tracing::info!(
                    "ALL replicas of cluster {} are crash/OOM-looping and it has at least one \
                     collection that is too far behind 'now'; ignoring cluster for caught-up \
                     checks",
                    cluster.id
                );
                return Ok(ClusterCaughtUpStatus::Ignored);
            } else if beyond_all_hope {
                tracing::info!(
                    ?live_write_frontier,
                    ?cutoff,
                    ?now,
                    "live write frontier of collection {id} is too far behind 'now'; \
                     ignoring for caught-up checks"
                );
                continue;
            }

            // This call is on the expensive side, because we have to do a call
            // across a task/channel boundary, and our work competes with other
            // things the compute/instance controller might be doing. But it's
            // okay because we only do these hydration checks when in read-only
            // mode, and only rarely.
            let collection_hydrated = match collection_type {
                CollectionType::Compute => {
                    self.controller
                        .compute
                        .collection_hydrated(cluster.id, id)
                        .await?
                }
                CollectionType::Storage => self.controller.storage.collection_hydrated(id)?,
            };

            let readiness = CollectionReadiness::classify(
                collection_hydrated
                    || (matches!(collection_type, CollectionType::Compute)
                        && leader_unhydrated.contains(&id)),
                &write_frontier,
                Some((live_write_frontier, allowed_lag)),
            );

            // We don't expect collections to get hydrated, ingestions to be
            // started, etc. when they are already at the empty write frontier.
            if live_write_frontier.is_empty() || readiness == CollectionReadiness::Ready {
                // This is a bit spammy, but log caught-up collections while we
                // investigate why environments are cutting over but then a lot
                // of compute collections are _not_ in fact hydrated on
                // clusters.
                tracing::info!(
                    %id,
                    ?readiness,
                    ?write_frontier,
                    ?live_write_frontier,
                    ?allowed_lag,
                    %cluster.id,
                    "collection is caught up");
            } else {
                // We are not within the allowed lag, or not hydrated!
                //
                // We continue with our loop instead of breaking out early, so
                // that we log all non-caught-up replicas.
                tracing::info!(
                    %id,
                    ?readiness,
                    ?write_frontier,
                    ?live_write_frontier,
                    ?allowed_lag,
                    %cluster.id,
                    "collection is not caught up"
                );
                all_caught_up = false;
            }
        }

        Ok(if all_caught_up {
            ClusterCaughtUpStatus::CaughtUp
        } else {
            ClusterCaughtUpStatus::NotCaughtUp
        })
    }

    /// Analyzes replica status history to detect replicas that are
    /// crash-looping or OOM-looping.
    ///
    /// A replica is considered problematic if it has multiple OOM kills in a
    /// short-ish window.
    async fn analyze_replica_looping(&self, now: EpochMillis) -> BTreeSet<ReplicaId> {
        // Look back 1 day for patterns.
        let lookback_window: u64 = Duration::from_secs(24 * 60 * 60)
            .as_millis()
            .try_into()
            .expect("fits into u64");
        let min_timestamp = now.saturating_sub(lookback_window);
        let min_timestamp_dt = mz_ore::now::to_datetime(min_timestamp);

        // Get the replica status collection GlobalId
        let replica_status_item_id = self
            .catalog()
            .resolve_builtin_storage_collection(&MZ_CLUSTER_REPLICA_STATUS_HISTORY);
        let replica_status_gid = self
            .catalog()
            .get_entry(&replica_status_item_id)
            .latest_global_id();

        // Acquire a read hold to determine the as_of timestamp for snapshot_and_stream
        let read_holds = self
            .controller
            .storage_collections
            .acquire_read_holds(vec![replica_status_gid])
            .expect("can't acquire read hold for mz_cluster_replica_status_history");
        let read_hold = if let Some(read_hold) = read_holds.into_iter().next() {
            read_hold
        } else {
            // Collection is not readable anymore, but we return an empty set
            // instead of panicing.
            return BTreeSet::new();
        };

        let as_of = read_hold
            .since()
            .iter()
            .next()
            .cloned()
            .expect("since should not be empty");

        let mut replica_statuses_stream = self
            .controller
            .storage_collections
            .snapshot_and_stream(replica_status_gid, as_of)
            .await
            .expect("can't read mz_cluster_replica_status_history");

        let mut replica_problem_counts: BTreeMap<ReplicaId, u32> = BTreeMap::new();

        while let Some((source_data, _ts, diff)) = replica_statuses_stream.next().await {
            // Only process inserts (positive diffs)
            if diff <= 0 {
                continue;
            }

            // Extract the Row from SourceData
            let row = match source_data.0 {
                Ok(row) => row,
                Err(err) => {
                    // This builtin collection shouldn't have errors, so we at
                    // least log an error so that tests or sentry will notice.
                    tracing::error!(
                        collection = MZ_CLUSTER_REPLICA_STATUS_HISTORY.name,
                        ?err,
                        "unexpected error in builtin collection"
                    );
                    continue;
                }
            };

            let mut iter = row.into_iter();

            let replica_id: ReplicaId = iter
                .next()
                .expect("missing replica_id")
                .unwrap_str()
                .parse()
                .expect("must parse as replica ID");
            let _process_id = iter.next().expect("missing process_id").unwrap_uint64();
            let status = iter
                .next()
                .expect("missing status")
                .unwrap_str()
                .to_string();
            let reason_datum = iter.next().expect("missing reason");
            let reason = if reason_datum.is_null() {
                None
            } else {
                Some(reason_datum.unwrap_str().to_string())
            };
            let occurred_at = iter
                .next()
                .expect("missing occurred_at")
                .unwrap_timestamptz();

            // Only consider events within the time window and that are problematic
            if occurred_at.naive_utc() >= min_timestamp_dt.naive_utc() {
                if Self::is_problematic_status(&status, reason.as_deref()) {
                    *replica_problem_counts.entry(replica_id).or_insert(0) += 1;
                }
            }
        }

        // Filter to replicas with 3 or more problematic events.
        let result = replica_problem_counts
            .into_iter()
            .filter_map(|(replica_id, count)| {
                if count >= 3 {
                    tracing::info!(
                        "Detected problematic cluster replica {}: {} problematic events in last {:?}",
                        replica_id,
                        count,
                        Duration::from_millis(lookback_window)
                    );
                    Some(replica_id)
                } else {
                    None
                }
            })
            .collect();

        // Explicitly keep the read hold alive until this point.
        drop(read_hold);

        result
    }

    /// Determines if a replica status indicates a problematic state that could
    /// indicate looping.
    fn is_problematic_status(_status: &str, reason: Option<&str>) -> bool {
        // For now, we only look at the reason, but we could change/expand this
        // if/when needed.
        if let Some(reason) = reason {
            return reason == OfflineReason::OomKilled.to_string();
        }

        false
    }
}

/// Only explicit unhydrated reports from every frontier-hosting replica permit
/// skipping local hydration. In particular, a missing report is not evidence of
/// an unhydrated collection, and one unhydrated replica cannot mask a hydrated one.
fn leader_unhydrated_collections(
    frontiers: &[(GlobalId, String, Antichain<Timestamp>)],
    rows: Vec<Row>,
) -> BTreeSet<GlobalId> {
    let mut hydrated = BTreeSet::new();
    let unhydrated: BTreeSet<_> = rows
        .into_iter()
        .filter_map(|row| {
            let mut values = row.iter();
            let replica = values
                .next()
                .expect("missing replica_id")
                .unwrap_str()
                .to_owned();
            let id: GlobalId = values
                .next()
                .expect("missing object_id")
                .unwrap_str()
                .parse()
                .expect("valid object ID");
            if values.next().expect("missing time_ns").is_null() {
                Some((id, replica))
            } else {
                hydrated.insert(id);
                None
            }
        })
        .collect();
    let mut collections = BTreeMap::new();
    for (id, replica, _) in frontiers {
        let all_unhydrated = collections.entry(*id).or_insert(true);
        *all_unhydrated &= !hydrated.contains(id) && unhydrated.contains(&(*id, replica.clone()));
    }
    collections
        .into_iter()
        .filter_map(|(id, unhydrated)| unhydrated.then_some(id))
        .collect()
}

/// Match the leader's healthy-run age at the incoming run's start, capped by
/// the configured period. The difference between run starts stays fixed as
/// both runs age. If the leader's run is newer, the incoming run is already
/// longer. Missing evidence or future timestamps require the full period.
fn replica_stability_period(
    period: u64,
    now: u64,
    healthy_since: Option<u64>,
    leader_healthy_since: Option<u64>,
) -> u64 {
    match healthy_since.zip(leader_healthy_since) {
        Some((incoming, leader)) if incoming <= now && leader <= now => {
            period.min(incoming.saturating_sub(leader))
        }
        _ => period,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::Datum;

    /// Builds a health snapshot with all restarts attributed to a single
    /// replica process. `change_secs` is the max status-change time as a
    /// unix-second offset, `restarts` that process's restart count.
    fn snapshot(all_healthy: bool, change_secs: i64, restarts: u64) -> ReplicaHealthSnapshot {
        ReplicaHealthSnapshot {
            all_healthy,
            healthy_since: None,
            max_status_change: DateTime::from_timestamp(change_secs, 0),
            restart_counts: BTreeMap::from([(0, restarts)]),
        }
    }

    #[mz_ore::test]
    fn stability_requires_sustained_health() {
        let period_ms = 1000;
        let mut state = ReplicaStabilityState::default();

        // The first healthy observation starts the streak but isn't yet stable.
        assert!(!state.observe(&snapshot(true, 100, 0), 0, period_ms).ready);
        // Still within the period.
        assert!(!state.observe(&snapshot(true, 100, 0), 500, period_ms).ready);
        // Past the period: ready.
        assert!(
            state
                .observe(&snapshot(true, 100, 0), 1000, period_ms)
                .ready
        );
    }

    #[mz_ore::test]
    fn unhealthy_resets_streak() {
        let period_ms = 1000;
        let mut state = ReplicaStabilityState::default();

        assert!(!state.observe(&snapshot(true, 100, 0), 0, period_ms).ready);
        // A currently-unhealthy observation resets the streak.
        assert!(
            !state
                .observe(&snapshot(false, 100, 0), 500, period_ms)
                .ready
        );
        // Healthy again, but the clock restarts from here.
        assert!(!state.observe(&snapshot(true, 100, 0), 600, period_ms).ready);
        assert!(
            !state
                .observe(&snapshot(true, 100, 0), 1599, period_ms)
                .ready
        );
        assert!(
            state
                .observe(&snapshot(true, 100, 0), 1600, period_ms)
                .ready
        );
    }

    #[mz_ore::test]
    fn status_flap_between_ticks_resets_streak() {
        let period_ms = 1000;
        let mut state = ReplicaStabilityState::default();

        assert!(!state.observe(&snapshot(true, 100, 0), 0, period_ms).ready);
        // Currently healthy, but the status-change time advanced, so a flap
        // happened and resolved between ticks: reset.
        assert!(
            !state
                .observe(&snapshot(true, 200, 0), 1000, period_ms)
                .ready
        );
        // A clean streak from here.
        assert!(
            !state
                .observe(&snapshot(true, 200, 0), 1500, period_ms)
                .ready
        );
        assert!(
            state
                .observe(&snapshot(true, 200, 0), 2500, period_ms)
                .ready
        );
    }

    #[mz_ore::test]
    fn restart_between_ticks_resets_streak() {
        let period_ms = 1000;
        let mut state = ReplicaStabilityState::default();

        assert!(!state.observe(&snapshot(true, 100, 3), 0, period_ms).ready);
        // Healthy with the same status-change time, but the restart count went
        // up: a restart happened and recovered between ticks, which the status
        // alone would miss. Reset.
        assert!(
            !state
                .observe(&snapshot(true, 100, 4), 1000, period_ms)
                .ready
        );
        assert!(
            !state
                .observe(&snapshot(true, 100, 4), 1500, period_ms)
                .ready
        );
        assert!(
            state
                .observe(&snapshot(true, 100, 4), 2500, period_ms)
                .ready
        );
    }

    #[mz_ore::test]
    fn offsetting_restart_changes_reset_streak() {
        // Two processes whose restart counts move in opposite directions by the
        // same amount. A cluster-wide sum would be unchanged and miss the
        // restart, but the per-process map differs, so the streak resets.
        let period_ms = 1000;
        let mut state = ReplicaStabilityState::default();

        let snapshot = |a: u64, b: u64| ReplicaHealthSnapshot {
            all_healthy: true,
            healthy_since: None,
            max_status_change: DateTime::from_timestamp(100, 0),
            restart_counts: BTreeMap::from([(0, a), (1, b)]),
        };

        // Start a streak with per-process counts summing to 2.
        assert!(!state.observe(&snapshot(1, 1), 0, period_ms).ready);
        // One process restarts (+1) while the other is recreated (-1). The sum
        // is still 2, but the per-process map changed: reset.
        assert!(!state.observe(&snapshot(2, 0), 1000, period_ms).ready);
        // A clean streak from here.
        assert!(!state.observe(&snapshot(2, 0), 1500, period_ms).ready);
        assert!(state.observe(&snapshot(2, 0), 2500, period_ms).ready);
    }

    #[mz_ore::test]
    fn reconstruct_health_after_environmentd_restart() {
        let mut health = snapshot(true, 100, 0);
        health.healthy_since = Some(100_000);
        let mut state = ReplicaStabilityState::default();
        assert!(!state.observe(&snapshot(false, 101, 0), 100_500, 1000).ready);
        assert!(!state.observe(&health, 100_999, 1000).ready);
        // Rebuilding coordinator state does not restart the period.
        let mut state = ReplicaStabilityState::default();
        assert!(state.observe(&health, 101_000, 1000).ready);
        // A restart must still reset it, even if its health anchor is stale.
        health.restart_counts.insert(0, 1);
        assert!(!state.observe(&health, 102_000, 1000).ready);
        assert!(!state.observe(&health, 103_000, 1000).ready);
        assert!(state.observe(&health, 104_000, 1000).ready);
    }

    #[mz_ore::test]
    fn comparable_health_has_a_fixed_deadline() {
        let period = replica_stability_period(90_000, 19_999, Some(15_000), Some(10_000));
        assert_eq!(period, 5_000);
        let mut health = snapshot(true, 15, 0);
        health.healthy_since = Some(15_000);
        let mut state = ReplicaStabilityState::default();
        assert!(!state.observe(&health, 19_999, period).ready);
        // Reconstructing after an envd restart still permits promotion at the
        // same deadline, even though the leader's run is now ten seconds old.
        assert_eq!(
            replica_stability_period(90_000, 20_000, Some(15_000), Some(10_000)),
            period
        );
        assert!(
            ReplicaStabilityState::default()
                .observe(&health, 20_000, period)
                .ready
        );
        assert_eq!(
            replica_stability_period(90_000, 200_000, Some(100_000), Some(1)),
            90_000
        );
        assert_eq!(
            replica_stability_period(90_000, 20_000, Some(15_000), None),
            90_000
        );
        assert_eq!(
            replica_stability_period(90_000, 20_000, None, Some(10_000)),
            90_000
        );
        // A leader restart makes its current run younger than the incoming run.
        assert_eq!(
            replica_stability_period(90_000, 20_000, Some(15_000), Some(16_000)),
            0
        );
        // An incoming restart requires a new comparison, not the old budget.
        assert_eq!(
            replica_stability_period(90_000, 30_000, Some(25_000), Some(10_000)),
            15_000
        );
        assert_eq!(
            replica_stability_period(90_000, 20_000, Some(15_000), Some(20_001)),
            90_000
        );
        assert_eq!(
            replica_stability_period(90_000, 20_000, Some(20_001), Some(10_000)),
            90_000
        );
    }

    #[mz_ore::test]
    fn leader_health_requires_all_processes() {
        let mut health = snapshot(true, 15, 0);
        health.restart_counts.insert(1, 0);
        let mut reports = BTreeMap::from([(0, Some(10_000))]);
        assert_eq!(health.leader_healthy_since(Some(&reports)), None);
        reports.insert(1, None);
        assert_eq!(health.leader_healthy_since(Some(&reports)), None);
        reports.insert(1, Some(12_000));
        assert_eq!(health.leader_healthy_since(Some(&reports)), Some(12_000));
        reports.remove(&1);
        reports.insert(2, Some(12_000));
        assert_eq!(health.leader_healthy_since(Some(&reports)), None);
    }

    #[mz_ore::test]
    fn leader_hydration_requires_complete_unhydrated_evidence() {
        let frontier = |id, replica: &str| {
            (
                GlobalId::User(id),
                replica.to_owned(),
                Antichain::from_elem(Timestamp::from(100)),
            )
        };
        let row = |id: &str, replica: &str, time| {
            Row::pack_slice(&[Datum::String(replica), Datum::String(id), time])
        };
        let frontiers = vec![
            frontier(1, "u10"),
            frontier(1, "u11"),
            frontier(2, "u10"),
            frontier(2, "u11"),
            frontier(3, "u10"),
            frontier(3, "u11"),
            frontier(4, "u10"),
            frontier(5, "u10"),
        ];
        let rows = vec![
            row("u1", "u10", Datum::Null),
            row("u1", "u11", Datum::Null),
            row("u2", "u10", Datum::Null),
            row("u2", "u11", Datum::UInt64(7)),
            row("u3", "u10", Datum::Null), // u11 has not reported.
            row("u4", "u10", Datum::Null),
            row("u4", "u12", Datum::UInt64(9)),
            // u5 has no hydration reports. u6 has no frontier.
            row("u6", "u10", Datum::Null),
        ];
        assert_eq!(
            leader_unhydrated_collections(&frontiers, rows),
            BTreeSet::from([GlobalId::User(1)])
        );
        assert!(leader_unhydrated_collections(&frontiers, Vec::new()).is_empty());
    }

    #[mz_ore::test]
    fn zero_period_ready_on_first_healthy_tick() {
        let mut state = ReplicaStabilityState::default();
        // With a zero period a single clean, healthy observation is enough.
        assert!(state.observe(&snapshot(true, 100, 0), 0, 0).ready);
    }
}
