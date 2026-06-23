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
//! per-tick caught-up classification we therefore run a stability gate. Once a
//! cluster is genuinely caught-up it must stay caught-up and have all replicas
//! healthy for a configurable period before we report it ready. Any disruption
//! (a replica not `Online`, a status flap between ticks, or a replica restart)
//! resets the streak, so a crash-looping replica never accumulates the required
//! stable time. [`ClusterStabilityState`] holds the per-cluster gate state
//! across ticks.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice as _;
use futures::StreamExt;
use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK, ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK,
    WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG, WITH_0DT_CAUGHT_UP_CHECK_CUTOFF,
    WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD,
};
use mz_catalog::builtin::{MZ_CLUSTER_REPLICA_FRONTIERS, MZ_CLUSTER_REPLICA_STATUS_HISTORY};
use mz_catalog::memory::objects::Cluster;
use mz_controller::clusters::{ClusterStatus, ProcessId};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::OfflineReason;
use mz_ore::channel::trigger::Trigger;
use mz_ore::now::EpochMillis;
use mz_repr::{GlobalId, Timestamp};
use timely::PartialOrder;
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
    /// Only genuinely caught-up clusters have an entry. Entries are dropped as
    /// soon as a cluster stops being caught-up, so the streak restarts from
    /// scratch when it becomes caught-up again.
    pub cluster_stability: BTreeMap<ClusterId, ClusterStabilityState>,
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

/// Per-cluster state for the stability gate, retained across caught-up checks.
///
/// The gate requires a cluster to stay caught-up and fully healthy for a
/// configurable period before we report it ready. A point-in-time check isn't
/// enough: a crash-looping replica can momentarily look hydrated and healthy, so
/// we'd cut over right into a crash. We therefore track health over time here.
#[derive(Debug, Default, Clone)]
pub struct ClusterStabilityState {
    /// Wall-clock time (environmentd's `now()`) at which the current
    /// uninterrupted caught-up-and-healthy streak began. `None` while the
    /// cluster isn't currently in such a streak.
    ///
    /// We anchor the window with environmentd's clock so the configured period
    /// means real wall-clock seconds, independent of orchestrator event times.
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
    last_restart_counts: Option<BTreeMap<(ReplicaId, ProcessId), u64>>,
}

/// A point-in-time view of a cluster's replica health, derived from the
/// in-memory mirror of orchestrator-reported replica statuses.
#[derive(Debug, Clone)]
struct ClusterHealthSnapshot {
    /// True iff the cluster has replicas and every process of every replica is
    /// `Online`. We deliberately require all replicas to be healthy, so we only
    /// cut over when the new environment is fully healthy.
    all_healthy: bool,
    /// Max status-change time across all of the cluster's replica processes.
    max_status_change: Option<DateTime<Utc>>,
    /// Restart count per replica process.
    ///
    /// Kept per process rather than summed: restart counts are not monotonic (a
    /// recreated process resets to zero), so a cluster-wide sum could cancel
    /// offsetting changes across processes and hide a restart. Comparing the
    /// whole map between ticks also catches replica/process churn.
    restart_counts: BTreeMap<(ReplicaId, ProcessId), u64>,
}

/// Why a caught-up cluster is being held back by the stability gate on a given
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

/// Outcome of folding one health snapshot into a [`ClusterStabilityState`].
#[derive(Debug, Clone, Copy)]
struct StabilityObservation {
    /// Whether the cluster has now been continuously caught-up and healthy for
    /// at least the required period.
    ready: bool,
    /// How long the current uninterrupted streak has lasted, in milliseconds.
    /// `None` when the cluster is not currently in a streak (this tick reset it).
    stable_for_ms: Option<u64>,
    /// Why the cluster is being held back, for logging. `None` once it's ready.
    blocked_by: Option<StabilityBlocker>,
}

impl ClusterStabilityState {
    /// Folds in the latest health snapshot and returns an observation: whether
    /// the cluster has now been continuously caught-up and healthy for at least
    /// `period_ms`, how long the current streak has lasted, and (when not ready)
    /// what is holding it back.
    ///
    /// A cluster is "good" on a tick only if all its replicas are currently
    /// healthy and nothing changed since the previous tick (no status flap, no
    /// restart). Any disruption resets the streak, so a crash-looping replica can
    /// never accumulate the required stable time.
    fn observe(
        &mut self,
        snapshot: &ClusterHealthSnapshot,
        now: EpochMillis,
        period_ms: u64,
    ) -> StabilityObservation {
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
        // We only compare the orchestrator-supplied times against each other, so
        // clock skew between the orchestrator and environmentd doesn't matter.
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
            self.stable_since.or(Some(now))
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
                &exclude_collections,
                &problematic_replicas,
            )
            .await;

        // Read the health snapshots for genuinely caught-up clusters now, while we
        // only hold a shared borrow of `self`. We update the stability state in a
        // separate, mutable pass below.
        let health: BTreeMap<ClusterId, ClusterHealthSnapshot> = classification
            .iter()
            .filter(|(_, status)| **status == ClusterCaughtUpStatus::CaughtUp)
            .map(|(&cluster_id, _)| (cluster_id, self.cluster_health(cluster_id)))
            .collect();

        let ctx = self.caught_up_check.as_mut().expect("known to exist");

        // Drop stability state for clusters that are no longer genuinely caught
        // up, so the streak restarts from scratch when they become caught-up
        // again.
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
                    let snapshot = health.get(&cluster_id).expect("computed above");
                    let state = ctx.cluster_stability.entry(cluster_id).or_default();
                    let observation = state.observe(snapshot, now, stability_period_ms);
                    if !observation.ready {
                        all_ready = false;
                        tracing::info!(
                            %cluster_id,
                            reason = ?observation.blocked_by,
                            all_healthy = snapshot.all_healthy,
                            stable_for_ms = ?observation.stable_for_ms,
                            required_period_ms = stability_period_ms,
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
    fn cluster_health(&self, cluster_id: ClusterId) -> ClusterHealthSnapshot {
        let Some(replicas) = self
            .cluster_replica_statuses
            .try_get_cluster_statuses(cluster_id)
            .filter(|replicas| !replicas.is_empty())
        else {
            // A cluster with no replica statuses is treated as not healthy.
            return ClusterHealthSnapshot {
                all_healthy: false,
                max_status_change: None,
                restart_counts: BTreeMap::new(),
            };
        };

        let mut all_healthy = true;
        let mut max_status_change = None;
        let mut restart_counts = BTreeMap::new();
        for (replica_id, processes) in replicas {
            if ClusterReplicaStatuses::cluster_replica_status(processes) != ClusterStatus::Online {
                all_healthy = false;
            }
            for (process_id, process) in processes {
                max_status_change = max_status_change.max(Some(process.time));
                restart_counts.insert((*replica_id, *process_id), process.restart_count);
            }
        }

        ClusterHealthSnapshot {
            all_healthy,
            max_status_change,
            restart_counts,
        }
    }

    /// Classifies every cluster for the caught-up check.
    ///
    /// Informally, a cluster is considered caught-up if it is at least as healthy as its
    /// counterpart in the leader environment. To determine that, we use the following rules:
    ///
    ///  (1) A cluster is caught-up if all non-transient, non-excluded collections installed on it
    ///      are either caught-up or ignored.
    ///  (2) A collection is caught-up when it is (a) hydrated and (b) its write frontier is within
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
                    // The collection didn't previously exist, so consider
                    // ourselves hydrated as long as our write_ts is > 0.
                    tracing::info!(?write_frontier, "collection {id} not in live frontiers");
                    if write_frontier.less_equal(&Timestamp::minimum()) {
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

            // We can't do easy comparisons and subtractions, so we bump up the
            // write frontier by the allowed lag, and then compare that against
            // the write frontier.
            let write_frontier_plus_allowed_lag = write_frontier
                .iter()
                .map(|t| t.step_forward_by(&allowed_lag));
            let bumped_write_plus_allowed_lag =
                Antichain::from_iter(write_frontier_plus_allowed_lag);

            let within_lag =
                PartialOrder::less_equal(live_write_frontier, &bumped_write_plus_allowed_lag);

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

            // We don't expect collections to get hydrated, ingestions to be
            // started, etc. when they are already at the empty write frontier.
            if live_write_frontier.is_empty() || (within_lag && collection_hydrated) {
                // This is a bit spammy, but log caught-up collections while we
                // investigate why environments are cutting over but then a lot
                // of compute collections are _not_ in fact hydrated on
                // clusters.
                tracing::info!(
                    %id,
                    %within_lag,
                    %collection_hydrated,
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
                    %within_lag,
                    %collection_hydrated,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a health snapshot with all restarts attributed to a single
    /// replica process. `change_secs` is the max status-change time as a
    /// unix-second offset, `restarts` that process's restart count.
    fn snapshot(all_healthy: bool, change_secs: i64, restarts: u64) -> ClusterHealthSnapshot {
        ClusterHealthSnapshot {
            all_healthy,
            max_status_change: DateTime::from_timestamp(change_secs, 0),
            restart_counts: BTreeMap::from([((ReplicaId::User(1), 0), restarts)]),
        }
    }

    #[mz_ore::test]
    fn stability_requires_sustained_health() {
        let period_ms = 1000;
        let mut state = ClusterStabilityState::default();

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
        let mut state = ClusterStabilityState::default();

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
        let mut state = ClusterStabilityState::default();

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
        let mut state = ClusterStabilityState::default();

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
        let mut state = ClusterStabilityState::default();

        let r = ReplicaId::User(1);
        let snapshot = |a: u64, b: u64| ClusterHealthSnapshot {
            all_healthy: true,
            max_status_change: DateTime::from_timestamp(100, 0),
            restart_counts: BTreeMap::from([((r, 0u64), a), ((r, 1u64), b)]),
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
    fn zero_period_ready_on_first_healthy_tick() {
        let mut state = ClusterStabilityState::default();
        // With a zero period a single clean, healthy observation is enough.
        assert!(state.observe(&snapshot(true, 100, 0), 0, 0).ready);
    }
}
