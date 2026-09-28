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
//! generation. A task spawned by [`Coordinator::spawn_caught_up_check_task`]
//! runs that check on an interval (see
//! [`WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL`]). We call one such run a
//! "tick", and the term is used throughout this module.
//!
//! Each tick the task asks the coordinator to classify the clusters, which
//! needs controller state only the coordinator loop can reach. The task then
//! applies the stability gate itself, because the replica queries it needs
//! call back into the coordinator and would deadlock on its loop.
//!
//! A point-in-time caught-up check is not enough on its own: a crash- or
//! OOM-looping replica can momentarily look hydrated and caught-up, and cutting
//! over right then drops us straight into a crashing replica. On top of the
//! per-tick caught-up classification we therefore run a stability gate. A
//! caught-up cluster is ready only once every replica is `Online` and has had
//! everything it hosts hydrated for a configurable period.
//!
//! Each replica reports its hydration times in its
//! `mz_compute_hydration_times_per_worker` introspection log. The log lives in
//! the replica process. It survives environmentd restarts, because a restarted
//! environmentd reconciles with the same replica processes and they keep
//! compatible dataflows, so a DDL-triggered restart does not restart the
//! period. A dataflow that changed rehydrates and restarts it. The log dies
//! with the process, so a restarted replica has to hydrate again and then wait
//! out the full period, and a crash-looping replica never becomes ready.
//!
//! Hydration times come from the replica's clock, while the period is measured
//! against environmentd's. A replica clock that runs ahead only lengthens the
//! wait, but one that runs behind shortens the period by the skew. We rely on
//! hosts keeping their clocks in sync.
//!
//! A replica without introspection logging cannot report hydration times. It
//! only has to be `Online`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice as _;
use futures::{StreamExt, future};
use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK, ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK,
    WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG, WITH_0DT_CAUGHT_UP_CHECK_CUTOFF,
    WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD, WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL,
};
use mz_catalog::builtin::{MZ_CLUSTER_REPLICA_FRONTIERS_RAW, MZ_CLUSTER_REPLICA_STATUS_HISTORY};
use mz_catalog::memory::objects::{CatalogItem, Cluster};
use mz_compute_client::controller::CollectionReadiness;
use mz_compute_client::logging::{ComputeLog, LogVariant};
use mz_controller::clusters::ClusterStatus;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::OfflineReason;
use mz_ore::channel::trigger::Trigger;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use timely::progress::{Antichain, Timestamp as _};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use crate::PeekClient;
use crate::coord::{ClusterReplicaStatuses, Coordinator, Message};

/// How long a check waits for a replica to answer the hydration query.
///
/// A replica that does not answer in time counts as not hydrated for that
/// check. Replicas are asked concurrently, so this also bounds how long a check
/// waits for replicas.
///
/// The check drops a query that times out, with the leftovers documented at
/// [`PeekClient::background_peek`]. We accept those: a stuck replica holds at
/// most one peek per check, on a log index that barely changes, and an
/// orphaned registration needs the coordinator to stall at exactly that point.
const HYDRATION_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns a query for when a replica last finished hydrating an export.
///
/// The one row it returns is NULL if some export, other than a transient one
/// or one in `ignored`, is not hydrated on some worker. It returns no row if
/// the replica has reported nothing yet.
fn hydration_query(ignored: &BTreeSet<GlobalId>) -> String {
    let mut query = "SELECT hydrated_at \
        FROM mz_introspection.mz_compute_hydration_times_per_worker \
        WHERE export_id NOT LIKE 't%'"
        .to_string();
    if !ignored.is_empty() {
        let ids = ignored.iter().map(|id| format!("'{id}'")).join(", ");
        write!(query, " AND export_id NOT IN ({ids})").expect("writing to a string");
    }
    query.push_str(" ORDER BY hydrated_at DESC NULLS FIRST LIMIT 1");
    query
}

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
}

/// A request from the caught-up check task for the coordinator's view of the
/// clusters, answered by [`Coordinator::handle_caught_up_check_request`].
#[derive(Debug)]
pub struct CaughtUpCheckRequest {
    exclude_collections: Arc<BTreeSet<GlobalId>>,
    policy_read_ts: Option<Timestamp>,
    tx: oneshot::Sender<CaughtUpSnapshot>,
}

/// The coordinator's view of the clusters at one check.
#[derive(Debug)]
pub struct CaughtUpSnapshot {
    now: EpochMillis,
    /// Whether every cluster is caught up or ignored.
    all_caught_up: bool,
    stability_check_enabled: bool,
    stability_period_ms: u64,
    /// Every caught-up cluster. Empty when the stability check is disabled.
    caught_up_clusters: BTreeMap<ClusterId, CaughtUpCluster>,
}

/// A caught-up cluster, as the stability gate needs to know it.
#[derive(Debug)]
struct CaughtUpCluster {
    /// Compute collections the classification ignored, which therefore need not
    /// hydrate.
    ignored_compute_collections: BTreeSet<GlobalId>,
    replicas: BTreeMap<ReplicaId, ReplicaTarget>,
}

/// What the coordinator knows about a replica of a caught-up cluster, and so
/// whether the stability gate has to ask it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaTarget {
    /// See [`ReplicaHealth::NotOnline`].
    NotOnline,
    /// See [`ReplicaHealth::Exempt`].
    Exempt,
    /// The replica is `Online` and has to report its hydration times.
    Ask,
}

/// How a cluster relates to the 0dt caught-up check on a given tick.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ClusterCaughtUpStatus {
    /// Hydrated and within lag, apart from `ignored_compute_collections`.
    /// Subject to the stability gate.
    CaughtUp {
        /// Compute collections the classification ignored, see
        /// [`CaughtUpCluster::ignored_compute_collections`].
        ignored_compute_collections: BTreeSet<GlobalId>,
    },
    /// Excluded by the existing checks (no replicas, or hopelessly behind with
    /// only crash/OOM-looping replicas). Does not block readiness and is not
    /// health-gated, so we keep ignoring clusters that are already unhealthy in
    /// the leader environment.
    Ignored,
    /// Not yet caught up. Blocks readiness.
    NotCaughtUp,
}

/// What the stability gate knows about one replica on a given tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaHealth {
    /// The replica is not currently `Online`.
    NotOnline,
    /// The replica is `Online`, but has not hydrated everything it hosts, or
    /// did not answer the hydration query.
    NotHydrated,
    /// The replica has been healthy since this wall-clock time.
    HealthySince(EpochMillis),
    /// The replica is `Online` but cannot report hydration times, so there is
    /// nothing to wait on.
    Exempt,
}

/// Why a caught-up cluster is being held back by the stability gate on a given
/// tick. Recorded so we can log the cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StabilityBlocker {
    /// Not all replicas are currently `Online`.
    NotOnline,
    /// Some replica has not hydrated everything it hosts.
    NotHydrated,
    /// Every replica is healthy, but not yet for the required period.
    WithinPeriod,
}

/// Outcome of folding a cluster's replica health.
#[derive(Debug, Clone, Copy)]
struct StabilityObservation {
    /// How long the cluster has been stable, in milliseconds. `None` when some
    /// replica has no evidence of health.
    stable_for_ms: Option<u64>,
    /// Why the cluster is held back. `None` once it is ready.
    blocked_by: Option<StabilityBlocker>,
}

/// Folds the health of a cluster's replicas into the gate's verdict.
///
/// A cluster is stable since the latest of its replicas' healthy-since times.
/// These can come from replica clocks, which may run ahead of `now`, so they
/// are clamped to `now`. A replica clock running behind shortens the period by
/// its skew.
fn observe_stability(
    replicas: &[ReplicaHealth],
    now: EpochMillis,
    period_ms: u64,
) -> StabilityObservation {
    let blocked = |blocker| StabilityObservation {
        stable_for_ms: None,
        blocked_by: Some(blocker),
    };
    if replicas.is_empty() || replicas.contains(&ReplicaHealth::NotOnline) {
        return blocked(StabilityBlocker::NotOnline);
    }
    let mut stable_since = EpochMillis::MIN;
    for health in replicas {
        match health {
            ReplicaHealth::HealthySince(since) => stable_since = stable_since.max(*since),
            ReplicaHealth::Exempt => {}
            ReplicaHealth::NotOnline | ReplicaHealth::NotHydrated => {
                return blocked(StabilityBlocker::NotHydrated);
            }
        }
    }
    let stable_for_ms = now.saturating_sub(stable_since.min(now));
    StabilityObservation {
        stable_for_ms: Some(stable_for_ms),
        blocked_by: (stable_for_ms < period_ms).then_some(StabilityBlocker::WithinPeriod),
    }
}

/// The part of the caught-up check that runs in its own task: it asks replicas
/// for their hydration times and applies the stability gate.
struct StabilityGate {
    client: PeekClient,
}

impl StabilityGate {
    /// Returns whether `snapshot` shows every cluster ready for promotion.
    async fn ready(&self, snapshot: CaughtUpSnapshot) -> bool {
        fail::fail_point!("0dt_caught_up_check", |_| false);
        let CaughtUpSnapshot {
            now,
            all_caught_up,
            stability_check_enabled,
            stability_period_ms,
            caught_up_clusters,
        } = snapshot;
        // Disabling the stability check is a fleet-wide break-glass that makes
        // caught-up clusters ready without waiting on replica health. While
        // some cluster is not caught up, readiness is blocked anyway, so no
        // replica needs to be asked.
        if !all_caught_up || !stability_check_enabled {
            tracing::info!(all_ready = %all_caught_up, "checked caught-up status of clusters");
            return all_caught_up;
        }

        let answers = self.ask_replicas(&caught_up_clusters).await;

        let mut all_ready = true;
        for (cluster_id, cluster) in caught_up_clusters {
            let health: BTreeMap<_, _> = cluster
                .replicas
                .iter()
                .map(|(replica_id, target)| {
                    let replica_health = match target {
                        ReplicaTarget::NotOnline => ReplicaHealth::NotOnline,
                        ReplicaTarget::Exempt => ReplicaHealth::Exempt,
                        ReplicaTarget::Ask => answers
                            .get(replica_id)
                            .copied()
                            .unwrap_or(ReplicaHealth::NotHydrated),
                    };
                    (*replica_id, replica_health)
                })
                .collect();

            let replica_health = health.values().copied().collect_vec();
            let observation = observe_stability(&replica_health, now, stability_period_ms);
            if let Some(reason) = observation.blocked_by {
                all_ready = false;
                tracing::info!(
                    %cluster_id,
                    ?reason,
                    replicas = ?health,
                    stable_for_ms = ?observation.stable_for_ms,
                    required_period_ms = stability_period_ms,
                    "cluster is caught up but not yet stable for the required period"
                );
            }
        }

        tracing::info!(%all_ready, "checked caught-up status of clusters");
        all_ready
    }

    /// Asks every [`ReplicaTarget::Ask`] replica, concurrently, when it last
    /// finished hydrating an export.
    async fn ask_replicas(
        &self,
        clusters: &BTreeMap<ClusterId, CaughtUpCluster>,
    ) -> BTreeMap<ReplicaId, ReplicaHealth> {
        let queries = clusters.iter().flat_map(|(&cluster_id, cluster)| {
            let sql = hydration_query(&cluster.ignored_compute_collections);
            cluster
                .replicas
                .iter()
                .filter(|(_, target)| **target == ReplicaTarget::Ask)
                .map(move |(&replica_id, _)| {
                    let mut client = self.client.clone();
                    let sql = sql.clone();
                    async move {
                        let query = client.background_peek(&sql, cluster_id, replica_id);
                        let answer = tokio::time::timeout(HYDRATION_QUERY_TIMEOUT, query).await;
                        let health = match answer {
                            Ok(Ok(rows)) => replica_health(&rows),
                            Ok(Err(error)) => {
                                tracing::warn!(
                                    %cluster_id, %replica_id, %error, "hydration query failed"
                                );
                                ReplicaHealth::NotHydrated
                            }
                            Err(_elapsed) => {
                                tracing::warn!(%cluster_id, %replica_id, "hydration query timed out");
                                ReplicaHealth::NotHydrated
                            }
                        };
                        (replica_id, health)
                    }
                })
        });
        future::join_all(queries).await.into_iter().collect()
    }
}

/// Interprets the rows of a [`hydration_query`].
fn replica_health(rows: &[Row]) -> ReplicaHealth {
    let Some(row) = rows.first() else {
        return ReplicaHealth::NotHydrated;
    };
    let hydrated_at = row.unpack_first();
    if hydrated_at.is_null() {
        return ReplicaHealth::NotHydrated;
    }
    let millis = hydrated_at.unwrap_timestamptz().timestamp_millis();
    // A time before the epoch is nonsense, and must not read as stable for
    // decades.
    u64::try_from(millis).map_or(ReplicaHealth::NotHydrated, ReplicaHealth::HealthySince)
}

impl Coordinator {
    /// Spawns the task that checks, on an interval, whether all
    /// clusters/collections are caught up, and fires the trigger of
    /// `self.caught_up_check` once they are.
    ///
    /// A no-op when there is no caught-up check, i.e. outside read-only mode.
    pub(crate) fn spawn_caught_up_check_task(&mut self) {
        let Some(CaughtUpCheckContext {
            trigger,
            exclude_collections,
        }) = self.caught_up_check.take()
        else {
            return;
        };
        let exclude_collections = Arc::new(exclude_collections);
        let period = WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL
            .get(self.catalog().system_config().dyncfgs());
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let gate = StabilityGate {
            client: self.background_peek_client(&self.owned_catalog()),
        };
        let policy_oracle = self
            .controller
            .replica_owned_compute()
            .then(|| self.get_local_timestamp_oracle());

        task::spawn(|| "caught_up_check", async move {
            let mut interval = tokio::time::interval(period);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                // Scheduling requires a fresh observation for each check. Await
                // it off the coordinator loop so provisioning can keep moving.
                let policy_read_ts = match &policy_oracle {
                    Some(oracle) => Some(oracle.read_ts().await),
                    None => None,
                };
                let (tx, rx) = oneshot::channel();
                let request = CaughtUpCheckRequest {
                    exclude_collections: Arc::clone(&exclude_collections),
                    policy_read_ts,
                    tx,
                };
                // A failed send or a dropped reply means the coordinator is
                // gone.
                //
                // NOTE: Returning drops `trigger`, which fires it, so a
                // coordinator shutdown reads as caught up.
                if internal_cmd_tx
                    .send(Message::CaughtUpCheck(request))
                    .is_err()
                {
                    return;
                }
                let Ok(snapshot) = rx.await else {
                    return;
                };
                if gate.ready(snapshot).await {
                    trigger.fire();
                    return;
                }
            }
        });
    }

    /// Answers a [`CaughtUpCheckRequest`] with the classification of every
    /// cluster and the replicas the stability gate has to hear from.
    pub(crate) async fn handle_caught_up_check_request(&self, request: CaughtUpCheckRequest) {
        let CaughtUpCheckRequest {
            exclude_collections,
            policy_read_ts,
            tx,
        } = request;

        let replica_frontier_item_id = self
            .catalog()
            .resolve_builtin_storage_collection(&MZ_CLUSTER_REPLICA_FRONTIERS_RAW);
        let replica_frontier_gid = self
            .catalog()
            .get_entry(&replica_frontier_item_id)
            .latest_global_id();

        // `snapshot_latest` requires that the collection consolidates to a
        // set. `mz_cluster_replica_frontiers_raw` is a controller-managed builtin
        // written with ±1 diffs, so it satisfies that invariant.
        //
        // Legacy deployments read the leader's shard. Native deployments share
        // the shard and must filter its rows to active deployment membership below.
        // `validate_migration_steps` forbids migrating `mz_cluster_replica_frontiers_raw` for this
        // reason, so a declared migration can't reach here. A test forcing replacement across all
        // builtins bypasses that guard, hands us a shard we write ourselves, and the lag check
        // below then compares this deployment against itself.
        let live_frontiers = self
            .controller
            .storage_collections
            .snapshot_latest(replica_frontier_gid)
            .await
            .expect("can't read mz_cluster_replica_frontiers_raw");

        let active_replicas = self.controller.replica_owned_compute().then(|| {
            let state = self.catalog().state();
            (
                state
                    .active_deployment_generation()
                    .expect("native catalog has an active deployment"),
                state.active_replica_ids().collect::<BTreeSet<_>>(),
            )
        });
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
                let replica_id: ReplicaId = iter
                    .next()
                    .expect("missing replica id")
                    .unwrap_str()
                    .parse()
                    .expect("cannot parse replica id");
                let maybe_upper_ts = iter.next().expect("missing upper_ts");
                let generation = iter.next().expect("missing deployment generation");
                let generation = if generation.is_null() {
                    None
                } else {
                    Some(generation.unwrap_uint64())
                };
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

                (id, replica_id, upper_frontier, generation)
            })
            .filter(
                |(_, replica, _, generation)| match active_replicas.as_ref() {
                    Some((active_generation, active)) => {
                        *generation == Some(*active_generation) && active.contains(replica)
                    }
                    None => generation.is_none(),
                },
            )
            .map(|(id, replica, frontier, _)| (id, replica, frontier))
            .collect_vec();

        // The classification only requires each collection to be hydrated on
        // _some_ replica. The stability gate separately requires every replica
        // to have hydrated everything it hosts.
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

        let classification = self
            .classify_clusters(
                allowed_lag.into(),
                cutoff.into(),
                now.into(),
                &live_collection_frontiers,
                &exclude_collections,
                &problematic_replicas,
                policy_read_ts,
            )
            .await;

        let all_caught_up = classification
            .values()
            .all(|status| *status != ClusterCaughtUpStatus::NotCaughtUp);
        let caught_up_clusters = if stability_check_enabled {
            classification
                .into_iter()
                .filter_map(|(cluster_id, status)| match status {
                    ClusterCaughtUpStatus::CaughtUp {
                        ignored_compute_collections,
                    } => Some((
                        cluster_id,
                        CaughtUpCluster {
                            ignored_compute_collections,
                            replicas: self.replica_targets(cluster_id),
                        },
                    )),
                    _ => None,
                })
                .collect()
        } else {
            BTreeMap::new()
        };

        // A dropped receiver means the task is gone, which leaves nothing to
        // answer.
        let _ = tx.send(CaughtUpSnapshot {
            now,
            all_caught_up,
            stability_check_enabled,
            stability_period_ms,
            caught_up_clusters,
        });
    }

    /// Classifies the replicas of a cluster for the stability gate.
    fn replica_targets(&self, cluster_id: ClusterId) -> BTreeMap<ReplicaId, ReplicaTarget> {
        let cluster = self.catalog().get_cluster(cluster_id);
        let has_hydration_log = cluster
            .log_indexes
            .contains_key(&LogVariant::Compute(ComputeLog::HydrationTime));
        cluster
            .replicas()
            .map(|replica| {
                let online = self
                    .cluster_replica_statuses
                    .try_get_cluster_replica_statuses(cluster_id, replica.replica_id)
                    .is_some_and(|processes| {
                        !processes.is_empty()
                            && ClusterReplicaStatuses::cluster_replica_status(processes)
                                == ClusterStatus::Online
                    });
                let logging = has_hydration_log && replica.config.compute.logging.enabled();
                let target = if !online {
                    ReplicaTarget::NotOnline
                } else if !logging {
                    ReplicaTarget::Exempt
                } else {
                    ReplicaTarget::Ask
                };
                (replica.replica_id, target)
            })
            .collect()
    }

    /// Classifies every cluster for the caught-up check.
    ///
    /// Informally, a cluster is considered caught-up if it is at least as healthy as its
    /// counterpart in the leader environment. To determine that, we use the following rules:
    ///
    ///  (1) A cluster is caught-up if all non-transient, non-excluded collections expected on it
    ///      are either caught-up or ignored.
    ///  (2) A collection is caught-up when it is (a) hydrated and (b) its progress frontier is within
    ///      `allowed_lag` of the "live" frontier, the collection's frontier reported by the leader
    ///      environment.
    ///      Native compute uses local output progress, not shared Persist write progress.
    ///      Storage retains status-based hydration plus shared output lag, not replay lag.
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
        policy_read_ts: Option<Timestamp>,
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
                    policy_read_ts,
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
        policy_read_ts: Option<Timestamp>,
    ) -> Result<ClusterCaughtUpStatus, anyhow::Error> {
        if self.controller.replica_owned_compute()
            && !self.cluster_has_required_realizations(cluster.id, policy_read_ts, now)
        {
            return Ok(ClusterCaughtUpStatus::NotCaughtUp);
        }
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
            NativeCompute { hydrated: bool },
        }

        let mut all_caught_up = true;
        let mut ignored_compute_collections: BTreeSet<_> = self
            .controller
            .compute
            .collection_ids(cluster.id)?
            .filter(|id| exclude_collections.contains(id))
            .collect();

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

        let compute_frontiers = if self.controller.replica_owned_compute() {
            // Match start_readiness_checks: catalog definitions include exports
            // not yet installed and builtin objects absent from bound_objects.
            // Catalog introspection indexes participate through their reported
            // output and hydration, rather than the raw replica-local log IDs.
            itertools::Either::Left(self.catalog().entries().filter_map(|entry| {
                if entry.item().cluster_id() != Some(cluster.id) {
                    return None;
                }
                let (id, target) = match entry.item() {
                    CatalogItem::Index(index) => (index.global_id(), None),
                    CatalogItem::MaterializedView(mv) => {
                        // Completed finite-refresh writers need no runtime.
                        // A replacement cannot borrow its target's completion.
                        let complete = mv.replacement_target.is_none()
                            && mv
                                .refresh_schedule
                                .as_ref()
                                .and_then(|s| s.last_refresh())
                                .is_some_and(|last| {
                                    self.controller
                                        .storage_collections
                                        .collection_frontiers(mv.global_id_writes())
                                        .is_ok_and(|f| !f.write_frontier.less_equal(&last))
                                });
                        if complete {
                            return None;
                        }
                        (mv.global_id_writes(), mv.target_replica)
                    }
                    CatalogItem::MetricSink(sink) => (sink.global_id, None),
                    _ => return None,
                };
                if id.is_transient() || exclude_collections.contains(&id) {
                    return None;
                }
                let output = self.query_client.as_ref().and_then(|client| {
                    client.hydrated_output_frontier(self.catalog(), cluster.id, id, target)
                });
                let hydrated = output.is_some();
                // Unknown observations must fail readiness, but only after the
                // policy's live cutoff and completed-live exceptions are applied.
                Some(Ok((
                    id,
                    output.unwrap_or_else(|| Antichain::from_elem(Timestamp::minimum())),
                    CollectionType::NativeCompute { hydrated },
                )))
            }))
        } else {
            itertools::Either::Right(
                self.controller
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
                    }),
            )
        };

        for res in itertools::chain(storage_frontiers, compute_frontiers) {
            let (id, write_frontier, collection_type) = res?;
            let live_write_frontier = match live_frontiers.get(&id) {
                Some(frontier) => frontier,
                None => {
                    // No live frontier to compare against, either because the collection didn't
                    // exist on the leader or because the leader hosts it as something
                    // `mz_cluster_replica_frontiers_raw` doesn't track. A table→MV conversion is the
                    // latter: it keeps the table's `GlobalId`, still a table on the leader, so the
                    // new MV lands here instead of the strong path below.
                    //
                    // Require hydration, not just a write frontier past the minimum. A fresh MV's
                    // sink reaches frontier 1 after one batch, which would otherwise look caught
                    // up mid-hydration and bring back the cut-over spike this gate prevents.
                    let collection_hydrated = match collection_type {
                        CollectionType::NativeCompute { hydrated } => hydrated,
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
                if matches!(collection_type, CollectionType::Compute) {
                    ignored_compute_collections.insert(id);
                }
                continue;
            }

            // This call is on the expensive side, because we have to do a call
            // across a task/channel boundary, and our work competes with other
            // things the compute/instance controller might be doing. But it's
            // okay because we only do these hydration checks when in read-only
            // mode, and only rarely.
            let collection_hydrated = match collection_type {
                CollectionType::NativeCompute { hydrated } => hydrated,
                CollectionType::Compute => {
                    self.controller
                        .compute
                        .collection_hydrated(cluster.id, id)
                        .await?
                }
                CollectionType::Storage => self.controller.storage.collection_hydrated(id)?,
            };

            let readiness = CollectionReadiness::classify(
                collection_hydrated,
                &write_frontier,
                Some((live_write_frontier, allowed_lag)),
            );

            // We don't expect collections to get hydrated, ingestions to be
            // started, etc. when they are already at the empty write frontier.
            if live_write_frontier.is_empty() || readiness == CollectionReadiness::Ready {
                if readiness != CollectionReadiness::Ready
                    && matches!(collection_type, CollectionType::Compute)
                {
                    ignored_compute_collections.insert(id);
                }
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
            ClusterCaughtUpStatus::CaughtUp {
                ignored_compute_collections,
            }
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

            // Local readiness must not count a peer deployment's events for a stable ID.
            let generation = iter.next().expect("missing deployment_generation");
            let own_generation = if self.controller.replica_owned_compute() {
                Datum::UInt64(self.query_deploy_generation)
            } else {
                Datum::Null
            };
            if generation != own_generation {
                continue;
            }

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
    use mz_repr::Datum;
    use mz_repr::adt::timestamp::CheckedTimestamp;

    use super::*;

    use ReplicaHealth::{Exempt, HealthySince, NotHydrated, NotOnline};

    #[mz_ore::test]
    fn stable_since_latest_replica() {
        let period_ms = 1000;
        let replicas = [HealthySince(100), HealthySince(500), Exempt];
        let observation = observe_stability(&replicas, 1499, period_ms);
        assert_eq!(observation.stable_for_ms, Some(999));
        assert_eq!(observation.blocked_by, Some(StabilityBlocker::WithinPeriod));
        assert_eq!(
            observe_stability(&replicas, 1500, period_ms).blocked_by,
            None
        );
    }

    #[mz_ore::test]
    fn every_replica_needs_evidence() {
        // `NotOnline` wins, because it is the more fundamental problem.
        for (replicas, blocker) in [
            (vec![], StabilityBlocker::NotOnline),
            (
                vec![HealthySince(0), NotOnline],
                StabilityBlocker::NotOnline,
            ),
            (vec![NotHydrated, NotOnline], StabilityBlocker::NotOnline),
            (
                vec![HealthySince(0), NotHydrated],
                StabilityBlocker::NotHydrated,
            ),
            (vec![Exempt, NotHydrated], StabilityBlocker::NotHydrated),
        ] {
            // Even a zero period requires evidence from every replica.
            let observation = observe_stability(&replicas, 10_000, 0);
            assert_eq!(observation.blocked_by, Some(blocker), "{replicas:?}");
            assert_eq!(observation.stable_for_ms, None, "{replicas:?}");
        }
    }

    #[mz_ore::test]
    fn replica_clock_ahead_is_clamped() {
        let observation = observe_stability(&[HealthySince(2000)], 1000, 0);
        assert_eq!(observation.stable_for_ms, Some(0));
        assert_eq!(observation.blocked_by, None);
    }

    #[mz_ore::test]
    fn hydration_answer_fails_closed() {
        let answer = |millis| {
            let time = chrono::DateTime::from_timestamp_millis(millis).expect("valid time");
            let time = CheckedTimestamp::from_timestamplike(time).expect("in range");
            vec![Row::pack_slice(&[Datum::TimestampTz(time)])]
        };
        assert_eq!(replica_health(&answer(1000)), HealthySince(1000));
        assert_eq!(replica_health(&answer(-1)), NotHydrated, "pre-epoch time");
        assert_eq!(
            replica_health(&[Row::pack_slice(&[Datum::Null])]),
            NotHydrated
        );
        assert_eq!(replica_health(&[]), NotHydrated, "nothing reported yet");
    }
}
