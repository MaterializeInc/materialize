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
//! A candidate deployment reports "ready to promote" once its clusters have
//! caught up with the active deployment. This is independent of its authority
//! to serve SQL and write tables. A task spawned by [`Coordinator::spawn_caught_up_check_task`]
//! runs that check on an interval (see
//! [`WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL`]). We call one such run a
//! "tick", and the term is used throughout this module.
//!
//! The task reads reference progress before asking the coordinator to capture
//! local execution state. Classification and stability queries run on the task,
//! leaving the coordinator free to serve SQL and maintenance.
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

use crate::catalog::Catalog;
use differential_dataflow::lattice::Lattice as _;
use futures::{StreamExt, future};
use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK, ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK,
    WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG, WITH_0DT_CAUGHT_UP_CHECK_CUTOFF,
    WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD, WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL,
};
use mz_catalog::builtin::{MZ_CLUSTER_REPLICA_STATUS_HISTORY, MZ_REPLICA_FRONTIER_OBSERVATIONS};
use mz_catalog::durable::objects::{ClientIncarnationValue, ClientRealization};
use mz_catalog::memory::objects::{CatalogItem, Cluster};
use mz_compute_client::controller::CollectionReadiness;
use mz_compute_client::logging::{ComputeLog, LogVariant};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::OfflineReason;
use mz_ore::channel::trigger::Trigger;
use mz_ore::now::EpochMillis;
use mz_ore::task;
use mz_persist_client::read::{ListenEvent, ReadHandle, Subscribe};
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp as _};
use tokio::sync::oneshot;

use crate::PeekClient;
use crate::coord::{Coordinator, Message};

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

/// Incremental active-reference state, owned by the readiness task. Dropping
/// that task releases its reader, including after promotion or shutdown.
#[derive(Debug)]
struct ReplicaFrontierReference {
    shard: ShardId,
    reader: Option<ReadHandle<SourceData, (), Timestamp, StorageDiff>>,
    subscription: Option<Subscribe<SourceData, (), Timestamp, StorageDiff>>,
    rows: BTreeMap<Row, StorageDiff>,
    upper: Timestamp,
}

impl ReplicaFrontierReference {
    async fn open(persist: &PersistClient, metadata: &CollectionMetadata) -> Self {
        let reader = persist
            .open_leased_reader(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("caught-up replica frontier reference"),
                false,
            )
            .await
            .expect("reference source codecs");
        Self {
            shard: metadata.data_shard,
            reader: Some(reader),
            subscription: None,
            rows: BTreeMap::new(),
            upper: Timestamp::MIN,
        }
    }

    /// Consume a completed prefix without cancelling leased-part reads. Only
    /// the first refresh snapshots the shard. Later checks incorporate diffs.
    async fn refresh(&mut self, persist: &PersistClient) -> Vec<Row> {
        let target = persist
            .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                self.shard,
                Diagnostics::from_purpose("caught-up replica frontier reference"),
            )
            .await
            .expect("reference source upper")
            .into_option()
            .expect("reference source remains open");
        if let Some(reader) = &self.reader {
            if reader.since().less_equal(&target) && reader.since() != &Antichain::from_elem(target)
            {
                self.subscription = Some(
                    self.reader
                        .take()
                        .expect("reader present")
                        .subscribe(Antichain::from_elem(
                            target.step_back().expect("positive upper"),
                        ))
                        .await
                        .expect("leased since protects reference snapshot"),
                );
            } else {
                // No completed readable timestamp yet. Missing reference uses
                // the strict hydration path, never fabricated completion.
                return Vec::new();
            }
        }
        let subscription = self.subscription.as_mut().expect("reference initialized");
        while self.upper < target {
            for event in subscription.next(None).await {
                match event {
                    ListenEvent::Updates(parts) => {
                        for part in parts {
                            let fetched = subscription.fetch_batch_part(part).await;
                            for (index, ((data, ()), _, diff)) in fetched.enumerate() {
                                let row = data.0.expect("reference row");
                                let count = self.rows.entry(row.clone()).or_default();
                                *count += diff;
                                if *count == 0 {
                                    self.rows.remove(&row);
                                }
                                if index % 1024 == 0 {
                                    tokio::task::yield_now().await;
                                }
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                    ListenEvent::Progress(upper) => {
                        self.upper = upper.into_option().expect("reference remains open");
                    }
                }
            }
            tokio::task::yield_now().await;
        }
        self.rows
            .iter()
            .map(|(row, diff)| {
                assert_eq!(*diff, 1, "reference must consolidate to a set");
                row.clone()
            })
            .collect()
    }
}

/// Requests local controller state without waiting for external reads.
#[derive(Debug)]
pub struct CaughtUpCheckRequest {
    exclude_collections: Arc<BTreeSet<GlobalId>>,
    policy_read_ts: Timestamp,
    tx: oneshot::Sender<CaughtUpCheck>,
}

/// Only replica executions in the candidate and active reference deployments
/// supply readiness evidence. Other registrations must not postpone readiness.
fn readiness_executions_match<'a>(
    before: impl Iterator<Item = (&'a u64, &'a ClientIncarnationValue)>,
    after: impl Iterator<Item = (&'a u64, &'a ClientIncarnationValue)>,
    active: Option<u64>,
    candidate: u64,
) -> bool {
    let relevant = |(_, value): &(&u64, &ClientIncarnationValue)| {
        matches!(value.realization, ClientRealization::Replica(_))
            && (Some(value.deployment_generation) == active
                || value.deployment_generation == candidate)
    };
    before.filter(relevant).eq(after.filter(relevant))
}

/// Inputs owned by one background check.
#[derive(Debug)]
struct CaughtUpCheck {
    catalog: Arc<Catalog>,
    now: EpochMillis,
    clusters: BTreeMap<ClusterId, Result<ClusterSnapshot, anyhow::Error>>,
}

struct LeaderSnapshot {
    rows: Vec<Row>,
    problematic_replicas: BTreeSet<ReplicaId>,
}

#[derive(Debug)]
struct ClusterSnapshot {
    replicas: BTreeMap<ReplicaId, ReplicaTarget>,
    ignored_compute_collections: BTreeSet<GlobalId>,
    collections: Vec<(GlobalId, Antichain<Timestamp>, CollectionType)>,
    required_realizations: bool,
}

#[derive(Debug)]
enum CollectionType {
    Storage { hydrated: bool },
    Compute { hydrated: bool },
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
    /// A no-op when no deployment-readiness check was requested.
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
        let policy_oracle = self.get_local_timestamp_oracle();

        let mut catalog = self.owned_catalog();
        let generation = self.query_deploy_generation;
        let storage_collections = Arc::clone(&self.controller.storage_collections);
        let persist = self.persist_client.clone();
        task::spawn(|| "caught_up_check", async move {
            let mut reference = None;
            let mut delay = None;
            loop {
                // A slow check must not cause back-to-back retries.
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                delay = Some(period);
                let leader_revision = catalog.transient_revision();
                let leader = match LeaderSnapshot::read(
                    &catalog,
                    &*storage_collections,
                    &persist,
                    generation,
                    &mut reference,
                )
                .await
                {
                    Ok(leader) => leader,
                    Err(error) => {
                        tracing::warn!(%error, "cannot read leader caught-up state");
                        continue;
                    }
                };
                // Read reference progress first. Neither the policy timestamp nor
                // local execution evidence should age behind a slow Persist read.
                let policy_read_ts = policy_oracle.read_ts().await;
                let (tx, rx) = oneshot::channel();
                let request = CaughtUpCheckRequest {
                    exclude_collections: Arc::clone(&exclude_collections),
                    policy_read_ts,
                    tx,
                };
                // Dropping the trigger on coordinator shutdown preserves the
                // existing shutdown behavior of the readiness task.
                if internal_cmd_tx
                    .send(Message::CaughtUpCheck(request))
                    .is_err()
                {
                    return;
                }
                let Ok(check) = rx.await else {
                    return;
                };
                catalog = Arc::clone(&check.catalog);
                if catalog.transient_revision() != leader_revision {
                    continue;
                }
                let snapshot = check.run(leader);
                if gate.ready(snapshot).await && catalog.transient_revision_is_current() {
                    // Planning revisions omit execution registrations. Fetch an
                    // uncached projection so retired execution evidence cannot
                    // authorize readiness after the asynchronous stability check.
                    let Ok(current) = gate
                        .client
                        .call_coordinator(|tx| crate::coord::Command::CatalogSnapshot {
                            tx,
                            include_durable_upper: false,
                            through: None,
                        })
                        .await
                    else {
                        return;
                    };
                    let valid = current.catalog.transient_revision()
                        == catalog.transient_revision()
                        && current.catalog.state().active_deployment_generation()
                            == catalog.state().active_deployment_generation()
                        && readiness_executions_match(
                            catalog.state().client_incarnations().iter(),
                            current.catalog.state().client_incarnations().iter(),
                            catalog.state().active_deployment_generation(),
                            generation,
                        );
                    catalog = current.catalog;
                    if valid {
                        trigger.fire();
                        return;
                    }
                }
            }
        });
    }

    /// Captures candidate execution and cluster-policy state without external reads.
    pub(crate) fn handle_caught_up_check_request(&self, request: CaughtUpCheckRequest) {
        let CaughtUpCheckRequest {
            exclude_collections,
            policy_read_ts,
            tx,
        } = request;
        let clusters = self
            .catalog()
            .clusters()
            .map(|cluster| {
                (
                    cluster.id,
                    self.caught_up_cluster_snapshot(cluster, &exclude_collections, policy_read_ts),
                )
            })
            .collect();
        let _ = tx.send(CaughtUpCheck {
            catalog: self.owned_catalog(),
            now: self.now(),
            clusters,
        });
    }

    fn caught_up_cluster_snapshot(
        &self,
        cluster: &Cluster,
        exclude_collections: &BTreeSet<GlobalId>,
        policy_read_ts: Timestamp,
    ) -> Result<ClusterSnapshot, anyhow::Error> {
        let mut ignored_compute_collections = exclude_collections.clone();
        let storage_frontiers = self
            .controller
            .storage
            .active_ingestion_exports(cluster.id)
            .copied()
            .filter(|id| !id.is_transient() && !exclude_collections.contains(id))
            .map(|id| {
                let (_read_frontier, write_frontier) =
                    self.controller.storage.collection_frontiers(id)?;
                Ok::<_, anyhow::Error>((
                    id,
                    write_frontier,
                    CollectionType::Storage {
                        hydrated: self.controller.storage.collection_hydrated(id)?,
                    },
                ))
            });

        // Match start_readiness_checks: catalog definitions include exports
        // not yet installed and builtin objects absent from bound_objects.
        // Catalog introspection indexes participate through their reported
        // output and hydration, rather than the raw replica-local log IDs.
        let compute_frontiers = self.catalog().entries().filter_map(|entry| {
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
                        ignored_compute_collections.insert(mv.global_id_writes());
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
                CollectionType::Compute { hydrated },
            )))
        });

        let collections = itertools::chain(storage_frontiers, compute_frontiers)
            .collect::<Result<Vec<_>, anyhow::Error>>()?;
        Ok(ClusterSnapshot {
            replicas: self.replica_targets(cluster.id),
            ignored_compute_collections,
            collections,
            required_realizations: self.cluster_has_required_realizations(
                cluster.id,
                Some(policy_read_ts),
                self.now().into(),
            ),
        })
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
                // Native adapters observe their own QueryReady connections,
                // not the separate provisioner's backend process events.
                let online = self.query_client.as_ref().is_some_and(|client| {
                    !client
                        .replica_clients(cluster_id, Some(replica.replica_id))
                        .is_empty()
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
}

impl LeaderSnapshot {
    async fn read(
        catalog: &Catalog,
        storage_collections: &(dyn StorageCollections + Send + Sync),
        persist: &PersistClient,
        generation: u64,
        reference: &mut Option<ReplicaFrontierReference>,
    ) -> Result<Self, anyhow::Error> {
        fail::fail_point!("0dt_caught_up_read");
        let replica_frontier_item_id =
            catalog.resolve_builtin_storage_collection(&MZ_REPLICA_FRONTIER_OBSERVATIONS);
        let replica_frontier_gid = catalog
            .get_entry(&replica_frontier_item_id)
            .latest_global_id();

        // The migration guard preserves the active deployment's reference shard,
        // so a candidate never substitutes its own progress for the reference.
        let metadata = storage_collections
            .collection_metadata(replica_frontier_gid)
            .expect("reference collection metadata");
        if reference.is_none() {
            *reference = Some(ReplicaFrontierReference::open(persist, &metadata).await);
        }
        let reference = reference.as_mut().expect("reference initialized");
        assert_eq!(
            reference.shard, metadata.data_shard,
            "reference must not migrate during prewarm"
        );
        let live_frontiers = reference.refresh(persist).await;

        let problematic_replicas =
            if ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK.get(catalog.system_config().dyncfgs()) {
                Self::analyze_replica_looping(
                    catalog,
                    storage_collections,
                    generation,
                    (catalog.config().now)(),
                )
                .await?
            } else {
                BTreeSet::new()
            };
        Ok(Self {
            rows: live_frontiers,
            problematic_replicas,
        })
    }

    /// Reference progress comes only from current executions in the active deployment.
    /// Unknown output is absent, while a known empty frontier means completion.
    fn collection_frontiers(
        &self,
        active_generation: Option<u64>,
        incarnations: &imbl::OrdMap<u64, ClientIncarnationValue>,
    ) -> BTreeMap<GlobalId, Antichain<Timestamp>> {
        let live_frontiers = self
            .rows
            .iter()
            .filter_map(|row| {
                let mut iter = row.into_iter();
                let id: GlobalId = iter
                    .next()
                    .expect("object id")
                    .unwrap_str()
                    .parse()
                    .expect("id");
                let replica: ReplicaId = iter
                    .next()
                    .expect("replica id")
                    .unwrap_str()
                    .parse()
                    .expect("id");
                let generation = iter.next().expect("deployment generation").unwrap_uint64();
                let incarnation = iter.next().expect("client incarnation").unwrap_uint64();
                let current = incarnations.get(&incarnation)?;
                if Some(generation) != active_generation
                    || current.deployment_generation != generation
                    || current.realization != ClientRealization::Replica(replica)
                {
                    return None;
                }
                let _read_known = iter.next().expect("read known");
                let _read_frontier = iter.next().expect("read frontier");
                if !iter.next().expect("write known").unwrap_bool() {
                    return None;
                }
                let upper = iter.next().expect("write frontier");
                // Unknown was excluded above. NULL now means completed, not missing.
                let upper = if upper.is_null() {
                    Antichain::new()
                } else {
                    Antichain::from_elem(upper.unwrap_mz_timestamp())
                };
                Some((id, replica, upper))
            })
            .collect_vec();

        // The classification only requires each collection to be hydrated on
        // _some_ replica. The stability gate separately requires every replica
        // to have hydrated everything it hosts.
        live_frontiers
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
            .collect()
    }

    /// Analyzes replica status history to detect replicas that are
    /// crash-looping or OOM-looping.
    ///
    /// A replica is considered problematic if it has multiple OOM kills in a
    /// short-ish window.
    async fn analyze_replica_looping(
        catalog: &Catalog,
        storage_collections: &(dyn StorageCollections + Send + Sync),
        generation: u64,
        now: EpochMillis,
    ) -> Result<BTreeSet<ReplicaId>, StorageError> {
        // Look back 1 day for patterns.
        let lookback_window: u64 = Duration::from_secs(24 * 60 * 60)
            .as_millis()
            .try_into()
            .expect("fits into u64");
        let min_timestamp = now.saturating_sub(lookback_window);
        let min_timestamp_dt = mz_ore::now::to_datetime(min_timestamp);

        // Get the replica status collection GlobalId
        let replica_status_item_id =
            catalog.resolve_builtin_storage_collection(&MZ_CLUSTER_REPLICA_STATUS_HISTORY);
        let replica_status_gid = catalog
            .get_entry(&replica_status_item_id)
            .latest_global_id();

        // A local hold supplies the desired lower bound, not protection against
        // shared compaction. Select the final cut only after acquiring a Persist lease.
        let read_holds = storage_collections.acquire_read_holds(vec![replica_status_gid])?;
        let unavailable = || StorageError::ReadBeforeSince(replica_status_gid);
        let read_hold = read_holds.into_iter().next().ok_or_else(unavailable)?;

        let as_of = read_hold
            .since()
            .iter()
            .next()
            .cloned()
            .ok_or_else(unavailable)?;

        let mut replica_statuses_stream = storage_collections
            .snapshot_and_stream_at_since(replica_status_gid, as_of)
            .await?;

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
            let row_generation = iter.next().expect("missing deployment_generation");
            if row_generation != Datum::UInt64(generation) {
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

        Ok(result)
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

impl CaughtUpCheck {
    fn run(self, leader: LeaderSnapshot) -> CaughtUpSnapshot {
        let state = self.catalog.state();
        let live_collection_frontiers = leader.collection_frontiers(
            state.active_deployment_generation(),
            state.client_incarnations(),
        );

        tracing::debug!(?live_collection_frontiers, "checking re-hydration status");

        let allowed_lag =
            WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG.get(self.catalog.system_config().dyncfgs());
        let allowed_lag: u64 = allowed_lag
            .as_millis()
            .try_into()
            .expect("must fit into u64");

        let cutoff = WITH_0DT_CAUGHT_UP_CHECK_CUTOFF.get(self.catalog.system_config().dyncfgs());
        let cutoff: u64 = cutoff.as_millis().try_into().expect("must fit into u64");

        let now = self.now;

        let stability_check_enabled =
            ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK.get(self.catalog.system_config().dyncfgs());
        let stability_period =
            WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD.get(self.catalog.system_config().dyncfgs());
        // Cap rather than panic on an absurdly large configured duration. A
        // period of u64::MAX milliseconds means "effectively never auto-ready",
        // which is the safe, conservative outcome: we won't cut over on our own,
        // and an operator can still force it via skip-catchup.
        let stability_period_ms = u64::try_from(stability_period.as_millis()).unwrap_or(u64::MAX);

        let classification = self.classify_clusters(
            allowed_lag.into(),
            cutoff.into(),
            now.into(),
            &live_collection_frontiers,
            &leader.problematic_replicas,
        );
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
                            replicas: self.clusters[&cluster_id]
                                .as_ref()
                                .expect("classified cluster")
                                .replicas
                                .clone(),
                        },
                    )),
                    _ => None,
                })
                .collect()
        } else {
            BTreeMap::new()
        };

        CaughtUpSnapshot {
            now,
            all_caught_up,
            stability_check_enabled,
            stability_period_ms,
            caught_up_clusters,
        }
    }

    fn classify_clusters(
        &self,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        problematic_replicas: &BTreeSet<ReplicaId>,
    ) -> BTreeMap<ClusterId, ClusterCaughtUpStatus> {
        let mut result = BTreeMap::new();
        for (&cluster_id, cluster) in &self.clusters {
            let status = match cluster {
                Ok(cluster) => Self::collections_caught_up(
                    cluster_id,
                    cluster,
                    allowed_lag,
                    cutoff,
                    now,
                    live_frontiers,
                    problematic_replicas,
                ),
                Err(error) => {
                    tracing::error!(
                        "unexpected error while checking if cluster {} caught up: {error:#}",
                        cluster_id
                    );
                    ClusterCaughtUpStatus::NotCaughtUp
                }
            };

            if status == ClusterCaughtUpStatus::NotCaughtUp {
                // We log all non-caught-up clusters instead of breaking out early.
                tracing::info!("cluster {} is not caught up", cluster_id);
            }

            result.insert(cluster_id, status);
        }

        result
    }

    /// Classifies the given cluster for the caught-up check.
    ///
    /// See [`Self::classify_clusters`] for details.
    fn collections_caught_up(
        cluster_id: ClusterId,
        cluster: &ClusterSnapshot,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        problematic_replicas: &BTreeSet<ReplicaId>,
    ) -> ClusterCaughtUpStatus {
        if !cluster.required_realizations {
            return ClusterCaughtUpStatus::NotCaughtUp;
        }
        if cluster.replicas.is_empty() {
            return ClusterCaughtUpStatus::Ignored;
        }

        // Check if all replicas in this cluster are crash/OOM-looping. As long
        // as there is at least one healthy replica, the cluster is okay-ish.
        let cluster_has_only_problematic_replicas = cluster
            .replicas
            .keys()
            .all(|id| problematic_replicas.contains(id));
        let mut all_caught_up = true;
        let mut ignored_compute_collections = cluster.ignored_compute_collections.clone();

        for (id, write_frontier, collection_type) in &cluster.collections {
            let id = *id;
            let reference = if matches!(collection_type, CollectionType::Storage { .. }) {
                Some(write_frontier)
            } else {
                live_frontiers.get(&id)
            };
            if let Some(live_write_frontier) = reference {
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
                        cluster_id
                    );
                    return ClusterCaughtUpStatus::Ignored;
                } else if beyond_all_hope {
                    tracing::info!(
                        ?live_write_frontier,
                        ?cutoff,
                        ?now,
                        "live write frontier of collection {id} is too far behind 'now'; \
                     ignoring for caught-up checks"
                    );
                    if matches!(collection_type, CollectionType::Compute { .. }) {
                        ignored_compute_collections.insert(id);
                    }
                    continue;
                }
            }
            let collection_hydrated = match collection_type {
                CollectionType::Storage { hydrated } | CollectionType::Compute { hydrated } => {
                    *hydrated
                }
            };
            let live_write_frontier = match reference {
                Some(frontier) => frontier,
                None => {
                    // No live frontier to compare against, either because the collection didn't
                    // exist on the leader or because the leader hosts it as something
                    // replica frontier observations do not track. A table→MV conversion is the
                    // latter: it keeps the table's `GlobalId`, still a table on the leader, so the
                    // new MV lands here instead of the strong path below.
                    //
                    // Require hydration, not just a write frontier past the minimum. A fresh MV's
                    // sink reaches frontier 1 after one batch, which would otherwise look caught
                    // up mid-hydration and bring back the cut-over spike this gate prevents.
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
                        write_frontier,
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

            let readiness = CollectionReadiness::classify(
                collection_hydrated,
                write_frontier,
                Some((live_write_frontier, allowed_lag)),
            );

            // We don't expect collections to get hydrated, ingestions to be
            // started, etc. when they are already at the empty write frontier.
            if live_write_frontier.is_empty() || readiness == CollectionReadiness::Ready {
                // Carry hydration waivers into the per-replica stability check.
                if readiness != CollectionReadiness::Ready
                    && matches!(collection_type, CollectionType::Compute { .. })
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
                    %cluster_id,
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
                    %cluster_id,
                    "collection is not caught up"
                );
                all_caught_up = false;
            }
        }

        if all_caught_up {
            ClusterCaughtUpStatus::CaughtUp {
                ignored_compute_collections,
            }
        } else {
            ClusterCaughtUpStatus::NotCaughtUp
        }
    }
}

#[cfg(test)]
mod tests {
    #[mz_ore::test]
    fn reference_uses_only_known_outputs_of_current_active_replicas() {
        let replica = |generation, id| ClientIncarnationValue {
            deployment_generation: generation,
            realization: ClientRealization::Replica(ReplicaId::User(id)),
        };
        let incarnations = [
            (11_u64, replica(1, 1)),
            (12, replica(1, 2)),
            (21, replica(2, 1)),
            (
                13,
                ClientIncarnationValue {
                    deployment_generation: 1,
                    realization: ClientRealization::Adapter(mz_repr::AdapterId(1)),
                },
            ),
        ]
        .into_iter()
        .collect();
        let row = |replica, generation, incarnation, known, upper| {
            Row::pack_slice(&[
                Datum::String("u1"),
                Datum::String(replica),
                Datum::UInt64(generation),
                Datum::UInt64(incarnation),
                Datum::False,
                Datum::Null,
                Datum::from(known),
                upper,
            ])
        };
        let upper = Datum::MzTimestamp(100.into());
        for (row, expected) in [
            (
                row("u1", 1, 11, true, upper),
                Some(Antichain::from_elem(100.into())),
            ),
            (row("u1", 1, 11, true, Datum::Null), Some(Antichain::new())),
            (row("u1", 1, 11, false, Datum::Null), None),
            (row("u1", 2, 21, true, Datum::Null), None), // Candidate completion is not reference progress.
            (row("u1", 1, 10, true, Datum::Null), None), // Retired incarnation.
            (row("u1", 1, 21, true, Datum::Null), None), // Wrong registered deployment.
            (row("u2", 1, 11, true, Datum::Null), None), // Wrong replica identity.
            (row("u1", 1, 13, true, Datum::Null), None), // Not a replica execution.
        ] {
            let leader = LeaderSnapshot {
                rows: vec![row],
                problematic_replicas: BTreeSet::new(),
            };
            let expected = expected
                .into_iter()
                .map(|f| (GlobalId::User(1), f))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                leader.collection_frontiers(Some(1), &incarnations),
                expected
            );
            assert!(leader.collection_frontiers(None, &incarnations).is_empty());
        }

        let leader = LeaderSnapshot {
            rows: vec![
                row("u1", 1, 11, true, upper),
                row("u2", 1, 12, true, Datum::MzTimestamp(200.into())),
                row("u1", 2, 21, true, Datum::Null),
            ],
            problematic_replicas: BTreeSet::new(),
        };
        assert_eq!(
            leader.collection_frontiers(Some(1), &incarnations),
            BTreeMap::from([(GlobalId::User(1), Antichain::from_elem(200.into()))]),
            "reference progress joins active replicas, not the candidate"
        );
    }

    #[mz_ore::test]
    fn readiness_identity_validation_ignores_unrelated_registrations() {
        let replica = |generation| ClientIncarnationValue {
            deployment_generation: generation,
            realization: ClientRealization::Replica(ReplicaId::User(1)),
        };
        let before = BTreeMap::from([(1, replica(0)), (2, replica(1))]);
        let mut after = before.clone();
        after.insert(3, replica(2));
        assert!(readiness_executions_match(
            before.iter(),
            after.iter(),
            Some(0),
            1
        ));
        after.insert(
            4,
            ClientIncarnationValue {
                deployment_generation: 1,
                realization: ClientRealization::Adapter(mz_repr::AdapterId(1)),
            },
        );
        assert!(readiness_executions_match(
            before.iter(),
            after.iter(),
            Some(0),
            1
        ));
        for incarnation in [1, 2] {
            let mut replaced = after.clone();
            let old = replaced
                .remove(&incarnation)
                .expect("fixture contains both replica incarnations");
            replaced.insert(5, old);
            assert!(!readiness_executions_match(
                before.iter(),
                replaced.iter(),
                Some(0),
                1
            ));
        }
    }

    #[mz_ore::test]
    fn cutoff_exemptions_do_not_require_hydration() {
        let id = GlobalId::User(1);
        let replica = ReplicaId::User(1);
        for problematic in [BTreeSet::new(), BTreeSet::from([replica])] {
            let cluster = ClusterSnapshot {
                replicas: BTreeMap::from([(replica, ReplicaTarget::Ask)]),
                ignored_compute_collections: BTreeSet::new(),
                collections: vec![(
                    id,
                    Antichain::from_elem(0.into()),
                    CollectionType::Compute { hydrated: false },
                )],
                required_realizations: true,
            };
            let status = CaughtUpCheck::collections_caught_up(
                ClusterId::User(1),
                &cluster,
                0.into(),
                100.into(),
                100.into(),
                &BTreeMap::from([(id, Antichain::from_elem(0.into()))]),
                &problematic,
            );
            let expected = if problematic.is_empty() {
                ClusterCaughtUpStatus::CaughtUp {
                    ignored_compute_collections: BTreeSet::from([id]),
                }
            } else {
                ClusterCaughtUpStatus::Ignored
            };
            assert_eq!(status, expected);
        }
    }

    #[mz_ore::test]
    fn readiness_distinguishes_unknown_completed_and_missing_realizations() {
        let id = GlobalId::User(1);
        for (hydrated, reference, required_realizations, expected) in [
            (false, None, true, ClusterCaughtUpStatus::NotCaughtUp),
            (
                false,
                Some(Antichain::from_elem(100.into())),
                true,
                ClusterCaughtUpStatus::NotCaughtUp,
            ),
            (
                true,
                Some(Antichain::from_elem(100.into())),
                true,
                ClusterCaughtUpStatus::CaughtUp {
                    ignored_compute_collections: BTreeSet::new(),
                },
            ),
            (
                false,
                Some(Antichain::new()),
                true,
                ClusterCaughtUpStatus::CaughtUp {
                    ignored_compute_collections: BTreeSet::from([id]),
                },
            ),
            (
                true,
                Some(Antichain::new()),
                false,
                ClusterCaughtUpStatus::NotCaughtUp,
            ),
        ] {
            let cluster = ClusterSnapshot {
                replicas: BTreeMap::from([(ReplicaId::User(1), ReplicaTarget::Ask)]),
                ignored_compute_collections: BTreeSet::new(),
                collections: vec![(
                    id,
                    Antichain::from_elem(100.into()),
                    CollectionType::Compute { hydrated },
                )],
                required_realizations,
            };
            let reference = reference
                .into_iter()
                .map(|frontier| (id, frontier))
                .collect();
            let result = CaughtUpCheck::collections_caught_up(
                ClusterId::User(1),
                &cluster,
                0.into(),
                1000.into(),
                100.into(),
                &reference,
                &BTreeSet::new(),
            );
            assert_eq!(result, expected);
        }
    }

    #[mz_ore::test]
    fn readiness_requires_hydration_and_bounded_lag_with_or_without_reference() {
        let id = GlobalId::User(1);
        for reference in [
            BTreeMap::new(),
            BTreeMap::from([(id, Antichain::from_elem(100.into()))]),
        ] {
            for (hydrated, upper, ready) in [
                (false, 100, false),
                (true, 0, false),
                (true, 1, false),
                (true, 89, false),
                (true, 90, true),
                (true, 100, true),
            ] {
                let cluster = ClusterSnapshot {
                    replicas: BTreeMap::from([(ReplicaId::User(1), ReplicaTarget::Ask)]),
                    ignored_compute_collections: BTreeSet::new(),
                    collections: vec![(
                        id,
                        Antichain::from_elem(upper.into()),
                        CollectionType::Compute { hydrated },
                    )],
                    required_realizations: true,
                };
                let result = CaughtUpCheck::collections_caught_up(
                    ClusterId::User(1),
                    &cluster,
                    10.into(),
                    50.into(),
                    100.into(),
                    &reference,
                    &BTreeSet::new(),
                );
                let expected = if ready {
                    ClusterCaughtUpStatus::CaughtUp {
                        ignored_compute_collections: BTreeSet::new(),
                    }
                } else {
                    ClusterCaughtUpStatus::NotCaughtUp
                };
                assert_eq!(
                    result, expected,
                    "hydrated={hydrated}, upper={upper}, reference={reference:?}"
                );
            }
        }
    }

    #[mz_ore::test]
    fn empty_cluster_is_ignored_only_when_policy_requires_no_realizations() {
        for required_realizations in [false, true] {
            let cluster = ClusterSnapshot {
                replicas: BTreeMap::new(),
                ignored_compute_collections: BTreeSet::new(),
                collections: Vec::new(),
                required_realizations,
            };
            let result = CaughtUpCheck::collections_caught_up(
                ClusterId::User(1),
                &cluster,
                0.into(),
                100.into(),
                100.into(),
                &BTreeMap::new(),
                &BTreeSet::new(),
            );
            assert_eq!(
                result,
                if required_realizations {
                    ClusterCaughtUpStatus::Ignored
                } else {
                    ClusterCaughtUpStatus::NotCaughtUp
                }
            );
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn reference_incorporates_peer_updates_and_empty_progress() {
        use mz_persist_client::PersistLocation;

        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let desc = MZ_REPLICA_FRONTIER_OBSERVATIONS.desc.clone();
        let metadata = CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: "mem://".parse().expect("blob URI"),
                consensus_uri: "mem://".parse().expect("consensus URI"),
            },
            data_shard: shard,
            relation_desc: desc.clone(),
            txns_shard: None,
        };
        let mut a = persist
            .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("first writer");
        let mut b = persist
            .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(desc),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("peer writer");
        let mut reference = ReplicaFrontierReference::open(&persist, &metadata).await;
        assert!(reference.refresh(&persist).await.is_empty());
        let row = |upper: u64| {
            Row::pack_slice(&[
                Datum::String("u1"),
                Datum::String("u1"),
                Datum::UInt64(1),
                Datum::UInt64(11),
                Datum::True,
                Timestamp::from(5).into(),
                Datum::True,
                Timestamp::from(upper).into(),
            ])
        };
        let first = row(10);
        a.compare_and_append(
            vec![((SourceData(Ok(first.clone())), ()), Timestamp::MIN, 1)],
            Antichain::from_elem(Timestamp::MIN),
            Antichain::from_elem(1.into()),
        )
        .await
        .expect("valid first append")
        .expect("first upper matches");
        assert_eq!(reference.refresh(&persist).await, vec![first.clone()]);
        let second = row(20);
        b.compare_and_append(
            vec![
                ((SourceData(Ok(first)), ()), Timestamp::from(1), -1),
                ((SourceData(Ok(second.clone())), ()), Timestamp::from(1), 1),
            ],
            Antichain::from_elem(1.into()),
            Antichain::from_elem(2.into()),
        )
        .await
        .expect("valid peer append")
        .expect("peer upper matches");
        assert_eq!(reference.refresh(&persist).await, vec![second.clone()]);
        a.compare_and_append(
            Vec::<((SourceData, ()), Timestamp, StorageDiff)>::new(),
            Antichain::from_elem(2.into()),
            Antichain::from_elem(3.into()),
        )
        .await
        .expect("valid empty advancement")
        .expect("empty advancement upper matches");
        assert_eq!(reference.refresh(&persist).await, vec![second.clone()]);
        assert_eq!(reference.upper, Timestamp::from(3));
        // A new readiness task replays the current relation, not predecessor rows.
        drop(reference);
        let mut replay = ReplicaFrontierReference::open(&persist, &metadata).await;
        assert_eq!(replay.refresh(&persist).await, vec![second]);
    }

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
