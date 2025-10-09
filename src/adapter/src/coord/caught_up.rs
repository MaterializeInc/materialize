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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use differential_dataflow::lattice::Lattice as _;
use futures::StreamExt;
use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_CAUGHT_UP_REPLICA_STATUS_CHECK, WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG,
    WITH_0DT_CAUGHT_UP_CHECK_CUTOFF,
};
use mz_catalog::builtin::{MZ_CLUSTER_REPLICA_FRONTIERS, MZ_CLUSTER_REPLICA_STATUS_HISTORY};
use mz_catalog::memory::objects::Cluster;
use mz_controller_types::ReplicaId;
use mz_orchestrator::OfflineReason;
use mz_ore::channel::trigger::Trigger;
use mz_ore::now::EpochMillis;
use mz_repr::{GlobalId, Timestamp};
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp as _};

use crate::coord::Coordinator;

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

impl Coordinator {
    /// Checks that all clusters/collections are caught up. If so, this will
    /// trigger `self.catchup_check.trigger`.
    ///
    /// This method is a no-op when the trigger has already been fired.
    pub async fn maybe_check_caught_up(&mut self) {
        let Some(ctx) = &self.caught_up_check else {
            return;
        };

        let replica_frontier_item_id = self
            .catalog()
            .resolve_builtin_storage_collection(&MZ_CLUSTER_REPLICA_FRONTIERS);
        let replica_frontier_gid = self
            .catalog()
            .get_entry(&replica_frontier_item_id)
            .latest_global_id();

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

        let caught_up = self
            .clusters_caught_up(
                allowed_lag.into(),
                cutoff.into(),
                now.into(),
                &live_collection_frontiers,
                &ctx.exclude_collections,
                &problematic_replicas,
            )
            .await;

        tracing::info!(%caught_up, "checked caught-up status of collections");

        if caught_up {
            let ctx = self.caught_up_check.take().expect("known to exist");
            ctx.trigger.fire();
        }
    }

    /// Returns whether all clusters are considered caught-up.
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
    /// For this check, zero-replica clusters are always considered caught up. Their collections
    /// would never normally be considered caught up but it's clearly intentional that they have no
    /// replicas.
    async fn clusters_caught_up(
        &self,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        exclude_collections: &BTreeSet<GlobalId>,
        problematic_replicas: &BTreeSet<ReplicaId>,
    ) -> bool {
        let mut result = true;
        for cluster in self.catalog().clusters() {
            let caught_up = self
                .collections_caught_up(
                    cluster,
                    allowed_lag.clone(),
                    cutoff.clone(),
                    now.clone(),
                    live_frontiers,
                    exclude_collections,
                    problematic_replicas,
                )
                .await;

            let caught_up = caught_up.unwrap_or_else(|e| {
                tracing::error!(
                    "unexpected error while checking if cluster {} caught up: {e:#}",
                    cluster.id
                );
                false
            });

            if !caught_up {
                result = false;

                // We continue with our loop instead of breaking out early, so
                // that we log all non-caught up clusters.
                tracing::info!("cluster {} is not caught up", cluster.id);
            }
        }

        result
    }

    /// Returns whether the given cluster is considered caught-up.
    ///
    /// See [`Coordinator::clusters_caught_up`] for details.
    async fn collections_caught_up(
        &self,
        cluster: &Cluster,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        exclude_collections: &BTreeSet<GlobalId>,
        problematic_replicas: &BTreeSet<ReplicaId>,
    ) -> Result<bool, anyhow::Error> {
        if cluster.replicas().next().is_none() {
            return Ok(true);
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
                return Ok(true);
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

        Ok(all_caught_up)
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
