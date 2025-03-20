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

use differential_dataflow::lattice::Lattice as _;
use itertools::Itertools;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_CAUGHT_UP_CHECK, WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG,
    WITH_0DT_CAUGHT_UP_CHECK_CUTOFF,
};
use mz_catalog::builtin::MZ_CLUSTER_REPLICA_FRONTIERS;
use mz_catalog::memory::objects::Cluster;
use mz_ore::channel::trigger::Trigger;
use mz_repr::{GlobalId, Timestamp};
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;

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
        let enable_caught_up_check =
            ENABLE_0DT_CAUGHT_UP_CHECK.get(self.catalog().system_config().dyncfgs());

        if enable_caught_up_check {
            self.maybe_check_caught_up_new().await
        } else {
            self.maybe_check_caught_up_legacy().await
        }
    }

    async fn maybe_check_caught_up_new(&mut self) {
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

        let compute_caught_up = self
            .clusters_caught_up(
                allowed_lag.into(),
                cutoff.into(),
                now.into(),
                &live_collection_frontiers,
                &ctx.exclude_collections,
            )
            .await;

        tracing::info!(%compute_caught_up, "checked caught-up status of collections");

        if compute_caught_up {
            let ctx = self.caught_up_check.take().expect("known to exist");
            ctx.trigger.fire();
        }
    }

    /// Returns `true` if all non-transient, non-excluded collections have their write
    /// frontier (aka. upper) within `allowed_lag` of the "live" frontier
    /// reported in `live_frontiers`. The "live" frontiers are frontiers as
    /// reported by a currently running `environmentd` deployment, during a 0dt
    /// upgrade.
    ///
    /// Collections whose write frontier is behind `now` by more than the cutoff
    /// are ignored.
    ///
    /// For this check, zero-replica clusters are always considered caught up.
    /// Their collections would never normally be considered caught up but it's
    /// clearly intentional that they have no replicas.
    async fn clusters_caught_up(
        &self,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        exclude_collections: &BTreeSet<GlobalId>,
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

    /// Returns `true` if all non-transient, non-excluded collections have their write
    /// frontier (aka. upper) within `allowed_lag` of the "live" frontier
    /// reported in `live_frontiers`. The "live" frontiers are frontiers as
    /// reported by a currently running `environmentd` deployment, during a 0dt
    /// upgrade.
    ///
    /// Collections whose write frontier is behind `now` by more than the cutoff
    /// are ignored.
    ///
    /// This also returns `true` in case this cluster does not have any
    /// replicas.
    async fn collections_caught_up(
        &self,
        cluster: &Cluster,
        allowed_lag: Timestamp,
        cutoff: Timestamp,
        now: Timestamp,
        live_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
        exclude_collections: &BTreeSet<GlobalId>,
    ) -> Result<bool, anyhow::Error> {
        if cluster.replicas().next().is_none() {
            return Ok(true);
        }

        enum CollectionType {
            Storage,
            Compute,
        }

        let mut all_caught_up = true;

        let storage_frontiers = self
            .controller
            .storage
            .active_ingestions(cluster.id)
            .iter()
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

            if beyond_all_hope {
                tracing::info!(?live_write_frontier, ?now, "live write frontier of collection {id} is too far behind 'now', ignoring for caught-up checks");
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

    async fn maybe_check_caught_up_legacy(&mut self) {
        let Some(ctx) = &self.caught_up_check else {
            return;
        };

        let compute_hydrated = self
            .controller
            .compute
            .clusters_hydrated(&ctx.exclude_collections)
            .await;
        tracing::info!(%compute_hydrated, "checked hydration status of clusters");

        if compute_hydrated {
            let ctx = self.caught_up_check.take().expect("known to exist");
            ctx.trigger.fire();
        }
    }
}
