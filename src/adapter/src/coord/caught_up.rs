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
use mz_ore::channel::trigger::Trigger;
use mz_repr::{GlobalId, Timestamp};
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
        let enable_caught_up_check =
            ENABLE_0DT_CAUGHT_UP_CHECK.get(self.catalog().system_config().dyncfgs());

        if enable_caught_up_check {
            self.maybe_check_caught_up_new().await
        } else {
            self.maybe_check_caught_up_legacy()
        }
    }

    async fn maybe_check_caught_up_new(&mut self) {
        let Some(ctx) = &self.caught_up_check else {
            return;
        };

        let replica_frontier_collection_id = self
            .catalog()
            .resolve_builtin_storage_collection(&MZ_CLUSTER_REPLICA_FRONTIERS);

        let live_frontiers = self
            .controller
            .storage
            .snapshot_latest(replica_frontier_collection_id)
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

        let compute_caught_up = self.controller.compute.clusters_caught_up(
            allowed_lag.into(),
            cutoff.into(),
            now.into(),
            &live_collection_frontiers,
            &ctx.exclude_collections,
        );

        tracing::info!(%compute_caught_up, "checked caught-up status of collections");

        if compute_caught_up {
            let ctx = self.caught_up_check.take().expect("known to exist");
            ctx.trigger.fire();
        }
    }

    fn maybe_check_caught_up_legacy(&mut self) {
        let Some(ctx) = &self.caught_up_check else {
            return;
        };

        let compute_hydrated = self
            .controller
            .compute
            .clusters_hydrated(&ctx.exclude_collections);
        tracing::info!(%compute_hydrated, "checked hydration status of clusters");

        if compute_hydrated {
            let ctx = self.caught_up_check.take().expect("known to exist");
            ctx.trigger.fire();
        }
    }
}
