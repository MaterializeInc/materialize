// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! State tracked for active standing queries.
//!
//! Each standing query runs an independent handler task that processes
//! subscribe batches and delivers results. The coordinator only holds
//! a lightweight handle for forwarding subscribe batches to the task.

use std::collections::BTreeSet;

use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use timely::progress::Antichain;
use tokio::sync::{mpsc, watch};

use crate::coord::Coordinator;
use crate::standing_query_client::StandingQueryExecuteClient;

/// Lightweight handle held by the coordinator for an active standing query.
///
/// The coordinator forwards subscribe batches to the handler task via
/// `subscribe_tx`. All result matching and delivery happens in the task.
#[derive(Debug)]
pub(crate) struct ActiveStandingQuery {
    /// The CatalogItemId of this standing query.
    pub item_id: CatalogItemId,
    /// The cluster on which the standing query's dataflow runs.
    #[allow(dead_code)]
    pub cluster_id: ClusterId,
    /// GlobalIds of non-param input collections whose write frontiers
    /// determine how far the param shard's upper should advance.
    pub input_ids: BTreeSet<GlobalId>,
    /// Shared client handle (cloned to session clients via GetStandingQueryClient).
    pub client: StandingQueryExecuteClient,
    /// Channel to forward subscribe batches to the handler task.
    pub subscribe_tx: mpsc::UnboundedSender<SubscribeBatch>,
    /// Channel to tell the handler task to advance the param shard upper.
    pub advance_upper_tx: watch::Sender<Timestamp>,
}

impl Coordinator {
    /// Advance the param shard upper for all active standing queries.
    ///
    /// For each standing query, computes the minimum write frontier of its
    /// non-param input collections and sends that as the target upper to the
    /// handler task. This keeps the param shard in sync with the rest of the
    /// system without introducing a circular dependency.
    pub(crate) fn advance_standing_query_uppers(&self) {
        for asq in self.active_standing_queries.values() {
            let ids: Vec<GlobalId> = asq.input_ids.iter().cloned().collect();
            if ids.is_empty() {
                continue;
            }
            let frontiers = self
                .controller
                .storage
                .collections_frontiers(ids)
                .expect("collections must exist");
            // Compute the minimum write frontier across all non-param inputs.
            let mut min_upper = Antichain::new();
            for (_id, _since, upper) in frontiers {
                min_upper.extend(upper);
            }
            if let Some(ts) = min_upper.as_option() {
                // Lag the param shard 1s behind the input frontiers. This
                // avoids advancing the param upper too aggressively while
                // still allowing compaction to proceed.
                let target = ts.saturating_sub(Timestamp::from(1000u64));
                // Send is cheap — watch::Sender only stores the latest value.
                let _ = asq.advance_upper_tx.send(target);
            }
        }
    }
}
