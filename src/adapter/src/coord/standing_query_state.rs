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

use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_repr::{CatalogItemId, Timestamp};
use tokio::sync::{mpsc, watch};
use tracing::debug;

use crate::coord::Coordinator;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timestamp_selection::TimestampProvider;
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
    /// Non-param input collections (storage + compute) whose write frontiers
    /// determine how far the param shard's upper should advance.
    pub input_ids: CollectionIdBundle,
    /// Shared client handle (cloned to session clients via GetStandingQueryClient).
    pub client: StandingQueryExecuteClient,
    /// Channel to forward subscribe batches to the handler task.
    pub subscribe_tx: mpsc::UnboundedSender<SubscribeBatch>,
    /// Channel to tell the handler task to advance the param shard upper.
    pub advance_upper_tx: watch::Sender<Timestamp>,
    /// The initial upper target (as_of + 1). We never lag below this to
    /// avoid reading at timestamps before the inputs had any data.
    pub initial_upper: Timestamp,
}

impl Coordinator {
    /// Advance the param shard upper for all active standing queries.
    ///
    /// For each standing query, computes the least valid write timestamp across
    /// its non-param input collections (both storage and compute) and sends
    /// that as the target upper to the batcher task. This keeps the param
    /// shard in sync with the rest of the system.
    ///
    /// For standing queries with no input dependencies (constant queries),
    /// uses the coordinator's current write timestamp instead.
    pub(crate) async fn advance_standing_query_uppers(&self) {
        for asq in self.active_standing_queries.values() {
            let ts = if asq.input_ids.is_empty() {
                self.peek_local_write_ts().await
            } else {
                let upper = self.least_valid_write(&asq.input_ids);
                match upper.as_option() {
                    Some(ts) => *ts,
                    None => continue,
                }
            };
            // Send a target that trails the input frontier by 1 second. The
            // batcher advances the param shard to this target, then writes
            // at that timestamp. By writing below the table's current upper,
            // the subscribe can resolve immediately (it needs all inputs
            // past the write timestamp, and the table is already 1s ahead).
            //
            // The 1s gap gives ~500 batches of headroom (2 timestamps per
            // batch, ~1000 timestamps per second). Without this gap, the
            // batcher would write at the table's exact upper, forcing the
            // subscribe to wait for the next AdvanceTimelines tick (~1s).
            //
            // Clamp to the initial upper so we never read at timestamps
            // before the inputs had any data (e.g. right after CREATE).
            let target = ts.saturating_sub(1000).max(asq.initial_upper);
            debug!(item_id = ?asq.item_id, %target, input_frontier = %ts, "advance upper");
            let _ = asq.advance_upper_tx.send(target);
        }
    }
}
