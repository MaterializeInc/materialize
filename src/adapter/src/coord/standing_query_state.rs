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
}

impl Coordinator {
    /// Advance the param shard upper for all active standing queries.
    ///
    /// For each standing query, computes the least valid write timestamp across
    /// its non-param input collections (both storage and compute) and sends
    /// that as the target upper to the batcher task. This keeps the param
    /// shard in sync with the rest of the system.
    pub(crate) fn advance_standing_query_uppers(&self) {
        for asq in self.active_standing_queries.values() {
            if asq.input_ids.is_empty() {
                continue;
            }
            let upper = self.least_valid_write(&asq.input_ids);
            if let Some(ts) = upper.as_option() {
                // Send is cheap — watch::Sender only stores the latest value.
                let _ = asq.advance_upper_tx.send(*ts);
            }
        }
    }
}
