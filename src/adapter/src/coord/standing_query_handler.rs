// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Handler for standing query subscribe batches.
//!
//! When the standing query's dataflow (a subscribe sink) produces batches,
//! this handler:
//! 1. Extracts `request_id` from the first column of each result row.
//! 2. Buffers rows per request_id in the standing query's result_buffer.
//! 3. When the subscribe frontier advances past a write timestamp T,
//!    delivers results for all request_ids written at T.
//! 4. Retracts delivered param rows from the param collection.

use mz_compute_client::protocol::response::SubscribeBatch;
use mz_repr::IntoRowIterator;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_client::client::TableData;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;

impl Coordinator {
    /// Handle a subscribe batch from a standing query's dataflow.
    ///
    /// Processes the batch, delivers completed results, and retracts
    /// delivered param rows from the param collection.
    pub(crate) async fn handle_standing_query_subscribe_batch(
        &mut self,
        sink_id: GlobalId,
        batch: SubscribeBatch,
    ) {
        let needs_retraction = self.process_standing_query_batch(sink_id, batch);

        if needs_retraction {
            self.flush_standing_query_retractions(sink_id).await;
        }
    }

    /// Process a subscribe batch: buffer results and deliver completed requests.
    ///
    /// Returns true if there are pending retractions to flush.
    fn process_standing_query_batch(&mut self, sink_id: GlobalId, batch: SubscribeBatch) -> bool {
        let SubscribeBatch {
            lower: _,
            upper,
            updates,
        } = batch;

        let Some(asq) = self.active_standing_queries.get_mut(&sink_id) else {
            return false;
        };

        // Process updates: buffer positive diffs per request_id.
        match updates {
            Ok(rows) => {
                for (_ts, row, diff) in rows {
                    if !diff.is_positive() {
                        // Negative diffs are retractions (from param deletions).
                        // Ignore them — the request was already fulfilled.
                        continue;
                    }

                    // The first column is request_id (UUID).
                    let mut datums = row.iter();
                    let request_id = match datums.next() {
                        Some(Datum::Uuid(id)) => id,
                        other => {
                            warn!(
                                "standing query {sink_id}: expected UUID request_id, got {:?}",
                                other
                            );
                            continue;
                        }
                    };

                    // Build a result row without the request_id column.
                    let result_row = {
                        let remaining: Vec<Datum> = datums.collect();
                        let mut row = Row::default();
                        row.packer().extend(remaining.iter());
                        row
                    };

                    debug!("standing query {sink_id}: buffering result for request {request_id}");
                    asq.result_buffer
                        .entry(request_id)
                        .or_default()
                        .push(result_row);
                }
            }
            Err(err) => {
                warn!("standing query {sink_id}: subscribe error: {err}");
                // TODO: Error all in-flight requests.
                return false;
            }
        }

        // Check if the frontier has advanced past any in-flight timestamps.
        // When upper > T, all results at T have been delivered.
        let completed_timestamps: Vec<Timestamp> = asq
            .in_flight
            .keys()
            .copied()
            .take_while(|ts| !upper.less_equal(ts))
            .collect();

        let mut had_deliveries = false;
        for ts in completed_timestamps {
            if let Some(request_ids) = asq.in_flight.remove(&ts) {
                for request_id in request_ids {
                    let results = asq.result_buffer.remove(&request_id).unwrap_or_default();
                    if let Some(ctx) = asq.request_map.remove(&request_id) {
                        debug!(
                            "standing query {sink_id}: delivering {} rows for request {request_id}",
                            results.len()
                        );
                        Self::deliver_standing_query_results(ctx, results);
                    }
                    // Queue retraction for the param row.
                    if let Some(param_row) = asq.param_rows.remove(&request_id) {
                        asq.pending_retractions.push(param_row);
                        had_deliveries = true;
                    }
                }
            }
        }

        had_deliveries
    }

    fn deliver_standing_query_results(ctx: crate::ExecuteContext, rows: Vec<Row>) {
        ctx.retire(Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows.into_row_iter()),
        }));
    }

    /// Flush pending retractions for a standing query by writing diff=-1 rows
    /// to the param collection.
    async fn flush_standing_query_retractions(&mut self, sink_id: GlobalId) {
        let asq = self
            .active_standing_queries
            .get_mut(&sink_id)
            .expect("standing query must exist");

        if asq.pending_retractions.is_empty() {
            return;
        }

        let param_collection_id = asq.param_collection_id;
        let retractions: Vec<Row> = asq.pending_retractions.drain(..).collect();

        let rows: Vec<(Row, Diff)> = retractions
            .into_iter()
            .map(|row| (row, Diff::from(-1i64)))
            .collect();

        debug!(
            "standing query {sink_id}: retracting {} param rows from {}",
            rows.len(),
            param_collection_id,
        );

        // Get a write timestamp for the retraction.
        let write_ts = self.get_local_write_ts().await;
        let timestamp = write_ts.timestamp;
        let advance_to = write_ts.advance_to;

        let appends = vec![(param_collection_id, vec![TableData::Rows(rows)])];
        let append_fut = self
            .controller
            .storage
            .append_table(timestamp, advance_to, appends)
            .expect("invalid updates");

        let apply_write_fut = self.apply_local_write(timestamp);

        match append_fut.await {
            Ok(result) => {
                result.unwrap_or_else(|e| {
                    tracing::warn!("standing query {sink_id}: retraction append failed: {e}");
                });
            }
            Err(_) => {
                tracing::warn!("standing query {sink_id}: retraction append channel dropped");
            }
        }

        apply_write_fut.await;

        debug!("standing query {sink_id}: retraction flush complete at ts {timestamp}");
    }

    fn handle_standing_query_dropped(&mut self, sink_id: GlobalId) {
        let Some(asq) = self.active_standing_queries.get_mut(&sink_id) else {
            return;
        };

        // Error all pending requests.
        let request_ids: Vec<Uuid> = asq.request_map.keys().copied().collect();
        for request_id in request_ids {
            if let Some(ctx) = asq.request_map.remove(&request_id) {
                ctx.retire(Err(crate::AdapterError::Unsupported(
                    "standing query subscribe was dropped",
                )));
            }
        }
        asq.in_flight.clear();
        asq.result_buffer.clear();
    }
}
