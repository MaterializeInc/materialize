// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::instrument;
use mz_repr::{Diff, GlobalId, Row};
use mz_sql::plan;
use mz_storage_client::client::TableData;
use tracing::debug;
use uuid::Uuid;

use crate::ExecuteContext;
use crate::coord::Coordinator;
use crate::coord::standing_query_state::PendingRequest;
use crate::error::AdapterError;

impl Coordinator {
    /// Sequence an EXECUTE STANDING QUERY plan.
    ///
    /// This enqueues the request into the standing query's batch buffer,
    /// then flushes the batch by writing param rows to the param collection.
    /// The ExecuteContext is NOT retired here — it stays open until results
    /// arrive from the subscribe handler.
    ///
    /// On error, returns both the error and the ExecuteContext so the caller
    /// can retire it.
    #[instrument]
    pub(crate) async fn sequence_execute_standing_query(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::ExecuteStandingQueryPlan,
    ) -> Result<(), (AdapterError, ExecuteContext)> {
        let plan::ExecuteStandingQueryPlan { id, params } = plan;

        // Find the active standing query state.
        let subscribe_sink_id = self
            .active_standing_queries
            .iter()
            .find(|(_, asq)| asq.item_id == id)
            .map(|(sink_id, _)| *sink_id);

        let Some(subscribe_sink_id) = subscribe_sink_id else {
            return Err((
                AdapterError::Unsupported("standing query is not active (SUBSCRIBE not running)"),
                ctx,
            ));
        };

        // Generate a unique request ID.
        let request_id = Uuid::new_v4();

        // Build the parameter row: (request_id, param_1, param_2, ...).
        let mut param_row = Row::default();
        {
            let mut packer = param_row.packer();
            packer.push(mz_repr::Datum::Uuid(request_id));
            for (value, _typ) in &params {
                // Each param Row contains a single datum.
                packer.push(value.unpack_first());
            }
        }

        // Enqueue the request.
        let asq = self
            .active_standing_queries
            .get_mut(&subscribe_sink_id)
            .expect("just looked this up");
        asq.batch_buffer.push(PendingRequest {
            request_id,
            param_row,
        });
        asq.request_map.insert(request_id, ctx);

        // Flush the batch immediately.
        self.flush_standing_query_batch(subscribe_sink_id).await;

        Ok(())
    }

    /// Flush the batch buffer for a standing query by writing param rows
    /// to the param collection.
    ///
    /// This obtains a write timestamp, writes all pending param rows via
    /// `append_table`, records which request_ids are in-flight at that
    /// timestamp, and applies the local write.
    async fn flush_standing_query_batch(&mut self, sink_id: GlobalId) {
        let asq = self
            .active_standing_queries
            .get_mut(&sink_id)
            .expect("standing query must exist");

        if asq.batch_buffer.is_empty() {
            return;
        }

        let param_collection_id = asq.param_collection_id;

        // Drain the batch buffer.
        let pending: Vec<PendingRequest> = asq.batch_buffer.drain(..).collect();
        let request_ids: Vec<Uuid> = pending.iter().map(|p| p.request_id).collect();

        // Save param rows for later retraction and build table data.
        let mut rows: Vec<(Row, Diff)> = Vec::with_capacity(pending.len());
        for p in pending {
            asq.param_rows.insert(p.request_id, p.param_row.clone());
            rows.push((p.param_row, Diff::from(1i64)));
        }

        debug!(
            "standing query {sink_id}: flushing {} param rows to {}",
            rows.len(),
            param_collection_id,
        );

        // Get a write timestamp.
        let write_ts = self.get_local_write_ts().await;
        let timestamp = write_ts.timestamp;
        let advance_to = write_ts.advance_to;

        // Record in-flight request_ids at this timestamp.
        let asq = self
            .active_standing_queries
            .get_mut(&sink_id)
            .expect("standing query must exist");
        asq.in_flight
            .entry(timestamp)
            .or_default()
            .extend(request_ids);

        // Write to the param collection.
        let appends = vec![(param_collection_id, vec![TableData::Rows(rows)])];
        let append_fut = self
            .controller
            .storage
            .append_table(timestamp, advance_to, appends)
            .expect("invalid updates");

        // Apply the local write to advance the timestamp oracle.
        let apply_write_fut = self.apply_local_write(timestamp);

        // Wait for the append to complete, then apply the write.
        match append_fut.await {
            Ok(result) => {
                result.unwrap_or_else(|e| {
                    tracing::warn!("standing query {sink_id}: append failed: {e}");
                });
            }
            Err(_) => {
                tracing::warn!("standing query {sink_id}: append channel dropped");
            }
        }

        apply_write_fut.await;

        debug!("standing query {sink_id}: flush complete at ts {timestamp}");
    }
}
