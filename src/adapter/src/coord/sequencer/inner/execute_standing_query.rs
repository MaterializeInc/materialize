// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::instrument;
use mz_repr::Row;
use mz_sql::plan;
use uuid::Uuid;

use crate::ExecuteContext;
use crate::coord::Coordinator;
use crate::coord::standing_query_state::PendingRequest;
use crate::error::AdapterError;

impl Coordinator {
    /// Sequence an EXECUTE STANDING QUERY plan.
    ///
    /// This enqueues the request into the standing query's batch buffer.
    /// The ExecuteContext is NOT retired here — it stays open until results
    /// arrive from the SUBSCRIBE reader.
    ///
    /// On error, returns both the error and the ExecuteContext so the caller
    /// can retire it.
    #[instrument]
    pub(crate) fn sequence_execute_standing_query(
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

        // TODO: Trigger a batch flush (write params to storage, track timestamps).
        // For now, the flush will be triggered by a timer or message.

        Ok(())
    }
}
