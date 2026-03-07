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

use crate::ExecuteContext;
use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;

impl Coordinator {
    /// Sequence an EXECUTE STANDING QUERY plan.
    ///
    /// This is the coordinator fallback path. The primary execution path
    /// bypasses the coordinator entirely via [`StandingQueryExecuteClient`](crate::standing_query_client::StandingQueryExecuteClient).
    /// This fallback handles the case where frontend execution is not available.
    #[instrument]
    pub(crate) async fn sequence_execute_standing_query(
        &self,
        ctx: ExecuteContext,
        plan: plan::ExecuteStandingQueryPlan,
    ) -> Result<(), (AdapterError, ExecuteContext)> {
        let plan::ExecuteStandingQueryPlan { id, params } = plan;

        // Find the active standing query's shared client.
        let sq_client = self
            .active_standing_queries
            .iter()
            .find(|(_, asq)| asq.item_id == id)
            .map(|(_, asq)| asq.client.clone());

        let Some(sq_client) = sq_client else {
            return Err((
                AdapterError::Unsupported("standing query is not active (SUBSCRIBE not running)"),
                ctx,
            ));
        };

        // Execute off the coordinator loop via the shared client.
        let params_clone = params.clone();
        mz_ore::task::spawn(|| "standing-query-execute-fallback", async move {
            match sq_client.execute(&params_clone).await {
                Ok(rows) => {
                    use mz_repr::IntoRowIterator;
                    ctx.retire(Ok(crate::command::ExecuteResponse::SendingRowsImmediate {
                        rows: Box::new(rows.into_row_iter()),
                    }));
                }
                Err(e) => {
                    ctx.retire(Err(AdapterError::Internal(e.to_string())));
                }
            }
        });

        Ok(())
    }

    /// EXPLAIN EXECUTE STANDING QUERY
    ///
    /// Produces a simple textual explanation showing which standing query is
    /// being executed and what parameter values are being passed.
    pub(crate) fn explain_execute_standing_query(
        &self,
        plan::ExplainPlanPlan {
            format: _,
            explainee,
            ..
        }: plan::ExplainPlanPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::Explainee::Statement(stmt) = explainee else {
            unreachable!()
        };
        let plan::ExplaineeStatement::ExecuteStandingQuery { plan, .. } = stmt else {
            unreachable!()
        };

        let plan::ExecuteStandingQueryPlan { id, params } = plan;

        let entry = self.catalog().get_entry(&id);
        let full_name = self
            .catalog()
            .resolve_full_name(entry.name(), entry.conn_id());

        let mut output = format!("Execute Standing Query {}\n", full_name);
        if params.is_empty() {
            output.push_str("  Parameters: (none)\n");
        } else {
            output.push_str("  Parameters:\n");
            for (i, (row, typ)) in params.iter().enumerate() {
                let datum = row.unpack_first();
                output.push_str(&format!("    ${}: {:?} = {}\n", i + 1, typ, datum));
            }
        }

        let rows = vec![Row::pack_slice(&[mz_repr::Datum::String(&output)])];
        Ok(Self::send_immediate_rows(rows))
    }

    /// Returns the [`StandingQueryExecuteClient`](crate::standing_query_client::StandingQueryExecuteClient) for a standing query, if active.
    ///
    /// Used by the session client to execute standing queries off the coordinator.
    pub(crate) fn standing_query_client(
        &self,
        item_id: mz_repr::CatalogItemId,
    ) -> Option<crate::standing_query_client::StandingQueryExecuteClient> {
        self.active_standing_queries
            .values()
            .find(|asq| asq.item_id == item_id)
            .map(|asq| asq.client.clone())
    }
}

// Removed: flush_all_standing_query_batches, flush_standing_query_batch
// Param writes now happen off the coordinator via StandingQueryExecuteClient.
// The coordinator only learns about writes through flush notifications
// (drained in the subscribe handler).
