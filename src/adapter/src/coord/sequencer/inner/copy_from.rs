// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_adapter_types::connection::ConnectionId;
use mz_ore::cast::CastInto;
use mz_persist_client::batch::ProtoBatch;
use mz_pgcopy::CopyFormatParams;
use mz_repr::table::TableData;
use mz_repr::{CatalogItemId, Datum, RowArena};
use mz_sql::plan::{self, CopyFromSource, HirScalarExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::oneshot_sources::OneshotIngestionRequest;
use smallvec::SmallVec;
use url::Url;

use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{Coordinator, TargetCluster};
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::session::{TransactionOps, WriteOp};
use crate::{AdapterError, ExecuteContext, ExecuteResponse};

impl Coordinator {
    pub(crate) async fn sequence_copy_from(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CopyFromPlan,
        target_cluster: TargetCluster,
    ) {
        let plan::CopyFromPlan {
            id,
            source,
            columns,
            params,
        } = plan;

        let from_expr = match source {
            CopyFromSource::Url(from_expr) => from_expr,
            CopyFromSource::Stdin => {
                unreachable!("COPY FROM STDIN should be handled elsewhere")
            }
        };

        let eval_url = |from: HirScalarExpr| -> Result<Url, AdapterError> {
            let style = ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable,
                session: ctx.session(),
                catalog_state: self.catalog().state(),
            };
            let mut from = from.lower_uncorrelated()?;
            prep_scalar_expr(&mut from, style)?;

            let temp_storage = RowArena::new();
            let eval_result = from.eval(&[], &temp_storage)?;

            if eval_result == Datum::Null {
                coord_bail!("COPY FROM target value cannot be NULL");
            }
            let eval_string = match eval_result {
                Datum::Null => coord_bail!("COPY FROM target value cannot be NULL"),
                Datum::String(url_str) => url_str,
                other => coord_bail!("programming error! COPY FROM target cannot be {other}"),
            };

            Url::parse(eval_string)
                .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!("{err}")))
        };
        let url = return_if_err!(eval_url(from_expr), ctx);

        let dest_table = self
            .catalog()
            .get_entry(&id)
            .table()
            .expect("TODO SHOULD BE A TABLE");

        let collection_id = dest_table.global_id_writes();
        let (_, ingestion_id) = self.transient_id_gen.allocate_id();

        let format = match params {
            CopyFormatParams::Csv(_) => mz_storage_types::oneshot_sources::ContentFormat::Csv,
            CopyFormatParams::Parquet => mz_storage_types::oneshot_sources::ContentFormat::Parquet,
            CopyFormatParams::Text(_) | CopyFormatParams::Binary => {
                ctx.retire(Err(AdapterError::Unsupported(
                    "text or binary in COPY FROM",
                )));
                return;
            }
        };
        let request = OneshotIngestionRequest {
            source: mz_storage_types::oneshot_sources::ContentSource::Http { url },
            format,
        };

        let cluster_id = self
            .catalog()
            .resolve_target_cluster(target_cluster, ctx.session())
            .expect("TODO do this in planning")
            .id;

        // When we finish staging the Batches in Persist, we'll send a command
        // to the Coordinator.
        let command_tx = self.internal_cmd_tx.clone();
        let conn_id = ctx.session().conn_id().clone();
        let closure = Box::new(move |batches| {
            let _ = command_tx.send(crate::coord::Message::StagedBatches {
                conn_id,
                table_id: id,
                batches,
            });
        });
        // Stash the execute context so we can cancel the COPY.
        self.active_copies
            .insert(ctx.session().conn_id().clone(), ctx);

        let _result = self
            .controller
            .storage
            .create_oneshot_ingestion(ingestion_id, collection_id, cluster_id, request, closure)
            .await;
    }

    pub(crate) fn commit_staged_batches(
        &mut self,
        conn_id: ConnectionId,
        table_id: CatalogItemId,
        batches: Vec<(ProtoBatch, u64)>,
    ) {
        let mut ctx = self.active_copies.remove(&conn_id).expect("TODO");

        let mut all_batches = SmallVec::default();
        let mut row_count = 0usize;

        // TODO(parkmycar): This count isn't correct, we set it to 0 in proto
        // decoding, but also there is no point in keeping it around since the
        // Batch itself tracks the length.
        for (batch, _count) in batches {
            let count = batch.batch.as_ref().expect("missing Batch").len.cast_into();
            all_batches.push(batch);
            row_count = row_count.saturating_add(count);
        }

        // Stage a WriteOp, then when the Session is retired we complete the
        // transaction, which handles acquiring the write lock for `table_id`,
        // advancing the timestamps of the staged batches, and waiting for
        // everything to complete before sending a response to the client.
        ctx.session_mut()
            .add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
                id: table_id,
                rows: TableData::Batches(all_batches),
            }]))
            .expect("TODO");
        ctx.retire(Ok(ExecuteResponse::Copied(row_count)));
    }
}
