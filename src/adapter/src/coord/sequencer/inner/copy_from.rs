// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use mz_adapter_types::connection::ConnectionId;
use mz_ore::cast::CastInto;
use mz_persist_client::batch::ProtoBatch;
use mz_pgcopy::CopyFormatParams;
use mz_repr::{CatalogItemId, Datum, RowArena};
use mz_sql::plan::{self, CopyFromFilter, CopyFromSource, HirScalarExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::client::TableData;
use mz_storage_types::oneshot_sources::{ContentShape, OneshotIngestionRequest};
use smallvec::SmallVec;
use url::Url;
use uuid::Uuid;

use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{ActiveCopyFrom, Coordinator, TargetCluster};
use crate::optimize::dataflows::{EvalTime, ExprPrepStyle, prep_scalar_expr};
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
            name: _,
            id,
            source,
            columns: _,
            source_desc,
            mfp,
            params,
            filter,
        } = plan;

        let eval_uri = |from: HirScalarExpr| -> Result<String, AdapterError> {
            let style = ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable,
                session: ctx.session(),
                catalog_state: self.catalog().state(),
            };
            let mut from = from.lower_uncorrelated()?;
            prep_scalar_expr(&mut from, style)?;

            // TODO(cf3): Add structured errors for the below uses of `coord_bail!`
            // and AdapterError::Unstructured.
            let temp_storage = RowArena::new();
            let eval_result = from.eval(&[], &temp_storage)?;
            let eval_string = match eval_result {
                Datum::Null => coord_bail!("COPY FROM target value cannot be NULL"),
                Datum::String(url_str) => url_str,
                other => coord_bail!("programming error! COPY FROM target cannot be {other}"),
            };

            Ok(eval_string.to_string())
        };

        // We check in planning that we're copying into a Table, but be defensive.
        let Some(dest_table) = self.catalog().get_entry(&id).table() else {
            let typ = self.catalog().get_entry(&id).item().typ();
            let msg = format!("programming error: expected a Table found {typ:?}");
            return ctx.retire(Err(AdapterError::Unstructured(anyhow::anyhow!(msg))));
        };

        // Generate a unique UUID for our ingestion.
        let ingestion_id = Uuid::new_v4();
        let collection_id = dest_table.global_id_writes();

        let format = match params {
            CopyFormatParams::Csv(csv) => {
                mz_storage_types::oneshot_sources::ContentFormat::Csv(csv.to_owned())
            }
            CopyFormatParams::Parquet => mz_storage_types::oneshot_sources::ContentFormat::Parquet,
            CopyFormatParams::Text(_) | CopyFormatParams::Binary => {
                mz_ore::soft_panic_or_log!("unsupported formats should be rejected in planning");
                ctx.retire(Err(AdapterError::Unsupported("COPY FROM URL format")));
                return;
            }
        };

        let source = match source {
            CopyFromSource::Url(from_expr) => {
                let url = return_if_err!(eval_uri(from_expr), ctx);
                // TODO(cf2): Structured errors.
                let result = Url::parse(&url)
                    .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!("{err}")));
                let url = return_if_err!(result, ctx);

                mz_storage_types::oneshot_sources::ContentSource::Http { url }
            }
            CopyFromSource::AwsS3 {
                uri,
                connection,
                connection_id,
            } => {
                let uri = return_if_err!(eval_uri(uri), ctx);

                // Validate the URI is an S3 URI, with a bucket name. We rely on validating here
                // and expect it in clusterd.
                //
                // TODO(cf2): Structured errors.
                let result = http::Uri::from_str(&uri)
                    .map_err(|err| {
                        AdapterError::Unstructured(anyhow::anyhow!("expected S3 uri: {err}"))
                    })
                    .and_then(|uri| {
                        if uri.scheme_str() != Some("s3") {
                            coord_bail!("only 's3://...' urls are supported as COPY FROM target");
                        }
                        Ok(uri)
                    })
                    .and_then(|uri| {
                        if uri.host().is_none() {
                            coord_bail!("missing bucket name from 's3://...' url");
                        }
                        Ok(uri)
                    });
                let uri = return_if_err!(result, ctx);

                mz_storage_types::oneshot_sources::ContentSource::AwsS3 {
                    connection,
                    connection_id,
                    uri: uri.to_string(),
                }
            }
            CopyFromSource::Stdin => {
                unreachable!("COPY FROM STDIN should be handled elsewhere")
            }
        };

        let filter = match filter {
            None => mz_storage_types::oneshot_sources::ContentFilter::None,
            Some(CopyFromFilter::Files(files)) => {
                mz_storage_types::oneshot_sources::ContentFilter::Files(files)
            }
            Some(CopyFromFilter::Pattern(pattern)) => {
                mz_storage_types::oneshot_sources::ContentFilter::Pattern(pattern)
            }
        };

        let source_mfp = mfp
            .into_plan()
            .map_err(|s| AdapterError::internal("copy_from", s))
            .and_then(|mfp| {
                mfp.into_nontemporal().map_err(|_| {
                    AdapterError::internal("copy_from", "temporal MFP not allowed in copy from")
                })
            });
        let source_mfp = return_if_err!(source_mfp, ctx);
        let shape = ContentShape {
            source_desc,
            source_mfp,
        };

        let request = OneshotIngestionRequest {
            source,
            format,
            filter,
            shape,
        };

        let target_cluster = match self
            .catalog()
            .resolve_target_cluster(target_cluster, ctx.session())
        {
            Ok(cluster) => cluster,
            Err(err) => {
                return ctx.retire(Err(err));
            }
        };
        let cluster_id = target_cluster.id;

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
        let conn_id = ctx.session().conn_id().clone();
        self.active_copies.insert(
            conn_id,
            ActiveCopyFrom {
                ingestion_id,
                cluster_id,
                table_id: id,
                ctx,
            },
        );

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
        batches: Vec<Result<ProtoBatch, String>>,
    ) {
        let Some(active_copy) = self.active_copies.remove(&conn_id) else {
            // Getting a successful response for a cancel COPY FROM is unexpected.
            tracing::warn!(%conn_id, ?batches, "got response for canceled COPY FROM");
            return;
        };

        let ActiveCopyFrom {
            ingestion_id,
            cluster_id: _,
            table_id: _,
            mut ctx,
        } = active_copy;
        tracing::info!(%ingestion_id, num_batches = ?batches.len(), "received batches to append");

        let mut all_batches = SmallVec::with_capacity(batches.len());
        let mut all_errors = SmallVec::<[String; 1]>::with_capacity(batches.len());
        let mut row_count = 0u64;

        for maybe_batch in batches {
            match maybe_batch {
                Ok(batch) => {
                    let count = batch.batch.as_ref().map(|b| b.len).unwrap_or(0);
                    all_batches.push(batch);
                    row_count = row_count.saturating_add(count);
                }
                Err(err) => all_errors.push(err),
            }
        }

        // If we got any errors we need to fail the whole operation.
        if let Some(error) = all_errors.pop() {
            tracing::warn!(?error, ?all_errors, "failed COPY FROM");

            // TODO(cf1): Cleanup the existing ProtoBatches to prevent leaking them.
            // TODO(cf2): Carry structured errors all the way through.

            ctx.retire(Err(AdapterError::Unstructured(anyhow::anyhow!(
                "COPY FROM: {error}"
            ))));

            return;
        }

        // Stage a WriteOp, then when the Session is retired we complete the
        // transaction, which handles acquiring the write lock for `table_id`,
        // advancing the timestamps of the staged batches, and waiting for
        // everything to complete before sending a response to the client.
        let stage_write = ctx
            .session_mut()
            .add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
                id: table_id,
                rows: TableData::Batches(all_batches),
            }]));

        if let Err(err) = stage_write {
            ctx.retire(Err(err));
        } else {
            ctx.retire(Ok(ExecuteResponse::Copied(row_count.cast_into())));
        }
    }

    /// Cancel any active `COPY FROM` statements/oneshot ingestions.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn cancel_pending_copy(&mut self, conn_id: &ConnectionId) {
        if let Some(ActiveCopyFrom {
            ingestion_id,
            cluster_id: _,
            table_id: _,
            ctx,
        }) = self.active_copies.remove(conn_id)
        {
            let cancel_result = self
                .controller
                .storage
                .cancel_oneshot_ingestion(ingestion_id);
            if let Err(err) = cancel_result {
                tracing::error!(?err, "failed to cancel OneshotIngestion");
            }

            ctx.retire(Err(AdapterError::Canceled));
        }
    }
}
