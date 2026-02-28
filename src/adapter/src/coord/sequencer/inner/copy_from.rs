// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use mz_adapter_types::connection::ConnectionId;
use mz_ore::cast::CastInto;
use mz_persist_client::Diagnostics;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_types::codec_impls::UnitSchema;
use mz_pgcopy::CopyFormatParams;
use mz_repr::{CatalogItemId, ColumnIndex, Datum, NotNullViolation, RelationDesc, Row, RowArena};
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::{self, CopyFromFilter, CopyFromSource, HirScalarExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_client::client::TableData;
use mz_storage_types::StorageDiff;
use mz_storage_types::oneshot_sources::{ContentShape, OneshotIngestionRequest};
use mz_storage_types::sources::SourceData;
use smallvec::SmallVec;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use uuid::Uuid;

use crate::command::CopyFromStdinWriter;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{ActiveCopyFrom, Coordinator, TargetCluster};
use crate::optimize;
use crate::optimize::dataflows::{EvalTime, ExprPrep, ExprPrepOneShot};
use crate::session::{Session, TransactionOps, WriteOp};
use crate::{AdapterError, ExecuteContext, ExecuteResponse};

/// Finalize persist batches periodically during COPY FROM STDIN to avoid
/// unbounded in-memory growth in a single giant batch.
const COPY_FROM_STDIN_MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;

impl Coordinator {
    pub(crate) async fn sequence_copy_from(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CopyFromPlan,
        target_cluster: TargetCluster,
    ) {
        let plan::CopyFromPlan {
            target_name: _,
            target_id,
            source,
            columns: _,
            source_desc,
            mfp,
            params,
            filter,
        } = plan;

        let eval_uri = |from: HirScalarExpr| -> Result<String, AdapterError> {
            let style = ExprPrepOneShot {
                logical_time: EvalTime::NotAvailable,
                session: ctx.session(),
                catalog_state: self.catalog().state(),
            };
            let mut from = from.lower_uncorrelated(self.catalog().state().system_config())?;
            style.prep_scalar_expr(&mut from)?;

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
        let Some(entry) = self.catalog().try_get_entry(&target_id) else {
            return ctx.retire(Err(AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "table",
                dependency_id: target_id.to_string(),
            }));
        };
        let Some(dest_table) = entry.table() else {
            let typ = entry.item().typ();
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
                ctx.retire(Err(AdapterError::Unsupported("COPY FROM URL/S3 format")));
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

        // Validate that all non-nullable columns in the target table will be populated.
        let target_desc = dest_table.desc.latest();
        for (col_idx, col_type) in target_desc.iter_types().enumerate() {
            if !col_type.nullable {
                // Check what value the MFP will produce for this column position.
                if let Some(&projection_idx) = source_mfp.projection.get(col_idx) {
                    // If the projection index is beyond the input arity, it references an expression.
                    let input_arity = source_mfp.input_arity;
                    if projection_idx >= input_arity {
                        let expr_idx = projection_idx - input_arity;
                        if let Some(expr) = source_mfp.expressions.get(expr_idx) {
                            // Check if the expression is a NULL literal.
                            // A NULL literal is represented as Literal(Ok(empty_row), _)
                            if matches!(
                                expr,
                                mz_expr::MirScalarExpr::Literal(Ok(row), _)
                                    if row.iter().next().map(|d| d.is_null()).unwrap_or(false)
                            ) {
                                let col_name = target_desc.get_name(col_idx);
                                return ctx.retire(Err(AdapterError::ConstraintViolation(
                                    NotNullViolation(col_name.clone()),
                                )));
                            }
                        }
                    }
                } else {
                    // If there's no projection for this column, that's a validation error
                    let col_name = target_desc.get_name(col_idx);
                    return ctx.retire(Err(AdapterError::ConstraintViolation(NotNullViolation(
                        col_name.clone(),
                    ))));
                }
            }
        }

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
                table_id: target_id,
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
                table_id: target_id,
                ctx,
            },
        );

        let _result = self
            .controller
            .storage
            .create_oneshot_ingestion(ingestion_id, collection_id, cluster_id, request, closure)
            .await;
    }

    /// Sets up a streaming COPY FROM STDIN operation.
    ///
    /// Spawns N parallel background batch builder tasks that each receive
    /// raw byte chunks, decode them, apply column defaults/reordering,
    /// and build persist batches. Returns a [`CopyFromStdinWriter`] for
    /// pgwire to distribute raw byte chunks across the workers.
    pub(crate) fn setup_copy_from_stdin(
        &self,
        session: &Session,
        target_id: CatalogItemId,
        target_name: String,
        columns: Vec<ColumnIndex>,
        row_desc: RelationDesc,
        params: CopyFormatParams<'static>,
    ) -> Result<CopyFromStdinWriter, AdapterError> {
        // Look up the table and its persist shard metadata.
        let Some(entry) = self.catalog().try_get_entry(&target_id) else {
            return Err(AdapterError::ConcurrentDependencyDrop {
                dependency_kind: "table",
                dependency_id: target_id.to_string(),
            });
        };
        let Some(dest_table) = entry.table() else {
            let typ = entry.item().typ();
            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                "programming error: expected a Table found {typ:?}"
            )));
        };
        let collection_id = dest_table.global_id_writes();

        let collection_meta = self
            .controller
            .storage
            .collection_metadata(collection_id)
            .map_err(|e| AdapterError::Unstructured(anyhow::anyhow!("{e}")))?;
        let shard_id = collection_meta.data_shard;
        let collection_desc = collection_meta.relation_desc.clone();

        // Pre-compute the column transformation.
        let pcx = session.pcx().clone();
        let session_meta = session.meta();
        let catalog = self.catalog().clone();
        let conn_catalog = catalog.for_session(session);
        let catalog_state = conn_catalog.state();
        let optimizer_config = optimize::OptimizerConfig::from(conn_catalog.system_vars());

        // Determine if we need column rewriting (defaults/reordering).
        let target_desc = catalog
            .try_get_entry(&target_id)
            .expect("table must exist")
            .relation_desc_latest()
            .expect("table has desc")
            .into_owned();
        let all_columns_in_order = columns.len() == target_desc.arity()
            && columns.iter().enumerate().all(|(i, c)| c.to_raw() == i);

        // If we need column rewriting, pre-compute the transform by running
        // plan_copy_from with a single dummy row through the optimizer.
        let column_transform = if all_columns_in_order {
            None
        } else {
            let dummy_datums: Vec<Datum> = columns.iter().map(|_| Datum::Null).collect();
            let dummy_row = Row::pack(&dummy_datums);

            let prep = ExprPrepOneShot {
                logical_time: EvalTime::NotAvailable,
                session: &session_meta,
                catalog_state,
            };
            let mut optimizer = optimize::view::Optimizer::new_with_prep_no_limit(
                optimizer_config.clone(),
                None,
                prep,
            );

            let hir = mz_sql::plan::plan_copy_from(
                &pcx,
                &conn_catalog,
                target_id,
                target_name.clone(),
                columns.clone(),
                vec![dummy_row],
            )?;
            let mir = optimize::Optimize::optimize(&mut optimizer, hir)?;
            let mir_expr = mir.into_inner();
            let (result_ref, _) = mir_expr
                .as_const()
                .expect("optimizer should produce constant");
            let result_rows = result_ref
                .clone()
                .map_err(|e| AdapterError::Unstructured(anyhow::anyhow!("eval error: {e}")))?;

            let (full_row, _) = result_rows.into_iter().next().expect("should have one row");
            let full_datums: Vec<Datum> = full_row.unpack();

            let col_to_source: std::collections::BTreeMap<ColumnIndex, usize> =
                columns.iter().enumerate().map(|(a, b)| (*b, a)).collect();

            let mut sources: Vec<ColumnSource> = Vec::with_capacity(target_desc.arity());
            let mut default_datums: Vec<Datum> = Vec::new();

            for i in 0..target_desc.arity() {
                let col_idx = ColumnIndex::from_raw(i);
                if let Some(&src_idx) = col_to_source.get(&col_idx) {
                    sources.push(ColumnSource::Input(src_idx));
                } else {
                    sources.push(ColumnSource::Default(default_datums.len()));
                    default_datums.push(full_datums[i]);
                }
            }

            let defaults_row = Row::pack(&default_datums);

            Some(ColumnTransform {
                sources,
                defaults_row,
            })
        };

        // Compute column types for decoding (same logic as pgwire used to do).
        let column_types: Arc<[mz_pgrepr::Type]> = row_desc
            .typ()
            .column_types
            .iter()
            .map(|x| &x.scalar_type)
            .map(mz_pgrepr::Type::from)
            .collect::<Vec<_>>()
            .into();

        // Determine number of parallel workers.
        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        tracing::info!(
            %target_id, num_workers,
            "starting parallel COPY FROM STDIN batch builders"
        );

        // Shared state across workers.
        let column_transform = Arc::new(column_transform);
        let target_desc = Arc::new(target_desc);
        let collection_desc = Arc::new(collection_desc);
        let persist_client = self.persist_client.clone();

        // Create per-worker channels and spawn workers on blocking threads.
        // Each worker does CPU-intensive TSV decoding + columnar encoding,
        // so they need dedicated OS threads (not tokio async tasks) for
        // true parallelism.
        let rt_handle = tokio::runtime::Handle::current();
        let mut batch_txs = Vec::with_capacity(num_workers);
        let mut worker_handles = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            // Keep in-flight buffering tight: at most one chunk queued per
            // worker in addition to the currently-processed chunk.
            let (batch_tx, batch_rx) = mpsc::channel::<Vec<u8>>(1);
            batch_txs.push(batch_tx);

            let persist_client = persist_client.clone();
            let column_types = Arc::clone(&column_types);
            let column_transform = Arc::clone(&column_transform);
            let target_desc = Arc::clone(&target_desc);
            let collection_desc = Arc::clone(&collection_desc);
            let params = params.clone();
            let rt = rt_handle.clone();

            let handle = mz_ore::task::spawn_blocking(
                || format!("copy_from_stdin_worker:{target_id}:{worker_id}"),
                move || {
                    rt.block_on(Self::copy_from_stdin_batch_builder(
                        persist_client,
                        shard_id,
                        collection_id,
                        collection_desc,
                        target_desc,
                        column_transform,
                        column_types,
                        params,
                        batch_rx,
                    ))
                },
            );
            worker_handles.push(handle);
        }

        // Spawn a collector task that waits for all workers.
        let (completion_tx, completion_rx) = oneshot::channel();
        mz_ore::task::spawn(
            || format!("copy_from_stdin_collector:{target_id}"),
            async move {
                let mut all_batches = Vec::with_capacity(num_workers);
                let mut total_rows: u64 = 0;

                for handle in worker_handles {
                    match handle.await {
                        Ok((proto_batches, count)) => {
                            all_batches.extend(proto_batches);
                            total_rows += count;
                        }
                        Err(e) => {
                            let _ = completion_tx.send(Err(e));
                            return;
                        }
                    }
                }

                let _ = completion_tx.send(Ok((all_batches, total_rows)));
            },
        );

        Ok(CopyFromStdinWriter {
            batch_txs,
            completion_rx,
        })
    }

    /// Background task: receives raw byte chunks, decodes rows, and builds
    /// persist batches. One instance runs per parallel worker.
    async fn copy_from_stdin_batch_builder(
        persist_client: mz_persist_client::PersistClient,
        shard_id: mz_persist_client::ShardId,
        collection_id: mz_repr::GlobalId,
        collection_desc: Arc<RelationDesc>,
        target_desc: Arc<RelationDesc>,
        column_transform: Arc<Option<ColumnTransform>>,
        column_types: Arc<[mz_pgrepr::Type]>,
        params: CopyFormatParams<'static>,
        mut batch_rx: mpsc::Receiver<Vec<u8>>,
    ) -> Result<(Vec<ProtoBatch>, u64), AdapterError> {
        let persist_diagnostics = Diagnostics {
            shard_name: collection_id.to_string(),
            handle_purpose: "CopyFromStdin::batch_builder".to_string(),
        };
        let write_handle = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                shard_id,
                collection_desc,
                Arc::new(UnitSchema),
                persist_diagnostics,
            )
            .await
            .map_err(|e| AdapterError::Unstructured(anyhow::anyhow!("persist open: {e}")))?;

        // Build a batch at the minimum timestamp. The coordinator will
        // re-timestamp it during commit.
        let lower = mz_repr::Timestamp::MIN;
        let upper = Antichain::from_elem(lower.step_forward());
        let mut batch_builder = write_handle.builder(Antichain::from_elem(lower));
        let mut row_count: u64 = 0;
        let mut row_count_in_batch: u64 = 0;
        let mut batch_bytes: usize = 0;
        let mut proto_batches = Vec::new();

        while let Some(raw_bytes) = batch_rx.recv().await {
            // Decode raw bytes into rows.
            let rows = mz_pgcopy::decode_copy_format(&raw_bytes, &column_types, params.clone())
                .map_err(|e| AdapterError::CopyFormatError(e.to_string()))?;

            for row in rows {
                // Apply column transform if needed (add defaults, reorder).
                let full_row = if let Some(ref transform) = *column_transform {
                    transform.apply(&row)
                } else {
                    row
                };

                // Check constraints.
                for (i, datum) in full_row.iter().enumerate() {
                    target_desc.constraints_met(i, &datum).map_err(|e| {
                        AdapterError::Unstructured(anyhow::anyhow!("constraint violation: {e}"))
                    })?;
                }

                let data = SourceData(Ok(full_row));
                batch_builder
                    .add(&data, &(), &lower, &1)
                    .await
                    .map_err(|e| AdapterError::Unstructured(anyhow::anyhow!("persist add: {e}")))?;
                row_count += 1;
                row_count_in_batch += 1;
            }

            batch_bytes = batch_bytes.saturating_add(raw_bytes.len());
            if batch_bytes >= COPY_FROM_STDIN_MAX_BATCH_BYTES {
                let batch = batch_builder.finish(upper.clone()).await.map_err(|e| {
                    AdapterError::Unstructured(anyhow::anyhow!("persist finish: {e}"))
                })?;
                proto_batches.push(batch.into_transmittable_batch());

                batch_builder = write_handle.builder(Antichain::from_elem(lower));
                row_count_in_batch = 0;
                batch_bytes = 0;
            }
        }

        if row_count_in_batch > 0 || proto_batches.is_empty() {
            let batch = batch_builder
                .finish(upper)
                .await
                .map_err(|e| AdapterError::Unstructured(anyhow::anyhow!("persist finish: {e}")))?;
            proto_batches.push(batch.into_transmittable_batch());
        }

        Ok((proto_batches, row_count))
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

/// Describes how to transform a partial row (with only specified columns)
/// into a full row matching the table schema.
struct ColumnTransform {
    /// For each column in the target table, where to get the value.
    sources: Vec<ColumnSource>,
    /// Pre-computed default values for columns not in the COPY column list.
    /// Packed as a Row; indexed by the `Default(idx)` variant.
    defaults_row: Row,
}

enum ColumnSource {
    /// Take the value from the input row at this position.
    Input(usize),
    /// Use the pre-computed default at this index in `defaults_row`.
    Default(usize),
}

impl ColumnTransform {
    /// Apply the transform to produce a full row from a partial input row.
    fn apply(&self, input: &Row) -> Row {
        let input_datums: Vec<Datum> = input.unpack();
        let default_datums: Vec<Datum> = self.defaults_row.unpack();
        let mut output_datums = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            match source {
                ColumnSource::Input(idx) => output_datums.push(input_datums[*idx]),
                ColumnSource::Default(idx) => output_datums.push(default_datums[*idx]),
            }
        }
        Row::pack(&output_datums)
    }
}
