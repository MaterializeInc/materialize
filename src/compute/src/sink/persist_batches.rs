// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Compute sink for staging data in Persist Batches.

use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{Collection, Hashable};
use futures::StreamExt;
use mz_compute_client::protocol::response::StagedPeekResponse;
use mz_compute_types::sinks::{ComputeSinkDesc, PersistBatchesConnection};
use mz_expr::MutationKind;
use mz_ore::cast::CastFrom;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::Diagnostics;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena, Timestamp};
use mz_row_stash::{StashedRow, StashedRowSchema};
use mz_storage_operators::oneshot_source::StorageErrorX;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Exchange, Map};
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;

impl<G> SinkRender<G> for PersistBatchesConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
        _ct_times: Option<Collection<G, (), Diff>>,
    ) -> Option<Rc<dyn std::any::Any>> {
        let scope = sinked_collection.scope();
        let dest_arity = self.collection_meta.relation_desc.arity();

        // Create a closure that each worker can use to respond with its results.
        let response_buffer = Rc::clone(&compute_state.staged_peek_reponse_buffer);
        let responder = Box::new(
            move |appends: Option<ProtoBatch>, returns: Option<ProtoBatch>| {
                let mut borrow = response_buffer.borrow_mut();
                let appends = match appends {
                    None => vec![],
                    Some(batch) => vec![batch],
                };
                tracing::warn!(?appends, ?returns, "staged BATCHES");
                let response = StagedPeekResponse {
                    append_batches: appends,
                    returned_rows: returns,
                };

                borrow.insert(sink_id, response);
            },
        );

        // 1. Apply the MFP to our input collection.
        let mfp = self.map_filter_project.clone();
        let arena = RowArena::new();
        let mut row_builder = Row::default();
        let mut datum_vec = DatumVec::new();

        let mfp_collection = sinked_collection.flat_map(move |row| {
            let mut borrow = datum_vec.borrow_with(&row);
            let maybe_row = mfp
                .evaluate_into(&mut borrow, &arena, &mut row_builder)
                .expect("TODO");
            maybe_row
        });

        // If the output is trivial we can just stream directly into Persist.
        let can_trivially_stream = self.finishing.is_trivial(dest_arity)
            && self.mutation_kind == MutationKind::Insert
            && self.returning.is_empty();

        // 2. Map the results into a stream that we can sink into Persist.
        let (append_stream, returning_stream) = if can_trivially_stream {
            (mfp_collection.inner, None)
        } else {
            // Consolidate the results so we're working with an easy to reason
            // about snapshot.
            let consolidated_collection = mfp_collection.consolidate();

            // Mutate the collection for the corresponding query type.
            let mutation_kind = self.mutation_kind.clone();
            let mut datum_vec = DatumVec::new();

            let mutated_stream = consolidated_collection
                .inner
                .flat_map(move |(row, ts, diff)| {
                    match &mutation_kind {
                        // No-op.
                        MutationKind::Insert => {
                            let value = (row, ts, diff);
                            itertools::Either::Left(std::iter::once(value))
                        }
                        MutationKind::Delete => {
                            let negated = diff.saturating_mul(-1);
                            let value = (row, ts, negated);
                            itertools::Either::Left(std::iter::once(value))
                        }
                        MutationKind::Update { assignments } => {
                            if assignments.is_empty() {
                                // If there are no assignments, then we can pass the row through.
                                let value = (row, ts, diff);
                                itertools::Either::Left(std::iter::once(value))
                            } else {
                                // Compute the updated row with the assignments.
                                let arena = RowArena::new();
                                let new_row = {
                                    let mut datums = datum_vec.borrow_with(&row);
                                    for (idx, expr) in assignments {
                                        let updated = expr.eval(&datums, &arena).expect("TODO");
                                        datums[*idx] = updated;
                                    }
                                    Row::pack_slice(&datums)
                                };

                                let old_row = (row, ts, diff.saturating_mul(-1));
                                let new_row = (new_row, ts, diff);

                                itertools::Either::Right([old_row, new_row].into_iter())
                            }
                        }
                    }
                });

            // Create the stream of rows to return to the user.
            let returning_exprs = self.returning.clone();
            let mut datum_vec = DatumVec::new();

            let returning_stream = if !self.returning.is_empty() {
                let stream = mutated_stream.flat_map(move |(row, _ts, diff)| {
                    // Filter out retractions from UPDATEs or DELETEs.
                    if diff < 1 {
                        return None;
                    }

                    // Generate the returned Row by running each expression.
                    let mut returning_row = Row::default();
                    let mut packer = returning_row.packer();
                    let datums = datum_vec.borrow_with(&row);
                    let arena = RowArena::default();

                    for expr in &returning_exprs {
                        let datum = expr.eval(&datums[..], &arena).expect("TODO");
                        packer.push(datum);
                    }

                    let diff = usize::try_from(diff).expect("known to be > 0");
                    let diff = NonZeroUsize::try_from(diff).expect("known to be > 1");

                    Some((returning_row, diff))
                });

                Some(stream)
            } else {
                None
            };

            (mutated_stream, returning_stream)
        };

        // 3. Stage all of our batches in Persist.
        let row_stream = append_stream.map(|(row, _ts, diff)| Ok((SourceData(Ok(row)), diff)));
        let persist_diagnostics = Diagnostics {
            shard_name: self.collection_id.to_string(),
            handle_purpose: "PersitBatchesConnection".to_string(),
        };
        let (appends_stream, appends_token) =
            mz_storage_operators::oneshot_source::render_stage_batches_operator(
                scope.clone(),
                self.collection_meta.persist_location.clone(),
                self.collection_meta.data_shard,
                Arc::clone(&compute_state.persist_clients),
                persist_diagnostics,
                &row_stream,
                self.collection_meta.relation_desc.clone(),
            );

        // 3a. Also stage our returned Rows into Persist for environmentd to
        // stream back to the client.
        let (returning_stream, returning_button) = if let Some(returning_stream) = returning_stream
        {
            let num_workers = scope.peers();
            let returns_worker =
                usize::cast_from((self.collection_id, "returns").hashed()) % num_workers;

            // Exchange all the records to a single worker so we stage all of
            // the Rows into a single Batch.
            let returning_stream = returning_stream
                .exchange(move |_| u64::cast_from(returns_worker))
                .map(|(row, diff)| {
                    let diff = i64::try_from(diff.get()).expect("TODO");
                    Ok((StashedRow::from(row), diff))
                });
            let persist_diagnostics = Diagnostics {
                shard_name: "staged_peek_response".to_string(),
                handle_purpose: "Staged Peek Response".to_string(),
            };

            // Stash all of our Rows into the "stashed rows shard".
            let (stream, button) =
                mz_storage_operators::oneshot_source::render_stage_batches_operator(
                    scope.clone(),
                    self.collection_meta.persist_location.clone(),
                    self.stashed_rows_shard,
                    Arc::clone(&compute_state.persist_clients),
                    persist_diagnostics,
                    &returning_stream,
                    StashedRowSchema,
                );

            (Some(stream), Some(button))
        } else {
            (None, None)
        };

        // 4. Report our result.
        render_completion_operator(
            scope.clone(),
            &appends_stream,
            returning_stream.as_ref(),
            responder,
        );

        let mut buttons = vec![appends_token];
        if let Some(button) = returning_button {
            buttons.push(button);
        }

        Some(Rc::new(buttons))
    }
}

/// Render an operator that given a stream of [`ProtoBatch`]es will call our `worker_callback` to
/// report the results upstream.
pub fn render_completion_operator<G, F>(
    scope: G,
    appends_batches: &TimelyStream<G, Result<ProtoBatch, StorageErrorX>>,
    returned_rows: Option<&TimelyStream<G, Result<ProtoBatch, StorageErrorX>>>,
    worker_callback: F,
) where
    G: Scope,
    F: FnOnce(Option<ProtoBatch>, Option<ProtoBatch>) -> () + 'static,
{
    let mut builder =
        AsyncOperatorBuilder::new("PersistBatches-completion".to_string(), scope.clone());

    let mut appends_batches_input = builder.new_disconnected_input(appends_batches, Pipeline);
    let returned_rows_input = returned_rows.map(|r| builder.new_disconnected_input(r, Pipeline));

    builder.build(move |_| async move {
        let (appends, returns) = async move {
            let mut maybe_append_batch: Option<ProtoBatch> = None;
            let mut maybe_returned_rows: Option<ProtoBatch> = None;

            while let Some(event) = appends_batches_input.next().await {
                if let AsyncEvent::Data(_cap, results) = event {
                    let [result] = results
                        .try_into()
                        .expect("only 1 event on the result stream");

                    // TODO(cf2): Lift this restriction.
                    if maybe_append_batch.is_some() {
                        panic!("expected only one batch!");
                    }

                    maybe_append_batch = match result {
                        Ok(batch) => Some(batch),
                        Err(err) => {
                            tracing::error!(?err, "appends, OHHH NOO");
                            None
                        }
                    }
                }
            }

            if let Some(mut input) = returned_rows_input {
                while let Some(event) = input.next().await {
                    if let AsyncEvent::Data(_cap, results) = event {
                        let [result] = results
                            .try_into()
                            .expect("only 1 event on the result stream");
                        if maybe_returned_rows.is_some() {
                            panic!("expected only one batch!");
                        }

                        maybe_returned_rows = match result {
                            Ok(batch) => {
                                let len = batch.batch.as_ref().map(|b| b.len).unwrap_or(0);
                                if len == 0 {
                                    None
                                } else {
                                    Some(batch)
                                }
                            }
                            Err(err) => {
                                tracing::error!(?err, "appends, OHHH NOO");
                                None
                            }
                        }
                    }
                }
            }

            (maybe_append_batch, maybe_returned_rows)
        }
        .await;

        // Report to the caller of our final status.
        worker_callback(appends, returns);
    });
}
