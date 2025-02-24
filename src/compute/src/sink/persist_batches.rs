// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Compute sink for staging data in Persist Batches.

use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{Collection, Hashable};
use futures::StreamExt;
use mz_compute_client::protocol::response::StagedPeekResponse;
use mz_compute_types::sinks::{ComputeSinkDesc, PersistBatchesConnection};
use mz_expr::{RowSetFinishing, SafeMfpPlan};
use mz_ore::cast::CastFrom;
use mz_persist_client::batch::ProtoBatch;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena, Timestamp};
use mz_storage_operators::oneshot_source::StorageErrorX;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
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

        // A single worker will collect pre-sorted groups and do a merge sort.
        let num_workers = scope.peers();
        let aggregator_id =
            usize::cast_from((self.collection_id, "aggregator").hashed()) % num_workers;

        let response_buffer = Rc::clone(&compute_state.staged_peek_reponse_buffer);
        let responder = Box::new(move |batches: Result<Option<ProtoBatch>, String>| {
            let mut borrow = response_buffer.borrow_mut();
            let batches = match batches.expect("TODO") {
                None => vec![],
                Some(batch) => vec![batch],
            };
            tracing::warn!(?batches, "staged BATCHES");
            let response = StagedPeekResponse {
                staged_batches: batches,
            };

            borrow.insert(sink_id, response);
        });

        // Each worker will collect and sort rows.
        let (collected_groups, collected_token) = render_collect_operator(
            &scope,
            sinked_collection,
            self.finishing.clone(),
            self.map_filter_project.clone(),
        );

        // If there is a sort order for the columns we need to do that on a single worker.
        let need_to_aggregate = !self.finishing.order_by.is_empty();

        let aggregator_id = need_to_aggregate.then_some(aggregator_id);
        let group_stream = if let Some(aggregator_id) = aggregator_id {
            collected_groups.exchange(move |_| u64::cast_from(aggregator_id))
        } else {
            collected_groups
        };

        // A single worker will get all of the pre-sorted groups and do a merge sort.
        //
        // TODO(parkmycar): Alternatively each worker could sync the data into Persist batches
        // and use compaction to drive the sorting.
        let (sorted_row_stream, sorted_row_token) =
            render_aggregate_operator(&scope, &group_stream, aggregator_id);
        let sorted_row_stream = sorted_row_stream.map(|data| Ok::<_, StorageErrorX>(data));

        // Stage all of our batches in Persist.
        let (batches_stream, batches_token) =
            mz_storage_operators::oneshot_source::render_stage_batches_operator(
                scope.clone(),
                self.collection_id,
                &self.collection_meta,
                Arc::clone(&compute_state.persist_clients),
                &sorted_row_stream,
            );

        mz_storage_operators::oneshot_source::render_completion_operator(
            scope.clone(),
            &batches_stream,
            responder,
        );

        Some(Rc::new(vec![
            collected_token,
            sorted_row_token,
            batches_token,
        ]))
    }
}

fn render_collect_operator<G>(
    scope: &G,
    input: Collection<G, Row, Diff>,
    finishing: RowSetFinishing,
    mfp: SafeMfpPlan,
) -> (TimelyStream<G, Vec<(Row, Diff)>>, PressOnDropButton)
where
    G: Scope,
{
    let mut builder =
        AsyncOperatorBuilder::new("PersistBatches-collect".to_string(), scope.clone());

    let (collection_handle, collection_stream) = builder.new_output();
    let mut input_handle = builder.new_disconnected_input(&input.inner, Pipeline);

    let shutdown = builder.build(move |caps| async move {
        let [collect_cap] = caps.try_into().unwrap();

        // TODO(parkmycar): Maybe collect the Rows into a BinaryHeap and sort
        // as we receive them.
        let mut all_rows = Vec::new();

        // Apply the MFP as we receive a row.
        let arena = RowArena::new();
        let mut row_builder = Row::default();
        let mut datum_vec = DatumVec::new();

        while let Some(event) = input_handle.next().await {
            let mut rows = match event {
                AsyncEvent::Data(_caps, rows) => rows,
                AsyncEvent::Progress(_) => continue,
            };

            for (row, diff) in rows.drain(..).map(|(row, _ts, diff)| (row, diff)) {
                let mut borrow = datum_vec.borrow_with(&row);
                let maybe_row = mfp
                    .evaluate_into(&mut borrow, &arena, &mut row_builder)
                    .expect("TODO");
                assert!(diff > 0, "got negative diff!");

                if let Some(row) = maybe_row {
                    all_rows.push((row, diff));
                }
            }
        }

        // Sort our rows, if required.
        if !finishing.order_by.is_empty() {
            let mut l_datum_vec = DatumVec::new();
            let mut r_datum_vec = DatumVec::new();

            all_rows.sort_by(|left, right| {
                let left_datums = l_datum_vec.borrow_with(&left.0);
                let right_datums = r_datum_vec.borrow_with(&right.0);
                mz_expr::compare_columns(&finishing.order_by, &left_datums, &right_datums, || {
                    left.0.cmp(&right.0)
                })
            });
        }

        collection_handle.give(&collect_cap, all_rows);
    });

    (collection_stream, shutdown.press_on_drop())
}

fn render_aggregate_operator<G>(
    scope: &G,
    input: &TimelyStream<G, Vec<(Row, Diff)>>,
    aggregator_id: Option<usize>,
) -> (TimelyStream<G, (Row, Diff)>, PressOnDropButton)
where
    G: Scope,
{
    let mut builder =
        AsyncOperatorBuilder::new("PersistBatches-aggregate".to_string(), scope.clone());

    let (sorted_handle, sorted_stream) = builder.new_output();
    let mut input_groups = builder.new_input_for(&input, Pipeline, &sorted_handle);

    // Only a single worker should receive row groups to merge together.
    let should_receive_data = if let Some(aggregator_id) = aggregator_id {
        scope.index() == aggregator_id
    } else {
        true
    };

    let shutdown = builder.build(move |caps| async move {
        let [aggregate_cap] = caps.try_into().unwrap();

        let mut all_groups = Vec::new();
        while let Some(event) = input_groups.next().await {
            let mut groups = match event {
                AsyncEvent::Data(_caps, groups) => groups,
                AsyncEvent::Progress(_) => continue,
            };
            all_groups.extend(groups.drain(..));
        }

        if should_receive_data {
            // TODO(parkmycar): Merge sort these rows.
            let all_rows = all_groups.into_iter().flatten();
            for row in all_rows {
                sorted_handle.give(&aggregate_cap, row);
            }
        } else {
            assert!(all_groups.is_empty(), "non-primary worker received data!");
        }
    });

    (sorted_stream, shutdown.press_on_drop())
}
