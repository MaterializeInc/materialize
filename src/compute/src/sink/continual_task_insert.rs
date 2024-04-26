// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::borrow::Borrow;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{Collection, Hashable};
use mz_compute_types::sinks::{ComputeSinkDesc, ContinualTaskInsertConnection};
use mz_ore::cast::CastFrom;
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Concat, Map};
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;

impl<G> SinkRender<G> for ContinualTaskInsertConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>> {
        let mut scope = sinked_collection.inner.scope();
        tracing::info!("render: {:#?}", sink);

        let name = format!("ContinualTaskInsert({})", sink_id);

        let (to_retract_ok_stream, to_retract_err_stream, to_retract_token) =
            mz_storage_operators::persist_source::persist_source(
                &mut scope,
                sink_id,
                Arc::clone(&compute_state.persist_clients),
                self.retract_from_metadata.clone(),
                Some(as_of.clone()),
                SnapshotMode::Include,
                Antichain::new(), // we want all updates
                None,             // no MFP
                compute_state.dataflow_max_inflight_bytes(),
                start_signal,
            );

        // Massage everything into SourceData.

        let to_retract_ok_source_data_stream =
            to_retract_ok_stream.map(|(x, ts, diff)| (SourceData(Ok(x)), ts, diff));

        let to_retract_err_source_data_stream =
            to_retract_err_stream.map(|(x, ts, diff)| (SourceData(Err(x)), ts, diff));

        let to_retract_stream =
            to_retract_ok_source_data_stream.concat(&to_retract_err_source_data_stream);

        let to_insert_ok_source_data_stream = sinked_collection
            .inner
            .map(|(x, ts, diff)| (SourceData(Ok(x)), ts, diff));

        let to_insert_err_source_data_stream = err_collection
            .inner
            .map(|(x, ts, diff)| (SourceData(Err(x)), ts, diff));

        let to_insert_stream =
            to_insert_ok_source_data_stream.concat(&to_insert_err_source_data_stream);

        let operator_name = format!("ContinualTaskInsert({})", sink_id);
        let mut task_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

        let hashed_id = sink_id.hashed();
        let active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

        let mut to_insert_input =
            task_op.new_disconnected_input(&to_insert_stream, Exchange::new(move |_| hashed_id));
        let mut to_retract_input =
            task_op.new_disconnected_input(&to_retract_stream, Exchange::new(move |_| hashed_id));

        let task_button = task_op.build(move |_capabilities| async move {
            if !active_worker {
                return;
            }

            let mut to_insert_upper = Antichain::from_elem(mz_repr::Timestamp::minimum());
            let mut to_retract_upper = Antichain::from_elem(mz_repr::Timestamp::minimum());

            // Correction buffers for things that want to make it into the next
            // transaction.
            let mut to_insert_buffer = Vec::new();
            let mut to_retract_buffer = Vec::new();

            loop {
                tokio::select! {
                    Some(event) = to_insert_input.next() => {
                        match event {
                            Event::Data(_ts, mut data) => {
                                tracing::info!("to_insert: {:?}", data);
                                to_insert_buffer.append(&mut data);
                            }
                            Event::Progress(upper) => {
                                // Trying not to be cute with join_assign and
                                // the like, for now.
                                to_insert_upper = upper;
                                // tracing::info!("to_insert, upper: {:?}", upper);
                            }
                        }
                    }
                    Some(event) = to_retract_input.next() => {
                        match event {
                            Event::Data(_ts, mut data) => {
                                tracing::info!("to_retract: {:?}", data);
                                to_retract_buffer.append(&mut data);
                            }
                            Event::Progress(upper) => {
                                // Trying not to be cute with join_assign and
                                // the like, for now.
                                to_retract_upper = upper;
                                // tracing::info!("to_retract, upper: {:?}", upper);
                            }
                        }
                    }
                    else => {
                        // All inputs are exhausted, so we can shut down.
                        return;
                    }
                };

                let mut upper = to_insert_upper.clone();
                upper.extend(to_retract_upper.iter().cloned());

                // Advance all timestamps in our buffers to the upper.
                for (_data, ts, _diff) in to_insert_buffer.iter_mut() {
                    ts.advance_by(upper.borrow());
                }
                for (_data, ts, _diff) in to_retract_buffer.iter_mut() {
                    ts.advance_by(upper.borrow());
                }

                differential_dataflow::consolidation::consolidate_updates(&mut to_insert_buffer);
                differential_dataflow::consolidation::consolidate_updates(&mut to_retract_buffer);

                tracing::info!(
                    ?upper,
                    ?to_insert_buffer,
                    ?to_retract_buffer,
                    "should transactionally append!"
                );
            }
        });

        let task_token = Rc::new(task_button.press_on_drop());
        Some(Rc::new((to_retract_token, task_token)))
    }
}
