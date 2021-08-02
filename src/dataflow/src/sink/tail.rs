// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::Collection;

use itertools::Itertools;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::progress::frontier::AntichainRef;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;
use timely::progress::Antichain;
use timely::PartialOrder;

use dataflow_types::{SinkAsOf, SinkDesc, TailResponse, TailSinkConnector};
use expr::GlobalId;
use ore::cast::CastFrom;
use repr::adt::numeric::{self, Numeric};
use repr::{Datum, Diff, RelationDesc, Row, Timestamp};

use crate::render::sinks::SinkRender;
use crate::render::RenderState;

use super::SinkBaseMetrics;

impl<G> SinkRender<G> for TailSinkConnector
where
    G: Scope<Timestamp = Timestamp>,
{
    fn uses_keys(&self) -> bool {
        false
    }

    fn get_key_desc(&self) -> Option<&RelationDesc> {
        None
    }

    fn get_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn get_value_desc(&self) -> &RelationDesc {
        &self.value_desc
    }

    fn render_continuous_sink(
        &self,
        render_state: &mut RenderState,
        sink: &SinkDesc,
        sink_id: GlobalId,
        sinked_collection: Collection<Child<G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>,
        _metrics: &SinkBaseMetrics,
    ) -> Option<Box<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        tail(
            sinked_collection,
            sink_id,
            self.clone(),
            render_state.tail_response_buffer.clone(),
            sink.as_of.clone(),
        );

        // no sink token
        None
    }
}

fn tail<G>(
    sinked_collection: Collection<Child<G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>,
    sink_id: GlobalId,
    connector: TailSinkConnector,
    tail_response_buffer: Rc<RefCell<Vec<(GlobalId, TailResponse)>>>,
    as_of: SinkAsOf,
) where
    G: Scope<Timestamp = Timestamp>,
{
    // `active_worker` indicates the index of the worker that receives all data.
    // This should line up with the exchange just below, but we test in the implementation
    // just to be certain.
    let scope = sinked_collection.scope();
    use differential_dataflow::Hashable;
    let active_worker = (sink_id.hashed() as usize) % scope.peers() == scope.index();
    // make sure all data is routed to one worker by keying on the sink id
    let batches = sinked_collection
        .map(move |(k, v)| {
            assert!(k.is_none(), "tail does not support keys");
            let v = v.expect("tail must have values");
            (sink_id, v)
        })
        .arrange_by_key()
        .stream;

    let mut packer = Row::default();

    // Initialize to the minimal input frontier.
    let mut input_frontier = Antichain::from_elem(<G::Timestamp as TimelyTimestamp>::minimum());
    // An encapsulation of the Tail response protocol.
    // Used to send rows and eventually mark complete.
    // Set to `None` for instances that should not produce output.
    let mut tail_protocol = if active_worker {
        Some(TailProtocol {
            sink_id,
            tail_response_buffer: Some(tail_response_buffer),
        })
    } else {
        None
    };

    batches.sink(Pipeline, &format!("tail-{}", sink_id), move |input| {
        input.for_each(|_, batches| {
            let mut results = vec![];
            for batch in batches.iter() {
                let mut cursor = batch.cursor();
                while cursor.key_valid(&batch) {
                    while cursor.val_valid(&batch) {
                        let row = cursor.val(&batch);
                        cursor.map_times(&batch, |time, diff| {
                            let diff = *diff;
                            let should_emit = if as_of.strict {
                                as_of.frontier.less_than(time)
                            } else {
                                as_of.frontier.less_equal(time)
                            };
                            if should_emit {
                                packer.push(Datum::from(numeric::Numeric::from(*time)));
                                if connector.emit_progress {
                                    // When sinking with PROGRESS, the output
                                    // includes an additional column that
                                    // indicates whether a timestamp is
                                    // complete. For regular "data" upates this
                                    // is always `false`.
                                    packer.push(Datum::False);
                                }

                                packer.push(Datum::Int64(i64::cast_from(diff)));

                                packer.extend_by_row(&row);

                                let row = packer.finish_and_reuse();

                                // Add the unpacked timestamp so we can sort by them later.
                                results.push((*time, row));
                            }
                        });
                        cursor.step_val(&batch);
                    }
                    cursor.step_key(&batch);
                }
            }

            // Sort results by time and convert to Vec<Row>. We use stable sort here because
            // it will produce deterministic results since the cursor will always produce
            // rows in the same order.
            results.sort_by_key(|(time, _)| *time);
            let mut results: Vec<Row> = results.into_iter().map(|(_, row)| row).collect();

            if let Some(batch) = batches.last() {
                let progress_row = update_progress(
                    &mut input_frontier,
                    batch.desc.upper().borrow(),
                    &mut packer,
                    connector.object_columns + 1,
                );
                if connector.emit_progress && tail_protocol.is_some() {
                    if let Some(progress_row) = progress_row {
                        results.push(progress_row);
                    }
                }
            }

            // Emit data if configured, otherwise it is an error to have data to send.
            if let Some(tail_protocol) = &mut tail_protocol {
                tail_protocol.send(results);
            } else {
                assert!(
                    results.is_empty(),
                    "Observed data at inactive TAIL instance"
                );
            }
        });

        let progress_row = update_progress(
            &mut input_frontier,
            input.frontier().frontier(),
            &mut packer,
            connector.object_columns + 1,
        );

        // Only emit updates if this operator/worker received actual data for emission.
        if let Some(tail_protocol) = &mut tail_protocol {
            if connector.emit_progress {
                if let Some(progress_row) = progress_row {
                    let results = vec![progress_row];
                    tail_protocol.send(results);
                }
            }
        }

        // If the frontier is empty the tailing is complete. We should say so!
        if input.frontier().frontier().is_empty() {
            if let Some(tail_protocol) = tail_protocol.take() {
                tail_protocol.complete();
            } else {
                // Not an error to notice the end of the stream at non-emitters,
                // or to notice it a second time at a previous emitter.
            }
        }
    })
}

/// A type that guides the transmission of rows back to the coordinator.
///
/// A protocol instance may `send` rows indefinitely, and is consumed by `complete`,
/// which is used only to indicate the end of a stream. The `Drop` implementation
/// otherwise sends an indication that the protocol has finished without completion.
struct TailProtocol {
    pub sink_id: GlobalId,
    pub tail_response_buffer: Option<Rc<RefCell<Vec<(GlobalId, TailResponse)>>>>,
}

impl TailProtocol {
    /// Send further rows as responses.
    ///
    /// Does nothing if `self.complete()` has been called.
    fn send(&mut self, rows: Vec<Row>) {
        if let Some(buffer) = &mut self.tail_response_buffer {
            buffer
                .borrow_mut()
                .push((self.sink_id, TailResponse::Rows(rows)));
        }
    }
    /// Completes the channel, preventing further transmissions.
    fn complete(mut self) {
        if let Some(buffer) = &mut self.tail_response_buffer {
            buffer
                .borrow_mut()
                .push((self.sink_id, TailResponse::Complete));
            // Set to `None` to avoid `TailResponse::Dropped`.
            self.tail_response_buffer = None;
        }
    }
}

impl Drop for TailProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = &mut self.tail_response_buffer {
            buffer
                .borrow_mut()
                .push((self.sink_id, TailResponse::Dropped));
            self.tail_response_buffer = None;
        }
    }
}

// Checks if there is progress between `current_input_frontier` and
// `new_input_frontier`. If yes, updates the `current_input_frontier` to
// `new_input_frontier` and returns a [`Row`] that encodes this progress. If
// there is no progress, returns [`None`].
fn update_progress(
    current_input_frontier: &mut Antichain<Timestamp>,
    new_input_frontier: AntichainRef<Timestamp>,
    packer: &mut Row,
    empty_columns: usize,
) -> Option<Row> {
    // Test to see if strict progress has occurred. This is true if the new
    // frontier is not less or equal to the old frontier.
    let progress = !PartialOrder::less_equal(&new_input_frontier, &current_input_frontier.borrow());

    if progress {
        current_input_frontier.clear();
        current_input_frontier.extend(new_input_frontier.iter().cloned());

        // This only looks at the first entry of the antichain.
        // If we ever have multi-dimensional time, this is not correct
        // anymore. There might not even be progress in the first dimension.
        // We panic, so that future developers introducing multi-dimensional
        // time in Materialize will notice.
        let upper = new_input_frontier
            .iter()
            .at_most_one()
            .expect("more than one element in the frontier")
            .cloned();

        // the input frontier might be empty
        upper.map(|upper| {
            packer.push(Datum::from(Numeric::from(upper)));
            packer.push(Datum::True);
            // Fill in the diff column and all table columns with NULL.
            for _ in 0..empty_columns {
                packer.push(Datum::Null);
            }
            packer.finish_and_reuse()
        })
    } else {
        None
    }
}
