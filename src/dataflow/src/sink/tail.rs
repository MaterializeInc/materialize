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

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::progress::frontier::AntichainRef;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;
use timely::progress::Antichain;
use timely::PartialOrder;

use dataflow_types::{SinkAsOf, SinkDesc, TailResponse, TailSinkConnector};
use expr::GlobalId;
use repr::{Diff, Row, Timestamp};

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

    fn get_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn render_continuous_sink(
        &self,
        render_state: &mut RenderState,
        sink: &SinkDesc,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        _metrics: &SinkBaseMetrics,
    ) -> Option<Box<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        tail(
            sinked_collection,
            sink_id,
            render_state.tail_response_buffer.clone(),
            sink.as_of.clone(),
        );

        // no sink token
        None
    }
}

fn tail<G>(
    sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    sink_id: GlobalId,
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
                                results.push((row.clone(), *time, diff));
                            }
                        });
                        cursor.step_val(&batch);
                    }
                    cursor.step_key(&batch);
                }
            }

            // Emit data if configured, otherwise it is an error to have data to send.
            if let Some(tail_protocol) = &mut tail_protocol {
                tail_protocol.send_rows(results);
            } else {
                assert!(
                    results.is_empty(),
                    "Observed data at inactive TAIL instance"
                );
            }

            if let Some(batch) = batches.last() {
                let progress = update_progress(&mut input_frontier, batch.desc.upper().borrow());
                if let (Some(tail_protocol), Some(progress)) = (&mut tail_protocol, progress) {
                    tail_protocol.send_progress(progress);
                }
            }
        });

        let progress = update_progress(&mut input_frontier, input.frontier().frontier());

        // Only emit updates if this operator/worker received actual data for emission.
        if let (Some(tail_protocol), Some(progress)) = (&mut tail_protocol, progress) {
            tail_protocol.send_progress(progress);
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
    /// Send the current upper frontier of the tail.
    ///
    /// Does nothing if `self.complete()` has been called.
    fn send_progress(&mut self, upper: Antichain<Timestamp>) {
        if let Some(buffer) = &mut self.tail_response_buffer {
            buffer
                .borrow_mut()
                .push((self.sink_id, TailResponse::Progress(upper)));
        }
    }

    /// Send further rows as responses.
    ///
    /// Does nothing if `self.complete()` has been called.
    fn send_rows(&mut self, rows: Vec<(Row, Timestamp, Diff)>) {
        if let Some(buffer) = &mut self.tail_response_buffer {
            buffer
                .borrow_mut()
                .push((self.sink_id, TailResponse::Rows(rows)));
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
) -> Option<Antichain<Timestamp>> {
    // Test to see if strict progress has occurred. This is true if the new
    // frontier is not less or equal to the old frontier.
    let progress = !PartialOrder::less_equal(&new_input_frontier, &current_input_frontier.borrow());

    if progress {
        current_input_frontier.clear();
        current_input_frontier.extend(new_input_frontier.iter().cloned());

        Some(new_input_frontier.to_owned())
    } else {
        None
    }
}
