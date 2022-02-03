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
use std::ops::DerefMut;
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

use mz_dataflow_types::{
    sinks::{SinkAsOf, SinkDesc, TailSinkConnector},
    TailResponse,
};
use mz_expr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};

use crate::render::sinks::SinkRender;

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
        compute_state: &mut crate::server::ComputeState,
        sink: &SinkDesc,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        // An encapsulation of the Tail response protocol.
        // Used to send rows and progress messages,
        // and alert if the dataflow was dropped before completing.
        let tail_protocol_handle = Rc::new(RefCell::new(if true {
            Some(TailProtocol {
                sink_id,
                tail_response_buffer: Some(Rc::clone(&compute_state.tail_response_buffer)),
            })
        } else {
            None
        }));
        let tail_protocol_weak = Rc::downgrade(&tail_protocol_handle);

        tail(
            sinked_collection,
            sink_id,
            sink.as_of.clone(),
            tail_protocol_handle,
        );

        // Inform the coordinator that we have been dropped,
        // and destroy the tail protocol so the sink operator
        // can't send spurious messages while shutting down.
        Some(Rc::new(scopeguard::guard((), move |_| {
            if let Some(tail_protocol_handle) = tail_protocol_weak.upgrade() {
                std::mem::drop(tail_protocol_handle.borrow_mut().take())
            }
        })))
    }
}

fn tail<G>(
    sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    sink_id: GlobalId,
    as_of: SinkAsOf,
    tail_protocol_handle: Rc<RefCell<Option<TailProtocol>>>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    // // make sure all data is routed to one worker by keying on the sink id
    // let batches = sinked_collection
    //     .map(move |(k, v)| {
    //         assert!(k.is_none(), "tail does not support keys");
    //         let v = v.expect("tail must have values");
    //         (sink_id, v)
    //     })
    //     .arrange_by_key()
    //     .stream;

    // Initialize to the minimal input frontier.
    let mut input_frontier = Antichain::from_elem(<G::Timestamp as TimelyTimestamp>::minimum());

    sinked_collection.inner.sink(Pipeline, &format!("tail-{}", sink_id), move |input| {
        input.for_each(|_, rows| {
            let mut results = vec![];
            for ((k, v), time, diff) in rows.iter() {
                assert!(k.is_none(), "tail does not support keys");
                let row = v.as_ref().expect("tail must have values");
                let should_emit = if as_of.strict {
                    as_of.frontier.less_than(time)
                } else {
                    as_of.frontier.less_equal(time)
                };
                if should_emit {
                    results.push((row.clone(), *time, *diff));
                }
            }

            // Emit data if configured, otherwise it is an error to have data to send.
            if let Some(tail_protocol) = tail_protocol_handle.borrow_mut().deref_mut() {
                tail_protocol.send_rows(results);
            }
        });

        let progress = update_progress(&mut input_frontier, input.frontier().frontier());

        // Only emit updates if this operator/worker received actual data for emission.
        if let (Some(tail_protocol), Some(progress)) =
            (tail_protocol_handle.borrow_mut().deref_mut(), progress)
        {
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
    fn send_progress(&mut self, upper: Antichain<Timestamp>) {
        let input_exhausted = upper.is_empty();
        let buffer = self
            .tail_response_buffer
            .as_mut()
            .expect("The tail response buffer is only cleared on drop.");
        buffer
            .borrow_mut()
            .push((self.sink_id, TailResponse::Progress(upper)));
        if input_exhausted {
            // The dataflow's input has been exhausted; clear the channel,
            // to avoid sending `TailResponse::Dropped`.
            self.tail_response_buffer = None;
        }
    }

    /// Send further rows as responses.
    fn send_rows(&mut self, rows: Vec<(Row, Timestamp, Diff)>) {
        let buffer = self
            .tail_response_buffer
            .as_mut()
            .expect("Unexpectedly saw more rows! The tail response buffer should only have been cleared when either the dataflow was dropped or the input was exhausted.");
        buffer
            .borrow_mut()
            .push((self.sink_id, TailResponse::Rows(rows)));
    }
}

impl Drop for TailProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.tail_response_buffer.take() {
            buffer
                .borrow_mut()
                .push((self.sink_id, TailResponse::Dropped));
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
