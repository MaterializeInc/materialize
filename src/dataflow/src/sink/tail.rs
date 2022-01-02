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

use differential_dataflow::operators::Consolidate;

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
    // `progress_sending_worker` indicates whether to send progress updates to the coordinator
    // (It is unnecessary and wasteful to notify the worker more than once for each timestamp that goes by).
    let scope = sinked_collection.scope();
    use differential_dataflow::Hashable;
    let progress_sending_worker = (sink_id.hashed() as usize) % scope.peers() == scope.index();

    // Initialize to the minimal input frontier.
    let mut input_frontier = Antichain::from_elem(<G::Timestamp as TimelyTimestamp>::minimum());
    // An encapsulation of the Tail response protocol.
    // Used to send rows and eventually mark complete.
    let mut tail_protocol = Some(TailProtocol {
        sink_id,
        tail_response_buffer: Some(tail_response_buffer),
    });

    sinked_collection.consolidate().inner.sink(
        Pipeline,
        &format!("tail-{}", sink_id),
        move |input| {
            input.for_each(|_, rows| {
                let mut results = vec![];
                for ((k, v), time, diff) in rows.iter() {
                    assert!(k.is_none(), "tail does not support keys");
                    let v = v.as_ref().expect("tail must have values");
                    let should_emit = if as_of.strict {
                        as_of.frontier.less_than(time)
                    } else {
                        as_of.frontier.less_equal(time)
                    };
                    if should_emit {
                        results.push((v.clone(), *time, *diff));
                    }
                }

                if let Some(tail_protocol) = &mut tail_protocol {
                    tail_protocol.send_rows(results);
                }
            });

            let progress = update_progress(&mut input_frontier, input.frontier().frontier());

            // Only emit updates if this operator/worker received actual data for emission.
            if let (true, Some(progress), Some(tail_protocol)) =
                (progress_sending_worker, progress, &mut tail_protocol)
            {
                tail_protocol.send_progress(progress);
            }

            // If the frontier is empty the tailing is complete. We should say so!
            if progress_sending_worker && input.frontier().frontier().is_empty() {
                if let Some(tail_protocol) = tail_protocol.take() {
                    tail_protocol.complete();
                }
            }
        },
    )
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
