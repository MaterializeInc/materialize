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

use differential_dataflow::Collection;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;
use timely::progress::Antichain;
use timely::PartialOrder;

use mz_compute_client::response::{SubscribeBatch, SubscribeResponse};
use mz_compute_client::types::sinks::{ComputeSinkDesc, SinkAsOf, SubscribeSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::errors::DataflowError;

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for SubscribeSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, Row, Diff>,
        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.
        _err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        // An encapsulation of the Subscribe response protocol.
        // Used to send rows and progress messages,
        // and alert if the dataflow was dropped before completing.
        let subscribe_protocol_handle = Rc::new(RefCell::new(Some(SubscribeProtocol {
            sink_id,
            subscribe_response_buffer: Some(Rc::clone(&compute_state.subscribe_response_buffer)),
            prev_upper: Antichain::from_elem(Timestamp::minimum()),
        })));
        let subscribe_protocol_weak = Rc::downgrade(&subscribe_protocol_handle);

        subscribe(
            sinked_collection,
            sink_id,
            sink.as_of.clone(),
            sink.up_to.clone(),
            subscribe_protocol_handle,
        );

        // Inform the coordinator that we have been dropped,
        // and destroy the subscribe protocol so the sink operator
        // can't send spurious messages while shutting down.
        Some(Rc::new(scopeguard::guard((), move |_| {
            if let Some(subscribe_protocol_handle) = subscribe_protocol_weak.upgrade() {
                std::mem::drop(subscribe_protocol_handle.borrow_mut().take())
            }
        })))
    }
}

fn subscribe<G>(
    sinked_collection: Collection<G, Row, Diff>,
    sink_id: GlobalId,
    as_of: SinkAsOf,
    up_to: Antichain<G::Timestamp>,
    subscribe_protocol_handle: Rc<RefCell<Option<SubscribeProtocol>>>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let mut results = Vec::new();
    let mut finished = false;
    let mut rows = Default::default();
    sinked_collection
        .inner
        .sink(Pipeline, &format!("subscribe-{}", sink_id), move |input| {
            if finished {
                return;
            }
            input.for_each(|_, data| {
                data.swap(&mut rows);
                for (row, time, diff) in rows.drain(..) {
                    let should_emit_as_of = if as_of.strict {
                        as_of.frontier.less_than(&time)
                    } else {
                        as_of.frontier.less_equal(&time)
                    };
                    let should_emit = should_emit_as_of && !up_to.less_equal(&time);
                    if should_emit {
                        results.push((time, row, diff));
                    }
                }
            });

            if let Some(subscribe_protocol) = subscribe_protocol_handle.borrow_mut().deref_mut() {
                subscribe_protocol.send_batch(input.frontier().frontier().to_owned(), &mut results);
            }

            if PartialOrder::less_equal(&up_to.borrow(), &input.frontier().frontier()) {
                finished = true;
                // We are done; indicate this by sending a batch at the
                // empty frontier.
                if let Some(subscribe_protocol) = subscribe_protocol_handle.borrow_mut().deref_mut()
                {
                    subscribe_protocol.send_batch(Antichain::default(), &mut Vec::new());
                }
            }
        })
}

/// A type that guides the transmission of rows back to the coordinator.
///
/// A protocol instance may `send` rows indefinitely, and is consumed by `complete`,
/// which is used only to indicate the end of a stream. The `Drop` implementation
/// otherwise sends an indication that the protocol has finished without completion.
struct SubscribeProtocol {
    pub sink_id: GlobalId,
    pub subscribe_response_buffer: Option<Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>>,
    pub prev_upper: Antichain<Timestamp>,
}

impl SubscribeProtocol {
    fn send_batch(&mut self, upper: Antichain<Timestamp>, rows: &mut Vec<(Timestamp, Row, Diff)>) {
        if self.prev_upper != upper {
            let mut ship = Vec::new();
            let mut keep = Vec::new();
            differential_dataflow::consolidation::consolidate_updates(rows);
            for (time, data, diff) in rows.drain(..) {
                if upper.less_equal(&time) {
                    keep.push((time, data, diff));
                } else {
                    ship.push((time, data, diff));
                }
            }
            *rows = keep;

            let input_exhausted = upper.is_empty();
            let buffer = self
                .subscribe_response_buffer
                .as_mut()
                .expect("The subscribe response buffer is only cleared on drop.");
            buffer.borrow_mut().push((
                self.sink_id,
                SubscribeResponse::Batch(SubscribeBatch {
                    lower: self.prev_upper.clone(),
                    upper: upper.clone(),
                    updates: Ok(ship),
                }),
            ));
            self.prev_upper = upper;
            if input_exhausted {
                // The dataflow's input has been exhausted; clear the channel,
                // to avoid sending `SubscribeResponse::DroppedAt`.
                self.subscribe_response_buffer = None;
            }
        }
    }
}

impl Drop for SubscribeProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.subscribe_response_buffer.take() {
            buffer.borrow_mut().push((
                self.sink_id,
                SubscribeResponse::DroppedAt(self.prev_upper.clone()),
            ));
        }
    }
}
