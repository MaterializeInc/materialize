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

use mz_compute_client::response::{TailBatch, TailResponse};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage::client::controller::CollectionMetadata;
use mz_storage::client::sinks::{SinkAsOf, SinkDesc, TailSinkConnection};

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for TailSinkConnection
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
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &SinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        // An encapsulation of the Tail response protocol.
        // Used to send rows and progress messages,
        // and alert if the dataflow was dropped before completing.
        let tail_protocol_handle = Rc::new(RefCell::new(Some(TailProtocol {
            sink_id,
            tail_response_buffer: Some(Rc::clone(&compute_state.tail_response_buffer)),
            prev_upper: Antichain::from_elem(Timestamp::minimum()),
        })));
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
    let mut results = Vec::new();
    sinked_collection
        .inner
        .sink(Pipeline, &format!("tail-{}", sink_id), move |input| {
            input.for_each(|_, rows| {
                for ((k, v), time, diff) in rows.iter() {
                    assert!(k.is_none(), "tail does not support keys");
                    let row = v.as_ref().expect("tail must have values");
                    let should_emit = if as_of.strict {
                        as_of.frontier.less_than(time)
                    } else {
                        as_of.frontier.less_equal(time)
                    };
                    if should_emit {
                        results.push((*time, row.clone(), *diff));
                    }
                }
            });

            if let Some(tail_protocol) = tail_protocol_handle.borrow_mut().deref_mut() {
                tail_protocol.send_batch(input.frontier().frontier().to_owned(), &mut results);
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
    pub prev_upper: Antichain<Timestamp>,
}

impl TailProtocol {
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
                .tail_response_buffer
                .as_mut()
                .expect("The tail response buffer is only cleared on drop.");
            buffer.borrow_mut().push((
                self.sink_id,
                TailResponse::Batch(TailBatch {
                    lower: self.prev_upper.clone(),
                    upper: upper.clone(),
                    updates: ship,
                }),
            ));
            self.prev_upper = upper;
            if input_exhausted {
                // The dataflow's input has been exhausted; clear the channel,
                // to avoid sending `TailResponse::DroppedAt`.
                self.tail_response_buffer = None;
            }
        }
    }
}

impl Drop for TailProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.tail_response_buffer.take() {
            buffer.borrow_mut().push((
                self.sink_id,
                TailResponse::DroppedAt(self.prev_upper.clone()),
            ));
        }
    }
}
