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
use mz_compute_client::protocol::response::CopyToResponse;
use mz_compute_types::sinks::{ComputeSinkDesc, CopyToS3OneshotSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for CopyToS3OneshotSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        // An encapsulation of the copy to response protocol.
        // Used to send rows and errors if this fails.
        let response_protocol_handle = Rc::new(RefCell::new(Some(ResponseProtocol {
            sink_id,
            response_buffer: Some(Rc::clone(&compute_state.copy_to_response_buffer)),
        })));
        let response_protocol_weak = Rc::downgrade(&response_protocol_handle);

        copy_to(
            sinked_collection,
            err_collection,
            sink_id,
            sink.up_to.clone(),
            response_protocol_handle,
        );

        Some(Rc::new(scopeguard::guard((), move |_| {
            if let Some(protocol_handle) = response_protocol_weak.upgrade() {
                std::mem::drop(protocol_handle.borrow_mut().take())
            }
        })))
    }
}

fn copy_to<G>(
    sinked_collection: Collection<G, Row, Diff>,
    err_collection: Collection<G, DataflowError, Diff>,
    sink_id: GlobalId,
    up_to: Antichain<G::Timestamp>,
    response_protocol_handle: Rc<RefCell<Option<ResponseProtocol>>>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let name = format!("s3-oneshot-{}", sink_id);
    let mut op = OperatorBuilder::new(name, sinked_collection.scope());
    let mut ok_input = op.new_input(&sinked_collection.inner, Pipeline);
    let mut err_input = op.new_input(&err_collection.inner, Pipeline);

    op.build(|_cap| {
        let mut row_count: u64 = 0;
        let mut finished = false;
        let mut ok_buf = Default::default();

        move |frontiers| {
            if finished {
                // Drain the inputs, to avoid the operator being constantly rescheduled
                ok_input.for_each(|_, _| {});
                err_input.for_each(|_, _| {});
                return;
            }

            let mut frontier = Antichain::new();
            for input_frontier in frontiers {
                frontier.extend(input_frontier.frontier().iter().copied());
            }

            ok_input.for_each(|_, data| {
                data.swap(&mut ok_buf);
                let count: u64 = ok_buf.drain(..).len().try_into().expect("usize into u64");
                // TODO (mouli): write these to s3 in subsequent PRs
                row_count = row_count + count;
            });
            // TODO (mouli): handle errors
            if PartialOrder::less_equal(&up_to, &frontier) {
                finished = true;
                // We are done, send the final count.
                if let Some(response_protocol) = response_protocol_handle.borrow_mut().deref_mut() {
                    response_protocol.send_final_count(row_count);
                }
            }
        }
    });
}

/// A type that guides the transmission of number of rows back to the coordinator.
struct ResponseProtocol {
    pub sink_id: GlobalId,
    pub response_buffer: Option<Rc<RefCell<Vec<(GlobalId, CopyToResponse)>>>>,
}

impl ResponseProtocol {
    fn send_final_count(&mut self, rows: u64) {
        let buffer = self.response_buffer.as_mut().expect("Copy response buffer");

        buffer
            .borrow_mut()
            .push((self.sink_id, CopyToResponse::RowCount(rows)));

        // The dataflow's input has been exhausted, clear the channel,
        // to avoid sending `CopyToResponse::Dropped`.
        self.response_buffer = None;
    }
}

impl Drop for ResponseProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.response_buffer.take() {
            buffer
                .borrow_mut()
                .push((self.sink_id, CopyToResponse::Dropped));
        }
    }
}
