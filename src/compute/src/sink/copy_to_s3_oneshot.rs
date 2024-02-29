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

use differential_dataflow::{Collection, Hashable};
use mz_compute_client::protocol::response::CopyToResponse;
use mz_compute_types::sinks::{ComputeSinkDesc, CopyToS3OneshotSinkConnection};
use mz_ore::cast::CastFrom;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::consolidate_pact;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::Scope;
use timely::progress::Antichain;

use crate::render::sinks::SinkRender;
use crate::typedefs::KeyBatcher;

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
        let connection_context = compute_state.context.connection_context.clone();

        let one_time_callback = move |count: Result<u64, String>| {
            if let Some(response_protocol) = response_protocol_handle.borrow_mut().deref_mut() {
                response_protocol.send(count);
            }
        };

        // Move all the data to a single worker and consolidate
        // TODO(#7256): split the data among worker to some number of buckets (either static or configurable
        // by the user) to split the load across the cluster.
        let scope = sinked_collection.scope();
        let active_worker = usize::cast_from(sink_id.hashed()) % scope.peers();
        let exchange = Exchange::new(move |_| u64::cast_from(active_worker));
        let error_exchange = Exchange::new(move |_| u64::cast_from(active_worker));
        let input = consolidate_pact::<KeyBatcher<_, _, _>, _, _, _, _, _>(
            &sinked_collection.map(|row| (row, ())),
            exchange,
            "Consolidated COPY TO S3 input",
        );

        let error = consolidate_pact::<KeyBatcher<_, _, _>, _, _, _, _, _>(
            &err_collection.map(|row| (row, ())),
            error_exchange,
            "Consolidated COPY TO S3 error",
        );

        mz_storage_operators::s3_oneshot_sink::copy_to(
            input,
            error,
            sink.up_to.clone(),
            self.upload_info.clone(),
            connection_context,
            self.aws_connection.clone(),
            self.connection_id,
            active_worker,
            one_time_callback,
        );

        Some(Rc::new(scopeguard::guard((), move |_| {
            if let Some(protocol_handle) = response_protocol_weak.upgrade() {
                std::mem::drop(protocol_handle.borrow_mut().take())
            }
        })))
    }
}

/// A type that guides the transmission of number of rows back to the coordinator.
struct ResponseProtocol {
    pub sink_id: GlobalId,
    pub response_buffer: Option<Rc<RefCell<Vec<(GlobalId, CopyToResponse)>>>>,
}

impl ResponseProtocol {
    // This method should only be called once otherwise this will panic.
    fn send(&mut self, count: Result<u64, String>) {
        // The dataflow's input has been exhausted, clear the channel,
        // to avoid sending `CopyToResponse::Dropped`.
        let buffer = self.response_buffer.take().expect("expect response buffer");
        let response = match count {
            Ok(count) => CopyToResponse::RowCount(count),
            Err(error) => CopyToResponse::Error(error),
        };
        buffer.borrow_mut().push((self.sink_id, response));
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
