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
use mz_pgcopy::encode_copy_row_text;
use mz_repr::{Diff, GlobalId, RelationType, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::builder_async::Event;
use mz_timely_util::operator::{consolidate_pact, StreamExt};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;

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
    input: Collection<G, Row, Diff>,
    _err_collection: Collection<G, DataflowError, Diff>,
    sink_id: GlobalId,
    up_to: Antichain<G::Timestamp>,
    response_protocol_handle: Rc<RefCell<Option<ResponseProtocol>>>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = input.scope();
    let worker_id = scope.index();

    // Move all the data to a single worker and consolidate
    // TODO: split the data among worker to some number of buckets (either static or configurable
    // by the user) to split the load across the cluster.
    let active_worker = usize::cast_from(sink_id.hashed()) % scope.peers();
    let exchange = Exchange::new(move |_| u64::cast_from(active_worker));
    let input = input.map(|row| (row, ()));
    let input = consolidate_pact::<KeyBatcher<_, _, _>, _, _, _, _, _>(
        &input,
        exchange,
        "Consolidated COPY TO S3 input",
    );

    input
        .inner
        .sink_async(Pipeline, "CopyToS3".into(), |_, mut input| async move {
            if worker_id != active_worker {
                return;
            }

            // TODO: get these from the SQL query
            let mut uploader = S3Uploader {
                batch_index: 0,
                typ: None.expect("TODO"),
                buf: Vec::new(),
                // TODO: get these from SQL
                batch_size: 1024 * 1024,
                prefix: "foobar".into(),
            };

            let mut row_count = 0;
            while let Some(event) = input.next().await {
                match event {
                    Event::Data(_ts, data) => {
                        for ((row, ()), _time, diff) in data {
                            assert!(diff > 0, "negative accumulation in copy to S3 input");
                            row_count += u64::try_from(diff).unwrap();
                            for _ in 0..diff {
                                uploader.append_row(&row).await;
                            }
                        }
                    }
                    Event::Progress(frontier) => {
                        if PartialOrder::less_equal(&up_to, &frontier) {
                            uploader.flush().await;
                            // We are done, send the final count.
                            // TODO: what should happen if the error collection is not empty?
                            if let Some(response_protocol) =
                                response_protocol_handle.borrow_mut().deref_mut()
                            {
                                response_protocol.send_final_count(row_count);
                            }
                            return;
                        }
                    }
                }
            }
        });
}

/// Required state to upload batches to S3
struct S3Uploader {
    /// The expected row schema
    typ: RelationType,
    /// The index of the next batch
    batch_index: usize,
    /// The bucket prefix batches should be uploaded to.
    prefix: String,
    /// A buffer of encoded data
    buf: Vec<u8>,
    /// The desired batch size. When the buffer exceeds this value a put operation will be
    /// performed.
    batch_size: usize,
}

impl S3Uploader {
    /// Flushes the internal buffer to S3 by creating a new batch. If there is no pending data to
    /// be uploaded this is a no-op.
    async fn flush(&mut self) {
        if !self.buf.is_empty() {
            // Actually put the data to S3
            self.batch_index += 1;
            self.buf.clear();
        }
    }

    async fn append_row(&mut self, row: &Row) {
        encode_copy_row_text(&row, &self.typ, &mut self.buf).expect("TODO");
        if self.buf.len() > self.batch_size {
            self.flush().await;
        }
    }
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
