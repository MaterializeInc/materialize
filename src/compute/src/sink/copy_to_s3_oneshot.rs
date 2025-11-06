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

use differential_dataflow::{Hashable, VecCollection};
use mz_compute_client::protocol::response::CopyToResponse;
use mz_compute_types::dyncfgs::{
    COPY_TO_S3_ARROW_BUILDER_BUFFER_RATIO, COPY_TO_S3_MULTIPART_PART_SIZE_BYTES,
    COPY_TO_S3_PARQUET_ROW_GROUP_FILE_RATIO,
};
use mz_compute_types::sinks::{ComputeSinkDesc, CopyToS3OneshotSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::consolidate_pact;
use mz_timely_util::probe::{Handle, ProbeNotify};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Operator;
use timely::progress::Antichain;
use tokio::sync::watch;

use crate::render::StartSignal;
use crate::render::sinks::SinkRender;
use crate::typedefs::KeyBatcher;

impl<G> SinkRender<G> for CopyToS3OneshotSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        sinked_collection: VecCollection<G, Row, Diff>,
        err_collection: VecCollection<G, DataflowError, Diff>,
        _ct_times: Option<VecCollection<G, (), Diff>>,
        output_probe: &Handle<Timestamp>,
        _read_only_rx: watch::Receiver<bool>,
    ) -> Option<Rc<dyn Any>> {
        // Set up a callback to communicate the result of the copy-to operation to the controller.
        let mut response_protocol = ResponseProtocol {
            sink_id,
            response_buffer: Some(Rc::clone(&compute_state.copy_to_response_buffer)),
        };
        let result_callback = move |count: Result<u64, String>| {
            response_protocol.send(count);
        };

        // Splitting the data across a known number of batches to distribute load across the cluster.
        // Each worker will be handling data belonging to 0 or more batches. We are doing this so that
        // we can write files to s3 deterministically across different replicas of different sizes
        // using the batch ID. Each worker will split a batch's data into 1 or more
        // files based on the user provided `MAX_FILE_SIZE`.
        let batch_count = self.output_batch_count;

        // We exchange the data according to batch, but we don't want to send the batch ID to the
        // sink. The sink can re-compute the batch ID from the data.
        let input = consolidate_pact::<KeyBatcher<_, _, _>, _, _>(
            &sinked_collection.map(move |row| (row, ())).inner,
            Exchange::new(move |((row, ()), _, _): &((Row, _), _, _)| row.hashed() % batch_count),
            "Consolidated COPY TO S3 input",
        )
        .probe_notify_with(vec![output_probe.clone()]);

        // We need to consolidate the error collection to ensure we don't act on retracted errors.
        let error = consolidate_pact::<KeyBatcher<_, _, _>, _, _>(
            &err_collection.map(move |err| (err, ())).inner,
            Exchange::new(move |((err, _), _, _): &((DataflowError, _), _, _)| {
                err.hashed() % batch_count
            }),
            "Consolidated COPY TO S3 errors",
        );

        // We can only propagate the one error back to the client, so filter the error
        // collection to the first error that is before the sink 'up_to' to avoid
        // sending the full error collection to the next operator. We ensure we find the
        // first error before the 'up_to' to avoid accidentally sending an irrelevant error.
        let error_stream =
            error.unary_frontier(Pipeline, "COPY TO S3 error filtering", |_cap, _info| {
                let up_to = sink.up_to.clone();
                let mut received_one = false;
                move |(input, _), output| {
                    input.for_each_time(|time, data| {
                        if !up_to.less_equal(time.time()) && !received_one {
                            received_one = true;
                            output.session(&time).give_iterator(
                                data.flatten()
                                    .flatten()
                                    .flat_map(|chunk| chunk.iter().cloned())
                                    .next()
                                    .map(|((err, ()), time, diff)| (err, time, diff))
                                    .into_iter(),
                            );
                        }
                    });
                }
            });

        let params = mz_storage_operators::s3_oneshot_sink::CopyToParameters {
            parquet_row_group_ratio: COPY_TO_S3_PARQUET_ROW_GROUP_FILE_RATIO
                .get(&compute_state.worker_config),
            arrow_builder_buffer_ratio: COPY_TO_S3_ARROW_BUILDER_BUFFER_RATIO
                .get(&compute_state.worker_config),
            s3_multipart_part_size_bytes: COPY_TO_S3_MULTIPART_PART_SIZE_BYTES
                .get(&compute_state.worker_config),
        };

        let token = mz_storage_operators::s3_oneshot_sink::copy_to(
            input,
            error_stream,
            sink.up_to.clone(),
            self.upload_info.clone(),
            compute_state.context.connection_context.clone(),
            self.aws_connection.clone(),
            sink_id,
            self.connection_id,
            params,
            result_callback,
            self.output_batch_count,
        );

        Some(token)
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
