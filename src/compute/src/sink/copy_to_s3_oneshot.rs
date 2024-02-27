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
use std::str::FromStr;

use aws_types::sdk_config::SdkConfig;
use bytes::Bytes;
use bytesize::ByteSize;
use differential_dataflow::{Collection, Hashable};
use http::Uri;
use mz_aws_util::s3_uploader::{S3MultiPartUploader, S3MultiPartUploaderConfig};
use mz_compute_client::protocol::response::CopyToResponse;
use mz_compute_types::sinks::{ComputeSinkDesc, CopyToS3OneshotSinkConnection};
use mz_ore::cast::CastFrom;
use mz_pgcopy::{encode_copy_format, CopyFormatParams};
use mz_repr::{Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use mz_timely_util::operator::consolidate_pact;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::info;

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
        copy_to(
            sinked_collection,
            err_collection,
            sink_id,
            sink.up_to.clone(),
            self.clone(),
            connection_context,
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
    input_collection: Collection<G, Row, Diff>,
    err_collection: Collection<G, DataflowError, Diff>,
    sink_id: GlobalId,
    up_to: Antichain<G::Timestamp>,
    sink_connection: CopyToS3OneshotSinkConnection,
    connection_context: ConnectionContext,
    response_protocol_handle: Rc<RefCell<Option<ResponseProtocol>>>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = input_collection.scope();
    let worker_id = scope.index();

    let mut builder = AsyncOperatorBuilder::new("CopyToS3".to_string(), input_collection.scope());

    // Move all the data to a single worker and consolidate
    // TODO: split the data among worker to some number of buckets (either static or configurable
    // by the user) to split the load across the cluster.
    let active_worker = usize::cast_from(sink_id.hashed()) % scope.peers();
    let exchange = Exchange::new(move |_| u64::cast_from(active_worker));
    let error_exchange = Exchange::new(move |_| u64::cast_from(active_worker));
    let input = consolidate_pact::<KeyBatcher<_, _, _>, _, _, _, _, _>(
        &input_collection.map(|row| (row, ())),
        exchange,
        "Consolidated COPY TO S3 input",
    );

    let error = consolidate_pact::<KeyBatcher<_, _, _>, _, _, _, _, _>(
        &err_collection.map(|row| (row, ())),
        error_exchange,
        "Consolidated COPY TO S3 error",
    );

    let mut input_handle = builder.new_disconnected_input(&input.inner, Pipeline);
    let mut error_handle = builder.new_disconnected_input(&error.inner, Pipeline);

    let send_response = move |response: CopyToResponse| {
        if let Some(response_protocol) = response_protocol_handle.borrow_mut().deref_mut() {
            response_protocol.send_response(response);
        }
    };

    builder.build(move |_caps| async move {
        if worker_id != active_worker {
            return;
        }

        while let Some(event) = error_handle.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    if let Some(((error, _), _, _)) = data.first() {
                        send_response(CopyToResponse::Error(error.to_string()));
                        return;
                    }
                }
                AsyncEvent::Progress(frontier) => {
                    if PartialOrder::less_equal(&up_to, &frontier) {
                        // No error, break from loop and proceed
                        break;
                    }
                }
            }
        }

        // fix global ID
        let sdk_config = match sink_connection
            .aws_connection
            .load_sdk_config(&connection_context, GlobalId::Transient(0))
            .await
        {
            Ok(sdk_config) => sdk_config,
            Err(e) => {
                send_response(CopyToResponse::Error(e.to_string()));
                return;
            }
        };

        let mut uploader = CopyToS3Uploader {
            file_index: 0,
            desc: sink_connection.desc,
            buf: Vec::new(),
            max_file_size: sink_connection.max_file_size,
            file_name_prefix: "part".into(),
            path_prefix: sink_connection.prefix,
            sdk_config,
        };

        let mut row_count = 0;
        while let Some(event) = input_handle.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    for ((row, ()), _time, diff) in data {
                        assert!(diff > 0, "negative accumulation in copy to S3 input");
                        row_count += u64::try_from(diff).unwrap();
                        for _ in 0..diff {
                            uploader.append_row(&row).await;
                        }
                    }
                }
                AsyncEvent::Progress(frontier) => {
                    if PartialOrder::less_equal(&up_to, &frontier) {
                        let result = uploader.flush().await;
                        println!("result: {:?}", result);
                        // We are done, send the final count.
                        send_response(CopyToResponse::RowCount(row_count));
                        return;
                    }
                }
            }
        }
    });
}

/// Required state to upload batches to S3
struct CopyToS3Uploader {
    /// The output description.
    desc: RelationDesc,
    /// The index of the next file.
    file_index: usize,
    /// The prefix for the file names.
    file_name_prefix: String,
    /// The bucket prefix batches should be uploaded to.
    path_prefix: String,
    /// A buffer of encoded data.
    buf: Vec<u8>,
    /// The desired batch size. When the buffer exceeds this value a put operation will be
    /// performed.
    max_file_size: u64,
    /// The aws sdk config.
    sdk_config: SdkConfig,
}

impl CopyToS3Uploader {
    fn extract_s3_bucket_path(prefix: &str) -> (String, String) {
        let uri = Uri::from_str(prefix).expect("valid s3 url");
        let bucket = uri.host().expect("s3 bucket");
        let path = uri
            .path()
            .strip_prefix('/')
            .expect("s3 path")
            .strip_suffix('/')
            .expect("s3 path");
        (bucket.to_string(), path.to_string())
    }

    /// Flushes the internal buffer to S3 by creating a new batch. If there is no pending data to
    /// be uploaded this is a no-op.
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if !self.buf.is_empty() {
            // Actually put the data to S3
            self.file_index += 1;
            // TODO (mouli) refactor to buffer properly
            let (bucket, path_prefix) = Self::extract_s3_bucket_path(&self.path_prefix);
            let file_path = format!(
                "{}/{}-{:04}.csv",
                path_prefix, self.file_name_prefix, self.file_index
            );
            info!("starting upload at bucket: {}, file {}", bucket, file_path);
            let mut uploader = S3MultiPartUploader::try_new(
                &self.sdk_config,
                bucket,
                file_path,
                S3MultiPartUploaderConfig {
                    part_size_limit: ByteSize::mib(10).as_u64(),
                    file_size_limit: self.max_file_size,
                },
            )
            .await?;

            uploader.add_chunk(Bytes::from(self.buf.clone())).await?;
            uploader.finish().await?;
            self.buf.clear();
        }
        Ok(())
    }

    async fn append_row(&mut self, row: &Row) {
        // TODO (mouli): plumb user specified copy format params
        encode_copy_format(
            CopyFormatParams::Csv(Default::default()),
            row,
            self.desc.typ(),
            &mut self.buf,
        )
        .expect("TODO");
        if u64::cast_from(self.buf.len()) > self.max_file_size {
            let result = self.flush().await;
            println!("result: {:?}", result);
        }
    }
}

/// A type that guides the transmission of number of rows back to the coordinator.
struct ResponseProtocol {
    pub sink_id: GlobalId,
    pub response_buffer: Option<Rc<RefCell<Vec<(GlobalId, CopyToResponse)>>>>,
}

impl ResponseProtocol {
    fn send_response(&mut self, response: CopyToResponse) {
        let buffer = self.response_buffer.as_mut().expect("Copy response buffer");

        buffer.borrow_mut().push((self.sink_id, response));

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
