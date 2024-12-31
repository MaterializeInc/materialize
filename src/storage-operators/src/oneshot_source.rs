// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use differential_dataflow::Hashable;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use mz_ore::cast::CastFrom;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::oneshot_sources::{ContentFormat, ContentSource, OneshotIngestionRequest};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::operator::StreamExt as TimelyStreamExt;
use mz_timely_util::pact::Distribute;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::LinkedList;
use std::fmt::{Debug, Display};
use std::future::Future;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;
use tracing::info;

use crate::oneshot_source::csv::{CsvDecoder, CsvRecord, CsvWorkRequest};
use crate::oneshot_source::http_source::{HttpChecksum, HttpObject, HttpOneshotSource};
use crate::oneshot_source::parquet::{ParquetFormat, ParquetRowGroup, ParquetWorkRequest};

pub mod csv;
pub mod parquet;

pub mod http_source;

pub fn render<G, F>(
    scope: G,
    persist_clients: Arc<PersistClientCache>,
    collection_id: GlobalId,
    collection_meta: CollectionMetadata,
    request: OneshotIngestionRequest,
    worker_callback: F,
) -> Vec<PressOnDropButton>
where
    G: Scope<Timestamp = Timestamp>,
    F: FnOnce(Result<(ProtoBatch, u64), String>) -> () + 'static,
{
    let OneshotIngestionRequest { source, format } = request;

    let source = match source {
        ContentSource::Http { url } => {
            let source = HttpOneshotSource::new(reqwest::Client::default(), url);
            SourceKind::Http(source)
        }
    };
    let format = match format {
        ContentFormat::Csv => {
            let format = CsvDecoder::default();
            FormatKind::Csv(format)
        }
        ContentFormat::Parquet => {
            let format = ParquetFormat::new(collection_meta.relation_desc.clone());
            FormatKind::Parquet(format)
        }
    };

    // Discover what objects are available to copy.
    let (objects_stream, objects_errors, discover_token) =
        render_discover_objects(scope.clone(), collection_id, source.clone());
    // Split the objects into individual units of work.
    let (work_stream, work_errors, split_token) = render_split_work(
        scope.clone(),
        collection_id,
        &objects_stream,
        source.clone(),
        format.clone(),
    );
    // Process each unit of work, returning chunks of records.
    let (records_stream, records_error, process_token) = render_process_work(
        scope.clone(),
        collection_id,
        source.clone(),
        format.clone(),
        &work_stream,
    );
    // Parse chunks of records into Rows.
    let (rows_stream, rows_errors, decode_token) =
        render_decode_chunk(scope.clone(), format.clone(), &records_stream);
    // Stage the Rows in Persist.
    let (batch_stream, batch_token) = render_stage_batches_operator2(
        scope.clone(),
        collection_id,
        &collection_meta,
        persist_clients,
        &rows_stream,
    );

    // Collect all of the errors into a single stream.
    let errors = objects_errors
        .concat(&work_errors)
        .concat(&records_error)
        .concat(&rows_errors);
    // Connect our final result and error stream.
    let results = batch_stream
        .map(|x| Ok::<_, String>(x))
        .concat(&errors.map(|err| Err::<(ProtoBatch, u64), _>(err)));

    // Collect all results together and notify the upstream of whether or not we succeeded.
    render_completion_operator(scope, collection_id, &results, worker_callback);

    let tokens = vec![
        discover_token,
        split_token,
        process_token,
        decode_token,
        batch_token,
    ];

    tokens
}

pub fn render_discover_objects<G, S>(
    scope: G,
    collection_id: GlobalId,
    source: S,
) -> (
    TimelyStream<G, (S::Object, S::Checksum)>,
    TimelyStream<G, String>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: OneshotSource + 'static,
{
    let worker_id = scope.index();
    let num_workers = scope.peers();
    let leader_id = usize::cast_from((collection_id, "discover").hashed()) % num_workers;
    let is_leader = worker_id == leader_id;

    let mut builder = AsyncOperatorBuilder::new("CopyFrom-discover".to_string(), scope.clone());

    let (start_handle, start_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (error_handle, error_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let shutdown = builder.build(move |caps| async move {
        let [start_cap, errors_cap] = caps.try_into().unwrap();

        if !is_leader {
            return;
        }

        info!(%collection_id, %worker_id, "CopyFrom Leader Discover");

        // Wrap our work in a block to capture `?`.
        let work = async move {
            mz_ore::task::spawn(|| "discover", async move { source.list().await })
                .await
                .expect("failed to spawn task")
        }
        .await
        .context("discover");

        match work {
            Ok(mut discovered) => start_handle.give_container(&start_cap, &mut discovered),
            Err(err) => error_handle.give(&errors_cap, err.to_string()),
        }
    });

    (
        start_stream.distribute(),
        error_stream,
        shutdown.press_on_drop(),
    )
}

pub fn render_split_work<G, S, F>(
    scope: G,
    collection_id: GlobalId,
    objects: &TimelyStream<G, (S::Object, S::Checksum)>,
    source: S,
    format: F,
) -> (
    TimelyStream<G, F::WorkRequest<S>>,
    TimelyStream<G, String>,
    PressOnDropButton,
)
where
    G: Scope,
    S: OneshotSource + Send + Sync + 'static,
    F: OneshotFormat + Send + Sync + 'static,
{
    let worker_id = scope.index();
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-split_work".to_string(), scope.clone());

    let (request_handle, request_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (error_handle, errors_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let mut objects_handle = builder.new_input_for(objects, Distribute, &request_handle);

    let shutdown = builder.build(move |caps| async move {
        let [objects_cap, errors_cap] = caps.try_into().unwrap();

        info!(%collection_id, %worker_id, "CopyFrom Split Work");

        // Wrap our work in a block to capture `?`.
        let result = async {
            while let Some(event) = objects_handle.next().await {
                let objects = match event {
                    AsyncEvent::Data(_cap, req) => req,
                    AsyncEvent::Progress(_) => continue,
                };

                for (object, checksum) in objects {
                    let format_ = format.clone();
                    let source_ = source.clone();
                    let mut work = mz_ore::task::spawn(|| "split-work", async move {
                        format_.split_work(source_.clone(), object, checksum).await
                    })
                    .await
                    .expect("failed to spawn task")?;
                    request_handle.give_container(&objects_cap, &mut work);
                }
            }

            Ok::<_, StorageErrorX>(())
        }
        .await
        .context("split");

        if let Err(err) = result {
            error_handle.give(&errors_cap, err.to_string());
        }
    });

    (
        request_stream.distribute(),
        errors_stream,
        shutdown.press_on_drop(),
    )
}

pub fn render_process_work<G, S, F>(
    scope: G,
    collection_id: GlobalId,
    source: S,
    format: F,
    work_requests: &TimelyStream<G, F::WorkRequest<S>>,
) -> (
    TimelyStream<G, F::RecordChunk>,
    TimelyStream<G, String>,
    PressOnDropButton,
)
where
    G: Scope,
    S: OneshotSource + Sync + 'static,
    F: OneshotFormat + Sync + 'static,
{
    let worker_id = scope.index();
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-process_work".to_string(), scope.clone());

    let (record_handle, record_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (error_handle, error_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let mut work_requests_handle = builder.new_input_for(work_requests, Distribute, &record_handle);

    let shutdown = builder.build(move |caps| async move {
        let [work_cap, errors_cap] = caps.try_into().unwrap();

        info!(%collection_id, %worker_id, "CopyFrom Process Work");

        // Wrap our work in a block to capture `?`.
        let result = async {
            while let Some(event) = work_requests_handle.next().await {
                let work_requests = match event {
                    AsyncEvent::Data(_cap, req) => req,
                    AsyncEvent::Progress(_) => continue,
                };

                // Process each stream of work, one at a time.
                for request in work_requests {
                    let mut work_stream = format.process_work(&source, request);
                    while let Some(result) = work_stream.next().await {
                        let record_chunk = result.context("process worker")?;
                        record_handle.give(&work_cap, record_chunk);
                    }
                }
            }

            Ok::<_, StorageErrorX>(())
        }
        .await
        .context("process work");

        if let Err(err) = result {
            error_handle.give(&errors_cap, err.to_string());
        }
    });

    (
        record_stream.distribute(),
        error_stream,
        shutdown.press_on_drop(),
    )
}

pub fn render_decode_chunk<G, F>(
    scope: G,
    format: F,
    record_chunks: &TimelyStream<G, F::RecordChunk>,
) -> (
    TimelyStream<G, Row>,
    TimelyStream<G, String>,
    PressOnDropButton,
)
where
    G: Scope,
    F: OneshotFormat + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-decode_chunk".to_string(), scope.clone());

    let (row_handle, row_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (error_handle, error_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let mut record_chunk_handle = builder.new_input_for(record_chunks, Pipeline, &row_handle);

    let shutdown = builder.build(move |caps| async move {
        let [row_cap, errors_cap] = caps.try_into().unwrap();
        let mut rows = Vec::new();

        let result = async {
            while let Some(event) = record_chunk_handle.next().await {
                let record_chunk = match event {
                    AsyncEvent::Data(_cap, data) => data,
                    AsyncEvent::Progress(_) => continue,
                };

                for chunk in record_chunk {
                    format.decode_chunk(chunk, &mut rows)?;
                    row_handle.give_container(&row_cap, &mut rows);
                    rows.clear();
                }
            }

            Ok::<_, StorageErrorX>(())
        }
        .await
        .context("decode");

        if let Err(err) = result {
            error_handle.give(&errors_cap, err.to_string());
        }
    });

    (row_stream, error_stream, shutdown.press_on_drop())
}

pub fn render_stage_batches_operator2<G>(
    scope: G,
    collection_id: GlobalId,
    collection_meta: &CollectionMetadata,
    persist_clients: Arc<PersistClientCache>,
    rows_stream: &TimelyStream<G, Row>,
) -> (TimelyStream<G, (ProtoBatch, u64)>, PressOnDropButton)
where
    G: Scope,
{
    let persist_location = collection_meta.persist_location.clone();
    let shard_id = collection_meta.data_shard;
    let collection_desc = collection_meta.relation_desc.clone();

    let mut builder =
        AsyncOperatorBuilder::new("CopyFrom-stage_batches".to_string(), scope.clone());

    let (proto_batch_handle, proto_batch_stream) =
        builder.new_output::<CapacityContainerBuilder<_>>();
    let mut rows_handle = builder.new_input_for(rows_stream, Pipeline, &proto_batch_handle);

    let shutdown = builder.build(move |caps| async move {
        let [proto_batch_cap] = caps.try_into().unwrap();

        let mut count = 0;

        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("failed to open Persist client");
        let persist_diagnostics = Diagnostics {
            shard_name: collection_id.to_string(),
            handle_purpose: "CopyFrom::stage_batches".to_string(),
        };
        let write_handle = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, Diff>(
                shard_id,
                Arc::new(collection_desc),
                Arc::new(UnitSchema),
                persist_diagnostics,
            )
            .await
            .expect("could not open Persist shard");

        // TODO(parkmcar): Use a different timestamp here.
        let lower = mz_repr::Timestamp::MIN;
        let mut batch_builder = write_handle.builder(Antichain::from_elem(lower));

        while let Some(event) = rows_handle.next().await {
            if let AsyncEvent::Data(_cap, row_batch) = event {
                count += u64::cast_from(row_batch.len());

                // Stage all of our rows into a single batch.
                for row in row_batch {
                    let data = SourceData(Ok(row));
                    batch_builder
                        .add(&data, &(), &lower, &1)
                        .await
                        .expect("failed to add Row to batch");
                }
            }
        }

        // TODO(parkmycar): Use a different upper here.
        let upper = Antichain::from_elem(lower.step_forward());
        let batch = batch_builder
            .finish(upper)
            .await
            .expect("failed to create Batch");

        // Turn our Batch into a ProtoBatch that will later be linked in to
        // the shard.
        //
        // Note: By turning this into a ProtoBatch, the onus is now on us to
        // cleanup the Batch if it's never linked into the shard.
        //
        // TODO(parkmycar): Make sure these batches get cleaned up if another
        // worker encounters an error.
        let proto_batch = batch.into_transmittable_batch();

        proto_batch_handle.give(&proto_batch_cap, (proto_batch, count));
    });

    (proto_batch_stream, shutdown.press_on_drop())
}

pub fn render_completion_operator<G, F>(
    scope: G,
    collection_id: GlobalId,
    results_stream: &TimelyStream<G, Result<(ProtoBatch, u64), String>>,
    worker_callback: F,
) where
    G: Scope,
    F: FnOnce(Result<(ProtoBatch, u64), String>) -> () + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-completion".to_string(), scope.clone());
    let mut results_input = builder.new_disconnected_input(&results_stream, Pipeline);

    builder.build(move |_| async move {
        let result = async move {
            let mut maybe_payload: Option<(ProtoBatch, u64)> = None;
            while let Some(event) = results_input.next().await {
                if let AsyncEvent::Data(_cap, results) = event {
                    let [result] = results
                        .try_into()
                        .expect("only 1 event on the result stream");
                    maybe_payload = Some(result?);
                }
            }
            let row_count = maybe_payload.ok_or_else(|| "did not receive row count".to_string())?;

            Ok(row_count)
        }
        .await;

        // Report to the caller of our final status.
        worker_callback(result);
    });
}

/// Experimental Error Type.
///
/// The goal of this type is to combine concepts from both `thiserror` and
/// `anyhow`. Having "stongly typed" errors from `thiserror` is useful for
/// determining what action to take and tracking the context of an error like
/// `anyhow` is useful for determining where an error came from.
///
/// TODO(parkmycar): Replace this with `snafu`.
#[derive(Debug)]
pub struct StorageErrorX {
    kind: StorageErrorXKind,
    context: LinkedList<String>,
}

impl fmt::Display for StorageErrorX {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "error: {}", self.kind)?;
        writeln!(f, "causes: {:?}", self.context)?;
        Ok(())
    }
}

/// Experimental Error Type, see [`StorageErrorX`].
#[derive(Debug, thiserror::Error)]
pub enum StorageErrorXKind {
    #[error("csv decoding error: {0}")]
    CsvDecoding(#[from] csv_async::Error),
    #[error("parquet error: {0}")]
    ParquetError(#[from] ::parquet::errors::ParquetError),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("invalid reqwest header: {0}")]
    InvalidHeader(#[from] reqwest::header::ToStrError),
    #[error("failed to get the size of an object")]
    MissingSize,
    #[error("something went wrong: {0}")]
    Generic(String),
}

impl StorageErrorXKind {
    pub fn with_context<C: Display>(self, context: C) -> StorageErrorX {
        StorageErrorX {
            kind: self,
            context: LinkedList::from([context.to_string()]),
        }
    }

    pub fn generic<C: Display>(error: C) -> StorageErrorXKind {
        StorageErrorXKind::Generic(error.to_string())
    }
}

impl<E> From<E> for StorageErrorX
where
    E: Into<StorageErrorXKind>,
{
    fn from(err: E) -> Self {
        StorageErrorX {
            kind: err.into(),
            context: LinkedList::new(),
        }
    }
}

trait StorageErrorXContext<T> {
    fn context<C>(self, context: C) -> Result<T, StorageErrorX>
    where
        C: Display;
}

impl<T, E> StorageErrorXContext<T> for Result<T, E>
where
    E: Into<StorageErrorXKind>,
{
    fn context<C>(self, context: C) -> Result<T, StorageErrorX>
    where
        C: Display,
    {
        match self {
            Ok(val) => Ok(val),
            Err(kind) => Err(StorageErrorX {
                kind: kind.into(),
                context: LinkedList::from([context.to_string()]),
            }),
        }
    }
}

impl<T> StorageErrorXContext<T> for Result<T, StorageErrorX> {
    fn context<C>(self, context: C) -> Result<T, StorageErrorX>
    where
        C: Display,
    {
        match self {
            Ok(val) => Ok(val),
            Err(mut e) => {
                e.context.push_back(context.to_string());
                Err(e)
            }
        }
    }
}

pub trait OneshotObject {
    /// Name of the object, including any extensions.
    fn name(&self) -> &str;

    /// Size of this object in bytes.
    fn size(&self) -> usize;

    /// Encodings of the _entire_ object, if any.
    ///
    /// Note: The object may internally use compression, e.g. a Parquet file
    /// could compress its column chunks, but if the Parquet file itself is not
    /// compressed then this would return `None`.
    fn encodings(&self) -> &[Encoding];
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Encoding {
    Brotli,
    Bzip2,
    Gzip,
    Xz,
    Zstd,
}

pub trait OneshotSource: Clone + Send + Unpin {
    /// An individual unit within the source, e.g. a file.
    type Object: OneshotObject
        + Debug
        + Clone
        + Send
        + Unpin
        + Serialize
        + DeserializeOwned
        + 'static;
    /// Checksum for a [`Self::Object`].
    type Checksum: Debug + Clone + Send + Unpin + Serialize + DeserializeOwned + 'static;

    /// Returns all of the objects for this source.
    fn list<'a>(
        &'a self,
    ) -> impl Future<Output = Result<Vec<(Self::Object, Self::Checksum)>, StorageErrorX>> + Send;

    /// Returns a stream of the data for a specific object.
    fn get<'s>(
        &'s self,
        object: Self::Object,
        checksum: Self::Checksum,
        range: Option<std::ops::RangeInclusive<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>>;
}

#[derive(Clone)]
pub(crate) enum SourceKind {
    Http(HttpOneshotSource),
}

impl OneshotSource for SourceKind {
    type Object = ObjectKind;
    type Checksum = ChecksumKind;

    async fn list<'a>(&'a self) -> Result<Vec<(Self::Object, Self::Checksum)>, StorageErrorX> {
        match self {
            SourceKind::Http(http) => {
                let objects = http.list().await?;
                let objects = objects
                    .into_iter()
                    .map(|(object, checksum)| {
                        (ObjectKind::Http(object), ChecksumKind::Http(checksum))
                    })
                    .collect();
                Ok(objects)
            }
        }
    }

    fn get<'s>(
        &'s self,
        object: Self::Object,
        checksum: Self::Checksum,
        range: Option<std::ops::RangeInclusive<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>> {
        match (self, object, checksum) {
            (SourceKind::Http(http), ObjectKind::Http(object), ChecksumKind::Http(checksum)) => {
                http.get(object, checksum, range)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ObjectKind {
    Http(HttpObject),
}

impl OneshotObject for ObjectKind {
    fn name(&self) -> &str {
        match self {
            ObjectKind::Http(object) => object.name(),
        }
    }

    fn size(&self) -> usize {
        match self {
            ObjectKind::Http(object) => object.size(),
        }
    }

    fn encodings(&self) -> &[Encoding] {
        match self {
            ObjectKind::Http(object) => object.encodings(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ChecksumKind {
    Http(HttpChecksum),
}

pub trait OneshotFormat: Clone {
    /// A single unit of work for decoding this format, e.g. a single Parquet RowGroup.
    type WorkRequest<S>: Debug + Clone + Send + Serialize + DeserializeOwned + 'static
    where
        S: OneshotSource;
    /// A chunk of records in this format that can be decoded into Rows.
    type RecordChunk: Debug + Clone + Send + Serialize + DeserializeOwned + 'static;

    fn split_work<S: OneshotSource + Send>(
        &self,
        source: S,
        object: S::Object,
        checksum: S::Checksum,
    ) -> impl Future<Output = Result<Vec<Self::WorkRequest<S>>, StorageErrorX>> + Send;

    fn process_work<'a, S: OneshotSource + Sync + 'static>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>>;

    fn decode_chunk(
        &self,
        chunk: Self::RecordChunk,
        rows: &mut Vec<Row>,
    ) -> Result<usize, StorageErrorX>;
}

#[derive(Debug, Clone)]
pub(crate) enum FormatKind {
    Csv(CsvDecoder),
    Parquet(ParquetFormat),
}

impl OneshotFormat for FormatKind {
    type WorkRequest<S>
        = RequestKind<S::Object, S::Checksum>
    where
        S: OneshotSource;
    type RecordChunk = RecordChunkKind;

    async fn split_work<S: OneshotSource + Send>(
        &self,
        source: S,
        object: S::Object,
        checksum: S::Checksum,
    ) -> Result<Vec<Self::WorkRequest<S>>, StorageErrorX> {
        match self {
            FormatKind::Csv(csv) => {
                let work = csv
                    .split_work(source, object, checksum)
                    .await?
                    .into_iter()
                    .map(|request| RequestKind::Csv(request))
                    .collect();
                Ok(work)
            }
            FormatKind::Parquet(parquet) => {
                let work = parquet
                    .split_work(source, object, checksum)
                    .await?
                    .into_iter()
                    .map(|request| RequestKind::Parquet(request))
                    .collect();
                Ok(work)
            }
        }
    }

    fn process_work<'a, S: OneshotSource + Sync + 'static>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>> {
        match (self, request) {
            (FormatKind::Csv(csv), RequestKind::Csv(request)) => csv
                .process_work(source, request)
                .map_ok(|chunk| RecordChunkKind::Csv(chunk))
                .boxed(),
            (FormatKind::Parquet(parquet), RequestKind::Parquet(request)) => parquet
                .process_work(source, request)
                .map_ok(|chunk| RecordChunkKind::Parquet(chunk))
                .boxed(),
            (FormatKind::Parquet(_), RequestKind::Csv(_))
            | (FormatKind::Csv(_), RequestKind::Parquet(_)) => {
                unreachable!("programming error, {self:?}")
            }
        }
    }

    fn decode_chunk(
        &self,
        chunk: Self::RecordChunk,
        rows: &mut Vec<Row>,
    ) -> Result<usize, StorageErrorX> {
        match (self, chunk) {
            (FormatKind::Csv(csv), RecordChunkKind::Csv(chunk)) => csv.decode_chunk(chunk, rows),
            (FormatKind::Parquet(parquet), RecordChunkKind::Parquet(chunk)) => {
                parquet.decode_chunk(chunk, rows)
            }
            (FormatKind::Parquet(_), RecordChunkKind::Csv(_))
            | (FormatKind::Csv(_), RecordChunkKind::Parquet(_)) => {
                unreachable!("programming error, {self:?}")
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RequestKind<O, C> {
    Csv(CsvWorkRequest<O, C>),
    Parquet(ParquetWorkRequest<O, C>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RecordChunkKind {
    Csv(CsvRecord),
    Parquet(ParquetRowGroup),
}

// #[cfg(test)]
// mod tests {
//     use std::sync::{Arc, Mutex};

//     use mz_ore::assert_ok;
//     use mz_persist_types::{PersistLocation, ShardId};
//     use mz_repr::{RelationDesc, ScalarType};

//     use super::*;

//     #[mz_ore::test(tokio::test)]
//     async fn smoketest() {
//         let relation_desc = RelationDesc::builder()
//             .with_column("a", ScalarType::String.nullable(false))
//             .with_column("b", ScalarType::String.nullable(false))
//             .with_column("c", ScalarType::String.nullable(false))
//             .with_column("d", ScalarType::String.nullable(false))
//             .with_column("e", ScalarType::String.nullable(false))
//             .with_column("f", ScalarType::String.nullable(false))
//             .with_column("g", ScalarType::String.nullable(false))
//             .finish();

//         let persist_clients = Arc::new(PersistClientCache::new_no_metrics());
//         let shard_id = ShardId::new();
//         let persist_location = PersistLocation::new_in_mem();

//         let collection_meta = CollectionMetadata {
//             persist_location: persist_location.clone(),
//             remap_shard: None,
//             data_shard: shard_id,
//             relation_desc: relation_desc.clone(),
//             txns_shard: None,
//         };

//         let results: Arc<Mutex<Vec<Result<(ProtoBatch, u64), String>>>> =
//             Arc::new(Mutex::new(Vec::new()));
//         let results_ = results.clone();

//         // TODO(parkmycar): Don't spawn a runtime here.
//         let runtime = tokio::runtime::Builder::new_multi_thread()
//             .enable_all()
//             .build()
//             .expect("TODO don't spawn a runtime");
//         let persist_clients_ = Arc::clone(&persist_clients);

//         timely::execute(timely::Config::process(8), move |worker| {
//             let request = Request {
//                 source: ContentSource::Http {
//                     url: Url::parse("http://localhost:8080").expect("valid"),
//                 },
//                 format: ContentFormat::Csv,
//             };
//             let tokio_handle = runtime.handle();

//             let results_ = results_.clone();
//             let callback = move |result: Result<(ProtoBatch, u64), String>| {
//                 let mut results = results_.lock().expect("poisoned");
//                 results.push(result);
//             };

//             worker.dataflow(|scope| {
//                 let tokens = render(
//                     scope.clone(),
//                     Arc::clone(&persist_clients_),
//                     GlobalId::User(1),
//                     collection_meta.clone(),
//                     request,
//                     callback,
//                     tokio_handle.clone(),
//                 );
//                 std::mem::forget(tokens);
//             });

//             for _ in 0..20 {
//                 worker.step();
//             }
//         })
//         .expect("timely to work");

//         let results = results.lock().expect("success");
//         // We spawned 8 workers above.
//         assert_eq!(results.len(), 8);

//         println!("{results:#?}");

//         for result in &*results {
//             let (proto_batch, row_count) = result.as_ref().unwrap();
//             let batch = proto_batch.batch.as_ref().unwrap();

//             assert!(*row_count > 0);
//             assert_eq!(batch.len, *row_count);
//             assert!(batch.parts.len() > 0);
//         }
//     }
// }
