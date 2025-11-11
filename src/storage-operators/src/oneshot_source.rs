// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! "Oneshot" sources are a one-time ingestion of data from an external system, unlike traditional
//! sources, they __do not__ run continuously. Oneshot sources are generally used for `COPY FROM`
//! SQL statements.
//!
//! The implementation of reading and parsing data is behind the [`OneshotSource`] and
//! [`OneshotFormat`] traits, respectively. Users looking to add new sources or formats, should
//! only need to add new implementations for these traits.
//!
//! * [`OneshotSource`] is an interface for listing and reading from an external system, e.g. an
//!   HTTP server.
//! * [`OneshotFormat`] is an interface for how to parallelize and parse data, e.g. CSV.
//!
//! Given a [`OneshotSource`] and a [`OneshotFormat`] we build a dataflow structured like the
//! following:
//!
//! ```text
//!             ┏━━━━━━━━━━━━━━━┓
//!             ┃    Discover   ┃
//!             ┃    objects    ┃
//!             ┗━━━━━━━┯━━━━━━━┛
//!           ┌───< Distribute >───┐
//!           │                    │
//!     ┏━━━━━v━━━━┓         ┏━━━━━v━━━━┓
//!     ┃  Split   ┃   ...   ┃  Split   ┃
//!     ┃  Work 1  ┃         ┃  Work n  ┃
//!     ┗━━━━━┯━━━━┛         ┗━━━━━┯━━━━┛
//!           │                    │
//!           ├───< Distribute >───┤
//!           │                    │
//!     ┏━━━━━v━━━━┓         ┏━━━━━v━━━━┓
//!     ┃  Fetch   ┃   ...   ┃  Fetch   ┃
//!     ┃  Work 1  ┃         ┃  Work n  ┃
//!     ┗━━━━━┯━━━━┛         ┗━━━━━┯━━━━┛
//!           │                    │
//!           ├───< Distribute >───┤
//!           │                    │
//!     ┏━━━━━v━━━━┓         ┏━━━━━v━━━━┓
//!     ┃  Decode  ┃   ...   ┃  Decode  ┃
//!     ┃  Chunk 1 ┃         ┃  Chunk n ┃
//!     ┗━━━━━┯━━━━┛         ┗━━━━━┯━━━━┛
//!           │                    │
//!           │                    │
//!     ┏━━━━━v━━━━┓         ┏━━━━━v━━━━┓
//!     ┃  Stage   ┃   ...   ┃  Stage   ┃
//!     ┃  Batch 1 ┃         ┃  Batch n ┃
//!     ┗━━━━━┯━━━━┛         ┗━━━━━┯━━━━┛
//!           │                    │
//!           └─────────┬──────────┘
//!               ┏━━━━━v━━━━┓
//!               ┃  Result  ┃
//!               ┃ Callback ┃
//!               ┗━━━━━━━━━━┛
//! ```
//!

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use differential_dataflow::Hashable;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use mz_expr::SafeMfpPlan;
use mz_ore::cast::CastFrom;
use mz_persist_client::Diagnostics;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{DatumVec, GlobalId, Row, RowArena, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::oneshot_sources::{
    ContentFilter, ContentFormat, ContentSource, OneshotIngestionRequest,
};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::pact::Distribute;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, LinkedList};
use std::fmt::{Debug, Display};
use std::future::Future;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;
use tracing::info;

use crate::oneshot_source::aws_source::{AwsS3Source, S3Checksum, S3Object};
use crate::oneshot_source::csv::{CsvDecoder, CsvRecord, CsvWorkRequest};
use crate::oneshot_source::http_source::{HttpChecksum, HttpObject, HttpOneshotSource};
use crate::oneshot_source::parquet::{ParquetFormat, ParquetRowGroup, ParquetWorkRequest};

pub mod csv;
pub mod parquet;

pub mod aws_source;
pub mod http_source;

mod util;

/// Render a dataflow to do a "oneshot" ingestion.
///
/// Roughly the operators we render do the following:
///
/// 1. Discover objects with a [`OneshotSource`].
/// 2. Split objects into separate units of work based on the [`OneshotFormat`].
/// 3. Fetch individual units of work (aka fetch byte blobs) with the
///    [`OneshotFormat`] and [`OneshotSource`].
/// 4. Decode the fetched byte blobs into [`Row`]s.
/// 5. Stage the [`Row`]s into Persist returning [`ProtoBatch`]es.
///
/// TODO(cf3): Benchmark combining operators 3, 4, and 5. Currently we keep them
/// separate for the [`CsvDecoder`]. CSV decoding is hard to do in parallel so we
/// currently have a single worker Fetch an entire file, and then distributes
/// chunks for parallel Decoding. We should benchmark if this is actually faster
/// than just a single worker both fetching and decoding.
///
pub fn render<G, F>(
    scope: G,
    persist_clients: Arc<PersistClientCache>,
    connection_context: ConnectionContext,
    collection_id: GlobalId,
    collection_meta: CollectionMetadata,
    request: OneshotIngestionRequest,
    worker_callback: F,
) -> Vec<PressOnDropButton>
where
    G: Scope<Timestamp = Timestamp>,
    F: FnOnce(Result<Option<ProtoBatch>, String>) -> () + 'static,
{
    let OneshotIngestionRequest {
        source,
        format,
        filter,
        shape,
    } = request;

    let source = match source {
        ContentSource::Http { url } => {
            let source = HttpOneshotSource::new(reqwest::Client::default(), url);
            SourceKind::Http(source)
        }
        ContentSource::AwsS3 {
            connection,
            connection_id,
            uri,
        } => {
            let source = AwsS3Source::new(connection, connection_id, connection_context, uri);
            SourceKind::AwsS3(source)
        }
    };
    tracing::info!(?source, "created oneshot source");

    let format = match format {
        ContentFormat::Csv(params) => {
            let format = CsvDecoder::new(params, &shape.source_desc);
            FormatKind::Csv(format)
        }
        ContentFormat::Parquet => {
            let format = ParquetFormat::new(shape.source_desc);
            FormatKind::Parquet(format)
        }
    };

    // Discover what objects are available to copy.
    let (objects_stream, discover_token) =
        render_discover_objects(scope.clone(), collection_id, source.clone(), filter);
    // Split the objects into individual units of work.
    let (work_stream, split_token) = render_split_work(
        scope.clone(),
        collection_id,
        &objects_stream,
        source.clone(),
        format.clone(),
    );
    // Fetch each unit of work, returning chunks of records.
    let (records_stream, fetch_token) = render_fetch_work(
        scope.clone(),
        collection_id,
        source.clone(),
        format.clone(),
        &work_stream,
    );
    // Parse chunks of records into Rows.
    let (rows_stream, decode_token) = render_decode_chunk(
        scope.clone(),
        format.clone(),
        &records_stream,
        shape.source_mfp,
    );
    // Stage the Rows in Persist.
    let (batch_stream, batch_token) = render_stage_batches_operator(
        scope.clone(),
        collection_id,
        &collection_meta,
        persist_clients,
        &rows_stream,
    );

    // Collect all results together and notify the upstream of whether or not we succeeded.
    render_completion_operator(scope, &batch_stream, worker_callback);

    let tokens = vec![
        discover_token,
        split_token,
        fetch_token,
        decode_token,
        batch_token,
    ];

    tokens
}

/// Render an operator that using a [`OneshotSource`] will discover what objects are available
/// for fetching.
pub fn render_discover_objects<G, S>(
    scope: G,
    collection_id: GlobalId,
    source: S,
    filter: ContentFilter,
) -> (
    TimelyStream<G, Result<(S::Object, S::Checksum), StorageErrorX>>,
    PressOnDropButton,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: OneshotSource + 'static,
{
    // Only a single worker is responsible for discovering objects.
    let worker_id = scope.index();
    let num_workers = scope.peers();
    let active_worker_id = usize::cast_from((collection_id, "discover").hashed()) % num_workers;
    let is_active_worker = worker_id == active_worker_id;

    let mut builder = AsyncOperatorBuilder::new("CopyFrom-discover".to_string(), scope.clone());

    let (start_handle, start_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    let shutdown = builder.build(move |caps| async move {
        let [start_cap] = caps.try_into().unwrap();

        if !is_active_worker {
            return;
        }

        let filter = match ObjectFilter::try_new(filter) {
            Ok(filter) => filter,
            Err(err) => {
                tracing::warn!(?err, "failed to create filter");
                start_handle.give(&start_cap, Err(StorageErrorXKind::generic(err).into()));
                return;
            }
        };

        let work = source.list().await.context("list");
        match work {
            Ok(objects) => {
                let (include, exclude): (Vec<_>, Vec<_>) = objects
                    .into_iter()
                    .partition(|(o, _checksum)| filter.filter::<S>(o));
                tracing::info!(%worker_id, ?include, ?exclude, "listed objects");

                include
                    .into_iter()
                    .for_each(|object| start_handle.give(&start_cap, Ok(object)))
            }
            Err(err) => {
                tracing::warn!(?err, "failed to list oneshot source");
                start_handle.give(&start_cap, Err(err))
            }
        }
    });

    (start_stream, shutdown.press_on_drop())
}

/// Render an operator that given a stream of [`OneshotSource::Object`]s will split them into units
/// of work based on the provided [`OneshotFormat`].
pub fn render_split_work<G, S, F>(
    scope: G,
    collection_id: GlobalId,
    objects: &TimelyStream<G, Result<(S::Object, S::Checksum), StorageErrorX>>,
    source: S,
    format: F,
) -> (
    TimelyStream<G, Result<F::WorkRequest<S>, StorageErrorX>>,
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
    let mut objects_handle = builder.new_input_for(objects, Distribute, &request_handle);

    let shutdown = builder.build(move |caps| async move {
        let [_objects_cap] = caps.try_into().unwrap();

        info!(%collection_id, %worker_id, "CopyFrom Split Work");

        while let Some(event) = objects_handle.next().await {
            let (capability, maybe_objects) = match event {
                AsyncEvent::Data(cap, req) => (cap, req),
                AsyncEvent::Progress(_) => continue,
            };

            // Nest the `split_work(...)` method in an async-block so we can use the `?`
            // without returning from the entire operator, and so we can add context.
            let result = async {
                let mut requests = Vec::new();

                for maybe_object in maybe_objects {
                    // Return early if the upstream Discover step failed.
                    let (object, checksum) = maybe_object?;

                    let format_ = format.clone();
                    let source_ = source.clone();
                    let work_requests = mz_ore::task::spawn(|| "split-work", async move {
                        info!(%worker_id, object = %object.name(), "splitting object");
                        format_.split_work(source_.clone(), object, checksum).await
                    })
                    .await
                    .expect("failed to spawn task")?;

                    requests.extend(work_requests);
                }

                Ok::<_, StorageErrorX>(requests)
            }
            .await
            .context("split");

            match result {
                Ok(requests) => requests
                    .into_iter()
                    .for_each(|req| request_handle.give(&capability, Ok(req))),
                Err(err) => request_handle.give(&capability, Err(err)),
            }
        }
    });

    (request_stream, shutdown.press_on_drop())
}

/// Render an operator that given a stream [`OneshotFormat::WorkRequest`]s will fetch chunks of the
/// remote [`OneshotSource::Object`] and return a stream of [`OneshotFormat::RecordChunk`]s that
/// can be decoded into [`Row`]s.
pub fn render_fetch_work<G, S, F>(
    scope: G,
    collection_id: GlobalId,
    source: S,
    format: F,
    work_requests: &TimelyStream<G, Result<F::WorkRequest<S>, StorageErrorX>>,
) -> (
    TimelyStream<G, Result<F::RecordChunk, StorageErrorX>>,
    PressOnDropButton,
)
where
    G: Scope,
    S: OneshotSource + Sync + 'static,
    F: OneshotFormat + Sync + 'static,
{
    let worker_id = scope.index();
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-fetch_work".to_string(), scope.clone());

    let (record_handle, record_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let mut work_requests_handle = builder.new_input_for(work_requests, Distribute, &record_handle);

    let shutdown = builder.build(move |caps| async move {
        let [_work_cap] = caps.try_into().unwrap();

        info!(%collection_id, %worker_id, "CopyFrom Fetch Work");

        while let Some(event) = work_requests_handle.next().await {
            let (capability, maybe_requests) = match event {
                AsyncEvent::Data(cap, req) => (cap, req),
                AsyncEvent::Progress(_) => continue,
            };

            // Wrap our work in a block to capture `?`.
            let result = async {
                // Process each stream of work, one at a time.
                for maybe_request in maybe_requests {
                    let request = maybe_request?;

                    let mut work_stream = format.fetch_work(&source, request);
                    while let Some(result) = work_stream.next().await {
                        // Returns early and stop consuming from the stream if we hit an error.
                        let record_chunk = result.context("fetch worker")?;

                        // Note: We want to give record chunks as we receive them, because a work
                        // request may be for an entire file.
                        //
                        // TODO(cf3): Investigate if some small batching here would help perf.
                        record_handle.give(&capability, Ok(record_chunk));
                    }
                }

                Ok::<_, StorageErrorX>(())
            }
            .await
            .context("fetch work");

            if let Err(err) = result {
                tracing::warn!(?err, "failed to fetch");
                record_handle.give(&capability, Err(err))
            }
        }
    });

    (record_stream, shutdown.press_on_drop())
}

/// Render an operator that given a stream of [`OneshotFormat::RecordChunk`]s will decode these
/// chunks into a stream of [`Row`]s.
pub fn render_decode_chunk<G, F>(
    scope: G,
    format: F,
    record_chunks: &TimelyStream<G, Result<F::RecordChunk, StorageErrorX>>,
    mfp: SafeMfpPlan,
) -> (
    TimelyStream<G, Result<Row, StorageErrorX>>,
    PressOnDropButton,
)
where
    G: Scope,
    F: OneshotFormat + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-decode_chunk".to_string(), scope.clone());

    let (row_handle, row_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let mut record_chunk_handle = builder.new_input_for(record_chunks, Distribute, &row_handle);

    let shutdown = builder.build(move |caps| async move {
        let [_row_cap] = caps.try_into().unwrap();

        let mut datum_vec = DatumVec::default();
        let row_arena = RowArena::default();
        let mut row_buf = Row::default();

        while let Some(event) = record_chunk_handle.next().await {
            let (capability, maybe_chunks) = match event {
                AsyncEvent::Data(cap, data) => (cap, data),
                AsyncEvent::Progress(_) => continue,
            };

            let result = async {
                let mut rows = Vec::new();
                for maybe_chunk in maybe_chunks {
                    let chunk = maybe_chunk?;
                    format.decode_chunk(chunk, &mut rows)?;
                }
                Ok::<_, StorageErrorX>(rows)
            }
            .await
            .context("decode chunk");

            match result {
                Ok(rows) => {
                    // For each row of source data, we pass it through an MFP to re-arrange column
                    // orders and/or fill in default values for missing columns.
                    for row in rows {
                        let mut datums = datum_vec.borrow_with(&row);
                        let result = mfp
                            .evaluate_into(&mut *datums, &row_arena, &mut row_buf)
                            .map(|row| row.cloned());

                        match result {
                            Ok(Some(row)) => row_handle.give(&capability, Ok(row)),
                            Ok(None) => {
                                // We only expect the provided MFP to map rows from the source data
                                // and project default values.
                                mz_ore::soft_panic_or_log!("oneshot source MFP filtered out data!");
                            }
                            Err(e) => {
                                let err = StorageErrorXKind::MfpEvalError(e.to_string().into())
                                    .with_context("decode");
                                row_handle.give(&capability, Err(err))
                            }
                        }
                    }
                }
                Err(err) => row_handle.give(&capability, Err(err)),
            }
        }
    });

    (row_stream, shutdown.press_on_drop())
}

/// Render an operator that given a stream of [`Row`]s will stage them in Persist and return a
/// stream of [`ProtoBatch`]es that can later be linked into a shard.
pub fn render_stage_batches_operator<G>(
    scope: G,
    collection_id: GlobalId,
    collection_meta: &CollectionMetadata,
    persist_clients: Arc<PersistClientCache>,
    rows_stream: &TimelyStream<G, Result<Row, StorageErrorX>>,
) -> (
    TimelyStream<G, Result<ProtoBatch, StorageErrorX>>,
    PressOnDropButton,
)
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

        // Open a Persist handle that we can use to stage a batch.
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("failed to open Persist client");
        let persist_diagnostics = Diagnostics {
            shard_name: collection_id.to_string(),
            handle_purpose: "CopyFrom::stage_batches".to_string(),
        };
        let write_handle = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                shard_id,
                Arc::new(collection_desc),
                Arc::new(UnitSchema),
                persist_diagnostics,
            )
            .await
            .expect("could not open Persist shard");

        // Create a batch using the minimum timestamp since these batches will
        // get sent back to `environmentd` and their timestamps re-written
        // before being finally appended.
        let lower = mz_repr::Timestamp::MIN;
        let upper = Antichain::from_elem(lower.step_forward());

        let mut batch_builder = write_handle.builder(Antichain::from_elem(lower));

        while let Some(event) = rows_handle.next().await {
            let AsyncEvent::Data(_, row_batch) = event else {
                continue;
            };

            // Pull Rows off our stream and stage them into a Batch.
            for maybe_row in row_batch {
                match maybe_row {
                    // Happy path, add the Row to our batch!
                    Ok(row) => {
                        let data = SourceData(Ok(row));
                        batch_builder
                            .add(&data, &(), &lower, &1)
                            .await
                            .expect("failed to add Row to batch");
                    }
                    // Sad path, something upstream hit an error.
                    Err(err) => {
                        // Clean up our in-progress batch so we don't leak data.
                        let batch = batch_builder
                            .finish(upper)
                            .await
                            .expect("failed to cleanup batch");
                        batch.delete().await;

                        // Pass on the error.
                        proto_batch_handle
                            .give(&proto_batch_cap, Err(err).context("stage batches"));
                        return;
                    }
                }
            }
        }

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
        // TODO(cf2): Make sure these batches get cleaned up if another
        // worker encounters an error.
        let proto_batch = batch.into_transmittable_batch();
        proto_batch_handle.give(&proto_batch_cap, Ok(proto_batch));
    });

    (proto_batch_stream, shutdown.press_on_drop())
}

/// Render an operator that given a stream of [`ProtoBatch`]es will call our `worker_callback` to
/// report the results upstream.
pub fn render_completion_operator<G, F>(
    scope: G,
    results_stream: &TimelyStream<G, Result<ProtoBatch, StorageErrorX>>,
    worker_callback: F,
) where
    G: Scope,
    F: FnOnce(Result<Option<ProtoBatch>, String>) -> () + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-completion".to_string(), scope.clone());
    let mut results_input = builder.new_disconnected_input(results_stream, Pipeline);

    builder.build(move |_| async move {
        let result = async move {
            let mut maybe_payload: Option<ProtoBatch> = None;

            while let Some(event) = results_input.next().await {
                if let AsyncEvent::Data(_cap, results) = event {
                    let [result] = results
                        .try_into()
                        .expect("only 1 event on the result stream");

                    // TODO(cf2): Lift this restriction.
                    if maybe_payload.is_some() {
                        panic!("expected only one batch!");
                    }

                    maybe_payload = Some(result.map_err(|e| e.to_string())?);
                }
            }

            Ok(maybe_payload)
        }
        .await;

        // Report to the caller of our final status.
        worker_callback(result);
    });
}

/// An object that will be fetched from a [`OneshotSource`].
pub trait OneshotObject {
    /// Name of the object, including any extensions.
    fn name(&self) -> &str;

    /// Path of the object within the remote source.
    fn path(&self) -> &str;

    /// Size of this object in bytes.
    fn size(&self) -> usize;

    /// Encodings of the _entire_ object, if any.
    ///
    /// Note: The object may internally use compression, e.g. a Parquet file
    /// could compress its column chunks, but if the Parquet file itself is not
    /// compressed then this would return `None`.
    fn encodings(&self) -> &[Encoding];
}

/// Encoding of a [`OneshotObject`].
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Encoding {
    Bzip2,
    Gzip,
    Xz,
    Zstd,
}

/// Defines a remote system that we can fetch data from for a "one time" ingestion.
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

    /// Resturns a stream of the data for a specific object.
    fn get<'s>(
        &'s self,
        object: Self::Object,
        checksum: Self::Checksum,
        range: Option<std::ops::RangeInclusive<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>>;
}

/// An enum wrapper around [`OneshotSource`]s.
///
/// An alternative to this wrapper would be to use `Box<dyn OneshotSource>`, but that requires
/// making the trait object safe and it's easier to just wrap it in an enum. Also, this wrapper
/// provides a convenient place to add [`StorageErrorXContext::context`] for all of our source
/// types.
#[derive(Clone, Debug)]
pub(crate) enum SourceKind {
    Http(HttpOneshotSource),
    AwsS3(AwsS3Source),
}

impl OneshotSource for SourceKind {
    type Object = ObjectKind;
    type Checksum = ChecksumKind;

    async fn list<'a>(&'a self) -> Result<Vec<(Self::Object, Self::Checksum)>, StorageErrorX> {
        match self {
            SourceKind::Http(http) => {
                let objects = http.list().await.context("http")?;
                let objects = objects
                    .into_iter()
                    .map(|(object, checksum)| {
                        (ObjectKind::Http(object), ChecksumKind::Http(checksum))
                    })
                    .collect();
                Ok(objects)
            }
            SourceKind::AwsS3(s3) => {
                let objects = s3.list().await.context("s3")?;
                let objects = objects
                    .into_iter()
                    .map(|(object, checksum)| {
                        (ObjectKind::AwsS3(object), ChecksumKind::AwsS3(checksum))
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
                    .map(|result| result.context("http"))
                    .boxed()
            }
            (SourceKind::AwsS3(s3), ObjectKind::AwsS3(object), ChecksumKind::AwsS3(checksum)) => s3
                .get(object, checksum, range)
                .map(|result| result.context("aws_s3"))
                .boxed(),
            (SourceKind::AwsS3(_) | SourceKind::Http(_), _, _) => {
                unreachable!("programming error! wrong source, object, and checksum kind");
            }
        }
    }
}

/// Enum wrapper for [`OneshotSource::Object`], see [`SourceKind`] for more details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ObjectKind {
    Http(HttpObject),
    AwsS3(S3Object),
}

impl OneshotObject for ObjectKind {
    fn name(&self) -> &str {
        match self {
            ObjectKind::Http(object) => object.name(),
            ObjectKind::AwsS3(object) => object.name(),
        }
    }

    fn path(&self) -> &str {
        match self {
            ObjectKind::Http(object) => object.path(),
            ObjectKind::AwsS3(object) => object.path(),
        }
    }

    fn size(&self) -> usize {
        match self {
            ObjectKind::Http(object) => object.size(),
            ObjectKind::AwsS3(object) => object.size(),
        }
    }

    fn encodings(&self) -> &[Encoding] {
        match self {
            ObjectKind::Http(object) => object.encodings(),
            ObjectKind::AwsS3(object) => object.encodings(),
        }
    }
}

/// Enum wrapper for [`OneshotSource::Checksum`], see [`SourceKind`] for more details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ChecksumKind {
    Http(HttpChecksum),
    AwsS3(S3Checksum),
}

/// Defines a format that we fetch for a "one time" ingestion.
pub trait OneshotFormat: Clone {
    /// A single unit of work for decoding this format, e.g. a single Parquet RowGroup.
    type WorkRequest<S>: Debug + Clone + Send + Serialize + DeserializeOwned + 'static
    where
        S: OneshotSource;
    /// A chunk of records in this format that can be decoded into Rows.
    type RecordChunk: Debug + Clone + Send + Serialize + DeserializeOwned + 'static;

    /// Given an upstream object, defines how we should parse this object in parallel.
    ///
    /// Note: It's totally fine to not process an object in parallel, and just return a single
    /// [`Self::WorkRequest`] here.
    fn split_work<S: OneshotSource + Send>(
        &self,
        source: S,
        object: S::Object,
        checksum: S::Checksum,
    ) -> impl Future<Output = Result<Vec<Self::WorkRequest<S>>, StorageErrorX>> + Send;

    /// Given a work request, fetch data from the [`OneshotSource`] and return it in a format that
    /// can later be decoded.
    fn fetch_work<'a, S: OneshotSource + Sync + 'static>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>>;

    /// Decode a chunk of records into [`Row`]s.
    fn decode_chunk(
        &self,
        chunk: Self::RecordChunk,
        rows: &mut Vec<Row>,
    ) -> Result<usize, StorageErrorX>;
}

/// An enum wrapper around [`OneshotFormat`]s.
///
/// An alternative to this wrapper would be to use `Box<dyn OneshotFormat>`, but that requires
/// making the trait object safe and it's easier to just wrap it in an enum. Also, this wrapper
/// provides a convenient place to add [`StorageErrorXContext::context`] for all of our format
/// types.
#[derive(Clone, Debug)]
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
                    .await
                    .context("csv")?
                    .into_iter()
                    .map(RequestKind::Csv)
                    .collect();
                Ok(work)
            }
            FormatKind::Parquet(parquet) => {
                let work = parquet
                    .split_work(source, object, checksum)
                    .await
                    .context("parquet")?
                    .into_iter()
                    .map(RequestKind::Parquet)
                    .collect();
                Ok(work)
            }
        }
    }

    fn fetch_work<'a, S: OneshotSource + Sync + 'static>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>> {
        match (self, request) {
            (FormatKind::Csv(csv), RequestKind::Csv(request)) => csv
                .fetch_work(source, request)
                .map_ok(RecordChunkKind::Csv)
                .map(|result| result.context("csv"))
                .boxed(),
            (FormatKind::Parquet(parquet), RequestKind::Parquet(request)) => parquet
                .fetch_work(source, request)
                .map_ok(RecordChunkKind::Parquet)
                .map(|result| result.context("parquet"))
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
            (FormatKind::Csv(csv), RecordChunkKind::Csv(chunk)) => {
                csv.decode_chunk(chunk, rows).context("csv")
            }
            (FormatKind::Parquet(parquet), RecordChunkKind::Parquet(chunk)) => {
                parquet.decode_chunk(chunk, rows).context("parquet")
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

pub(crate) enum ObjectFilter {
    None,
    Files(BTreeSet<Box<str>>),
    Pattern(glob::Pattern),
}

impl ObjectFilter {
    pub fn try_new(filter: ContentFilter) -> Result<Self, anyhow::Error> {
        match filter {
            ContentFilter::None => Ok(ObjectFilter::None),
            ContentFilter::Files(files) => {
                let files = files.into_iter().map(|f| f.into()).collect();
                Ok(ObjectFilter::Files(files))
            }
            ContentFilter::Pattern(pattern) => {
                let pattern = glob::Pattern::new(&pattern)?;
                Ok(ObjectFilter::Pattern(pattern))
            }
        }
    }

    /// Returns if the object should be included.
    pub fn filter<S: OneshotSource>(&self, object: &S::Object) -> bool {
        match self {
            ObjectFilter::None => true,
            ObjectFilter::Files(files) => files.contains(object.path()),
            ObjectFilter::Pattern(pattern) => pattern.matches(object.path()),
        }
    }
}

/// Experimental Error Type.
///
/// The goal of this type is to combine concepts from both `thiserror` and
/// `anyhow`. Having "stongly typed" errors from `thiserror` is useful for
/// determining what action to take and tracking the context of an error like
/// `anyhow` is useful for determining where an error came from.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum StorageErrorXKind {
    #[error("csv decoding error: {0}")]
    CsvDecoding(Arc<str>),
    #[error("parquet error: {0}")]
    ParquetError(Arc<str>),
    #[error("reqwest error: {0}")]
    Reqwest(Arc<str>),
    #[error("aws s3 request error: {0}")]
    AwsS3Request(String),
    #[error("aws s3 bytestream error: {0}")]
    AwsS3Bytes(Arc<str>),
    #[error("invalid reqwest header: {0}")]
    InvalidHeader(Arc<str>),
    #[error("failed to decode Row from a record batch: {0}")]
    InvalidRecordBatch(Arc<str>),
    #[error("programming error: {0}")]
    ProgrammingError(Arc<str>),
    #[error("failed to get the size of an object")]
    MissingSize,
    #[error("object is missing the required '{0}' field")]
    MissingField(Arc<str>),
    #[error("failed while evaluating the provided mfp: '{0}'")]
    MfpEvalError(Arc<str>),
    #[error("something went wrong: {0}")]
    Generic(String),
}

impl From<csv_async::Error> for StorageErrorXKind {
    fn from(err: csv_async::Error) -> Self {
        StorageErrorXKind::CsvDecoding(err.to_string().into())
    }
}

impl From<reqwest::Error> for StorageErrorXKind {
    fn from(err: reqwest::Error) -> Self {
        StorageErrorXKind::Reqwest(err.to_string().into())
    }
}

impl From<reqwest::header::ToStrError> for StorageErrorXKind {
    fn from(err: reqwest::header::ToStrError) -> Self {
        StorageErrorXKind::InvalidHeader(err.to_string().into())
    }
}

impl From<aws_smithy_types::byte_stream::error::Error> for StorageErrorXKind {
    fn from(err: aws_smithy_types::byte_stream::error::Error) -> Self {
        StorageErrorXKind::AwsS3Request(err.to_string())
    }
}

impl From<::parquet::errors::ParquetError> for StorageErrorXKind {
    fn from(err: ::parquet::errors::ParquetError) -> Self {
        StorageErrorXKind::ParquetError(err.to_string().into())
    }
}

impl StorageErrorXKind {
    pub fn with_context<C: Display>(self, context: C) -> StorageErrorX {
        StorageErrorX {
            kind: self,
            context: LinkedList::from([context.to_string()]),
        }
    }

    pub fn invalid_record_batch<S: Into<Arc<str>>>(error: S) -> StorageErrorXKind {
        StorageErrorXKind::InvalidRecordBatch(error.into())
    }

    pub fn generic<C: Display>(error: C) -> StorageErrorXKind {
        StorageErrorXKind::Generic(error.to_string())
    }

    pub fn programming_error<S: Into<Arc<str>>>(error: S) -> StorageErrorXKind {
        StorageErrorXKind::ProgrammingError(error.into())
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
