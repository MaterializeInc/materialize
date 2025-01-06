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
//!           ┌─────────┴──────────┐
//!           │                    │
//!     ┏━━━━━v━━━━┓         ┏━━━━━v━━━━┓
//!     ┃  Split   ┃   ...   ┃  Split   ┃
//!     ┃  Work 1  ┃         ┃  Work n  ┃
//!     ┗━━━━━┯━━━━┛         ┗━━━━━┯━━━━┛
//!           │                    │
//!           ├───< Distribute >───┤
//!           │                    │
//!     ┏━━━━━v━━━━┓         ┏━━━━━v━━━━┓
//!     ┃  Process ┃   ...   ┃  Process ┃
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
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;
use tracing::info;
use url::Url;

use crate::oneshot_source::csv::{CsvDecoder, CsvRecord, CsvWorkRequest};
use crate::oneshot_source::http_source::{HttpChecksum, HttpOneshotSource};

pub mod csv;
pub mod http_source;

/// Render a dataflow to do a "oneshot" ingestion.
///
/// Roughly the operators we render do the following:
///
/// 1. Discover objects with a [`OneshotSource`].
///   1a. Distribute
/// 2. Split objects into separate units of work based on the [`OneshotFormat`].
///   2a. Distribute
/// 3. Process individual units of work (aka fetch byte blobs) with the
///    [`OneshotFormat`] and [`OneshotSource`].
///   3a. Distribute
/// 4. Decode the fetched byte blobs into [`Row`]s.
/// 5. Stage the [`Row`]s into Persist returning [`ProtoBatch`]es.
///
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
    F: FnOnce(Result<Option<ProtoBatch>, String>) -> () + 'static,
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
    };

    // Discover what objects are available to copy.
    let (objects_stream, discover_token) =
        render_discover_objects(scope.clone(), collection_id, source.clone());
    // Split the objects into individual units of work.
    let (work_stream, split_token) = render_split_work(
        scope.clone(),
        collection_id,
        &objects_stream,
        source.clone(),
        format.clone(),
    );
    // Process each unit of work, returning chunks of records.
    let (records_stream, process_token) = render_process_work(
        scope.clone(),
        collection_id,
        source.clone(),
        format.clone(),
        &work_stream,
    );
    // Parse chunks of records into Rows.
    let (rows_stream, decode_token) =
        render_decode_chunk(scope.clone(), format.clone(), &records_stream);
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
        process_token,
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
) -> (
    TimelyStream<G, Result<(S::Object, S::Checksum), StorageErrorX>>,
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

    let shutdown = builder.build(move |caps| async move {
        let [start_cap] = caps.try_into().unwrap();

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
            Ok(objects) => objects
                .into_iter()
                .for_each(|object| start_handle.give(&start_cap, Ok(object))),
            Err(err) => start_handle.give(&start_cap, Err(err)),
        }
    });

    (start_stream.distribute(), shutdown.press_on_drop())
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

    (request_stream.distribute(), shutdown.press_on_drop())
}

/// Render an operator that given a stream [`OneshotFormat::WorkRequest`]s will fetch chunks of the
/// remote [`OneshotSource::Object`] and return a stream of [`OneshotFormat::RecordChunk`]s that
/// can be decoded into [`Row`]s.
pub fn render_process_work<G, S, F>(
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
    let mut builder = AsyncOperatorBuilder::new("CopyFrom-process_work".to_string(), scope.clone());

    let (record_handle, record_stream) = builder.new_output::<CapacityContainerBuilder<_>>();
    let mut work_requests_handle = builder.new_input_for(work_requests, Distribute, &record_handle);

    let shutdown = builder.build(move |caps| async move {
        let [_work_cap] = caps.try_into().unwrap();

        info!(%collection_id, %worker_id, "CopyFrom Process Work");

        while let Some(event) = work_requests_handle.next().await {
            let (capability, maybe_requests) = match event {
                AsyncEvent::Data(cap, req) => (cap, req),
                AsyncEvent::Progress(_) => continue,
            };

            // Wrap our work in a block to capture `?`.
            let result = async {
                let mut record_chunks = Vec::new();

                // Process each stream of work, one at a time.
                for maybe_request in maybe_requests {
                    let request = maybe_request?;

                    let mut work_stream = format.process_work(&source, request);
                    while let Some(result) = work_stream.next().await {
                        let record_chunk = result.context("process worker")?;
                        record_chunks.push(record_chunk);
                    }
                }

                Ok::<_, StorageErrorX>(record_chunks)
            }
            .await
            .context("process work");

            match result {
                Ok(record_chunks) => record_chunks
                    .into_iter()
                    .for_each(|chunk| record_handle.give(&capability, Ok(chunk))),
                Err(err) => record_handle.give(&capability, Err(err)),
            }
        }
    });

    (record_stream.distribute(), shutdown.press_on_drop())
}

/// Render an operator that given a stream of [`OneshotFormat::RecordChunk`]s will decode these
/// chunks into a stream of [`Row`]s.
pub fn render_decode_chunk<G, F>(
    scope: G,
    format: F,
    record_chunks: &TimelyStream<G, Result<F::RecordChunk, StorageErrorX>>,
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
    let mut record_chunk_handle = builder.new_input_for(record_chunks, Pipeline, &row_handle);

    let shutdown = builder.build(move |caps| async move {
        let [_row_cap] = caps.try_into().unwrap();

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
                Ok(rows) => rows
                    .into_iter()
                    .for_each(|row| row_handle.give(&capability, Ok(row))),
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
        let [_proto_batch_cap] = caps.try_into().unwrap();

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
            .open_writer::<SourceData, (), mz_repr::Timestamp, Diff>(
                shard_id,
                Arc::new(collection_desc),
                Arc::new(UnitSchema),
                persist_diagnostics,
            )
            .await
            .expect("could not open Persist shard");

        // TODO(parkmcar): Use different timestamps here.
        let lower = mz_repr::Timestamp::MIN;
        let upper = Antichain::from_elem(lower.step_forward());

        let mut batch_builder = write_handle.builder(Antichain::from_elem(lower));
        let mut caps = CapabilitySet::new();

        while let Some(event) = rows_handle.next().await {
            let AsyncEvent::Data(cap, row_batch) = event else {
                continue;
            };

            // Note: In the current implementation of `COPY FROM` this set should
            // only ever contain one capability.
            caps.insert(cap);

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
                        let send_cap = caps.first().expect("didn't get any capabilities?");
                        proto_batch_handle.give(send_cap, Err(err).context("stage batches"));
                        return;
                    }
                }
            }
        }

        let batch = batch_builder
            .finish(upper)
            .await
            .expect("failed to create Batch");

        // Turn out Batch into a ProtoBatch that will later be linked in to
        // the shard.
        //
        // Note: By turning this into a ProtoBatch, the onus is now on us to
        // cleanup the Batch if it's never linked into the shard.
        //
        // TODO(parkmycar): Make sure these batches get cleaned up if another
        // worker encounters an error.
        let proto_batch = batch.into_transmittable_batch();

        // Use the first (lowest?) capability from our set.
        let send_cap = caps.first().expect("didn't get any capabilities?");
        proto_batch_handle.give(send_cap, Ok(proto_batch));
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

/// Defines a remote system that we can fetch data from for a "one time" ingestion.
pub trait OneshotSource: Clone + Send {
    /// An individual unit within the source, e.g. a file.
    type Object: Debug + Clone + Send + Serialize + DeserializeOwned + 'static;
    /// Checksum for a [`Self::Object`].
    type Checksum: Debug + Clone + Send + Serialize + DeserializeOwned + 'static;

    /// Returns all of the objects for this source.
    fn list<'a>(
        &'a self,
    ) -> impl Future<Output = Result<Vec<(Self::Object, Self::Checksum)>, StorageErrorX>> + Send;

    /// Resturns a stream of the data for a specific object.
    fn get<'s>(
        &'s self,
        object: Self::Object,
        checksum: Self::Checksum,
        range: Option<std::ops::Range<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>>;
}

/// An enum wrapper around [`OneshotSource`]s.
///
/// An alternative to this wrapper would be to use `Box<dyn OneshotSource>`, but that requires
/// making the trait object safe and it's easier to just wrap it in an enum. Also, this wrapper
/// provides a convenient place to add [`StorageErrorXContext::context`] for all of our source
/// types.
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
                let objects = http.list().await.context("http")?;
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
        range: Option<std::ops::Range<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>> {
        match (self, object, checksum) {
            (SourceKind::Http(http), ObjectKind::Http(object), ChecksumKind::Http(checksum)) => {
                http.get(object, checksum, range)
                    .map(|result| result.context("http"))
                    .boxed()
            }
        }
    }
}

/// Enum wrapper for [`OneshotSource::Object`], see [`SourceKind`] for more details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ObjectKind {
    Http(Url),
}

/// Enum wrapper for [`OneshotSource::Checksum`], see [`SourceKind`] for more details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ChecksumKind {
    Http(HttpChecksum),
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
    fn process_work<'a, S: OneshotSource + Sync>(
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
#[derive(Clone)]
pub(crate) enum FormatKind {
    Csv(CsvDecoder),
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
        }
    }

    fn process_work<'a, S: OneshotSource + Sync>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>> {
        match (self, request) {
            (FormatKind::Csv(csv), RequestKind::Csv(request)) => csv
                .process_work(source, request)
                .map_ok(RecordChunkKind::Csv)
                .map(|result| result.context("csv"))
                .boxed(),
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
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RequestKind<O, C> {
    Csv(CsvWorkRequest<O, C>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RecordChunkKind {
    Csv(CsvRecord),
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
    #[error("reqwest error: {0}")]
    Reqwest(Arc<str>),
    #[error("invalid reqwest header: {0}")]
    InvalidHeader(Arc<str>),
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
