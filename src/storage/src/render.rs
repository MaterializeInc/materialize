// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders ingestions and exports into timely dataflow
//!
//! ## Ingestions
//!
//! ### Overall structure
//!
//! Before describing any of the timely operators involved in ingesting a source it helps to
//! understand the high level structure of the timely scopes involved. The reason for this
//! structure is the fact that we ingest external sources with a source-specific, and source
//! implementation defined, timestamp type which tracks progress in a way that the source
//! implementation understands. Each source specific timestamp must be compatible with timely's
//! `timely::progress::Timestamp` trait and so it's suitable to represent timely streams and by
//! extension differential collections.
//!
//! On the other hand, Materialize expects a specific timestamp type for all its collections
//! (currently `mz_repr::Timestamp`) so at some point the dataflow's timestamp must change. More
//! generally, the ingestion dataflow starts with some timestamp type `FromTime` and ends with
//! another timestamp type `IntoTime`.
//!
//! Here we run into a problem though because we want to start with a timely stream of type
//! `Stream<G1: Scope<Timestamp=FromTime>, ..>` and end up using it in a scope `G2` whose timestamp
//! type is `IntoTime`. Timely dataflows are organized in scopes where each scope has an associated
//! timestamp type that must refine the timestamp type of its parent scope. What "refines" means is
//! defined by the [`timely::progress::timestamp::Refines`] trait in timely. `FromTime` however
//! does not refine `IntoTime` nor does `IntoTime` refine `FromTime`.
//!
//! In order to acomplish this we split ingestion dataflows in two scopes, both of which are
//! children of the root timely scope. The first scope is timestamped with `FromTime` and the
//! second one with `IntoTime`. To move timely streams from the one scope to the other we must do
//! so manually. Each stream that needs to be transferred between scopes is first captured using
//! [`timely::dataflow::operators::capture::capture::Capture`] into a tokio unbounded mpsc channel.
//! The data in the channel record in full detail the worker-local view of the original stream and
//! whoever controls the receiver can read in the events, in the standard way of consuming the
//! async channel, and work with it. How the receiver is turned back into a timely stream in the
//! destination scope is described in the next section.
//!
//! For now keep in mind the general structure of the dataflow:
//!
//!
//! ```text
//! +----------------RootScope(Timestamp=())------------------+
//! |                                                         |
//! |  +---FromTime Scope---+         +---IntoTime Scope--+   |                                                   |
//! |  |                    |         |                   |   |
//! |  |                 *--+---------+-->                |   |
//! |  |                    |         |                   |   |
//! |  |                 <--+---------+--*                |   |
//! |  +--------------------+    ^    +-------------------+   |
//! |                            |                            |
//! |                            |                            |
//! |                  data exchanged between                 |
//! |                 scopes with capture/reclock             |
//! +---------------------------------------------------------+
//! ```
//!
//! ### Detailed dataflow
//!
//! We are now ready to describe the detailed structure of the ingestion dataflow. The dataflow
//! begins with the `source reader` dataflow fragment which is rendered in a `FromTime` timely
//! scope. This scope's timestamp is controlled by the [`crate::source::types::SourceRender::Time`]
//! associated type and can be anything the source implementation desires.
//!
//! Each source is free to render any arbitrary dataflow fragment in that scope as long as it
//! produces the collections expected by the rest of the framework. The rendering is handled by the
//! `[crate::source::types::SourceRender::render] method.
//!
//! When rendering a source dataflow we expect three outputs. First, a health output, which is how
//! the source communicates status updates about its health. Second, a data output, which is the
//! main output of a source and contains the data that will eventually be recorded in the persist
//! shard. Finally, an optional upper frontier output, which tracks the overall upstream upper
//! frontier. When a source doesn't provide a dedicated progress output the framework derives one
//! by observing the progress of the data output. This output (derived or not) is what drives
//! reclocking. When a source provides a dedicated upper output, it can manage it independently of
//! the data output frontier. For example, it's possible that a source implementation queries the
//! upstream system to learn what are the latest offsets for and set the upper output based on
//! that, even before having started the actual ingestion, which would be presented as data and
//! progress trickling in via the data output.
//!
//! ```text
//!                                                   resume upper
//!                                              ,--------------------.
//!                                             /                     |
//!                            health     ,----+---.                  |
//!                            output     | source |                  |
//!                           ,-----------| reader |                  |
//!                          /            +--,---.-+                  |
//!                         /               /     \                   |
//!                  +-----/----+   data   /       \  upper           |
//!                  |  health  |   output/         \ output          |
//!                  | operator |         |          \                |
//!                  +----------+         |           |               |
//!  FromTime                             |           |               |
//!     scope                             |           |               |
//!  -------------------------------------|-----------|---------------|---
//!  IntoTime                             |           |               |
//!     scope                             |      ,----+-----.         |
//!                                       |     |  remap   |          |
//!                                       |     | operator |          |
//!                                       |     +---,------+          |
//!                                       |        /                  |
//!                                       |       / bindings          |
//!                                       |      /                    |
//!                                     ,-+-----+--.                  |
//!                                     | reclock  |                  |
//!                                     | operator |                  |
//!                                     +-,--,---.-+                  |
//!                           ,----------´.-´     \                   |
//!                       _.-´         .-´         \                  |
//!                   _.-´          .-´             \                 |
//!                .-´            ,´                 \                |
//!               /              /                    \               |
//!        ,----------.   ,----------.           ,----------.         |
//!        |  decode  |   |  decode  |   ....    |  decode  |         |
//!        | output 0 |   | output 1 |           | output N |         |
//!        +-----+----+   +-----+----+           +-----+----+         |
//!              |              |                      |              |
//!              |              |                      |              |
//!        ,-----+----.   ,-----+----.           ,-----+----.         |
//!        | envelope |   | envelope |   ....    | envelope |         |
//!        | output 0 |   | output 1 |           | output N |         |
//!        +----------+   +-----+----+           +-----+----+         |
//!              |              |                      |              |
//!              |              |                      |              |
//!        ,-----+----.   ,-----+----.           ,-----+----.         |
//!        |  persist |   |  persist |   ....    |  persist |         |
//!        |  sink 0  |   |  sink 1  |           |  sink N  |         |
//!        +-----+----+   +-----+----+           +-----+----+         |
//!               \              \                    /               |
//!                `-.            `,                 /                |
//!                   `-._          `-.             /                 |
//!                       `-._         `-.         /                  |
//!                           `---------. `-.     /                   |
//!                                     +`---`---+---,                |
//!                                     |   resume   |                |
//!                                     | calculator |                |
//!                                     +------+-----+                |
//!                                             \                     |
//!                                              `-------------------´
//! ```
//!
//! #### Reclocking
//!
//! Whenever a dataflow edge crosses the scope boundaries it must first be converted into a
//! captured stream via the `[mz_timely_util::capture::UnboundedTokioCapture`] utility. This
//! disassociates the stream and its progress information from the original timely scope and allows
//! it to be read from a different place. The downside of this mechanism is that it's invisible to
//! timely's progress tracking, but that seems like a necessary evil if we want to do reclocking.
//!
//! The two main ways these tokio-fied streams are turned back into normal timely streams in the
//! destination scope are by the `reclock operator` and the `remap operator` which process the
//! `data output` and `upper output` of the source reader respectively.
//!
//! The `remap operator` reads the `upper output`, which is composed only of frontiers, mints new
//! bindings, and writes them into the remap shard. The final durable timestamp bindings are
//! emitted as its output for consumption by the `reclock operator`.
//!
//! The `reclock operator` reads the `data output`, which contains both data and progress
//! statements, and uses the bindings it receives from the `remap operator` to reclock each piece
//! of data and each frontier statement into the target scope's timestamp and emit the reclocked
//! stream in its output.
//!
//! #### Partitioning
//!
//! At this point we have a timely stream with correctly timestamped data in the mz time domain
//! (`mz_repr::Timestamp`) which contains multiplexed messages for each of the potential subsources
//! of this source. Each message selects the output it belongs to by setting the output field in
//! [`crate::source::types::SourceMessage`]. By convention, the main source output is always output
//! zero and subsources get the outputs from one onwards.
//!
//! However, regardless of whether the output is the main source or a subsource it is treated
//! identically by the pipeline. Each output is demultiplexed into its own timely stream using
//! [`timely::dataflow::operators::partition::Partition`] and the rest of the ingestion pipeline is
//! rendered independently.
//!
//! #### Resumption frontier
//!
//! At the end of each per-output dataflow fragment is an instance of `persist_sink`, which is
//! responsible for writing the final `Row` data into the corresponding output shard. The durable
//! upper of each of the output shards is then recombined in a way that calculates the minimum
//! upper frontier between them. This is what we refer to as the "resumption frontier" or "resume
//! upper" and at this stage it is expressed in terms of `IntoTime` timestamps. As a final step,
//! this resumption frontier is converted back into a `FromTime` timestamped frontier using
//! `ReclockFollower::source_upper_at_frontier` and connected back to the source reader operator.
//! This frontier is what drives the `OffsetCommiter` which informs the upstream system to release
//! resources until the specified offsets.
//!
//! ## Exports
//!
//! Not yet documented

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use mz_ore::error::ErrorExt;
use mz_repr::{GlobalId, Row};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::dyncfgs;
use mz_storage_types::oneshot_sources::{OneshotIngestionDescription, OneshotIngestionRequest};
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::{GenericSourceConnection, IngestionDescription, SourceConnection};
use mz_timely_util::antichain::AntichainExt;
use timely::communication::Allocate;
use timely::dataflow::Scope;
use timely::dataflow::operators::{Concatenate, ConnectLoop, Feedback, Leave, Map};
use timely::dataflow::scopes::Child;
use timely::progress::Antichain;
use timely::worker::{AsWorker, Worker as TimelyWorker};
use tokio::sync::Semaphore;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::RawSourceCreationConfig;
use crate::storage_state::StorageState;

mod persist_sink;
pub mod sinks;
pub mod sources;

/// Assemble the "ingestion" side of a dataflow, i.e. the sources.
///
/// This method creates a new dataflow to host the implementations of sources for the `dataflow`
/// argument, and returns assets for each source that can import the results into a new dataflow.
pub fn build_ingestion_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    storage_state: &mut StorageState,
    primary_source_id: GlobalId,
    description: IngestionDescription<CollectionMetadata>,
    as_of: Antichain<mz_repr::Timestamp>,
    resume_uppers: BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>,
    source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
) {
    let worker_id = timely_worker.index();
    let worker_logging = timely_worker.logger_for("timely").map(Into::into);
    let debug_name = primary_source_id.to_string();
    let name = format!("Source dataflow: {debug_name}");
    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, root_scope| {
        let root_scope: &mut Child<_, ()> = root_scope;
        // Here we need to create two scopes. One timestamped with `()`, which is the root scope,
        // and one timestamped with `mz_repr::Timestamp` which is the final scope of the dataflow.
        // Refer to the module documentation for an explanation of this structure.
        // The scope.clone() occurs to allow import in the region.
        root_scope.clone().scoped(&name, |mz_scope| {
            let debug_name = format!("{debug_name}-sources");

            let mut tokens = vec![];

            let (feedback_handle, feedback) = mz_scope.feedback(Default::default());

            let connection = description.desc.connection.clone();
            tracing::info!(
                id = %primary_source_id,
                as_of = %as_of.pretty(),
                resume_uppers = ?resume_uppers,
                source_resume_uppers = ?source_resume_uppers,
                "timely-{worker_id} building {} source pipeline", connection.name(),
            );

            let busy_signal = if dyncfgs::SUSPENDABLE_SOURCES
                .get(storage_state.storage_configuration.config_set())
            {
                Arc::new(Semaphore::new(1))
            } else {
                Arc::new(Semaphore::new(Semaphore::MAX_PERMITS))
            };

            let base_source_config = RawSourceCreationConfig {
                name: format!("{}-{}", connection.name(), primary_source_id),
                id: primary_source_id,
                source_exports: description.source_exports.clone(),
                timestamp_interval: description.desc.timestamp_interval,
                worker_id: mz_scope.index(),
                worker_count: mz_scope.peers(),
                now_fn: storage_state.now.clone(),
                metrics: storage_state.metrics.clone(),
                as_of: as_of.clone(),
                resume_uppers: resume_uppers.clone(),
                source_resume_uppers,
                remap_metadata: description.remap_metadata.clone(),
                persist_clients: Arc::clone(&storage_state.persist_clients),
                statistics: storage_state.aggregated_statistics.get_local_source_stats(),
                shared_remap_upper: Rc::clone(
                    &storage_state.source_uppers[&description.remap_collection_id],
                ),
                // This might quite a large clone, but its just during rendering
                config: storage_state.storage_configuration.clone(),
                remap_collection_id: description.remap_collection_id.clone(),
                busy_signal: Arc::clone(&busy_signal),
            };

            let (outputs, source_health, source_tokens) = match connection {
                GenericSourceConnection::Kafka(c) => crate::render::sources::render_source(
                    mz_scope,
                    &debug_name,
                    c,
                    description.clone(),
                    &feedback,
                    storage_state,
                    base_source_config,
                ),
                GenericSourceConnection::Postgres(c) => crate::render::sources::render_source(
                    mz_scope,
                    &debug_name,
                    c,
                    description.clone(),
                    &feedback,
                    storage_state,
                    base_source_config,
                ),
                GenericSourceConnection::MySql(c) => crate::render::sources::render_source(
                    mz_scope,
                    &debug_name,
                    c,
                    description.clone(),
                    &feedback,
                    storage_state,
                    base_source_config,
                ),
                GenericSourceConnection::SqlServer(c) => crate::render::sources::render_source(
                    mz_scope,
                    &debug_name,
                    c,
                    description.clone(),
                    &feedback,
                    storage_state,
                    base_source_config,
                ),
                GenericSourceConnection::LoadGenerator(c) => crate::render::sources::render_source(
                    mz_scope,
                    &debug_name,
                    c,
                    description.clone(),
                    &feedback,
                    storage_state,
                    base_source_config,
                ),
            };
            tokens.extend(source_tokens);

            let mut upper_streams = vec![];
            let mut health_streams = Vec::with_capacity(source_health.len() + outputs.len());
            health_streams.extend(source_health);
            for (export_id, (ok, err)) in outputs {
                let export = &description.source_exports[&export_id];
                let source_data = ok.map(Ok).concat(&err.map(Err));

                let metrics = storage_state.metrics.get_source_persist_sink_metrics(
                    export_id,
                    primary_source_id,
                    worker_id,
                    &export.storage_metadata.data_shard,
                );

                tracing::info!(
                    id = %primary_source_id,
                    "timely-{worker_id}: persisting export {} of {}",
                    export_id,
                    primary_source_id
                );
                let (upper_stream, errors, sink_tokens) = crate::render::persist_sink::render(
                    mz_scope,
                    export_id,
                    export.storage_metadata.clone(),
                    source_data,
                    storage_state,
                    metrics,
                    Arc::clone(&busy_signal),
                );
                upper_streams.push(upper_stream);
                tokens.extend(sink_tokens);

                let sink_health = errors.map(move |err: Rc<anyhow::Error>| {
                    let halt_status =
                        HealthStatusUpdate::halting(err.display_with_causes().to_string(), None);
                    HealthStatusMessage {
                        id: None,
                        namespace: StatusNamespace::Internal,
                        update: halt_status,
                    }
                });
                health_streams.push(sink_health.leave());
            }

            mz_scope
                .concatenate(upper_streams)
                .connect_loop(feedback_handle);

            let health_stream = root_scope.concatenate(health_streams);
            let health_token = crate::healthcheck::health_operator(
                root_scope,
                storage_state.now.clone(),
                resume_uppers
                    .iter()
                    .filter_map(|(id, frontier)| {
                        // If the collection isn't closed, then we will remark it as Starting as
                        // the dataflow comes up.
                        (!frontier.is_empty()).then_some(*id)
                    })
                    .collect(),
                primary_source_id,
                "source",
                &health_stream,
                crate::healthcheck::DefaultWriter {
                    command_tx: storage_state.internal_cmd_tx.clone(),
                    updates: Rc::clone(&storage_state.shared_status_updates),
                },
                storage_state
                    .storage_configuration
                    .parameters
                    .record_namespaced_errors,
                dyncfgs::STORAGE_SUSPEND_AND_RESTART_DELAY
                    .get(storage_state.storage_configuration.config_set()),
            );
            tokens.push(health_token);

            storage_state
                .source_tokens
                .insert(primary_source_id, tokens);
        })
    });
}

/// do the export dataflow thing
pub fn build_export_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    storage_state: &mut StorageState,
    id: GlobalId,
    description: StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>,
) {
    let worker_logging = timely_worker.logger_for("timely").map(Into::into);
    let debug_name = id.to_string();
    let name = format!("Source dataflow: {debug_name}");
    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, root_scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        root_scope.region_named(&name, |scope| {
            let mut tokens = vec![];
            let (health_stream, sink_tokens) =
                crate::render::sinks::render_sink(scope, storage_state, id, &description);
            tokens.extend(sink_tokens);

            // Note that sinks also have only 1 active worker, which simplifies the work that
            // `health_operator` has to do internally.
            let health_token = crate::healthcheck::health_operator(
                scope,
                storage_state.now.clone(),
                [id].into_iter().collect(),
                id,
                "sink",
                &health_stream,
                crate::healthcheck::DefaultWriter {
                    command_tx: storage_state.internal_cmd_tx.clone(),
                    updates: Rc::clone(&storage_state.shared_status_updates),
                },
                storage_state
                    .storage_configuration
                    .parameters
                    .record_namespaced_errors,
                dyncfgs::STORAGE_SUSPEND_AND_RESTART_DELAY
                    .get(storage_state.storage_configuration.config_set()),
            );
            tokens.push(health_token);

            storage_state.sink_tokens.insert(id, tokens);
        })
    });
}

pub(crate) fn build_oneshot_ingestion_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    storage_state: &mut StorageState,
    ingestion_id: uuid::Uuid,
    collection_id: GlobalId,
    collection_meta: CollectionMetadata,
    description: OneshotIngestionRequest,
) {
    let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
    let callback = move |result| {
        // TODO(cf3): Do we care if the receiver has gone away?
        //
        // Persist is working on cleaning up leaked blobs, we could also use `OneshotReceiverExt`
        // here, but that might run into the infamous async-Drop problem.
        let _ = results_tx.send(result);
    };
    let connection_context = storage_state
        .storage_configuration
        .connection_context
        .clone();

    let tokens = timely_worker.dataflow(|scope| {
        mz_storage_operators::oneshot_source::render(
            scope.clone(),
            Arc::clone(&storage_state.persist_clients),
            connection_context,
            collection_id,
            collection_meta,
            description,
            callback,
        )
    });
    let ingestion_description = OneshotIngestionDescription {
        tokens,
        results: results_rx,
    };

    storage_state
        .oneshot_ingestions
        .insert(ingestion_id, ingestion_description);
}
