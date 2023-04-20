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

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use mz_repr::{GlobalId, Row};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::sinks::{MetadataFilled, StorageSinkDesc};
use mz_storage_client::types::sources::IngestionDescription;
use timely::communication::Allocate;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;

use crate::source::types::SourcePersistSinkMetrics;
use crate::storage_state::StorageState;

mod debezium;
mod persist_sink;
pub mod sinks;
pub mod sources;
mod upsert;

/// Assemble the "ingestion" side of a dataflow, i.e. the sources.
///
/// This method creates a new dataflow to host the implementations of sources for the `dataflow`
/// argument, and returns assets for each source that can import the results into a new dataflow.
pub fn build_ingestion_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    storage_state: &mut StorageState,
    primary_source_id: GlobalId,
    description: IngestionDescription<CollectionMetadata>,
    resume_upper: Antichain<mz_repr::Timestamp>,
    source_resume_upper: BTreeMap<GlobalId, Vec<Row>>,
) {
    let worker_id = timely_worker.index();
    let worker_logging = timely_worker.log_register().get("timely");
    let debug_name = primary_source_id.to_string();
    let name = format!("Source dataflow: {debug_name}");
    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, root_scope| {
        // Here we need to create two scopes. One timestamped with `()`, which is the root scope,
        // and one timestamped with `mz_repr::Timestamp` which is the final scope of the dataflow.
        // Refer to the module documentation for an explanation of this structure.
        // The scope.clone() occurs to allow import in the region.
        root_scope.clone().scoped(&name, |into_time_scope| {
            let debug_name = format!("{debug_name}-sources");

            let mut tokens = vec![];

            let (mut outputs, health_stream, token) = crate::render::sources::render_source(
                into_time_scope,
                &debug_name,
                primary_source_id,
                description.clone(),
                resume_upper.clone(),
                source_resume_upper,
                storage_state,
            );
            tokens.push(token);

            let mut health_configs = BTreeMap::new();

            for (export_id, export) in description.source_exports {
                let (ok, err) = outputs
                    .get_mut(export.output_index)
                    .expect("known to exist");
                let source_data = ok.map(Ok).concat(&err.map(Err));

                let metrics = SourcePersistSinkMetrics::new(
                    &storage_state.source_metrics,
                    export_id,
                    primary_source_id,
                    worker_id,
                    &export.storage_metadata.data_shard,
                    export.output_index,
                );

                tracing::info!(
                    "timely-{worker_id} rendering {export_id} with multi-worker persist_sink",
                );
                let token = crate::render::persist_sink::render(
                    into_time_scope,
                    export_id,
                    export.storage_metadata.clone(),
                    source_data,
                    storage_state,
                    metrics,
                    export.output_index,
                );
                tokens.push(token);

                health_configs.insert(export.output_index, (export_id, export.storage_metadata));
            }

            let health_token = crate::source::health_operator(
                into_time_scope,
                storage_state,
                resume_upper,
                primary_source_id,
                &health_stream,
                health_configs,
            );
            tokens.push(health_token);

            storage_state
                .source_tokens
                .insert(primary_source_id, Rc::new(tokens));
        })
    });
}

/// do the export dataflow thing
pub fn build_export_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    storage_state: &mut StorageState,
    id: GlobalId,
    description: StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>,
) {
    let worker_logging = timely_worker.log_register().get("timely");
    let debug_name = id.to_string();
    let name = format!("Source dataflow: {debug_name}");
    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        scope.clone().region_named(&name, |region| {
            let _debug_name = format!("{debug_name}-sinks");
            let _: &mut timely::dataflow::scopes::Child<
                timely::dataflow::scopes::Child<TimelyWorker<A>, _>,
                mz_repr::Timestamp,
            > = region;
            let mut tokens = BTreeMap::new();
            let import_ids = BTreeSet::new();
            crate::render::sinks::render_sink(
                region,
                storage_state,
                &mut tokens,
                import_ids,
                id,
                &description,
            );
        })
    });
}
