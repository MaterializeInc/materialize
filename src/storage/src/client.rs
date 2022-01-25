// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::rc::Rc;

/// A client to the storage service.
pub struct Client {

}

impl Client {
    /// Creates a source.
    ///
    /// Returns an identifier for the resulting definite collection.
    pub fn create_source(def: SourceDesc) -> Id {
        todo!()
    }

    /// Subscribes to a stream of updates for the specified collection...
    pub fn subscribe(id: Id) {
        todo!()
    }
}

/// Describes a source.
pub struct SourceDesc {

}

/// Identifies a definite collection.
///
/// An `Id` is unique for the lifetime of a [`Server`](crate::server::Server).
pub struct Id {

}

/// Candidate client structure for dataflow operators.
///
/// We could have the client present non-dataflow access to information, and have COMPUTE
/// build the operators, but for incremental migration it seems like it makes sense to allow
/// STORAGE to use dataflow machinery rather than forbid it.
pub struct DataflowClient {
        // // Locally maintained storage-specific state in a CLUSTER instance.
        // // Presently this contains a handle to `persist` among other things.
        // state: dataflow_types::render::StorageState,
}

impl DataflowClient {
    /// Imports a source identifier into a provided dataflow scope.
    ///
    /// The returned collection bundle contains the collection identified by `source_identifier`,
    /// advanced in time no further than `as_of_frontier`, and subjected to `map_filter_project`,
    /// with the caveat that it's resulting action must still be applied to the collection bundle.
    ///
    /// The returned pair of source token and "additional tokens" can be dropped to terminate the
    /// source.
    fn import_source_into<G>(
        &mut self,
        // The storage provided source identifier.
        source_identifier: crate::client::Id,
        // The scope into which the source is imported.
        // Potentially a `Region` to provide encapsulation.
        scope: &mut G,
        // Requirement on how little compaction must be applied.
        as_of_frontier: &timely::progress::Antichain<repr::Timestamp>,
        // In-place logic that can be applied to records.
        // The mutable reference allows the callee to absorb any amount of the logic,
        // but it must leave in place any corresponding finishing work.
        map_filter_project: &mut expr::MapFilterProject,
    ) -> (
        // dataflow::render::CollectionBundle<G, repr::Row, repr::Timestamp>,
        timely::dataflow::Stream<G, (repr::Row, repr::Timestamp, repr::Diff)>,
        (
            Rc<Option<dataflow::source::SourceToken>>,
            Vec<Rc<dyn std::any::Any>>,
        ),
    )
    where
        G: timely::dataflow::Scope<Timestamp = repr::Timestamp>,
    {
        unimplemented!()
    }

    /// Export the sink described by `sink` from the rendering context.
    ///
    /// TODO: have the function expose progress, determine that contract.
    /// Current signature calls for an output stream that communicates by
    /// way of its frontier which timestamps are as yet uncommitted.
    pub(crate) fn export_sink_from<G>(
        &mut self,
        // A description of how to capture the sinked data.
        sink: &dataflow_types::sinks::SinkDesc,
        // The collection to sink, presented as arrangements and/or a stream.
        // collection: dataflow::render::CollectionBundle<G, repr::Row, repr::Timestamp>,
        collection: timely::dataflow::Stream<G, (repr::Row, repr::Timestamp, repr::Diff)>,
        // Input tokens that must be held for the sink's lifetime.
        tokens: Vec<Rc<dyn std::any::Any>>,
    ) -> timely::dataflow::Stream<G, ()>
    where
        G: timely::dataflow::Scope<Timestamp = repr::Timestamp>,
    {
        unimplemented!()
    }
}