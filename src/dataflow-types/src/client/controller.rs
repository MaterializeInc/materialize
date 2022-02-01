// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client that maintains summaries of the involved objects.

use crate::client::{
    Client, Command, ComputeCommand, ComputeResponse, Response, StorageCommand, StorageResponse,
};
use expr::GlobalId;
use repr::Timestamp;
use timely::progress::{frontier::AntichainRef, Antichain, ChangeBatch};

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<C> {
    /// The underlying client,
    client: C,
    /// Sources that have been created.
    ///
    /// A `None` variant means that the source was dropped before it was first created.
    source_descriptions: std::collections::BTreeMap<GlobalId, Option<crate::sources::SourceDesc>>,
    /// Tracks `since` and `upper` frontiers for indexes and sinks.
    compute_since_uppers: SinceUpperMap,
    /// Tracks `since` and `upper` frontiers for sources and tables.
    storage_since_uppers: SinceUpperMap,
}

#[async_trait::async_trait]
impl<C: Client> Client for Controller<C> {
    async fn send(&mut self, cmd: Command) {
        self.send(cmd).await
    }
    async fn recv(&mut self) -> Option<Response> {
        self.recv().await
    }
}

impl<C: Client> Controller<C> {
    pub async fn send(&mut self, cmd: Command) {
        match &cmd {
            Command::Storage(StorageCommand::CreateSources(bindings)) => {
                // Maintain the list of bindings, and complain if one attempts to either
                // 1. create a dropped source identifier, or
                // 2. create an existing source identifier with a new description.
                for (id, (description, since)) in bindings.iter() {
                    let description = Some(description.clone());
                    match self.source_descriptions.get(&id) {
                        Some(None) => {
                            panic!("Attempt to recreate dropped source id: {:?}", id);
                        }
                        Some(prior_description) => {
                            if prior_description != &description {
                                panic!(
                                    "Multiple distinct descriptions created for source id {}: {:?} and {:?}",
                                    id,
                                    prior_description.as_ref().unwrap(),
                                    description.as_ref().unwrap(),
                                );
                            }
                        }
                        None => {
                            // All is well; no reason to panic.
                        }
                    }

                    self.source_descriptions.insert(*id, description.clone());
                    // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                    self.storage_since_uppers
                        .insert(*id, (since.clone(), Antichain::from_elem(0)));
                }
            }
            Command::Storage(StorageCommand::DropSources(identifiers)) => {
                for id in identifiers.iter() {
                    if !self.source_descriptions.contains_key(id) {
                        tracing::error!("Source id {} dropped without first being created", id);
                    } else {
                        self.source_descriptions.insert(*id, None);
                    }
                }
            }
            Command::Compute(ComputeCommand::EnableLogging(logging_config)) => {
                for id in logging_config.log_identifiers() {
                    self.compute_since_uppers
                        .insert(id, (Antichain::from_elem(0), Antichain::from_elem(0)));
                }
            }
            Command::Compute(ComputeCommand::CreateDataflows(dataflows)) => {
                // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
                // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
                for dataflow in dataflows.iter() {
                    let as_of = dataflow
                        .as_of
                        .as_ref()
                        .expect("Dataflow constructed without as_of set");

                    // Validate sources have `since.less_equal(as_of)`.
                    // TODO(mcsherry): Instead, return an error from the constructing method.
                    for (source_id, _) in dataflow.source_imports.iter() {
                        let (since, _upper) = self
                            .source_since_upper_for(*source_id)
                            .expect("Source frontiers absent in dataflow construction");
                        assert!(<_ as timely::order::PartialOrder>::less_equal(
                            &since,
                            &as_of.borrow()
                        ));
                    }

                    // Validate indexes have `since.less_equal(as_of)`.
                    // TODO(mcsherry): Instead, return an error from the constructing method.
                    for (index_id, _) in dataflow.index_imports.iter() {
                        let (since, _upper) = self
                            .index_since_upper_for(*index_id)
                            .expect("Index frontiers absent in dataflow construction");
                        assert!(<_ as timely::order::PartialOrder>::less_equal(
                            &since,
                            &as_of.borrow()
                        ));
                    }

                    for (sink_id, _) in dataflow.sink_exports.iter() {
                        // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                        self.compute_since_uppers
                            .insert(*sink_id, (as_of.clone(), Antichain::from_elem(0)));
                    }
                    for (index_id, _, _) in dataflow.index_exports.iter() {
                        // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                        self.compute_since_uppers
                            .insert(*index_id, (as_of.clone(), Antichain::from_elem(0)));
                    }
                }
            }
            Command::Compute(ComputeCommand::AllowIndexCompaction(frontiers)) => {
                for (id, frontier) in frontiers.iter() {
                    self.compute_since_uppers.advance_since_for(*id, frontier);
                }
            }
            Command::Storage(StorageCommand::AllowSourceCompaction(frontiers)) => {
                for (id, frontier) in frontiers.iter() {
                    self.storage_since_uppers.advance_since_for(*id, frontier);
                }
            }

            _ => {}
        }
        self.client.send(cmd).await
    }

    pub async fn recv(&mut self) -> Option<Response> {
        let response = self.client.recv().await;

        if let Some(response) = response.as_ref() {
            match response {
                Response::Compute(ComputeResponse::FrontierUppers(updates)) => {
                    for (id, changes) in updates.iter() {
                        self.compute_since_uppers.update_upper_for(*id, changes);
                    }
                }
                Response::Storage(StorageResponse::Frontiers(updates)) => {
                    for (id, changes) in updates.iter() {
                        self.storage_since_uppers.update_upper_for(*id, changes);
                    }
                }
                _ => {}
            }
        }

        response
    }
}

impl<C> Controller<C> {
    /// Create a new controller from a client it should wrap.
    pub fn new(client: C) -> Self {
        Self {
            client,
            source_descriptions: Default::default(),
            compute_since_uppers: Default::default(),
            storage_since_uppers: Default::default(),
        }
    }
    /// Returns the source description for a given identifier.
    ///
    /// The response does not distinguish between an as yet uncreated source description,
    /// and one that has been created and then dropped (or dropped without creation).
    /// There is a distinction and the client is aware of it, and could plausibly return
    /// this information if we had a use for it.
    pub fn source_description_for(&self, id: GlobalId) -> Option<&crate::sources::SourceDesc> {
        self.source_descriptions.get(&id).unwrap_or(&None).as_ref()
    }

    /// Returns the pair of `since` and `upper` for a maintained index, if it exists.
    ///
    /// The `since` frontier indicates that the maintained data are certainly valid for times greater
    /// or equal to the frontier, but they may not be for other times. Attempting to create a dataflow
    /// using this `id` with an `as_of` that is not at least `since` will result in an error.
    ///
    /// The `upper` frontier indicates that the data are reported available for all times not greater
    /// or equal to the frontier. Dataflows with an `as_of` greater or equal to this frontier may not
    /// immediately produce results.
    pub fn index_since_upper_for(
        &self,
        id: GlobalId,
    ) -> Option<(AntichainRef<Timestamp>, AntichainRef<Timestamp>)> {
        self.compute_since_uppers.get(&id)
    }

    /// Returns the pair of `since` and `upper` for a source, if it exists.
    ///
    /// The `since` frontier indicates that the maintained data are certainly valid for times greater
    /// or equal to the frontier, but they may not be for other times. Attempting to create a dataflow
    /// using this `id` with an `as_of` that is not at least `since` will result in an error.
    ///
    /// The `upper` frontier indicates that the data are reported available for all times not greater
    /// or equal to the frontier. Dataflows with an `as_of` greater or equal to this frontier may not
    /// immediately produce results.
    pub fn source_since_upper_for(
        &self,
        id: GlobalId,
    ) -> Option<(AntichainRef<Timestamp>, AntichainRef<Timestamp>)> {
        self.storage_since_uppers.get(&id)
    }
}

#[derive(Default)]
struct SinceUpperMap {
    since_uppers:
        std::collections::BTreeMap<GlobalId, (Antichain<Timestamp>, Antichain<Timestamp>)>,
}

impl SinceUpperMap {
    fn insert(&mut self, id: GlobalId, since_upper: (Antichain<Timestamp>, Antichain<Timestamp>)) {
        self.since_uppers.insert(id, since_upper);
    }
    fn get(&self, id: &GlobalId) -> Option<(AntichainRef<Timestamp>, AntichainRef<Timestamp>)> {
        self.since_uppers
            .get(id)
            .map(|(since, upper)| (since.borrow(), upper.borrow()))
    }
    fn advance_since_for(&mut self, id: GlobalId, frontier: &Antichain<Timestamp>) {
        if let Some((since, _upper)) = self.since_uppers.get_mut(&id) {
            use differential_dataflow::lattice::Lattice;
            since.join_assign(frontier);
        } else {
            // If we allow compaction before the item is created, pre-restrict the valid range.
            // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
            self.since_uppers
                .insert(id, (frontier.clone(), Antichain::from_elem(0)));
        }
    }
    fn update_upper_for(&mut self, id: GlobalId, changes: &ChangeBatch<Timestamp>) {
        if let Some((_since, upper)) = self.since_uppers.get_mut(&id) {
            // Apply `changes` to `upper`.
            let mut changes = changes.clone();
            for time in upper.elements().iter() {
                changes.update(time.clone(), 1);
            }
            upper.clear();
            for (time, count) in changes.drain() {
                assert_eq!(count, 1);
                upper.insert(time);
            }
        } else {
            // No panic, as we could have recently dropped this.
            // If we can tell these are updates to an id that could still be constructed,
            // something is weird and we should error.
        }
    }
}
