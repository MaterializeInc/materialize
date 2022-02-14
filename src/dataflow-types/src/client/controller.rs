// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client that maintains summaries of the involved objects.
use std::collections::BTreeMap;

use crate::client::{
    Client, Command, ComputeCommand, ComputeInstanceId, ComputeResponse, Response, StorageCommand,
};
use mz_expr::GlobalId;
use timely::progress::Timestamp;
use timely::progress::{Antichain, ChangeBatch};
use timely::progress::frontier::AntichainRef;
use differential_dataflow::lattice::Lattice;

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<C> {
    /// The underlying client,
    client: C,
    /// Sources that have been created.
    ///
    /// A `None` variant means that the source was dropped before it was first created.
    source_descriptions: std::collections::BTreeMap<GlobalId, Option<crate::sources::SourceDesc>>,
    /// Tracks `since` and `upper` frontiers for indexes and sinks.
    compute_since_uppers: BTreeMap<ComputeInstanceId, SinceUpperMap<mz_repr::Timestamp>>,
    /// Tracks `since` and `upper` frontiers for sources and tables.
    storage_since_uppers: SinceUpperMap<mz_repr::Timestamp>,
    /// Constraints on `since` bounds from users and dependent indexes and sinks.
    compaction_constraints: Constraints<mz_repr::Timestamp>,
}

#[async_trait::async_trait]
impl<C: Client> Client for Controller<C> {
    async fn send(&mut self, cmd: Command) {
        self.compaction_constraints.maintain();
        self.send(cmd).await
    }
    async fn recv(&mut self) -> Option<Response> {
        self.compaction_constraints.maintain();
        self.recv().await
    }
}

impl<C: Client> Controller<C> {
    pub async fn send(&mut self, cmd: Command) {
        match &cmd {
            Command::Compute(ComputeCommand::CreateInstance(logging), instance) => {
                self.compute_since_uppers
                    .insert(*instance, Default::default());

                if let Some(logging_config) = logging {
                    for id in logging_config.log_identifiers() {
                        self.compute_since_uppers
                            .get_mut(instance)
                            .expect("Reference to absent instance")
                            .insert(id, (Antichain::from_elem(0), Antichain::from_elem(0)));
                    }
                }
            }
            Command::Compute(ComputeCommand::DropInstance, instance) => {
                self.compute_since_uppers.remove(instance);
            }
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
            Command::Compute(ComputeCommand::CreateDataflows(dataflows), instance) => {
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
                            .compute_since_uppers
                            .get_mut(instance)
                            .expect("Reference to absent instance")
                            .get(index_id)
                            .expect("Index frontiers absent in dataflow construction");
                        assert!(<_ as timely::order::PartialOrder>::less_equal(
                            &since,
                            &as_of.borrow()
                        ));
                    }

                    for (sink_id, _) in dataflow.sink_exports.iter() {
                        // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                        self.compute_since_uppers
                            .get_mut(instance)
                            .expect("Reference to absent instance")
                            .insert(*sink_id, (as_of.clone(), Antichain::from_elem(0)));
                    }
                    for (index_id, _, _) in dataflow.index_exports.iter() {
                        // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                        self.compute_since_uppers
                            .get_mut(instance)
                            .expect("Reference to absent instance")
                            .insert(*index_id, (as_of.clone(), Antichain::from_elem(0)));
                    }
                }
            }
            Command::Compute(ComputeCommand::AllowIndexCompaction(frontiers), instance) => {
                for (id, frontier) in frontiers.iter() {
                    self.compute_since_uppers
                        .get_mut(instance)
                        .expect("Reference to absent instance")
                        .advance_since_for(*id, frontier);
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
                Response::Compute(ComputeResponse::FrontierUppers(updates), instance) => {
                    for (id, changes) in updates.iter() {
                        self.compute_since_uppers
                            .get_mut(instance)
                            .expect("Reference to absent instance")
                            .update_upper_for(*id, changes);
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
            compaction_constraints: Constraints::new(),
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
    ) -> Option<(AntichainRef<mz_repr::Timestamp>, AntichainRef<mz_repr::Timestamp>)> {
        self.storage_since_uppers.get(&id)
    }

    pub fn pin_compaction(&mut self, ids: Vec<GlobalId>) -> ConstraintPin<mz_repr::Timestamp> {
        self.compaction_constraints.pin_compaction(ids)
    }
}

#[derive(Default)]
struct SinceUpperMap<T: Timestamp+Lattice> {
    since_uppers:
        std::collections::BTreeMap<GlobalId, (Antichain<T>, Antichain<T>)>,
}

impl<T: Timestamp+Lattice> SinceUpperMap<T> {
    fn insert(&mut self, id: GlobalId, since_upper: (Antichain<T>, Antichain<T>)) {
        self.since_uppers.insert(id, since_upper);
    }
    fn get(&self, id: &GlobalId) -> Option<(AntichainRef<T>, AntichainRef<T>)> {
        self.since_uppers
            .get(id)
            .map(|(since, upper)| (since.borrow(), upper.borrow()))
    }
    fn advance_since_for(&mut self, id: GlobalId, frontier: &Antichain<T>) {
        if let Some((since, _upper)) = self.since_uppers.get_mut(&id) {
            since.join_assign(frontier);
        } else {
            // If we allow compaction before the item is created, pre-restrict the valid range.
            // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
            self.since_uppers
                .insert(id, (frontier.clone(), Antichain::from_elem(T::minimum())));
        }
    }
    fn update_upper_for(&mut self, id: GlobalId, changes: &ChangeBatch<T>) {
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

pub struct Constraints<T: Timestamp> {
    pub constraints: DependencyMap<T>,
    /// Send and Recv pair for information about changes in expressed constraints.
    dependency_send: crossbeam_channel::Sender<(Vec<GlobalId>, ChangeBatch<T>)>,
    dependency_recv: crossbeam_channel::Receiver<(Vec<GlobalId>, ChangeBatch<T>)>,
}

impl<T: Timestamp+Lattice> Constraints<T> {
    fn new() -> Self {
        let (dependency_send, dependency_recv) = crossbeam_channel::unbounded();
        Self {
            constraints: Default::default(),
            dependency_send,
            dependency_recv,
        }
    }
    fn pin_compaction(&mut self, ids: Vec<GlobalId>) -> ConstraintPin<T> {
        ConstraintPin {
            constraint: self.constraints.constraint_from(ids),
            channel: self.dependency_send.clone(),
        }
    }
    /// Applies accumulated maintenance work from constraint pins.
    ///
    /// Returns consequent changes in constraints at other operators.
    fn maintain(&mut self) -> BTreeMap<GlobalId, ChangeBatch<T>> {
        let mut updates = BTreeMap::new();
        while let Ok((ids, mut update)) = self.dependency_recv.try_recv() {
            for id in ids.into_iter() {
                updates.entry(id).or_insert_with(ChangeBatch::new).extend(update.iter().cloned());
            }
        }
        self.constraints.propagate(&mut updates)
    }
}

/// A `Constraint` that can be owned and will correctly propagate changes to the constraint.
pub struct ConstraintPin<T: Timestamp> {
    constraint: Constraint<T>,
    channel: crossbeam_channel::Sender<(Vec<GlobalId>, ChangeBatch<T>)>,
}

impl<T: Timestamp> ConstraintPin<T> {
    pub fn frontier(&self) -> AntichainRef<T> {
        self.constraint.frontier.frontier()
    }

    pub fn downgrade_to(&mut self, frontier: AntichainRef<T>) {
        let mut updates = ChangeBatch::default();
        updates.extend(frontier.iter().map(|time| (time.clone(), 1)));
        updates.extend(self.constraint.frontier.frontier().iter().map(|time| (time.clone(), -1)));
        self.constraint.frontier.clear();
        self.constraint.frontier.update_iter(frontier.into_iter().map(|time| (time.clone(), 1)));
        let depends_on = self.constraint.depends_on.clone();
        // If the other end has hung up, we are not longer tracking constraints.
        let _ = self.channel.send((depends_on, updates));
    }
}

impl<T: Timestamp> Drop for ConstraintPin<T> {
    fn drop(&mut self) {
        let mut updates = ChangeBatch::default();
        updates.extend(self.constraint.frontier.frontier().iter().map(|time| (time.clone(), -1)));
        let depends_on = std::mem::take(&mut self.constraint.depends_on);
        // If the other end has hung up, we are not longer tracking constraints.
        let _ = self.channel.send((depends_on, updates));
    }
}

pub use dependency_map::{DependencyMap, Constraint};
/// Tracking the constraints on compaction across collections.
mod dependency_map {

    use std::collections::BTreeMap;

    use differential_dataflow::lattice::Lattice;
    use timely::progress::frontier::MutableAntichain;
    use timely::progress::Timestamp;
    use timely::progress::{Antichain, ChangeBatch};

    use mz_expr::GlobalId;

    /// Contains dependencies of outputs on inputs, and propagates constraints along them.
    ///
    /// Specifically, we track for various `GlobalId`s for which times the id must be "valid",
    /// using the `since` frontier. If an output must be valid for some `since`, then the inputs
    /// must also remain valid, so that the ouputs could be reconstructed in the case of a fault.
    ///
    /// Sources in particular must not be allowed to compact their representation until we are
    /// certain that downstream consumers no longer rely on the information. It is less clear
    /// whether we *must* hold back intermediate index compaction, though it would be of use in
    /// the case that we wanted to migrate dataflows around (e.g. stop and restart a dataflow,
    /// without a COMPUTE-wide fault).
    pub struct DependencyMap<T> {
        /// Constraints identifiers impose on each other.
        ///
        /// Each constraint names other *strictly prior* identifiers, as well as holds the
        /// constrained frontier that these identifiers must respect. The frontier contains
        /// the accumulated constraints imposed on it as well.
        constraints: BTreeMap<GlobalId, Constraint<T>>
    }

    impl<T> Default for DependencyMap<T> {
        fn default() -> Self {
            Self { constraints: BTreeMap::default() }
        }
    }

    impl<T: Timestamp + Lattice> DependencyMap<T> {
        /// Introduces a new identifier to be tracked.
        ///
        /// To remove an identifier, downgrade its constraint to the empty frontier.
        pub fn add(&mut self, id: GlobalId, depends_on: Vec<GlobalId>) {
            // Only allow dependence on strictly prior identifier.
            assert!(depends_on.iter().all(|d| d < &id));
            // Introduce state about `id`.
            let constraint = self.constraint_from(depends_on);
            self.constraints.insert(id, constraint);
        }
        /// Applies `updates` to each frontier, and transitively applies the effects to those it depends on.
        ///
        /// If the updates or their transitive effects result in constraints advancing to the empty frontier,
        /// they are removed from the tracking structure.
        ///
        /// Returns the transitive effects applied to all identifiers, which includes the initial `updates`.
        pub fn propagate(
            &mut self,
            updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
        ) -> BTreeMap<GlobalId, ChangeBatch<T>> {
            let mut results = BTreeMap::<GlobalId, ChangeBatch<T>>::default();
            // Repeatedly extract the *greatest* identifier, topologically last.
            while let Some(last) = updates.keys().rev().next().cloned() {
                let mut update = updates.remove(&last).expect("Certain to exist");
                let constraint = self
                    .constraints
                    .get_mut(&last)
                    .expect("Propagating changes for absent constraint");
                let changes = constraint.frontier.update_iter(update.drain());
                update.extend(changes);
                // `update` now contains the change in constraints to pass along to those `last` depends on.
                for id in constraint.depends_on.iter() {
                    // Communicate change
                    updates
                        .get_mut(id)
                        .expect("Depended on an absent identifier")
                        .extend(update.iter().cloned());
                }
                results.insert(last, update);
                // Clean up, if the constraint is now vacuuous.
                if constraint.frontier.frontier().is_empty() {
                    self.constraints.remove(&last);
                }
            }
            results
        }

        pub fn constraint_from(&mut self, depends_on: Vec<GlobalId>) -> Constraint<T> {
            // The initial state of `id`'s constraints are determined by the state of those it depends on.
            let mut initially = Antichain::from_elem(T::minimum());
            for depended_on in depends_on.iter() {
                let mut upper = Antichain::new();
                for time1 in initially.elements().iter() {
                    for time2 in self.constraints[depended_on].frontier.frontier().iter() {
                        upper.insert(time1.join(time2));
                    }
                }
                initially = upper;
            }
            // Inform each of those `id` depends up of the new constraint.
            let mut update = ChangeBatch::new();
            update.extend(initially.iter().map(|time| (time.clone(), 1)));
            let mut updates = BTreeMap::default();
            for depended_on in depends_on.iter() {
                updates.insert(*depended_on, update.clone());
            }
            self.propagate(&mut updates);
            let mut frontier = MutableAntichain::default();
            frontier.update_iter(initially.iter().map(|time| (time.clone(), 1)));
            Constraint {
                frontier,
                depends_on,
            }
        }
    }

    /// A constraint imposed on the frontiers of other identifiers.
    pub struct Constraint<T> {
        /// The frontier the constraint holds.
        pub(super) frontier: MutableAntichain<T>,
        /// Those whose frontiers must be held to `frontier`.
        pub(super) depends_on: Vec<GlobalId>,
    }
}
