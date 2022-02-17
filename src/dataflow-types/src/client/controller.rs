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

use differential_dataflow::lattice::Lattice;
use timely::progress::{frontier::AntichainRef, Antichain, ChangeBatch, Timestamp};

use crate::client::{
    Client, Command, ComputeCommand, ComputeInstanceId, ComputeResponse, Response, SourceConnector,
    StorageCommand,
};
use crate::logging::LoggingConfig;
use crate::DataflowDescription;
use crate::Update;
use mz_expr::{GlobalId, PartitionId, RowSetFinishing};
use mz_repr::Row;

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<C, T = mz_repr::Timestamp>
where
    T: Timestamp + Lattice,
{
    /// The underlying client,
    client: C,
    /// Sources that have been created.
    ///
    /// A `None` variant means that the source was dropped before it was first created.
    source_descriptions:
        std::collections::BTreeMap<GlobalId, Option<(crate::sources::SourceDesc, Antichain<T>)>>,
    /// Tracks `since` and `upper` frontiers for indexes and sinks.
    compute_since_uppers: BTreeMap<ComputeInstanceId, SinceUpperMap>,
    /// Tracks `since` and `upper` frontiers for sources and tables.
    storage_since_uppers: SinceUpperMap,
    /// Machinery to propagate compaction constraints from outputs to inputs.
    compaction: ConstraintPropagation<T>,
    /// Temporary stash of read capabilities.
    ///
    /// Contains a read capability for each compactable identifier we manage.
    /// This exists to support imperative `allow_compaction` methods which
    /// presume there is a read capability to directly manipulate.
    // TODO: Remove this when we remove `allow_compaction` methods.
    temp_stashed_capabilities: BTreeMap<ConstraintId, ReadCapability<T>>,
}

/// Controller errors, either from compute or storage commands.
#[derive(Debug)]
pub enum ControllerError {
    /// Errors arising from compute commands.
    Compute(ComputeError),
    /// Errors arising from storage commands.
    Storage(StorageError),
}

impl From<ComputeError> for ControllerError {
    fn from(err: ComputeError) -> ControllerError {
        ControllerError::Compute(err)
    }
}
impl From<StorageError> for ControllerError {
    fn from(err: StorageError) -> ControllerError {
        ControllerError::Storage(err)
    }
}

/// Errors arising from compute commands.
#[derive(Debug)]
pub enum ComputeError {
    /// Command referenced an instance that was not present.
    InstanceMissing(ComputeInstanceId),
    /// Command referenced an identifier that was not present.
    IdentifierMissing(GlobalId),
    /// Dataflow was malformed (e.g. missing `as_of`).
    DataflowMalformed,
    /// The dataflow `as_of` was not greater than the `since` of the identifier.
    DataflowSinceViolation(GlobalId),
    /// The peek `timestamp` was not greater than the `since` of the identifier.
    PeekSinceViolation(GlobalId),
}

#[derive(Debug)]
pub enum StorageError {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    SourceIdReused(GlobalId),
}

// Implementation of COMPUTE commands.
impl<C: Client> Controller<C> {
    pub async fn create_instance(
        &mut self,
        instance: ComputeInstanceId,
        logging: Option<LoggingConfig>,
    ) -> Result<(), ControllerError> {
        self.compute_since_uppers
            .insert(instance, Default::default());

        if let Some(logging_config) = logging.as_ref() {
            for id in logging_config.log_identifiers() {
                // This cannot fail, as we inserted just above.
                self.compute_since_uppers
                    .get_mut(&instance)
                    .ok_or(ComputeError::InstanceMissing(instance))?
                    .insert(id, (Antichain::from_elem(0), Antichain::from_elem(0)));
            }
            // Introduce logging indexes as compactable.
            self.compaction.ext_requirements.borrow_mut().extend(
                logging_config.log_identifiers().map(|id| {
                    (
                        (
                            (id, Some(instance)),
                            vec![<mz_repr::Timestamp as Timestamp>::minimum()],
                        ),
                        1,
                    )
                }),
            );
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::CreateInstance(logging),
                instance,
            ))
            .await;

        Ok(())
    }
    pub async fn drop_instance(&mut self, instance: ComputeInstanceId) {
        self.compute_since_uppers.remove(&instance);

        self.client
            .send(Command::Compute(ComputeCommand::DropInstance, instance))
            .await;
    }
    pub async fn create_dataflows(
        &mut self,
        instance: ComputeInstanceId,
        dataflows: Vec<DataflowDescription<crate::plan::Plan>>,
    ) -> Result<Vec<ReadCapability<mz_repr::Timestamp>>, ControllerError> {
        let compute_since_uppers = self
            .compute_since_uppers
            .get_mut(&instance)
            .ok_or(ComputeError::InstanceMissing(instance))?;

        // TODO: Validate *all* dataflows before building any dataflow.
        let mut capabilities = Vec::with_capacity(dataflows.len());

        // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
        for dataflow in dataflows.iter() {
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(ComputeError::DataflowMalformed)?;

            // Validate sources have `since.less_equal(as_of)`.
            for (source_id, _) in dataflow.source_imports.iter() {
                let (since, _upper) = self
                    .storage_since_uppers
                    .get(source_id)
                    .ok_or(ComputeError::IdentifierMissing(*source_id))?;
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }
            }

            // Validate indexes have `since.less_equal(as_of)`.
            for (index_id, _) in dataflow.index_imports.iter() {
                let (since, _upper) = compute_since_uppers
                    .get(index_id)
                    .ok_or(ComputeError::IdentifierMissing(*index_id))?;
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                }
            }
        }

        for dataflow in dataflows.iter() {

            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(ComputeError::DataflowMalformed)?;


            // Track inputs for compaction constraints.
            let mut inputs = Vec::new();
            inputs.extend(dataflow.source_imports.iter().map(|(id, _)| (*id, None)));
            inputs.extend(dataflow.index_imports.iter().map(|(id, _)| (*id, Some(instance))));

            // Collect identifiers for `ReadCapability`.
            // TODO: Record introduce dependencies so that we can retract when ids are dropped.
            let mut identifiers = Vec::new();

            for (sink_id, _) in dataflow.sink_exports.iter() {
                // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                compute_since_uppers.insert(*sink_id, (as_of.clone(), Antichain::from_elem(0)));
                identifiers.push((*sink_id, Some(instance)));
                self.compaction.ext_constraints.borrow_mut().extend(identifiers.iter().map(|input| (((*sink_id, Some(instance)), *input), 1)));
            }
            for (index_id, _, _) in dataflow.index_exports.iter() {
                // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
                compute_since_uppers.insert(*index_id, (as_of.clone(), Antichain::from_elem(0)));
                identifiers.push((*index_id, Some(instance)));
                self.compaction.ext_constraints.borrow_mut().extend(identifiers.iter().map(|input| (((*index_id, Some(instance)), *input), 1)));
            }

            capabilities.push(ReadCapability::new(as_of.clone(), identifiers, std::rc::Rc::clone(&self.compaction.ext_requirements)));
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::CreateDataflows(dataflows),
                instance,
            ))
            .await;

        Ok(capabilities)
    }
    pub async fn drop_sinks(
        &mut self,
        instance: ComputeInstanceId,
        sink_identifiers: Vec<GlobalId>,
    ) {
        self.client
            .send(Command::Compute(
                ComputeCommand::DropSinks(sink_identifiers),
                instance,
            ))
            .await;
    }
    pub async fn drop_indexes(
        &mut self,
        instance: ComputeInstanceId,
        index_identifiers: Vec<GlobalId>,
    ) {
        self.client
            .send(Command::Compute(
                ComputeCommand::DropIndexes(index_identifiers),
                instance,
            ))
            .await;
    }
    pub async fn peek(
        &mut self,
        instance: ComputeInstanceId,
        id: GlobalId,
        key: Option<Row>,
        conn_id: u32,
        timestamp: mz_repr::Timestamp,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
    ) -> Result<(), ControllerError> {
        let (since, _upper) = self
            .compute_since_uppers
            .get(&instance)
            .ok_or(ComputeError::InstanceMissing(instance))?
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))?;

        if !since.less_equal(&timestamp) {
            Err(ComputeError::PeekSinceViolation(id))?;
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::Peek {
                    id,
                    key,
                    conn_id,
                    timestamp,
                    finishing,
                    map_filter_project,
                },
                instance,
            ))
            .await;

        Ok(())
    }
    pub async fn cancel_peek(&mut self, instance: ComputeInstanceId, conn_id: u32) {
        self.client
            .send(Command::Compute(
                ComputeCommand::CancelPeek { conn_id },
                instance,
            ))
            .await;
    }
    pub async fn allow_index_compaction(
        &mut self,
        instance: ComputeInstanceId,
        frontiers: Vec<(GlobalId, Antichain<mz_repr::Timestamp>)>,
    ) -> Result<(), ControllerError> {
        let compute_since_uppers = self
            .compute_since_uppers
            .get_mut(&instance)
            .ok_or(ComputeError::InstanceMissing(instance))?;

        for (id, frontier) in frontiers.iter() {
            compute_since_uppers.advance_since_for(*id, frontier);
        }

        self.client
            .send(Command::Compute(
                ComputeCommand::AllowIndexCompaction(frontiers),
                instance,
            ))
            .await;

        Ok(())
    }
}

// Implementation of STORAGE commands.
impl<C: Client> Controller<C> {
    /// Creates sources as described, and returns a vector of read capabilities if successful.
    pub async fn create_sources(
        &mut self,
        mut bindings: Vec<(
            GlobalId,
            (crate::sources::SourceDesc, Antichain<mz_repr::Timestamp>),
        )>,
    ) -> Result<Vec<ReadCapability<mz_repr::Timestamp>>, ControllerError> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped source identifier, or
        // 2. create an existing source identifier with a new description.
        // Make sure to check for errors within `bindings` as well.
        bindings.sort_by_key(|b| b.0);
        bindings.dedup();
        for pos in 1..bindings.len() {
            if bindings[pos - 1].0 == bindings[pos].0 {
                Err(StorageError::SourceIdReused(bindings[pos].0))?;
            }
        }
        for (id, description_since) in bindings.iter() {
            match self.source_descriptions.get(&id) {
                Some(None) => Err(StorageError::SourceIdReused(*id))?,
                Some(Some(prior_description)) => {
                    if prior_description != description_since {
                        Err(StorageError::SourceIdReused(*id))?
                    }
                }
                None => {
                    // All is well; no reason to panic.
                }
            }
        }

        // Establish capabilities to return, and to temporarily stash.
        let mut read_capabilities = Vec::new();
        for (id, (description, since)) in bindings.iter() {
            self.source_descriptions
                .insert(*id, Some((description.clone(), since.clone())));
            // We start tracking `upper` at 0; correct this should that change (e.g. to `as_of`).
            self.storage_since_uppers
                .insert(*id, (since.clone(), Antichain::from_elem(0)));

            let capability = ReadCapability::new(since.clone().into(), vec![(*id, None)], std::rc::Rc::clone(&self.compaction.ext_requirements));
            read_capabilities.push(capability.clone());
            self.temp_stashed_capabilities.insert((*id, None), capability);
        }

        self.client
            .send(Command::Storage(StorageCommand::CreateSources(bindings)))
            .await;

        Ok(read_capabilities)
    }
    pub async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) {
        for id in identifiers.iter() {
            if !self.source_descriptions.contains_key(id) {
                // This isn't an unrecoverable error, just .. probably wrong.
                tracing::error!("Source id {} dropped without first being created", id);
            } else {
                self.source_descriptions.insert(*id, None);
                self.temp_stashed_capabilities.remove(&(*id, None));
            }
        }

        self.client
            .send(Command::Storage(StorageCommand::DropSources(identifiers)))
            .await
    }
    pub async fn table_insert(&mut self, id: GlobalId, updates: Vec<Update>) {
        self.client
            .send(Command::Storage(StorageCommand::Insert { id, updates }))
            .await
    }
    pub async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<mz_repr::Timestamp>)>,
    ) {
        self.client
            .send(Command::Storage(StorageCommand::DurabilityFrontierUpdates(
                updates,
            )))
            .await
    }
    pub async fn add_source_timestamping(
        &mut self,
        id: GlobalId,
        connector: SourceConnector,
        bindings: Vec<(PartitionId, mz_repr::Timestamp, crate::sources::MzOffset)>,
    ) {
        self.client
            .send(Command::Storage(StorageCommand::AddSourceTimestamping {
                id,
                connector,
                bindings,
            }))
            .await
    }
    pub async fn allow_source_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<mz_repr::Timestamp>)>,
    ) {
        for (id, frontier) in frontiers.iter() {
            self.storage_since_uppers.advance_since_for(*id, frontier);
            self.temp_stashed_capabilities.get_mut(&(*id, None)).map(|cap| cap.downgrade(frontier.clone()));
        }

        self.client
            .send(Command::Storage(StorageCommand::AllowSourceCompaction(
                frontiers,
            )))
            .await
    }
    pub async fn drop_source_timestamping(&mut self, id: GlobalId) {
        self.client
            .send(Command::Storage(StorageCommand::DropSourceTimestamping {
                id,
            }))
            .await
    }
    pub async fn advance_all_table_timestamps(&mut self, advance_to: mz_repr::Timestamp) {
        self.client
            .send(Command::Storage(StorageCommand::AdvanceAllLocalInputs {
                advance_to,
            }))
            .await
    }
}

impl<C: Client> Controller<C> {
    pub async fn recv(&mut self) -> Option<Response> {
        let response = self.client.recv().await;

        if let Some(response) = response.as_ref() {
            match response {
                Response::Compute(ComputeResponse::FrontierUppers(updates), instance) => {
                    for (id, changes) in updates.iter() {
                        self.compute_since_uppers
                            .get_mut(instance)
                            // TODO: determine if this is an error, or perhaps just a late
                            // response about a terminated instance.
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
            compaction: ConstraintPropagation::new(),
            temp_stashed_capabilities: Default::default(),
        }
    }
    /// Returns the source description for a given identifier.
    ///
    /// The response does not distinguish between an as yet uncreated source description,
    /// and one that has been created and then dropped (or dropped without creation).
    /// There is a distinction and the client is aware of it, and could plausibly return
    /// this information if we had a use for it.
    pub fn source_description_for(
        &self,
        id: GlobalId,
    ) -> Option<&(crate::sources::SourceDesc, Antichain<mz_repr::Timestamp>)> {
        self.source_descriptions.get(&id).unwrap_or(&None).as_ref()
    }

    pub fn since_uppers_in(
        &self,
        instance: ComputeInstanceId,
    ) -> (&SinceUpperMap, Option<&SinceUpperMap>) {
        (
            &self.storage_since_uppers,
            self.compute_since_uppers.get(&instance),
        )
    }

    /// Acquire a token that allows reads of `ids` at their least common read frontier.
    ///
    /// To pin a specific time the user can downgrade the `ReadCapability` the method returns.
    pub fn read_capability(
        &mut self,
        instance: ComputeInstanceId,
        ids: Vec<GlobalId>,
    ) -> Result<ReadCapability<mz_repr::Timestamp>, ControllerError> {
        use differential_dataflow::lattice::antichain_join;
        let mut frontier = Antichain::from_elem(<mz_repr::Timestamp>::minimum());
        let mut constrains = Vec::with_capacity(ids.len());
        for id in ids.iter() {
            if let Some((since, _upper)) = self.storage_since_uppers.get(id) {
                frontier = antichain_join(&frontier, &since);
                constrains.push((*id, None));
            } else {
                let (since, _upper) = self
                    .compute_since_uppers
                    .get(&instance)
                    .ok_or(ComputeError::InstanceMissing(instance))?
                    .get(id)
                    .ok_or(ComputeError::IdentifierMissing(*id))?;

                frontier = antichain_join(&frontier, &since);
                constrains.push((*id, Some(instance)));
            }
        }

        // This will prime the compaction computation, so that it does not downgrade when next run.
        Ok(ReadCapability::new(
            frontier,
            constrains,
            std::rc::Rc::clone(&self.compaction.ext_requirements),
        ))
    }

    /// Effects any allowed compaction, which may otherwise be deferred until this point.
    pub fn allow_compaction(&mut self) {
        let mut changes = ChangeBatch::default();
        self.compaction.step(&mut changes);
        for (((id, instance), frontier), diff) in changes.drain() {
            unimplemented!()
        }
    }
}

#[derive(Default)]
pub struct SinceUpperMap<T = mz_repr::Timestamp>
where
    T: Timestamp,
{
    since_uppers: std::collections::BTreeMap<GlobalId, (Antichain<T>, Antichain<T>)>,
}

impl<T: Timestamp + Lattice> SinceUpperMap<T> {
    fn insert(&mut self, id: GlobalId, since_upper: (Antichain<T>, Antichain<T>)) {
        self.since_uppers.insert(id, since_upper);
    }
    fn get(&self, id: &GlobalId) -> Option<(AntichainRef<T>, AntichainRef<T>)> {
        self.since_uppers
            .get(id)
            .map(|(since, upper)| (since.borrow(), upper.borrow()))
    }
    /// Sets `since` to `frontier` for `id`.
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
    /// Modifies `since` by `changes` for `id`.
    fn update_since_for(&mut self, id: GlobalId, changes: &ChangeBatch<T>) {
        if let Some((since, _upper)) = self.since_uppers.get_mut(&id) {
            // Apply `changes` to `upper`.
            let mut changes = changes.clone();
            for time in since.elements().iter() {
                changes.update(time.clone(), 1);
            }
            since.clear();
            for (time, count) in changes.drain() {
                assert_eq!(count, 1);
                since.insert(time);
            }
        } else {
            // No panic, to parallel `update_upper_for`, but perhaps we should error?
        }
    }
    /// Modifies `upper` by `changes` for `id`.
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

/// A global id and optional cluster identifier.
pub type ConstraintId = (GlobalId, Option<ComputeInstanceId>);
pub use read_capability::ReadCapability;
mod read_capability {

    use std::cell::RefCell;
    use std::rc::Rc;

    use timely::progress::{Antichain, ChangeBatch, Timestamp};

    use super::ConstraintId;

    pub struct ReadCapability<T: Timestamp> {
        frontier: Antichain<T>,
        constrains: Vec<ConstraintId>,
        to_post: Rc<RefCell<ChangeBatch<(ConstraintId, Vec<T>)>>>,
    }

    impl<T: Timestamp> ReadCapability<T> {
        pub(super) fn new(
            frontier: Antichain<T>,
            constrains: Vec<ConstraintId>,
            to_post: Rc<RefCell<ChangeBatch<(ConstraintId, Vec<T>)>>>,
        ) -> Self {
            to_post.borrow_mut().extend(
                constrains
                    .iter()
                    .map(|id| ((*id, frontier.clone().into()), 1)),
            );
            Self {
                frontier,
                constrains,
                to_post,
            }
        }

        pub fn downgrade(&mut self, frontier: Antichain<T>) {
            use timely::order::PartialOrder;
            if <_ as PartialOrder>::less_equal(&self.frontier, &frontier) {
                let mut to_post = self.to_post.borrow_mut();
                to_post.extend(
                    self.constrains
                        .iter()
                        .map(|id| ((*id, frontier.clone().into()), 1)),
                );
                to_post.extend(
                    self.constrains
                        .iter()
                        .map(|id| ((*id, self.frontier.clone().into()), -1)),
                );
                self.frontier = frontier;
            }
        }
    }

    impl<T: Timestamp> Clone for ReadCapability<T> {
        fn clone(&self) -> Self {
            Self::new(
                self.frontier.clone(),
                self.constrains.clone(),
                Rc::clone(&self.to_post),
            )
        }
    }

    impl<T: Timestamp> Drop for ReadCapability<T> {
        fn drop(&mut self) {
            let mut to_post = self.to_post.borrow_mut();
            to_post.extend(
                self.constrains
                    .iter()
                    .map(|id| ((*id, self.frontier.clone().into()), -1)),
            );
            self.frontier.clear();
        }
    }
}

use compaction::ConstraintPropagation;
mod compaction {

    use std::cell::RefCell;
    use std::rc::Rc;

    use timely::dataflow::ProbeHandle;
    use timely::progress::{Antichain, ChangeBatch, Timestamp};
    use timely::WorkerConfig;
    use timely::{communication::allocator::thread::Thread, worker::Worker};

    use differential_dataflow::input::InputSession;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::operators::Iterate;
    use differential_dataflow::operators::Join;
    use differential_dataflow::operators::Reduce;

    use super::ConstraintId;

    pub struct ConstraintPropagation<T: Timestamp + Lattice> {
        /// Externally supplied changes to apply.
        pub ext_requirements: Rc<RefCell<ChangeBatch<(ConstraintId, Vec<T>)>>>,
        pub ext_constraints: Rc<RefCell<ChangeBatch<(ConstraintId, ConstraintId)>>>,

        /// Single-threaded timely dataflow worker.
        worker: Worker<Thread>,

        /// Entry (a, b) means that since[a] >= since[b].
        constraints: InputSession<usize, (ConstraintId, ConstraintId), i64>,
        /// Entry `(a, frontier)` means `since[a] >= frontier`.
        requirements: InputSession<usize, (ConstraintId, Vec<T>), i64>,
        /// Output changes that need to be communicated onward.
        consequences: Rc<RefCell<ChangeBatch<(ConstraintId, Vec<T>)>>>,
        /// Probe to indicate when computation is complete.
        probe: ProbeHandle<usize>,
    }

    impl<T: Timestamp + Lattice> ConstraintPropagation<T> {
        pub fn new() -> Self {
            // The timely worker that will coordinate the execution.
            let mut worker = Worker::new(WorkerConfig::default(), Thread::new());
            // Inputs and probes for introduces and monitoring progress.
            let mut constraints = InputSession::<usize, (ConstraintId, ConstraintId), i64>::new();
            let mut requirements = InputSession::<usize, (ConstraintId, Vec<T>), i64>::new();
            let mut probe = ProbeHandle::new();
            // Shared buffer to extract output.
            let consequences = Rc::new(RefCell::new(ChangeBatch::new()));

            worker.dataflow(|dataflow| {
                let constraints = constraints.to_collection(dataflow);
                let requirements = requirements.to_collection(dataflow);
                let consequences = consequences.clone();

                requirements
                    .iterate(|inner| {
                        constraints
                            .enter(&inner.scope())
                            .join_map(&inner, |_dependent, other, frontier| {
                                (*other, frontier.clone())
                            })
                            .reduce(|_kep, input, output| {
                                let mut frontier = input[0].0.clone();
                                for index in 1..input.len() {
                                    frontier = antichain_join(&frontier[..], &input[index].0[..]);
                                }
                                output.push((frontier, 1));
                            })
                    })
                    .inspect_batch(move |_time, updates| {
                        consequences.borrow_mut().extend(
                            updates
                                .iter()
                                .map(|(id_frontier, _time, diff)| (id_frontier.clone(), *diff)),
                        );
                    })
                    .probe_with(&mut probe);
            });

            Self {
                ext_constraints: Default::default(),
                ext_requirements: Default::default(),
                worker,
                constraints,
                requirements,
                consequences,
                probe,
            }
        }
        /// Applies updates to `constraints` and `requirements` and populates `consequences`.
        pub fn step(&mut self, consequences_output: &mut ChangeBatch<(ConstraintId, Vec<T>)>) {
            // Introduce updates into the inputs.
            for (pair, diff) in self.ext_requirements.borrow_mut().drain() {
                self.requirements.update(pair, diff);
            }
            for (pair, diff) in self.ext_constraints.borrow_mut().drain() {
                self.constraints.update(pair, diff);
            }
            // Advance inputs
            let next_round = *self.constraints.time() + 1;
            self.constraints.advance_to(next_round);
            self.requirements.advance_to(next_round);
            while self.probe.less_than(&next_round) {
                self.worker.step();
            }
            // Capture observed changes.
            let mut borrow = self.consequences.borrow_mut();
            consequences_output.extend(borrow.drain());
        }
    }

    fn antichain_join<T: Timestamp + Lattice>(a: &[T], b: &[T]) -> Vec<T> {
        let mut upper = Antichain::new();
        for time1 in a.iter() {
            for time2 in b.iter() {
                upper.insert(time1.join(time2));
            }
        }
        upper.into()
    }
}
