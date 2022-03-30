// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
//! A canonical implementation of the
//! [`StorageController`][`mz_dataflow_types::client::controller::storage::StorageController`]
//! trait, that can depend on everything this crate depends on.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::path::PathBuf;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use uuid::Uuid;

use mz_dataflow_types::client::controller::storage::{
    CollectionState, StorageController, StorageControllerState, StorageError,
};
use mz_dataflow_types::client::controller::ReadPolicy;
use mz_dataflow_types::client::LinearizedTimestampBindingFeedback;
use mz_dataflow_types::client::{
    CreateSourceCommand, StorageClient, StorageCommand, StorageResponse, TimestampBindingFeedback,
};
use mz_dataflow_types::sources::MzOffset;
use mz_dataflow_types::sources::SourceDesc;
use mz_dataflow_types::Update;
use mz_expr::{GlobalId, PartitionId};
use mz_stash::Stash;

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T> {
    state: StorageControllerState<T>,
    /// lalala
    pub internal_responses: tokio::sync::mpsc::UnboundedReceiver<StorageResponse<T>>,
    responder: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,
}

#[async_trait]
impl<T> StorageController for Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64>,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    type Timestamp = T;

    fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, StorageError> {
        self.state
            .collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collection_mut(&mut self, id: GlobalId) -> Result<&mut CollectionState<T>, StorageError> {
        self.state
            .collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    async fn create_sources(
        &mut self,
        mut bindings: Vec<(GlobalId, (SourceDesc, Antichain<T>))>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        // 1. create a dropped source identifier, or
        // 2. create an existing source identifier with a new description.
        // Make sure to check for errors within `bindings` as well.
        bindings.sort_by_key(|(id, _)| *id);
        bindings.dedup();
        for pos in 1..bindings.len() {
            if bindings[pos - 1].0 == bindings[pos].0 {
                return Err(StorageError::SourceIdReused(bindings[pos].0));
            }
        }
        for (id, description_since) in bindings.iter() {
            if let Ok(collection) = self.collection(*id) {
                if &collection.description != description_since {
                    return Err(StorageError::SourceIdReused(*id));
                }
            }
        }

        let mut dataflow_commands = vec![];

        // Install collection state for each bound source.
        for (id, (desc, since)) in bindings {
            let ts_binding_collection = self
                .state
                .stash
                .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))?;

            let mut ts_bindings = Vec::new();
            let mut last_bindings: HashMap<_, MzOffset> = HashMap::new();
            for ((pid, _), time, diff) in self.state.stash.iter(ts_binding_collection)? {
                let prev_offset = last_bindings.entry(pid.clone()).or_default();
                ts_bindings.push((
                    pid,
                    T::try_from(time).expect("timestamp overflowed i64"),
                    MzOffset {
                        offset: prev_offset.offset + diff,
                    },
                ));
                prev_offset.offset += diff;
            }

            let collection_state = CollectionState::new(desc.clone(), since.clone(), last_bindings);
            self.state.collections.insert(id, collection_state);

            let command = CreateSourceCommand {
                id,
                desc,
                since,
                ts_bindings,
            };

            dataflow_commands.push(command);
        }

        self.state
            .client
            .send(StorageCommand::CreateSources(dataflow_commands))
            .await
            .expect("Storage command failed; unrecoverable");

        Ok(())
    }

    async fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_ids(identifiers.iter().cloned())?;
        let policies = identifiers
            .into_iter()
            .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();
        self.set_read_policy(policies).await?;
        Ok(())
    }

    async fn table_insert(
        &mut self,
        id: GlobalId,
        updates: Vec<Update<T>>,
    ) -> Result<(), StorageError> {
        self.state
            .client
            .send(StorageCommand::Insert { id, updates })
            .await
            .map_err(StorageError::from)
    }

    async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), StorageError> {
        self.state
            .client
            .send(StorageCommand::DurabilityFrontierUpdates(updates))
            .await
            .map_err(StorageError::from)
    }

    async fn advance_all_table_timestamps(&mut self, advance_to: T) -> Result<(), StorageError> {
        self.state
            .client
            .send(StorageCommand::AdvanceAllLocalInputs { advance_to })
            .await
            .map_err(StorageError::from)
    }

    async fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), StorageError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(collection) = self.collection_mut(id) {
                let mut new_read_capability = policy.frontier(collection.write_frontier.frontier());

                if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                    if !update.is_empty() {
                        read_capability_changes.insert(id, update);
                    }
                }

                collection.read_policy = policy;
            } else {
                tracing::error!("Reference to unregistered id: {:?}", id);
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await?;
        }
        Ok(())
    }

    /// Persist timestamp bindings updates received from ingestion workers
    async fn persist_timestamp_bindings(
        &mut self,
        feedback: &TimestampBindingFeedback<T>,
    ) -> Result<(), StorageError> {
        for (id, bindings) in &feedback.bindings {
            let ts_binding_collection = self
                .state
                .stash
                .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))?;

            let upper = self.state.stash.upper(ts_binding_collection)?;

            let collection_state = self.collection_mut(*id).expect("missing source id");

            // Here we differentialize the bindings we got from workers
            // Timestamp bindings as represented as a TVC whose data, time, and diff types
            // correspond to PartitionId, Time, MzOffset.
            //
            // For example, suppose we read from a kafka topic with two partitions and at timestamp
            // 1 we've read up to offsets 5 and 10 for each partition and at timestamp 2 we read up
            // to offsets 7 and 17. The differentialized collection will contain the following
            // updates:
            //
            // (PartitionId::Kafka(1), 1, +5)
            // (PartitionId::Kafka(2), 1, +10)
            // (PartitionId::Kafka(1), 2, +2)  <-- This is how much the offset changed, 7 - 5 = 2
            // (PartitionId::Kafka(2), 2, +7)
            //
            // This representation allows us to compact timestamp bindings simply by adding
            // together offsets and collapsing their timestamps. For example, if we were to compact
            // through timestamp 2 the collection would contain the following updates:
            //
            // (PartitionId::Kafka(1), 2, +7)  <-- 5 + 2 = 7
            // (PartitionId::Kafka(2), 2, +17) <-- 10 + 7 = 17
            let mut bindings = bindings.clone();
            // Sort the bindings by (pid, ts, offset)
            bindings.sort_unstable();
            let mut updates = vec![];
            for (pid, ts, offset) in bindings {
                let prev_offset = collection_state
                    .last_reported_ts_bindings
                    .entry(pid.clone())
                    .or_default();

                let ts = ts.try_into().expect("timestamp overflowed i64");
                let update = ((pid, ()), ts, offset.offset - prev_offset.offset);

                prev_offset.offset = offset.offset;
                // TODO(petrosagg): refactor timestamp binding handling so that we never enter a
                // situation where the previous bindings are re-reported by workers
                if upper.less_equal(&ts) {
                    updates.push(update);
                }
            }
            self.state
                .stash
                .update_many(ts_binding_collection, updates)?;
        }

        let mut durability_updates = vec![];
        let mut seals = vec![];
        for (id, _changes) in &feedback.changes {
            let ts_binding_collection = self
                .state
                .stash
                .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))?;
            let collection = self.collection_mut(*id).expect("missing source id");
            let write_frontier = collection.write_frontier.frontier().to_owned();
            let seal_frontier = Antichain::from_iter(
                write_frontier
                    .as_option()
                    .map(|ts| ts.clone().try_into().expect("negative timestamp")),
            );
            let upper = self.state.stash.upper(ts_binding_collection)?;
            // TODO(petrosagg): This guard should go away by ensuring storage workers never re-send
            // the bindings and frontiers they were initialized with
            if PartialOrder::less_than(&upper, &seal_frontier) {
                seals.push((ts_binding_collection, seal_frontier));
            }
            durability_updates.push((*id, write_frontier));
        }
        self.state.stash.seal_batch(&seals)?;

        self.update_durability_frontiers(durability_updates).await?;

        Ok(())
    }

    async fn update_write_frontiers(
        &mut self,
        updates: &[(GlobalId, ChangeBatch<T>)],
    ) -> Result<(), StorageError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, changes) in updates.iter() {
            let collection = self
                .collection_mut(*id)
                .expect("Reference to absent collection");

            collection
                .write_frontier
                .update_iter(changes.clone().drain());

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.frontier());
            if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                // TODO: reuse change batch above?
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                if !update.is_empty() {
                    read_capability_changes.insert(*id, update);
                }
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await?;
        }
        Ok(())
    }

    async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) -> Result<(), StorageError> {
        // Location to record consequences that we need to act on.
        let mut storage_net = Vec::default();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);
                storage_net.push((key, update));
            } else {
                // This is confusing and we should probably error.
                panic!("Unknown collection identifier {}", key);
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands.
        let mut compaction_commands = Vec::new();
        let mut stash_compactions = vec![];
        let mut stash_consolidations = vec![];
        for (id, change) in storage_net.iter_mut() {
            if !change.is_empty() {
                let frontier = self
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();

                let ts_binding_collection = self
                    .state
                    .stash
                    .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))?;

                let mut since = self.state.stash.since(ts_binding_collection)?;
                since.extend(
                    frontier
                        .iter()
                        .map(|t| t.clone().try_into().expect("timestamp overflowed i64")),
                );
                stash_compactions.push((ts_binding_collection, since));
                stash_consolidations.push(ts_binding_collection);
                compaction_commands.push((*id, frontier));
            }
        }
        self.state.stash.compact_batch(&stash_compactions)?;
        self.state.stash.consolidate_batch(&stash_consolidations)?;

        if !compaction_commands.is_empty() {
            self.state
                .client
                .send(StorageCommand::AllowCompaction(compaction_commands))
                .await
                .expect(
                    "Failed to send storage command; aborting as compute instance state corrupted",
                );
        }
        Ok(())
    }

    /// "Linearize" the listed sources.
    ///
    /// If these sources are valid and "linearizable", then the response
    /// will respond with timestamps that are guaranteed to be up-to-date
    /// with the max offset found at the time of the command issuance.
    ///
    /// Note: "linearizable" in this context may not represent
    /// true linearizability in all cases.
    async fn linearize_sources(
        &mut self,
        peek_id: Uuid,
        source_ids: Vec<GlobalId>,
    ) -> Result<(), anyhow::Error> {
        dbg!(&source_ids);
        for id in source_ids {
            let desc = self.collection(id).unwrap().description.0.clone();
            let sender = self.responder.clone();
            let ts_binding_collection = self
                .state
                .stash
                .collection::<PartitionId, ()>(&format!("timestamp-bindings-{id}"))
                .unwrap();
            let stash = self.state.stash.clone();
            tokio::spawn(async move {
                match desc.give_answer().await {
                    SourceLinearizationResult::Answer(answer) => {
                        let mut i = tokio::time::interval(std::time::Duration::from_secs(1));
                        let t = loop {
                            let mut last_bindings: HashMap<_, (T, MzOffset)> = HashMap::new();
                            for ((pid, _), time, diff) in stash.iter(ts_binding_collection).unwrap()
                            {
                                let entry = last_bindings
                                    .entry(pid.clone())
                                    .or_insert((T::minimum(), MzOffset::default()));
                                let time = T::try_from(time).expect("timestamp overflowed i64");
                                entry.1.offset += diff;
                                entry.0 = std::cmp::max(time, entry.0.clone());
                            }

                            dbg!(&last_bindings);

                            let mut out = vec![];
                            for (pid, offset) in &answer {
                                if let Some(p) = last_bindings.get(&pid) {
                                    if &p.1 <= offset {
                                        out.push(p.0.clone());
                                    }
                                }
                            }

                            if out.len() == answer.len() {
                                break out.iter().max().unwrap().clone();
                            }
                            i.tick().await;
                        };

                        sender.send(StorageResponse::LinearizedTimestamps(
                            LinearizedTimestampBindingFeedback {
                                timestamp: t,
                                peek_id,
                            },
                        ))
                    }
                    .unwrap(),
                    _ => {}
                }
            });
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<StorageResponse<Self::Timestamp>>, anyhow::Error> {
        tokio::select! {
            biased;
            thing = self.internal_responses.recv() => {
                Ok(thing)
            },
            thing = self.state.client.recv() => {
                thing
            }
        }
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64>,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
{
    /// Create a new storage controller from a client it should wrap.
    pub fn new(client: Box<dyn StorageClient<T>>, state_dir: PathBuf) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            state: StorageControllerState::new(client, state_dir),
            internal_responses: rx,
            responder: tx,
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }
}

/// lalala
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceLinearizationResult {
    /// lalala
    NotAvailable,
    /// lalala
    NotImplemented,
    /// lalala
    Answer(Vec<(mz_expr::PartitionId, mz_dataflow_types::sources::MzOffset)>),
}

/// lalala
#[async_trait]
pub trait SourceTimestampLinearizer {
    /// lalal
    async fn give_answer(&self) -> SourceLinearizationResult;
}

#[async_trait]
impl SourceTimestampLinearizer for mz_dataflow_types::sources::SourceDesc {
    async fn give_answer(&self) -> SourceLinearizationResult {
        use mz_dataflow_types::sources::ExternalSourceConnector::*;
        use mz_dataflow_types::sources::SourceConnector::*;
        use std::sync::Arc;
        use SourceLinearizationResult::*;
        match &self.connector {
            Local { .. } => SourceLinearizationResult::NotImplemented,
            External { connector, .. } => match connector {
                Kafka(connector) => {
                    let connector2 = connector.clone();
                    let consumer = Arc::new(
                        mz_ore::task::spawn_blocking(
                            || "lalala",
                            move || {
                                crate::source::create_consumer(
                                    "lalala",
                                    mz_kafka_util::client::MzClientContext,
                                    &connector2,
                                )
                            },
                        )
                        .await
                        .unwrap(),
                    );

                    let offsets = crate::source::fetch_max_offsets(consumer, &connector.topic)
                        .await
                        .unwrap();
                    Answer(offsets)
                }
                Kinesis(_connector) => NotAvailable,
                File(_connector) => NotImplemented,
                _ => todo!("implement others"),
            },
        }
    }
}
