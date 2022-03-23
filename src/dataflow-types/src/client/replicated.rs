// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client backed by multiple replicas.
//!
//! This client accepts commands and responds as would a correctly implemented client.
//! Its implementation is wrapped around clients that may fail at any point, and restart.
//! To accommodate this, it records the commands it accepts, and should a client restart
//! the commands are replayed at it, with some modification. As the clients respond, the
//! wrapper client tracks the responses and ensures that they are "logically deduplicated",
//! so that the receiver need not be aware of the replication and restarting.
//!
//! This tactic requires that dataflows be restartable, which they generally are not, due
//! to allowed compaction of their source data. This client must correctly observe commands
//! that allow for compaction of its assets, and only attempt to rebuild them as of those
//! compacted frontiers, as the underlying resources to rebuild them any earlier may not
//! exist any longer.

use std::collections::{HashMap, HashSet};

use timely::progress::{frontier::MutableAntichain, Antichain};

use mz_expr::GlobalId;

use super::{ComputeClient, GenericClient};
use super::{ComputeCommand, ComputeResponse};

/// A client backed by multiple replicas.
#[derive(Debug)]
pub struct ActiveReplication<C, T> {
    /// The replicas themselves.
    replicas: Vec<C>,
    /// Outstanding peek identifiers, to guide responses (and which to suppress).
    peeks: HashSet<uuid::Uuid>,
    /// Frontier information, both from each replica and unioned across all replicas.
    uppers: HashMap<GlobalId, (Antichain<T>, Vec<MutableAntichain<T>>)>,
    /// The command history, used when introducing new replicas or restarting existing replicas.
    ///
    /// In principle, this history can be thinned down, either periodically,
    /// or pro-actively when new replicas are introduced.
    history: Vec<ComputeCommand<T>>,
    /// Cursor to use among the replicas
    cursor: usize,
}

impl<C: ComputeClient<T>, T> ActiveReplication<C, T>
where
    T: timely::progress::Timestamp,
{
    /// Creates a new active replica client from a list of active replicas.
    pub fn new(replicas: Vec<C>) -> Self {
        Self {
            replicas,
            peeks: HashSet::new(),
            uppers: HashMap::new(),
            history: Vec::new(),
            cursor: 0,
        }
    }

    /// Introduce a new replica, and catch it up to the commands of other replicas.
    ///
    /// It is not yet clear under which circumstances a replica can be removed.
    pub async fn add_replica(&mut self, client: C) {
        for (_, frontiers) in self.uppers.values_mut() {
            frontiers.push({
                let mut frontier = timely::progress::frontier::MutableAntichain::new();
                frontier.update_iter(Some((T::minimum(), 1)));
                frontier
            })
        }
        self.replicas.push(client);
        self.hydrate_replica(self.replicas.len() - 1).await.unwrap();
    }

    /// Pipes a command stream at the indicated replica, introducing new dataflow identifiers.
    async fn hydrate_replica(&mut self, replica_index: usize) -> Result<(), anyhow::Error> {
        // Zero out frontiers maintained by this replica.
        for (_id, (_, frontiers)) in self.uppers.iter_mut() {
            frontiers[replica_index] = timely::progress::frontier::MutableAntichain::new();
            frontiers[replica_index].update_iter(Some((T::minimum(), 1)));
        }
        // Take this opportunity to clean up the history we should present.
        self.reduce_history();
        // Replay the commands at the client, creating new dataflow identifiers.
        let client = self.replicas.get_mut(replica_index).unwrap();
        for command in self.history.iter() {
            let mut command = command.clone();
            // Replace dataflow identifiers with new unique ids.
            if let ComputeCommand::CreateDataflows(dataflows) = &mut command {
                for dataflow in dataflows.iter_mut() {
                    dataflow.id = uuid::Uuid::new_v4();
                }
            }
            // TODO(mcsherry): This will panic if we fail on rehydration.
            // Ideally we don't do that, or figure out what we should do (try again?).
            client.send(command).await?;
        }

        Ok(())
    }

    /// Reduces `self.history` to a minimal form.
    ///
    /// This action not only simplifies the issued history, but importantly reduces the instructions
    /// to only reference inputs from times that are still certain to be valid. Commands that allow
    /// compaction of a collection also remove certainty that the inputs will be available for times
    /// not greater or equal to that compaction frontier.
    fn reduce_history(&mut self) {
        // First determine what the final compacted frontiers will be for each collection.
        // These will determine for each collection whether the command that creates it is required,
        // and if required what `as_of` frontier should be used for its updated command.
        let mut final_frontiers = std::collections::BTreeMap::new();
        let mut live_dataflows = Vec::new();
        let mut live_peeks = Vec::new();
        let mut live_cancels = std::collections::BTreeSet::new();

        let mut create_command = None;
        let mut drop_command = None;

        for command in self.history.drain(..) {
            match command {
                create @ ComputeCommand::CreateInstance(_) => {
                    // We should be able to handle this, should this client need to be restartable.
                    assert!(create_command.is_none());
                    create_command = Some(create);
                }
                cmd @ ComputeCommand::DropInstance => {
                    assert!(drop_command.is_none());
                    drop_command = Some(cmd);
                }
                ComputeCommand::CreateDataflows(dataflows) => {
                    live_dataflows.extend(dataflows);
                }
                ComputeCommand::AllowCompaction(frontiers) => {
                    for (id, frontier) in frontiers {
                        final_frontiers.insert(id, frontier.clone());
                    }
                }
                peek @ ComputeCommand::Peek { .. } => {
                    // We could pre-filter here, but seems hard to access `uuid`
                    // and take ownership of `peek` at the same time.
                    live_peeks.push(peek);
                }
                ComputeCommand::CancelPeeks { mut uuids } => {
                    uuids.retain(|uuid| self.peeks.contains(uuid));
                    live_cancels.extend(uuids);
                }
            }
        }

        // Discard dataflows whose outputs have all been allowed to compact away.
        live_dataflows.retain(|dataflow| {
            // If any index or sink has not been compacted to the empty frontier, it remains active.
            // Importantly, an `id` may have not been compacted, and not appear in `final_frontiers`;
            // this is fine and normal, and is not evidence that the collection is not in use.
            let index_active = dataflow
                .index_exports
                .iter()
                .any(|(id, _, _)| final_frontiers.get(id) != Some(Antichain::new()).as_ref());
            let sink_active = dataflow
                .sink_exports
                .iter()
                .any(|(id, _)| final_frontiers.get(id) != Some(Antichain::new()).as_ref());

            let retain = index_active || sink_active;

            // If we are going to drop the dataflow, we should remove the frontier information so that we
            // do not instruct anyone to compact a frontier they have not heard of.
            if !retain {
                for (id, _, _) in dataflow.index_exports.iter() {
                    final_frontiers.remove(id);
                }
                for (id, _) in dataflow.sink_exports.iter() {
                    final_frontiers.remove(id);
                }
            }

            retain
        });

        // Update dataflow `as_of` frontiers to the least of the final frontiers of their outputs.
        for dataflow in live_dataflows.iter_mut() {
            let mut same_as_of = false;
            let mut as_of = Antichain::new();
            for (id, _, _) in dataflow.index_exports.iter() {
                if let Some(frontier) = final_frontiers.get(id) {
                    as_of.extend(frontier.clone());
                } else {
                    same_as_of = true;
                }
            }
            for (id, _) in dataflow.sink_exports.iter() {
                if let Some(frontier) = final_frontiers.get(id) {
                    as_of.extend(frontier.clone());
                } else {
                    same_as_of = true;
                }
            }
            if !same_as_of {
                dataflow.as_of = Some(as_of);
            }
        }

        // Retain only those peeks that have not yet been processed.
        live_peeks.retain(|peek| {
            if let ComputeCommand::Peek { uuid, .. } = peek {
                self.peeks.contains(uuid)
            } else {
                unreachable!()
            }
        });

        // Reconstitute the commands as a compact history.
        self.history.push(create_command.unwrap());
        self.history
            .push(ComputeCommand::CreateDataflows(live_dataflows));
        self.history.push(ComputeCommand::AllowCompaction(
            final_frontiers.into_iter().collect(),
        ));
        self.history.extend(live_peeks);
        self.history.push(ComputeCommand::CancelPeeks {
            uuids: live_cancels,
        });
        if let Some(drop_command) = drop_command {
            self.history.push(drop_command);
        }
    }
}

#[async_trait::async_trait]
impl<C: ComputeClient<T>, T> GenericClient<ComputeCommand<T>, ComputeResponse<T>>
    for ActiveReplication<C, T>
where
    T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice + std::fmt::Debug,
{
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        // Register an interest in the peek.
        if let ComputeCommand::Peek { uuid, .. } = &cmd {
            self.peeks.insert(*uuid);
        }

        // Initialize any necessary frontier tracking.
        let mut start = Vec::new();
        let mut cease = Vec::new();
        cmd.frontier_tracking(&mut start, &mut cease);
        for id in start.into_iter() {
            let frontier = timely::progress::Antichain::from_elem(T::minimum());
            let frontiers = self
                .replicas
                .iter()
                .map(|_| {
                    let mut frontier = timely::progress::frontier::MutableAntichain::new();
                    frontier.update_iter(Some((T::minimum(), 1)));
                    frontier
                })
                .collect::<Vec<_>>();
            let previous = self.uppers.insert(id, (frontier, frontiers));
            assert!(previous.is_none());
        }
        for id in cease.into_iter() {
            let previous = self.uppers.remove(&id);
            assert!(previous.is_some());
        }

        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());

        // Clone the command for each active replica.
        let replicas_len = self.replicas.len();
        for index in 0..replicas_len {
            // for (index, client) in self.replicas.iter_mut().enumerate() {
            let mut command = cmd.clone();
            // Replace dataflow identifiers with new unique ids.
            if let ComputeCommand::CreateDataflows(dataflows) = &mut command {
                for dataflow in dataflows.iter_mut() {
                    dataflow.id = uuid::Uuid::new_v4();
                }
            }
            let mut result = self.replicas[index].send(command).await;
            while result.is_err() {
                result = self.hydrate_replica(index).await;
            }
        }

        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
        if self.replicas.is_empty() {
            // We want to communicate that the result is not ready
            futures::future::pending().await
        } else {
            // We may need to iterate, if a replica needs rehydration.
            let mut clean_recv = false;
            while !clean_recv {
                let mut errored_replica = None;

                // Receive responses from any of the replicas, and take appropriate action.
                self.cursor = (self.cursor + 1) % self.replicas.len();
                let (head, tail) = self.replicas.split_at_mut(self.cursor);
                let mut stream: tokio_stream::StreamMap<_, _> = tail
                    .iter_mut()
                    .chain(head.iter_mut())
                    .map(|shard| shard.as_stream())
                    .enumerate()
                    .collect();

                use futures::StreamExt;
                while let Some((replica_id, message)) = stream.next().await {
                    match message {
                        Ok(ComputeResponse::PeekResponse(uuid, response)) => {
                            // If this is the first response, forward it; otherwise do not.
                            // TODO: we could collect the other responses to assert equivalance?
                            // Trades resources (memory) for reassurances; idk which is best.
                            if self.peeks.remove(&uuid) {
                                return Ok(Some(ComputeResponse::PeekResponse(uuid, response)));
                            }
                        }
                        Ok(ComputeResponse::FrontierUppers(mut list)) => {
                            for (id, changes) in list.iter_mut() {
                                if let Some((frontier, frontiers)) = self.uppers.get_mut(id) {
                                    // Apply changes to replica `replica_id`
                                    frontiers[replica_id].update_iter(changes.drain());
                                    // We can swap `frontier` into `changes, negated, and then use that to repopulate `frontier`.
                                    // Working
                                    changes.extend(frontier.iter().map(|t| (t.clone(), -1)));
                                    frontier.clear();
                                    for (time1, _neg_one) in changes.iter() {
                                        for time2 in frontiers[replica_id].frontier().iter() {
                                            frontier.insert(time1.join(time2));
                                        }
                                    }
                                    changes.extend(frontier.iter().map(|t| (t.clone(), 1)));
                                    changes.compact();
                                }
                            }
                            if !list.is_empty() {
                                return Ok(Some(ComputeResponse::FrontierUppers(list)));
                            }
                        }
                        Ok(_message) => {
                            unimplemented!("TAIL not yet implemented for replication");
                        }
                        Err(_error) => {
                            errored_replica = Some(replica_id);
                            break;
                        }
                    }
                }
                drop(stream);

                if let Some(replica_index) = errored_replica {
                    tracing::warn!("Rehydrating replica {:?}", replica_index);
                    self.hydrate_replica(replica_index).await.unwrap();
                }

                clean_recv = errored_replica.is_none();
            }
            // Indicate completion of the communication.
            Ok(None)
        }
    }
}
