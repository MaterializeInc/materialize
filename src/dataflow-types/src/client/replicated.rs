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

use crate::client::Peek;
use mz_repr::GlobalId;

use super::{ComputeClient, GenericClient};
use super::{ComputeCommand, ComputeResponse};

/// A client backed by multiple replicas.
#[derive(Debug)]
pub struct ActiveReplication<C, T> {
    /// The replicas themselves.
    replicas: HashMap<String, C>,
    /// Outstanding peek identifiers, to guide responses (and which to suppress).
    peeks: HashSet<uuid::Uuid>,
    /// Reported frontier of each in-progress tail.
    tails: HashMap<GlobalId, Antichain<T>>,
    /// Frontier information, both unioned across all replicas and from each individual replica.
    uppers: HashMap<GlobalId, (Antichain<T>, HashMap<String, MutableAntichain<T>>)>,
    /// The command history, used when introducing new replicas or restarting existing replicas.
    history: crate::client::ComputeCommandHistory<T>,
    /// Most recent count of the volume of unpacked commands (e.g. dataflows in `CreateDataflows`).
    last_command_count: usize,
}

impl<C, T> Default for ActiveReplication<C, T> {
    fn default() -> Self {
        Self {
            replicas: Default::default(),
            peeks: Default::default(),
            tails: Default::default(),
            uppers: Default::default(),
            history: Default::default(),
            last_command_count: 0,
        }
    }
}

impl<C: ComputeClient<T>, T> ActiveReplication<C, T>
where
    T: timely::progress::Timestamp,
{
    /// Introduce a new replica, and catch it up to the commands of other replicas.
    ///
    /// It is not yet clear under which circumstances a replica can be removed.
    pub async fn add_replica(&mut self, identifier: String, client: C) {
        for (_, frontiers) in self.uppers.values_mut() {
            frontiers.insert(identifier.clone(), {
                let mut frontier = timely::progress::frontier::MutableAntichain::new();
                frontier.update_iter(Some((T::minimum(), 1)));
                frontier
            });
        }
        self.replicas.insert(identifier.clone(), client);
        self.hydrate_replica(&identifier).await;
    }

    pub fn get_replica_identifiers(&self) -> impl Iterator<Item = &String> {
        self.replicas.keys()
    }

    /// Remove a replica by its identifier.
    pub fn remove_replica(&mut self, id: &str) {
        self.replicas.remove(id);
        for (_frontier, frontiers) in self.uppers.iter_mut() {
            frontiers.1.remove(id);
        }
    }

    /// Pipes a command stream at the indicated replica, introducing new dataflow identifiers.
    async fn hydrate_replica(&mut self, replica_id: &str) {
        // Zero out frontiers maintained by this replica.
        for (_id, (_, frontiers)) in self.uppers.iter_mut() {
            *frontiers.get_mut(replica_id).unwrap() =
                timely::progress::frontier::MutableAntichain::new();
            frontiers
                .get_mut(replica_id)
                .unwrap()
                .update_iter(Some((T::minimum(), 1)));
        }
        // Take this opportunity to clean up the history we should present.
        self.last_command_count = self.history.reduce(&self.peeks);

        // Replay the commands at the client, creating new dataflow identifiers.
        let client = self.replicas.get_mut(replica_id).unwrap();
        for command in self.history.iter() {
            let mut command = command.clone();
            // Replace dataflow identifiers with new unique ids.
            if let ComputeCommand::CreateDataflows(dataflows) = &mut command {
                for dataflow in dataflows.iter_mut() {
                    dataflow.id = uuid::Uuid::new_v4();
                }
            }
            // Suppress errors, as we will observe them in `recv` and react there.
            let _ = client.send(command).await;
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
        if let ComputeCommand::Peek(Peek { uuid, .. }) = &cmd {
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
                .keys()
                .map(|id| {
                    let mut frontier = timely::progress::frontier::MutableAntichain::new();
                    frontier.update_iter(Some((T::minimum(), 1)));
                    (id.clone(), frontier)
                })
                .collect();
            let previous = self.uppers.insert(id, (frontier, frontiers));
            assert!(previous.is_none());
        }
        for id in cease.into_iter() {
            let previous = self.uppers.remove(&id);
            assert!(previous.is_some());
        }

        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());

        // If we have reached a point that justifies history reduction, do that.
        if self.history.len() > 2 * self.last_command_count {
            self.last_command_count = self.history.reduce(&self.peeks);
        }

        // Clone the command for each active replica.
        for (_id, replica) in self.replicas.iter_mut() {
            let mut command = cmd.clone();
            // Replace dataflow identifiers with new unique ids.
            if let ComputeCommand::CreateDataflows(dataflows) = &mut command {
                for dataflow in dataflows.iter_mut() {
                    dataflow.id = uuid::Uuid::new_v4();
                }
            }

            // Errors are suppressed by this client, which awaits a reconnection in `recv` and
            // will rehydrate the client when that happens.
            let _ = replica.send(command).await;
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
                let mut stream: tokio_stream::StreamMap<_, _> = self
                    .replicas
                    .iter_mut()
                    .map(|(id, shard)| (id.clone(), shard.as_stream()))
                    .collect();

                use futures::StreamExt;
                while let Some((replica_id, message)) = stream.next().await {
                    match message {
                        Ok(ComputeResponse::PeekResponse(uuid, response)) => {
                            // If this is the first response, forward it; otherwise do not.
                            // TODO: we could collect the other responses to assert equivalence?
                            // Trades resources (memory) for reassurances; idk which is best.
                            if self.peeks.remove(&uuid) {
                                return Ok(Some(ComputeResponse::PeekResponse(uuid, response)));
                            }
                        }
                        Ok(ComputeResponse::FrontierUppers(mut list)) => {
                            for (id, changes) in list.iter_mut() {
                                if let Some((frontier, frontiers)) = self.uppers.get_mut(id) {
                                    // Apply changes to replica `replica_id`
                                    frontiers
                                        .get_mut(&replica_id)
                                        .unwrap()
                                        .update_iter(changes.drain());
                                    // We can swap `frontier` into `changes, negated, and then use that to repopulate `frontier`.
                                    // Working
                                    changes.extend(frontier.iter().map(|t| (t.clone(), -1)));
                                    frontier.clear();
                                    for (time1, _neg_one) in changes.iter() {
                                        for time2 in frontiers[&replica_id].frontier().iter() {
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
                        Ok(ComputeResponse::TailResponse(id, response)) => {
                            use crate::{TailBatch, TailResponse};
                            match response {
                                TailResponse::Batch(TailBatch {
                                    lower: _,
                                    upper,
                                    mut updates,
                                }) => {
                                    // It is sufficient to compare `upper` against the last reported frontier for `id`,
                                    // and if `upper` is not less or equal to that frontier, some progress has happened.
                                    // If so, we retain only the updates greater or equal to that last reported frontier,
                                    // and announce a batch from that frontier to its join with `upper`.

                                    // Ensure that we have a recorded frontier ready to go.
                                    let entry = self
                                        .tails
                                        .entry(id)
                                        .or_insert_with(|| Antichain::from_elem(T::minimum()));
                                    // If the upper frontier has changed, we have a statement to make.
                                    // This happens if there is any element of `entry` not greater or
                                    // equal to some element of `upper`.
                                    use differential_dataflow::lattice::Lattice;
                                    let new_upper = entry.join(&upper);
                                    if &new_upper != entry {
                                        let new_lower = entry.clone();
                                        entry.clone_from(&new_upper);
                                        updates.retain(|(time, _data, _diff)| {
                                            new_lower.less_equal(time)
                                        });
                                        return Ok(Some(ComputeResponse::TailResponse(
                                            id,
                                            TailResponse::Batch(TailBatch {
                                                lower: new_lower,
                                                upper: new_upper,
                                                updates,
                                            }),
                                        )));
                                    }
                                }
                                TailResponse::DroppedAt(frontier) => {
                                    // Introduce a new terminal frontier to suppress all future responses.
                                    // We cannot simply remove the entry, as we currently create new entries in response
                                    // to observed responses; if we pre-load the entries in response to commands we can
                                    // clean up the state here.
                                    self.tails.insert(id, Antichain::new());
                                    return Ok(Some(ComputeResponse::TailResponse(
                                        id,
                                        TailResponse::DroppedAt(frontier),
                                    )));
                                }
                            }
                        }
                        Err(_error) => {
                            errored_replica = Some(replica_id);
                            break;
                        }
                    }
                }
                drop(stream);

                if let Some(replica_id) = &errored_replica {
                    tracing::warn!("Rehydrating replica {:?}", replica_id);
                    self.hydrate_replica(replica_id).await;
                }

                clean_recv = errored_replica.is_none();
            }
            // Indicate completion of the communication.
            Ok(None)
        }
    }
}
