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
    history: crate::client::ComputeCommandHistory<T>,
    /// Cursor to use among the replicas
    cursor: usize,
    /// Most recent count of the volume of unpacked commands (e.g. dataflows in `CreateDataflows`).
    last_command_count: usize,
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
            history: Default::default(),
            cursor: 0,
            last_command_count: 0,
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
        self.hydrate_replica(self.replicas.len() - 1).await;
    }

    /// Pipes a command stream at the indicated replica, introducing new dataflow identifiers.
    async fn hydrate_replica(&mut self, replica_index: usize) {
        // Zero out frontiers maintained by this replica.
        for (_id, (_, frontiers)) in self.uppers.iter_mut() {
            frontiers[replica_index] = timely::progress::frontier::MutableAntichain::new();
            frontiers[replica_index].update_iter(Some((T::minimum(), 1)));
        }
        // Take this opportunity to clean up the history we should present.
        self.last_command_count = self.history.reduce(&self.peeks);

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

        // If we have reached a point that justifies history reduction, do that.
        if self.history.len() > 2 * self.last_command_count {
            self.last_command_count = self.history.reduce(&self.peeks);
        }

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

            // Errors are suppressed by this client, which awaits a reconnection in `recv` and
            // will rehydrate the client when that happens.
            let _ = self.replicas[index].send(command).await;
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
                    self.hydrate_replica(replica_index).await;
                }

                clean_recv = errored_replica.is_none();
            }
            // Indicate completion of the communication.
            Ok(None)
        }
    }
}
