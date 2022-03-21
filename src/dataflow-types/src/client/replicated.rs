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
        self.thin_history();
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

    /// Thins out `self.history`.
    ///
    /// This thinning is primarily by peeks that are no longer outstanding, as well as
    /// consequences thereof (e.g. irrelevance of dataflows serving only those peeks).
    fn thin_history(&mut self) {
        // TODO: This could be much more aggressively thinned, down to just the dataflows
        // that need to be installed, and none of the dataflows that have been dropped.
        self.history.retain(|cmd| {
            match cmd {
                ComputeCommand::Peek { uuid, .. } => {
                    // If the peek has been responded to or canceled we can remove it.
                    self.peeks.contains(&uuid)
                }
                ComputeCommand::CancelPeeks { .. } => {
                    // Peek cancelation can always be removed.
                    // TODO: Should we never add it? Confusing?
                    false
                }
                _ => true,
            }
        });
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
