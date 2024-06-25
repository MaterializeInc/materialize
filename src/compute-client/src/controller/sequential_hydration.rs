// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sequential dataflow hydration support for replicas.
//!
//! Sequential hydration enforces a configurable "hydration concurrency" that limits how many
//! dataflows may be hydrating at the same time. Limiting hydrating concurrency can be beneficial
//! in reducing peak memory usage, cross-dataflow thrashing, and hydration time.
//!
//! The configured hydration concurrency is enforced by delaying the delivery of `Schedule` compute
//! commands to the replica. Those commands are emitted by the controller for collections that
//! become ready to hydrate (based on availability of input data) and are directly applied by
//! replicas by unsuspending the corresponding dataflows. Delaying `Schedule` commands allows us to
//! ensure only a limited number of dataflows can hydrate at the same time.
//!
//! Note that a dataflow may export multiple collections. `Schedule` commands are produced per
//! collection but hydration is a dataflow-level mechanism. In practice Materialize today only
//! produces dataflow with a single export and we rely on this assumption here to simplify the
//! implementation. If the assumption ever ceases to hold, we will need to adjust the code in this
//! module.
//!
//! Sequential hydration is enforeced by a `SequentialHydration` client that sits between the
//! controller and the `PartitionedState` client that splits commands across replica processes.
//! This location is important:
//!
//!  * It needs to be behind the controller since hydration is a per-replica mechanism. Different
//!    replicas can progress through hydration at different paces.
//!  * It needs to be before the `PartitionedState` client because all replica workers must see
//!    `Schedule` commands in the same order. Otherwise we risk getting stuck when different
//!    workers hydrate different dataflows and wait on each other for progress in these dataflows.
//!  * It also needs to be before the `PartitionedState` client because it needs to be able to
//!    observe all compute commands. Clients behind `PartitionedState` are not guaranteed to do so,
//!    since commands are only forwarded to the first process.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use mz_compute_types::dyncfgs::HYDRATION_CONCURRENCY;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_assert_eq_or_log;
use mz_repr::GlobalId;
use mz_service::client::GenericClient;
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::debug;

use crate::controller::ComputeControllerTimestamp;
use crate::metrics::ReplicaMetrics;
use crate::protocol::command::ComputeCommand;
use crate::protocol::response::{ComputeResponse, FrontiersResponse};
use crate::service::ComputeClient;

/// A shareable token.
type Token = Arc<()>;

/// A client enforcing sequential dataflow hydration.
#[derive(Debug)]
pub(super) struct SequentialHydration<C, T> {
    /// The wrapped client.
    client: C,
    /// Dynamic system configuration.
    dyncfg: Arc<ConfigSet>,
    /// Tracked metrics.
    metrics: ReplicaMetrics,
    /// Tracked collections.
    ///
    /// Entries are inserted in response to observed `CreateDataflow` commands.
    /// Entries are removed in response to `Frontiers` commands that report collection
    /// hydration, or in response to `AllowCompaction` commands that specify the empty frontier.
    collections: BTreeMap<GlobalId, Collection<T>>,
    /// A queue of scheduled collections that are awaiting hydration.
    hydration_queue: VecDeque<GlobalId>,
    /// A token held by hydrating collections.
    ///
    /// Useful to efficiently determine how many collections are currently in the process of
    /// hydration, and thus how much capacity is available.
    hydration_token: Token,
}

impl<C, T> SequentialHydration<C, T>
where
    C: ComputeClient<T>,
    T: ComputeControllerTimestamp,
{
    /// Create a new `SequentialHydration` client.
    pub fn new(client: C, dyncfg: Arc<ConfigSet>, metrics: ReplicaMetrics) -> Self {
        Self {
            client,
            dyncfg,
            metrics,
            collections: Default::default(),
            hydration_queue: Default::default(),
            hydration_token: Default::default(),
        }
    }

    /// Return the number of hydrating collections.
    fn hydration_count(&self) -> usize {
        Arc::strong_count(&self.hydration_token) - 1
    }

    /// Absorb a command and send resulting commands to the wrapped client.
    async fn absorb_command(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        // Whether to forward this command to the wrapped client.
        let mut forward = true;

        match &cmd {
            // We enforce sequential hydration only for non-transient dataflows, assuming that
            // transient dataflows are created for interactive user queries and should always be
            // scheduled as soon as possible.
            ComputeCommand::CreateDataflow(dataflow) if !dataflow.is_transient() => {
                let export_ids: Vec<_> = dataflow.export_ids().collect();
                let id = export_ids.expect_element(|| "multi-export dataflows are not supported");
                let as_of = dataflow.as_of.clone().unwrap();

                debug!(%id, ?as_of, "tracking collection");
                self.collections.insert(id, Collection::new(as_of));
            }
            ComputeCommand::Schedule(id) => {
                if let Some(collection) = self.collections.get_mut(id) {
                    debug!(%id, "enqueuing collection for hydration");
                    self.hydration_queue.push_back(*id);
                    collection.set_scheduled();
                    forward = false;
                }
            }
            ComputeCommand::AllowCompaction { id, frontier } if frontier.is_empty() => {
                // The collection was dropped by the controller. Remove it from the tracking state
                // to ensure we don't produce any more commands for it.
                if self.collections.remove(id).is_some() {
                    debug!(%id, "collection dropped");
                }
            }
            _ => (),
        }

        if forward {
            self.client.send(cmd).await?;
        }

        // Schedule collections that are ready now.
        self.hydrate_collections().await
    }

    /// Observe a response and send resulting commands to the wrapped client.
    async fn observe_response(&mut self, resp: &ComputeResponse<T>) -> Result<(), anyhow::Error> {
        if let ComputeResponse::Frontiers(
            id,
            FrontiersResponse {
                output_frontier: Some(frontier),
                ..
            },
        ) = resp
        {
            if let Some(collection) = self.collections.remove(id) {
                let hydrated = PartialOrder::less_than(&collection.as_of, frontier);
                if hydrated || frontier.is_empty() {
                    debug!(%id, "collection hydrated");

                    // Note that it is possible to observe hydration even for collections for which
                    // we never sent a `Schedule` command, if the replica decided to not suspend
                    // the dataflow after creation. The compute protocol does not require replicas
                    // to create dataflows in suspended state. It seems like a good idea to still
                    // send a `Schedule` command in this case, rather than swallowing it, to make
                    // the protocol communication more predicatable.

                    match collection.state {
                        State::Created => {
                            // We haven't seen a `Schedule` command yet, so no obligations to send
                            // one either.
                        }
                        State::QueuedForHydration => {
                            // We are holding back the `Schedule` command for this collection. Send
                            // it now.
                            self.send(ComputeCommand::Schedule(*id)).await?;
                        }
                        State::Hydrating(token) => {
                            // We freed some hydration capacity and may be able to start hydrating
                            // new collections.
                            drop(token);
                            self.hydrate_collections().await?;
                        }
                    }
                } else {
                    self.collections.insert(*id, collection);
                }
            }
        }
        Ok(())
    }

    /// Allow hydration based on the available capacity.
    async fn hydrate_collections(&mut self) -> Result<(), anyhow::Error> {
        let capacity = HYDRATION_CONCURRENCY.get(&self.dyncfg);
        while self.hydration_count() < capacity {
            let Some(id) = self.hydration_queue.pop_front() else {
                // Hydration queue is empty.
                break;
            };
            let Some(collection) = self.collections.get_mut(&id) else {
                // Collection has already been dropped.
                continue;
            };

            debug!(%id, "starting collection hydration");
            self.client.send(ComputeCommand::Schedule(id)).await?;

            let token = Arc::clone(&self.hydration_token);
            collection.set_hydrating(token);
        }

        let queue_size = u64::cast_from(self.hydration_queue.len());
        self.metrics.inner.hydration_queue_size.set(queue_size);

        Ok(())
    }
}

#[async_trait]
impl<C, T> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for SequentialHydration<C, T>
where
    C: ComputeClient<T>,
    T: ComputeControllerTimestamp,
{
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        self.absorb_command(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
        let resp = self.client.recv().await?;
        if let Some(response) = &resp {
            self.observe_response(response).await?;
        }
        Ok(resp)
    }
}

/// Information about a tracked collection.
#[derive(Debug)]
struct Collection<T> {
    /// The as-of frontier at collection creation.
    as_of: Antichain<T>,
    /// The current state of the collection.
    state: State,
}

impl<T> Collection<T> {
    /// Create a new `Collection`.
    fn new(as_of: Antichain<T>) -> Self {
        Self {
            as_of,
            state: State::Created,
        }
    }

    /// Advance this collection's state to `Scheduled`.
    fn set_scheduled(&mut self) {
        soft_assert_eq_or_log!(self.state, State::Created);
        self.state = State::QueuedForHydration;
    }

    fn set_hydrating(&mut self, token: Token) {
        soft_assert_eq_or_log!(self.state, State::QueuedForHydration);
        self.state = State::Hydrating(token);
    }
}

/// The state of a tracked collection.
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// Collection has been created and is waiting for a `Schedule` command.
    Created,
    /// The collection has received a `Schedule` command and has been added to the hydration queue,
    /// waiting for hydration capacity.
    QueuedForHydration,
    /// Collection is hydrating and waiting for hydration to complete.
    Hydrating(Token),
}
