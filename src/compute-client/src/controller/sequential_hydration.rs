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
//! Sequential hydration is enforced by a `SequentialHydration` interceptor that sits between the
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
//!
//! `SequentialHydration` is a synchronous interceptor: the replica task feeds it every command it
//! is about to send and every response it receives, and the interceptor returns the commands that
//! should actually be sent to the replica. The task is responsible for sending those commands, so
//! the interceptor holds no client and spawns no task of its own.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use mz_compute_types::dyncfgs::HYDRATION_CONCURRENCY;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_assert_eq_or_log;
use mz_repr::{GlobalId, Timestamp};
use timely::PartialOrder;
use timely::progress::Antichain;
use tracing::debug;

use crate::metrics::ReplicaMetrics;
use crate::protocol::command::ComputeCommand;
use crate::protocol::response::{ComputeResponse, FrontiersResponse};

/// A shareable token.
type Token = Arc<()>;

/// An interceptor enforcing sequential dataflow hydration.
///
/// The replica task drives this interceptor by feeding it the commands it intends to send (via
/// [`SequentialHydration::absorb_command`]) and the responses it receives (via
/// [`SequentialHydration::observe_response`]). Both methods return the commands the task should
/// send to the replica, with `Schedule` commands held back or released according to the configured
/// hydration concurrency.
#[derive(Debug)]
pub(super) struct SequentialHydration {
    /// Dynamic system configuration.
    dyncfg: Arc<ConfigSet>,
    /// Tracked metrics.
    metrics: ReplicaMetrics,
    /// Tracked collections.
    ///
    /// Entries are inserted in response to observed `CreateDataflow` commands.
    /// Entries are removed in response to `Frontiers` commands that report collection
    /// hydration, or in response to `AllowCompaction` commands that specify the empty frontier.
    collections: BTreeMap<GlobalId, Collection>,
    /// A queue of scheduled collections that are awaiting hydration.
    hydration_queue: VecDeque<GlobalId>,
    /// A token held by hydrating collections.
    ///
    /// Useful to efficiently determine how many collections are currently in the process of
    /// hydration, and thus how much capacity is available.
    hydration_token: Token,
}

impl SequentialHydration {
    /// Create a new `SequentialHydration` interceptor.
    pub(super) fn new(dyncfg: Arc<ConfigSet>, metrics: ReplicaMetrics) -> Self {
        Self {
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

    /// Absorb a command the task intends to send, returning the commands it should actually send.
    pub(super) fn absorb_command(&mut self, cmd: ComputeCommand) -> Vec<ComputeCommand> {
        // Whether to forward this command to the replica.
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

        let mut commands = Vec::new();
        if forward {
            commands.push(cmd);
        }

        // Schedule collections that are ready now.
        commands.extend(self.hydrate_collections());
        commands
    }

    /// Observe a response the task received, returning the commands it should send in reaction.
    pub(super) fn observe_response(&mut self, resp: &ComputeResponse) -> Vec<ComputeCommand> {
        let mut commands = Vec::new();

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
                            commands.push(ComputeCommand::Schedule(*id));
                        }
                        State::Hydrating(token) => {
                            // We freed some hydration capacity and may be able to start hydrating
                            // new collections.
                            drop(token);
                            commands.extend(self.hydrate_collections());
                        }
                    }
                } else {
                    self.collections.insert(*id, collection);
                }
            }
        }

        commands
    }

    /// Allow hydration based on the available capacity, returning the `Schedule` commands to send.
    fn hydrate_collections(&mut self) -> Vec<ComputeCommand> {
        let mut commands = Vec::new();

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
            commands.push(ComputeCommand::Schedule(id));

            let token = Arc::clone(&self.hydration_token);
            collection.set_hydrating(token);
        }

        let queue_size = u64::cast_from(self.hydration_queue.len());
        self.metrics.inner.hydration_queue_size.set(queue_size);

        commands
    }
}

/// Information about a tracked collection.
#[derive(Debug)]
struct Collection {
    /// The as-of frontier at collection creation.
    as_of: Antichain<Timestamp>,
    /// The current state of the collection.
    state: State,
}

impl Collection {
    /// Create a new `Collection`.
    fn new(as_of: Antichain<Timestamp>) -> Self {
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
