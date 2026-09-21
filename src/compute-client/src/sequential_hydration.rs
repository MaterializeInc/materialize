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
//! Use one `SequentialHydration` interceptor per replica incarnation, before partitioning commands
//! across processes or workers. Different replicas can progress through hydration at different
//! paces. The interceptor must observe every command and the replica's aggregated responses. All
//! workers must see the returned commands in the same order, or they risk hydrating different
//! dataflows and waiting on each other for progress. Both the controller's `PartitionedState` client
//! and a native replica's `ReplicaCompute` worker partitioning belong after this interceptor.
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
use mz_ore::metrics::raw::UIntGaugeVec;
use mz_ore::metrics::{DeleteOnDropGauge, MetricTag, MetricVisibility, MetricsRegistry};
use mz_ore::soft_assert_eq_or_log;
use mz_repr::{GlobalId, Timestamp};
use prometheus::core::AtomicU64;
use timely::PartialOrder;
use timely::progress::Antichain;
use tracing::debug;

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
///
/// Both methods take the replica's effective configuration, which the task owns and keeps current.
/// Apply `CreateInstance`'s initial configuration and `UpdateConfiguration`'s dynamic updates to
/// that set before passing the command to [`Self::absorb_command`]. Reading
/// [`HYDRATION_CONCURRENCY`] from this set makes replica-scoped limits effective.
///
/// Send each returned batch in order before feeding the next command or response. Returned commands
/// go directly to the replica, not back through this interceptor. Responses are only observed and
/// must still be handled by the caller. Recreate the interceptor when starting a new incarnation.
#[derive(Debug)]
pub struct SequentialHydration {
    /// Gauge tracking the size of the hydration queue.
    hydration_queue_size: DeleteOnDropGauge<AtomicU64, Vec<String>>,
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
    /// Registers the queue metric once per runtime registry. Both lifecycle
    /// owners use the same descriptor and replica labels.
    pub fn register_queue_metric(registry: &MetricsRegistry) -> UIntGaugeVec {
        registry.register(mz_ore::metric! {
            name: "mz_compute_controller_hydration_queue_size",
            help: "The size of the compute hydration queue.",
            var_labels: ["instance_id", "replica_id"],
            visibility: MetricVisibility::Public,
            tags: [MetricTag::Compute],
        })
    }

    /// Create a new `SequentialHydration` interceptor.
    ///
    /// The caller supplies a native queue-size gauge labeled for this replica. Clones share its
    /// registration, which is removed when the last handle is dropped.
    pub fn new(hydration_queue_size: DeleteOnDropGauge<AtomicU64, Vec<String>>) -> Self {
        Self {
            hydration_queue_size,
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
    ///
    /// `dyncfg` is the replica's effective configuration, as maintained by the task.
    pub fn absorb_command(
        &mut self,
        cmd: ComputeCommand,
        dyncfg: &ConfigSet,
    ) -> Vec<ComputeCommand> {
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
        commands.extend(self.hydrate_collections(dyncfg));
        commands
    }

    /// Observe a response the task received, returning the commands it should send in reaction.
    ///
    /// `dyncfg` is the replica's effective configuration, as maintained by the task.
    pub fn observe_response(
        &mut self,
        resp: &ComputeResponse,
        dyncfg: &ConfigSet,
    ) -> Vec<ComputeCommand> {
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
                            commands.extend(self.hydrate_collections(dyncfg));
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
    fn hydrate_collections(&mut self, dyncfg: &ConfigSet) -> Vec<ComputeCommand> {
        let mut commands = Vec::new();

        let capacity = HYDRATION_CONCURRENCY.get(dyncfg);
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
        self.hydration_queue_size.set(queue_size);

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

#[cfg(test)]
mod tests {
    use mz_cluster_client::metrics::ControllerMetrics;
    use mz_compute_types::ComputeInstanceId;
    use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::ReprRelationType;

    use crate::metrics::ComputeControllerMetrics;
    use crate::protocol::command::ComputeParameters;

    use super::*;

    fn metrics() -> DeleteOnDropGauge<AtomicU64, Vec<String>> {
        let registry = MetricsRegistry::new();
        let shared = ControllerMetrics::new(&registry);
        ComputeControllerMetrics::new(&registry, shared)
            .for_instance(ComputeInstanceId::User(1))
            .for_replica(mz_cluster_client::ReplicaId::User(1))
            .inner
            .hydration_queue_size
            .clone()
    }

    /// A `CreateDataflow` command for a non-transient dataflow exporting `id`.
    fn create_dataflow(id: GlobalId) -> ComputeCommand {
        let mut desc = DataflowDescription::new("test".into());
        desc.as_of = Some(Antichain::from_elem(Timestamp::MIN));
        desc.index_exports.insert(
            id,
            (
                IndexDesc {
                    on_id: id,
                    key: Vec::new(),
                },
                ReprRelationType::empty(),
            ),
        );
        ComputeCommand::CreateDataflow(Box::new(desc))
    }

    /// The interceptor enforces the hydration concurrency of the configuration it is handed, which
    /// is the replica's own, specialized by the replica task. This is the regression guard for it
    /// reading the environment-wide value instead, which would make the config's `Replica` scope
    /// inert, given that it is enforced here and never read on the replica.
    #[mz_ore::test]
    fn hydration_concurrency_follows_supplied_config() {
        let dyncfg = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&HYDRATION_CONCURRENCY, 1);
        updates.apply(&dyncfg);

        let mut hydration = SequentialHydration::new(metrics());

        let id1 = GlobalId::User(1);
        let id2 = GlobalId::User(2);
        for id in [id1, id2] {
            let commands = hydration.absorb_command(create_dataflow(id), &dyncfg);
            assert_eq!(commands, vec![create_dataflow(id)]);
        }

        // At a concurrency of one, only the first `Schedule` is released.
        let commands = hydration.absorb_command(ComputeCommand::Schedule(id1), &dyncfg);
        assert_eq!(commands, vec![ComputeCommand::Schedule(id1)]);
        let commands = hydration.absorb_command(ComputeCommand::Schedule(id2), &dyncfg);
        assert_eq!(commands, vec![]);

        // Raising the concurrency in the supplied configuration releases the held-back command.
        let mut updates = ConfigUpdates::default();
        updates.add(&HYDRATION_CONCURRENCY, 2);
        updates.apply(&dyncfg);

        let update = ComputeCommand::UpdateConfiguration(Box::new(ComputeParameters::default()));
        let commands = hydration.absorb_command(update.clone(), &dyncfg);
        assert_eq!(commands, vec![update, ComputeCommand::Schedule(id2)]);
    }
}
