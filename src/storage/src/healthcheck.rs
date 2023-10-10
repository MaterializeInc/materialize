// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthcheck common

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_persist_client::PersistLocation;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Enter, Map};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tracing::{info, trace, warn};

use crate::internal_control::InternalStorageCommand;

pub async fn write_to_persist(
    collection_id: GlobalId,
    new_status: &str,
    new_error: Option<&str>,
    now: NowFn,
    client: &PersistClient,
    status_shard: ShardId,
    relation_desc: &RelationDesc,
    hint: Option<&str>,
) {
    let now_ms = now();
    let row = mz_storage_client::healthcheck::pack_status_row(
        collection_id,
        new_status,
        new_error,
        now_ms,
        hint,
    );

    let mut handle = client
        .open_writer(
            status_shard,
            Arc::new(relation_desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics {
                // TODO: plumb through the GlobalId of the status collection
                shard_name: format!("status({})", collection_id),
                handle_purpose: format!("healthcheck::write_to_persist {}", collection_id),
            },
        )
        .await
        .expect(
            "Invalid usage of the persist \
            client for collection {collection_id} status history shard",
        );

    let mut recent_upper = handle.upper().clone();
    let mut append_ts = Timestamp::from(now_ms);
    'retry_loop: loop {
        // We don't actually care so much about the timestamp we append at; it's best-effort.
        // Ensure that the append timestamp is not less than the current upper, and the new upper
        // is past that timestamp. (Unless we've advanced to the empty antichain, but in
        // that case we're already in trouble.)
        for t in recent_upper.elements() {
            append_ts.join_assign(t);
        }
        let mut new_upper = Antichain::from_elem(append_ts.step_forward());
        new_upper.join_assign(&recent_upper);

        let updates = vec![((SourceData(Ok(row.clone())), ()), append_ts, 1i64)];
        let cas_result = handle
            .compare_and_append(updates, recent_upper.clone(), new_upper)
            .await;
        match cas_result {
            Ok(Ok(())) => break 'retry_loop,
            Ok(Err(mismatch)) => {
                recent_upper = mismatch.current;
            }
            Err(e) => {
                panic!("Invalid usage of the persist client for collection {collection_id} status history shard: {e:?}");
            }
        }
    }

    handle.expire().await
}

/// How long to wait before initiating a `SuspendAndRestart` command, to
/// prevent hot restart loops.
const SUSPEND_AND_RESTART_DELAY: Duration = Duration::from_secs(30);

/// Configuration for a specific index in an indexed health stream.
pub(crate) struct ObjectHealthConfig {
    /// The id of the object.
    pub(crate) id: GlobalId,
    /// The status schema for the object.
    pub(crate) schema: &'static RelationDesc,
    /// The (optional) location to write status updates.
    pub(crate) status_shard: Option<ShardId>,
    /// The location of `status_shard`.
    pub(crate) persist_location: PersistLocation,
}

struct HealthState<'a> {
    id: GlobalId,
    persist_details: Option<(ShardId, &'a PersistClient)>,
    healths: Vec<Option<HealthStatusUpdate>>,
    schema: &'static RelationDesc,
    last_reported_status: Option<HealthStatusUpdate>,
    halt_with: Option<HealthStatusUpdate>,
}

impl<'a> HealthState<'a> {
    fn new(
        id: GlobalId,
        status_shard: Option<ShardId>,
        persist_location: PersistLocation,
        persist_clients: &'a BTreeMap<PersistLocation, PersistClient>,
        schema: &'static RelationDesc,
        worker_count: usize,
    ) -> HealthState<'a> {
        let persist_details = match (status_shard, persist_clients.get(&persist_location)) {
            (Some(shard), Some(persist_client)) => Some((shard, persist_client)),
            _ => None,
        };

        HealthState {
            id,
            persist_details,
            healths: vec![None; worker_count],
            schema,
            last_reported_status: None,
            halt_with: None,
        }
    }
}

/// Writes updates that come across `health_stream` to the collection's status shards, as identified
/// by their `CollectionMetadata`.
///
/// Only one worker will be active and write to the status shard.
///
/// The `OutputIndex` values that come across `health_stream` must be a strict subset of thosema,
/// `configs`'s keys.
pub(crate) fn health_operator<'g, G>(
    scope: &Child<'g, G, mz_repr::Timestamp>,
    storage_state: &crate::storage_state::StorageState,
    // A set of id's that should be marked as `HealthStatus::starting()` during startup.
    mark_starting: BTreeSet<GlobalId>,
    // An id that is allowed to halt the dataflow. Others are ignored, and panic during debug
    // mode.
    halting_id: GlobalId,
    // A description of the object type we are writing status updates about. Used in log lines.
    object_type: &'static str,
    // An indexed stream of health updates. Indexes are configured in `configs`.
    health_stream: &Stream<G, (usize, HealthStatusUpdate)>,
    // A configuration per _index_. Indexes support things like subsources, and allow the
    // `health_operator` to understand where each sub-object should have its status written down
    // at.
    configs: BTreeMap<usize, ObjectHealthConfig>,
) -> Rc<dyn Any>
where
    G: Scope<Timestamp = ()>,
{
    // Derived config options
    let healthcheck_worker_id = scope.index();
    let worker_count = scope.peers();
    let now = storage_state.now.clone();
    let persist_clients = Arc::clone(&storage_state.persist_clients);
    let internal_cmd_tx = Rc::clone(&storage_state.internal_cmd_tx);

    // Inject the originating worker id to each item before exchanging to the chosen worker
    let health_stream = health_stream.map(move |status| (healthcheck_worker_id, status));

    // We'll route all the work to a single arbitrary worker;
    // there's not much to do, and we need a global view.
    let chosen_worker_id = usize::cast_from(configs.keys().next().hashed()) % worker_count;
    let is_active_worker = chosen_worker_id == healthcheck_worker_id;

    let operator_name = format!("healthcheck({})", healthcheck_worker_id);
    let mut health_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    let health = health_stream.enter(scope);

    let mut input = health_op.new_input(
        &health,
        Exchange::new(move |_| u64::cast_from(chosen_worker_id)),
    );

    // Construct a minimal number of persist clients
    let persist_locations: BTreeSet<_> = configs
        .values()
        .filter_map(
            |ObjectHealthConfig {
                 id,
                 status_shard,
                 persist_location,
                 ..
             }| {
                if is_active_worker {
                    match &status_shard {
                        Some(status_shard) => {
                            info!("Health for {object_type} {id} being written to {status_shard}");
                            Some(persist_location.clone())
                        }
                        None => {
                            trace!(
                                "Health for {object_type} {id} not being written to status shard"
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            },
        )
        .collect();

    let button = health_op.build(move |mut _capabilities| async move {
        // Convert the persist locations into persist clients
        let mut persist_clients_per_location = BTreeMap::new();
        for persist_location in persist_locations {
            persist_clients_per_location.insert(
                persist_location.clone(),
                persist_clients.open(persist_location).await.expect(
                    "error creating persist \
                            client for Healthchecker",
                ),
            );
        }

        let mut health_states: BTreeMap<_, _> = configs
            .into_iter()
            .map(
                |(
                    output_idx,
                    ObjectHealthConfig {
                        id,
                        status_shard,
                        persist_location,
                        schema,
                        ..
                    },
                )| {
                    (
                        output_idx,
                        HealthState::new(
                            id,
                            status_shard,
                            persist_location,
                            &persist_clients_per_location,
                            schema,
                            worker_count,
                        ),
                    )
                },
            )
            .collect();

        // Write the initial starting state to the status shard for all managed objects
        if is_active_worker {
            for state in health_states.values_mut() {
                if mark_starting.contains(&state.id) {
                    if let Some((status_shard, persist_client)) = state.persist_details {
                        let status = HealthStatusUpdate::starting();
                        write_to_persist(
                            state.id,
                            status.name(),
                            status.error(),
                            now.clone(),
                            persist_client,
                            status_shard,
                            state.schema,
                            status.hint(),
                        )
                        .await;

                        state.last_reported_status = Some(status);
                    }
                }
            }
        }

        let mut outputs_seen = BTreeSet::new();
        while let Some(event) = input.next_mut().await {
            if let AsyncEvent::Data(_cap, rows) = event {
                for (worker_id, (output_index, health_event)) in rows.drain(..) {
                    let HealthState {
                        id,
                        healths,
                        halt_with,
                        ..
                    } = match health_states.get_mut(&output_index) {
                        Some(health) => health,
                        // This is a health status update for a sub-object_type that we did not request to
                        // be generated, which means it doesn't have a GlobalId and should not be
                        // propagated to the shard.
                        None => continue,
                    };

                    let new_round = outputs_seen.insert(output_index);

                    if !is_active_worker {
                        warn!(
                            "Health messages for {object_type} {id} passed to \
                              an unexpected worker id: {healthcheck_worker_id}"
                        )
                    }

                    if health_event.should_halt() {
                        *halt_with = Some(health_event.clone());
                    }

                    let update = Some(health_event);
                    // Keep the max of the messages in each round; this ensures that errors don't
                    // get lost while also letting us frequently update to the newest status.
                    if new_round || &healths[worker_id] < &update {
                        healths[worker_id] = update;
                    }
                }

                let mut halt_with_outer = None;

                while let Some(output_index) = outputs_seen.pop_first() {
                    let HealthState {
                        id,
                        healths,
                        persist_details,
                        schema,
                        last_reported_status,
                        halt_with,
                    } = health_states
                        .get_mut(&output_index)
                        .expect("known to exist");

                    let overall_status = healths.iter().filter_map(Option::as_ref).max();

                    if let Some(new_status) = overall_status {
                        if new_status.can_transition_from(last_reported_status.as_ref()) {
                            info!(
                                "Health transition for {object_type} {id}: \
                                  {last_reported_status:?} -> {new_status:?}"
                            );
                            if let Some((status_shard, persist_client)) = persist_details {
                                write_to_persist(
                                    *id,
                                    new_status.name(),
                                    new_status.error(),
                                    now.clone(),
                                    persist_client,
                                    *status_shard,
                                    schema,
                                    new_status.hint(),
                                )
                                .await;
                            }

                            *last_reported_status = Some(new_status.clone());
                        }
                    }

                    // Set halt with if None.
                    if halt_with_outer.is_none() && halt_with.is_some() {
                        halt_with_outer = Some((*id, halt_with.clone()));
                    }
                }

                // TODO(aljoscha): Instead of threading through the
                // `should_halt` bit, we can give an internal command sender
                // directly to the places where `should_halt = true` originates.
                // We should definitely do that, but this is okay for a PoC.
                if let Some((id, halt_with)) = halt_with_outer {
                    mz_ore::soft_assert!(
                        id == halting_id,
                        "sub{object_type}s should not produce \
                        halting errors, however {:?} halted while primary \
                                            {object_type} is {:?}",
                        id,
                        halting_id
                    );

                    info!(
                        "Broadcasting suspend-and-restart \
                        command because of {:?} after {:?} delay",
                        halt_with, SUSPEND_AND_RESTART_DELAY
                    );
                    tokio::time::sleep(SUSPEND_AND_RESTART_DELAY).await;
                    internal_cmd_tx.borrow_mut().broadcast(
                        InternalStorageCommand::SuspendAndRestart {
                            // Suspend and restart is expected to operate on the primary object and
                            // not any of the sub-objects
                            id,
                            reason: format!("{:?}", halt_with),
                        },
                    );
                }
            }
        }
    });

    Rc::new(button.press_on_drop())
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct HealthStatusUpdate {
    pub update: HealthStatus,
    pub should_halt: bool,
}

/// NB: we derive Ord here, so the enum order matters. Generally, statuses later in the list
/// take precedence over earlier ones: so if one worker is stalled, we'll consider the entire
/// source to be stalled.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum HealthStatus {
    Starting,
    Running,
    StalledWithError { error: String, hint: Option<String> },
}

impl HealthStatus {
    pub fn name(&self) -> &'static str {
        match self {
            HealthStatus::Starting => "starting",
            HealthStatus::Running => "running",
            HealthStatus::StalledWithError { .. } => "stalled",
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            HealthStatus::Starting | HealthStatus::Running => None,
            HealthStatus::StalledWithError { error, .. } => Some(error),
        }
    }

    pub fn hint(&self) -> Option<&str> {
        match self {
            HealthStatus::Starting | HealthStatus::Running => None,
            HealthStatus::StalledWithError { error: _, hint } => hint.as_deref(),
        }
    }
}

impl HealthStatusUpdate {
    /// Generates a starting [`HealthStatusUpdate`].
    pub(crate) fn starting() -> Self {
        HealthStatusUpdate {
            update: HealthStatus::Starting,
            should_halt: true,
        }
    }

    /// Generates a running [`HealthStatusUpdate`].
    pub(crate) fn running() -> Self {
        HealthStatusUpdate {
            update: HealthStatus::Running,
            should_halt: true,
        }
    }

    /// Generates a non-halting [`HealthStatusUpdate`] with `update`.
    pub(crate) fn stalled(error: String, hint: Option<String>) -> Self {
        HealthStatusUpdate {
            update: HealthStatus::StalledWithError { error, hint },
            should_halt: false,
        }
    }

    /// Generates a halting [`HealthStatusUpdate`] with `update`.
    pub(crate) fn halting(error: String, hint: Option<String>) -> Self {
        HealthStatusUpdate {
            update: HealthStatus::StalledWithError { error, hint },
            should_halt: true,
        }
    }

    /// The user-readable name of the status state.
    pub(crate) fn name(&self) -> &'static str {
        self.update.name()
    }

    /// The user-readable error string, if there is one.
    pub(crate) fn error(&self) -> Option<&str> {
        self.update.error()
    }

    /// A hint for solving the error, if there is one.
    pub(crate) fn hint(&self) -> Option<&str> {
        self.update.hint()
    }

    /// Whether or not we should halt the dataflow instances and restart it.
    pub(crate) fn should_halt(&self) -> bool {
        self.should_halt
    }

    /// Whether or not we can transition from a state (or lack of one).
    ///
    /// Each time this returns `true`, a new status message will be communicated to the user.
    /// Note that messages that are identical except for `should_halt` don't need to be
    /// able to transition between each other, that is handled separately.
    pub(crate) fn can_transition_from(&self, other: Option<&Self>) -> bool {
        if let Some(other) = other {
            self.update != other.update
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn starting() -> HealthStatusUpdate {
        HealthStatusUpdate::starting()
    }

    fn running() -> HealthStatusUpdate {
        HealthStatusUpdate::running()
    }

    fn stalled() -> HealthStatusUpdate {
        HealthStatusUpdate::stalled("".into(), None)
    }

    fn should_halt() -> HealthStatusUpdate {
        HealthStatusUpdate::halting("".into(), None)
    }

    #[mz_ore::test]
    fn test_can_transition() {
        let test_cases = [
            // Allowed transitions
            (
                Some(starting()),
                vec![running(), stalled(), should_halt()],
                true,
            ),
            (
                Some(running()),
                vec![starting(), stalled(), should_halt()],
                true,
            ),
            (Some(stalled()), vec![starting(), running()], true),
            (Some(should_halt()), vec![starting(), running()], true),
            (
                None,
                vec![starting(), running(), stalled(), should_halt()],
                true,
            ),
            // Forbidden transitions
            (Some(starting()), vec![starting()], false),
            (Some(running()), vec![running()], false),
            (Some(stalled()), vec![stalled()], false),
            (Some(should_halt()), vec![should_halt()], false),
            // In practice these can't happen, so they are disallowed.
            (Some(should_halt()), vec![stalled()], false),
            (Some(stalled()), vec![should_halt()], false),
        ];

        for test_case in test_cases {
            run_test(test_case)
        }

        fn run_test(test_case: (Option<HealthStatusUpdate>, Vec<HealthStatusUpdate>, bool)) {
            let (from_status, to_status, allowed) = test_case;
            for status in to_status {
                assert_eq!(
                    allowed,
                    status.can_transition_from(from_status.as_ref()),
                    "Bad can_transition: {from_status:?} -> {status:?}; expected allowed: {allowed:?}"
                );
            }
        }
    }
}
