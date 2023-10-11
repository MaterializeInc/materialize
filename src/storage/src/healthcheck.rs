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
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
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

use crate::internal_control::{InternalCommandSender, InternalStorageCommand};

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

/// A trait that lets a user configure the `health_operator` with custom
/// behavior. This is mostly useful for testing, and the [`DefaultWriter`]
/// should be the correct implementation for everyone.
#[async_trait::async_trait(?Send)]
pub trait HealthOperator {
    /// Record a new status.
    async fn record_new_status(
        &self,
        collection_id: GlobalId,
        new_status: &str,
        new_error: Option<&str>,
        now: NowFn,
        // TODO(guswynn): not urgent:
        // Ideally this would be entirely included in the `DefaultWriter`, but that
        // requires a fairly heavy change to the `health_operator`, which hardcodes
        // some use of persist. For now we just leave it and ignore it in tests.
        client: &PersistClient,
        status_shard: ShardId,
        relation_desc: &RelationDesc,
        hint: Option<&str>,
    );
    async fn send_halt(&self, id: GlobalId, error: Option<HealthStatusUpdate>);

    /// Optionally override the chosen worker index. Default is semi-random.
    /// Only useful for tests.
    fn chosen_worker(&self) -> Option<usize> {
        None
    }
}

/// A default `HealthOperator` for use in normal cases.
pub struct DefaultWriter(pub Rc<RefCell<dyn InternalCommandSender>>);

#[async_trait::async_trait(?Send)]
impl HealthOperator for DefaultWriter {
    async fn record_new_status(
        &self,
        collection_id: GlobalId,
        new_status: &str,
        new_error: Option<&str>,
        now: NowFn,
        client: &PersistClient,
        status_shard: ShardId,
        relation_desc: &RelationDesc,
        hint: Option<&str>,
    ) {
        write_to_persist(
            collection_id,
            new_status,
            new_error,
            now,
            client,
            status_shard,
            relation_desc,
            hint,
        )
        .await
    }

    async fn send_halt(&self, id: GlobalId, error: Option<HealthStatusUpdate>) {
        self.0
            .borrow_mut()
            .broadcast(InternalStorageCommand::SuspendAndRestart {
                // Suspend and restart is expected to operate on the primary object and
                // not any of the sub-objects
                id,
                reason: format!("{:?}", error),
            });
    }
}

/// Writes updates that come across `health_stream` to the collection's status shards, as identified
/// by their `CollectionMetadata`.
///
/// Only one worker will be active and write to the status shard.
///
/// The `OutputIndex` values that come across `health_stream` must be a strict subset of thosema,
/// `configs`'s keys.
pub(crate) fn health_operator<'g, G, P>(
    scope: &Child<'g, G, mz_repr::Timestamp>,
    persist_clients: Arc<PersistClientCache>,
    now: NowFn,
    // A set of id's that should be marked as `HealthStatusUpdate::starting()` during startup.
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
    // An impl of `HealthOperator` that configures the output behavior of this operator.
    health_operator_impl: P,
) -> Rc<dyn Any>
where
    G: Scope<Timestamp = ()>,
    P: HealthOperator + 'static,
{
    // Derived config options
    let healthcheck_worker_id = scope.index();
    let worker_count = scope.peers();

    // Inject the originating worker id to each item before exchanging to the chosen worker
    let health_stream = health_stream.map(move |status| (healthcheck_worker_id, status));

    let chosen_worker_id = if let Some(index) = health_operator_impl.chosen_worker() {
        index
    } else {
        // We'll route all the work to a single arbitrary worker;
        // there's not much to do, and we need a global view.
        usize::cast_from(configs.keys().next().hashed()) % worker_count
    };

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
                        health_operator_impl
                            .record_new_status(
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
                                health_operator_impl
                                    .record_new_status(
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
                    health_operator_impl.send_halt(id, halt_with).await;
                }
            }
        }
    });

    Rc::new(button.press_on_drop())
}

use serde::{Deserialize, Serialize};

/// NB: we derive Ord here, so the enum order matters. Generally, statuses later in the list
/// take precedence over earlier ones: so if one worker is stalled, we'll consider the entire
/// source to be stalled.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum HealthStatusUpdate {
    Starting,
    Running,
    Stalled {
        error: String,
        hint: Option<String>,
        should_halt: bool,
    },
}
impl HealthStatusUpdate {
    /// Generates a starting [`HealthStatusUpdate`].
    pub(crate) fn starting() -> Self {
        HealthStatusUpdate::Starting
    }

    /// Generates a running [`HealthStatusUpdate`].
    pub(crate) fn running() -> Self {
        HealthStatusUpdate::Running
    }

    /// Generates a non-halting [`HealthStatusUpdate`] with `update`.
    pub(crate) fn stalled(error: String, hint: Option<String>) -> Self {
        HealthStatusUpdate::Stalled {
            error,
            hint,
            should_halt: false,
        }
    }

    /// Generates a halting [`HealthStatusUpdate`] with `update`.
    pub(crate) fn halting(error: String, hint: Option<String>) -> Self {
        HealthStatusUpdate::Stalled {
            error,
            hint,
            should_halt: true,
        }
    }

    /// The user-readable name of the status state.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            HealthStatusUpdate::Starting => "starting",
            HealthStatusUpdate::Running => "running",
            HealthStatusUpdate::Stalled { .. } => "stalled",
        }
    }

    /// The user-readable error string, if there is one.
    pub(crate) fn error(&self) -> Option<&str> {
        match self {
            HealthStatusUpdate::Starting | HealthStatusUpdate::Running => None,
            HealthStatusUpdate::Stalled { error, .. } => Some(error),
        }
    }

    /// A hint for solving the error, if there is one.
    pub(crate) fn hint(&self) -> Option<&str> {
        match self {
            HealthStatusUpdate::Starting | HealthStatusUpdate::Running => None,
            HealthStatusUpdate::Stalled { hint, .. } => hint.as_deref(),
        }
    }

    /// Whether or not we should halt the dataflow instances and restart it.
    pub(crate) fn should_halt(&self) -> bool {
        match self {
            HealthStatusUpdate::Starting | HealthStatusUpdate::Running => false,
            HealthStatusUpdate::Stalled { should_halt, .. } => *should_halt,
        }
    }

    /// Whether or not we can transition from a state (or lack of one).
    ///
    /// Each time this returns `true`, a new status message will be communicated to the user.
    /// Note that messages that are identical except for `should_halt` don't need to be
    /// able to transition between each other, that is handled separately.
    pub(crate) fn can_transition_from(&self, other: Option<&Self>) -> bool {
        match (self, other) {
            (_, None) => true,
            (
                HealthStatusUpdate::Stalled {
                    hint: to_hint,
                    error: to_error,
                    ..
                },
                Some(HealthStatusUpdate::Stalled {
                    hint: from_hint,
                    error: from_error,
                    ..
                }),
            ) => {
                // We ignore `should_halt` while checking if we can transition.
                to_hint != from_hint || to_error != from_error
            }
            (to, Some(from)) => from != to,
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
    fn stalled2() -> HealthStatusUpdate {
        HealthStatusUpdate::stalled("other".into(), None)
    }

    fn should_halt() -> HealthStatusUpdate {
        HealthStatusUpdate::halting("".into(), None)
    }
    fn should_halt2() -> HealthStatusUpdate {
        HealthStatusUpdate::halting("other".into(), None)
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
            (
                Some(stalled()),
                vec![starting(), running(), stalled2(), should_halt2()],
                true,
            ),
            (
                Some(should_halt()),
                vec![starting(), running(), stalled2(), should_halt2()],
                true,
            ),
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

    // Actual timely tests for `health_operator`.

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // Reports undefined behavior
    fn test_health_operator_basic() {
        use Step::*;

        // Test 2 inputs across 2 workers.
        health_operator_runner(
            2,
            2,
            vec![
                AssertStatus(vec![
                    // Assert both inputs started.
                    StatusToAssert {
                        collection_index: 0,
                        status: "starting".to_string(),
                        ..Default::default()
                    },
                    StatusToAssert {
                        collection_index: 1,
                        status: "starting".to_string(),
                        ..Default::default()
                    },
                ]),
                // Update and assert one is running.
                Update(TestUpdate {
                    worker_id: 1,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "running".to_string(),
                    ..Default::default()
                }]),
                // Assert the other can be stalled by 1 worker.
                Update(TestUpdate {
                    worker_id: 0,
                    input_index: 1,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                Update(TestUpdate {
                    worker_id: 1,
                    input_index: 1,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: "stalled".to_string(),
                    error: Some("uhoh".to_string()),
                    ..Default::default()
                }]),
                // And that it can recover.
                Update(TestUpdate {
                    worker_id: 0,
                    input_index: 1,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: "running".to_string(),
                    ..Default::default()
                }]),
            ],
        );
    }

    // The below is ALL test infrastructure for the above

    use timely::dataflow::operators::exchange::Exchange;
    use timely::dataflow::Scope;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    // Doesn't really matter which we use
    use mz_storage_client::healthcheck::MZ_SOURCE_STATUS_HISTORY_DESC;

    /// A status to assert.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct StatusToAssert {
        collection_index: usize,
        status: String,
        error: Option<String>,
        hint: Option<String>,
    }

    /// An update to push into the operator.
    /// Can come from any worker, and from any input.
    #[derive(Debug, Clone)]
    struct TestUpdate {
        worker_id: u64,
        input_index: usize,
        update: HealthStatusUpdate,
    }

    #[derive(Debug, Clone)]
    enum Step {
        /// Insert a new health update.
        Update(TestUpdate),
        /// Assert a set of outputs. Note that these should
        /// have unique `collection_index`'s
        AssertStatus(Vec<StatusToAssert>),
    }

    struct TestWriter {
        sender: UnboundedSender<StatusToAssert>,
        input_mapping: BTreeMap<GlobalId, usize>,
    }

    #[async_trait::async_trait(?Send)]
    impl HealthOperator for TestWriter {
        async fn record_new_status(
            &self,
            collection_id: GlobalId,
            new_status: &str,
            new_error: Option<&str>,
            _now: NowFn,
            _client: &PersistClient,
            _status_shard: ShardId,
            _relation_desc: &RelationDesc,
            hint: Option<&str>,
        ) {
            let _ = self.sender.send(StatusToAssert {
                collection_index: *self.input_mapping.get(&collection_id).unwrap(),
                status: new_status.to_string(),
                error: new_error.map(str::to_string),
                hint: hint.map(str::to_string),
            });
        }

        async fn send_halt(&self, _id: GlobalId, _error: Option<HealthStatusUpdate>) {
            // Not yet unit-tested
            unimplemented!()
        }

        fn chosen_worker(&self) -> Option<usize> {
            // We input and assert outputs on the first worker.
            Some(0)
        }
    }

    /// Setup a `health_operator` with a set number of workers and inputs, and the
    /// steps on the first worker.
    fn health_operator_runner(workers: usize, inputs: usize, steps: Vec<Step>) {
        let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
        let tokio_handle = tokio_runtime.handle().clone();

        let inputs: BTreeMap<GlobalId, usize> = (0..inputs)
            .map(|index| (GlobalId::User(u64::cast_from(index)), index))
            .collect();

        timely::execute::execute(
            timely::execute::Config {
                communication: timely::CommunicationConfig::Process(workers),
                worker: Default::default(),
            },
            move |worker| {
                let steps = steps.clone();
                let inputs = inputs.clone();

                let _tokio_guard = tokio_handle.enter();
                let (in_tx, in_rx) = unbounded_channel();
                let (out_tx, mut out_rx) = unbounded_channel();

                worker.dataflow::<(), _, _>(|root_scope| {
                    root_scope
                        .clone()
                        .scoped::<mz_repr::Timestamp, _, _>("gus", |scope| {
                            let input = producer(root_scope.clone(), in_rx);
                            Box::leak(Box::new(health_operator(
                                scope,
                                Arc::new(PersistClientCache::new_no_metrics()),
                                mz_ore::now::SYSTEM_TIME.clone(),
                                inputs.keys().copied().collect(),
                                *inputs.first_key_value().unwrap().0,
                                "source_test",
                                &input,
                                inputs
                                    .iter()
                                    .map(|(id, index)| {
                                        (
                                            *index,
                                            ObjectHealthConfig {
                                                id: *id,
                                                schema: &*MZ_SOURCE_STATUS_HISTORY_DESC,
                                                status_shard: Some(ShardId::new()),
                                                persist_location: PersistLocation::new_in_mem(),
                                            },
                                        )
                                    })
                                    .collect(),
                                TestWriter {
                                    sender: out_tx,
                                    input_mapping: inputs,
                                },
                            )));
                        });
                });

                // We arbitrarily do all the testing on the first worker.
                if worker.index() == 0 {
                    use Step::*;
                    for step in steps {
                        match step {
                            Update(update) => {
                                let _ = in_tx.send(update);
                            }
                            AssertStatus(mut statuses) => loop {
                                match out_rx.try_recv() {
                                    Err(_) => {
                                        worker.step();
                                        // This makes testing easier.
                                        std::thread::sleep(std::time::Duration::from_millis(50));
                                    }
                                    Ok(update) => {
                                        let pos = statuses
                                            .iter()
                                            .position(|s| {
                                                s.collection_index == update.collection_index
                                            })
                                            .unwrap();

                                        let status_to_assert = &statuses[pos];
                                        assert_eq!(&update, status_to_assert);

                                        statuses.remove(pos);
                                        if statuses.is_empty() {
                                            break;
                                        }
                                    }
                                }
                            },
                        }
                    }

                    // Assert that nothing is left in the channel.
                    assert!(out_rx.try_recv().is_err());
                }
            },
        )
        .unwrap();
    }

    /// Produces (input_index, HealthStatusUpdate)'s based on the input channel.
    ///
    /// Only the first worker is used, all others immediately drop their capabilities and channels.
    /// After the channel is empty on the first worker, then the frontier will go to [].
    /// Also ensures that updates are routed to the correct worker based on the `TestUpdate`
    /// using an exchange.
    fn producer<G: Scope<Timestamp = ()>>(
        scope: G,
        mut input: UnboundedReceiver<TestUpdate>,
    ) -> Stream<G, (usize, HealthStatusUpdate)> {
        let mut iterator = AsyncOperatorBuilder::new("iterator".to_string(), scope.clone());
        let (mut output_handle, output) = iterator.new_output();

        let index = scope.index();
        iterator.build(|mut caps| async move {
            // We input and assert outputs on the first worker.
            if index != 0 {
                return;
            }
            let mut capability = Some(caps.pop().unwrap());
            while let Some(element) = input.recv().await {
                output_handle
                    .give(
                        capability.as_ref().unwrap(),
                        (element.worker_id, element.input_index, element.update),
                    )
                    .await;
            }

            capability.take();
        });

        let output = output.exchange(|d| d.0).map(|d| (d.1, d.2));

        output
    }
}
