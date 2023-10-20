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
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
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
use tracing::{error, info, trace};

use crate::internal_control::{InternalCommandSender, InternalStorageCommand};

pub async fn write_to_persist(
    collection_id: GlobalId,
    new_status: &str,
    new_error: Option<&str>,
    hints: &BTreeSet<String>,
    namespaced_errors: &BTreeMap<StatusNamespace, String>,
    now: NowFn,
    client: &PersistClient,
    status_shard: ShardId,
    relation_desc: &RelationDesc,
    write_namespaced_map: bool,
) {
    let now_ms = now();
    let row = mz_storage_client::healthcheck::pack_status_row(
        collection_id,
        new_status,
        new_error,
        hints,
        // `pack_status_row` requires keys sorted by their string representation.
        // This is technically an few extra allocations, but its relatively rare so
        // we don't worry about it. Sorting it in a vec like with `Itertools::sorted`
        // would also cost allocations.
        &if write_namespaced_map {
            namespaced_errors
                .iter()
                .map(|(ns, val)| (ns.to_string(), val))
                .collect()
        } else {
            BTreeMap::new()
        },
        now_ms,
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

/// The namespace of the update. The `Ord` impl matter here, later variants are
/// displayed over earlier ones.
///
/// Some namespaces (referred to as "sidechannels") can come from any worker_id,
/// and `Running` statuses from them do not mark the entire object as running.
///
/// Ensure you update `is_sidechannel` when adding variants.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum StatusNamespace {
    /// A normal status namespaces. Any `Running` status from any worker will mark the object
    /// `Running`.
    Generator,
    TestScript,
    Kafka,
    Postgres,
    Ssh,
    Upsert,
    Decode,
    Internal,
}

impl StatusNamespace {
    fn is_sidechannel(&self) -> bool {
        matches!(self, StatusNamespace::Ssh)
    }
}

impl fmt::Display for StatusNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StatusNamespace::*;
        match self {
            Generator => write!(f, "generator"),
            TestScript => write!(f, "testscript"),
            Kafka => write!(f, "kafka"),
            Postgres => write!(f, "postgres"),
            Ssh => write!(f, "ssh"),
            Upsert => write!(f, "upsert"),
            Decode => write!(f, "decode"),
            Internal => write!(f, "internal"),
        }
    }
}

struct PerWorkerHealthStatus {
    pub(crate) errors_by_worker: Vec<BTreeMap<StatusNamespace, HealthStatusUpdate>>,
}

impl PerWorkerHealthStatus {
    fn merge_update(
        &mut self,
        mut worker: usize,
        namespace: StatusNamespace,
        update: HealthStatusUpdate,
        only_greater: bool,
    ) {
        if namespace.is_sidechannel() {
            worker = 0;
        }

        let errors = &mut self.errors_by_worker[worker];
        match errors.entry(namespace) {
            Entry::Vacant(v) => {
                v.insert(update);
            }
            Entry::Occupied(mut o) => {
                if !only_greater || o.get() < &update {
                    o.insert(update);
                }
            }
        }
    }

    fn decide_status(&self) -> OverallStatus {
        let mut output_status = OverallStatus::Starting;
        let mut namespaced_errors: BTreeMap<StatusNamespace, String> = BTreeMap::new();
        let mut hints: BTreeSet<String> = BTreeSet::new();

        for status in self.errors_by_worker.iter() {
            use HealthStatusUpdate::*;
            for (ns, ns_status) in status.iter() {
                if let Stalled { error, hint, .. } = ns_status {
                    if Some(error) > namespaced_errors.get(ns).as_deref() {
                        namespaced_errors.insert(*ns, error.to_string());
                    }

                    if let Some(hint) = hint {
                        hints.insert(hint.to_string());
                    }
                }

                if !ns.is_sidechannel() && matches!(ns_status, HealthStatusUpdate::Running) {
                    output_status = OverallStatus::Running;
                }
            }
        }

        if !namespaced_errors.is_empty() {
            // Pick the most important error.
            let (ns, err) = namespaced_errors.last_key_value().unwrap();
            output_status = OverallStatus::Stalled {
                error: format!("{}: {}", ns, err),
                hints,
                namespaced_errors,
            }
        }

        output_status
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum OverallStatus {
    Starting,
    Running,
    Stalled {
        error: String,
        hints: BTreeSet<String>,
        namespaced_errors: BTreeMap<StatusNamespace, String>,
    },
}

impl OverallStatus {
    /// The user-readable name of the status state.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            OverallStatus::Starting => "starting",
            OverallStatus::Running => "running",
            OverallStatus::Stalled { .. } => "stalled",
        }
    }

    /// The user-readable error string, if there is one.
    pub(crate) fn error(&self) -> Option<&str> {
        match self {
            OverallStatus::Starting | OverallStatus::Running => None,
            OverallStatus::Stalled { error, .. } => Some(error),
        }
    }

    /// A set of namespaced errors, if there are any.
    pub(crate) fn errors(&self) -> Option<&BTreeMap<StatusNamespace, String>> {
        match self {
            OverallStatus::Starting | OverallStatus::Running => None,
            OverallStatus::Stalled {
                namespaced_errors, ..
            } => Some(namespaced_errors),
        }
    }

    /// A set of hints, if there are any.
    pub(crate) fn hints(&self) -> Option<&BTreeSet<String>> {
        match self {
            OverallStatus::Starting | OverallStatus::Running => None,
            OverallStatus::Stalled { hints, .. } => Some(hints),
        }
    }
}

struct HealthState<'a> {
    id: GlobalId,
    persist_details: Option<(ShardId, &'a PersistClient)>,
    healths: PerWorkerHealthStatus,
    schema: &'static RelationDesc,
    last_reported_status: Option<OverallStatus>,
    halt_with: Option<(StatusNamespace, HealthStatusUpdate)>,
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
            healths: PerWorkerHealthStatus {
                errors_by_worker: vec![Default::default(); worker_count],
            },
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
        hints: &BTreeSet<String>,
        namespaced_errors: &BTreeMap<StatusNamespace, String>,
        now: NowFn,
        // TODO(guswynn): not urgent:
        // Ideally this would be entirely included in the `DefaultWriter`, but that
        // requires a fairly heavy change to the `health_operator`, which hardcodes
        // some use of persist. For now we just leave it and ignore it in tests.
        client: &PersistClient,
        status_shard: ShardId,
        relation_desc: &RelationDesc,
        write_namespaced_map: bool,
    );
    async fn send_halt(&self, id: GlobalId, error: Option<(StatusNamespace, HealthStatusUpdate)>);

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
        hints: &BTreeSet<String>,
        namespaced_errors: &BTreeMap<StatusNamespace, String>,
        now: NowFn,
        client: &PersistClient,
        status_shard: ShardId,
        relation_desc: &RelationDesc,
        write_namespaced_map: bool,
    ) {
        write_to_persist(
            collection_id,
            new_status,
            new_error,
            hints,
            namespaced_errors,
            now,
            client,
            status_shard,
            relation_desc,
            write_namespaced_map,
        )
        .await
    }

    async fn send_halt(&self, id: GlobalId, error: Option<(StatusNamespace, HealthStatusUpdate)>) {
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

/// A health message consumed by the `health_operator`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct HealthStatusMessage {
    /// The index of the object this message describes.
    ///
    /// Useful for sub-objects like sub-sources.
    pub index: usize,
    /// The namespace of the health update.
    pub namespace: StatusNamespace,
    /// The update itself.
    pub update: HealthStatusUpdate,
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
    health_stream: &Stream<G, HealthStatusMessage>,
    // A configuration per _index_. Indexes support things like subsources, and allow the
    // `health_operator` to understand where each sub-object should have its status written down
    // at.
    configs: BTreeMap<usize, ObjectHealthConfig>,
    // An impl of `HealthOperator` that configures the output behavior of this operator.
    health_operator_impl: P,
    // Whether or not we should actually write namespaced errors in the `details` column.
    write_namespaced_map: bool,
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
                        let status = OverallStatus::Starting;
                        health_operator_impl
                            .record_new_status(
                                state.id,
                                status.name(),
                                status.error(),
                                status.hints().unwrap_or(&BTreeSet::new()),
                                status.errors().unwrap_or(&BTreeMap::new()),
                                now.clone(),
                                persist_client,
                                status_shard,
                                state.schema,
                                write_namespaced_map,
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
                for (
                    worker_id,
                    HealthStatusMessage {
                        index: output_index,
                        namespace: ns,
                        update: health_event,
                    },
                ) in rows.drain(..)
                {
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
                        error!(
                            "Health messages for {object_type} {id} passed to \
                              an unexpected worker id: {healthcheck_worker_id}"
                        )
                    }

                    if health_event.should_halt() {
                        *halt_with = Some((ns.clone(), health_event.clone()));
                    }

                    healths.merge_update(worker_id, ns, health_event, !new_round);
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

                    let new_status = healths.decide_status();

                    if Some(&new_status) != last_reported_status.as_ref() {
                        info!(
                            "Health transition for {object_type} {id}: \
                                  {last_reported_status:?} -> {:?}",
                            Some(&new_status)
                        );
                        if let Some((status_shard, persist_client)) = persist_details {
                            health_operator_impl
                                .record_new_status(
                                    *id,
                                    new_status.name(),
                                    new_status.error(),
                                    new_status.hints().unwrap_or(&BTreeSet::new()),
                                    new_status.errors().unwrap_or(&BTreeMap::new()),
                                    now.clone(),
                                    persist_client,
                                    *status_shard,
                                    schema,
                                    write_namespaced_map,
                                )
                                .await;
                        }

                        *last_reported_status = Some(new_status.clone());
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
    Running,
    Stalled {
        error: String,
        hint: Option<String>,
        should_halt: bool,
    },
}

impl HealthStatusUpdate {
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

    /// Whether or not we should halt the dataflow instances and restart it.
    pub(crate) fn should_halt(&self) -> bool {
        match self {
            HealthStatusUpdate::Running => false,
            HealthStatusUpdate::Stalled { should_halt, .. } => *should_halt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    // Actual timely tests for `health_operator`.

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    fn test_health_operator_basic() {
        use Step::*;

        // Test 2 inputs across 2 workers.
        health_operator_runner(
            2,
            2,
            true,
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
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "running".to_string(),
                    ..Default::default()
                }]),
                // Assert the other can be stalled by 1 worker.
                //
                // TODO(guswynn): ideally we could push these updates
                // at the same time, but because they are coming from separately
                // workers, they could end up in different rounds, causing flakes.
                // For now, we just do this.
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::TestScript,
                    input_index: 1,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: "running".to_string(),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
                    input_index: 1,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: "stalled".to_string(),
                    error: Some("testscript: uhoh".to_string()),
                    errors: Some("testscript: uhoh".to_string()),
                    ..Default::default()
                }]),
                // And that it can recover.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    fn test_health_operator_write_namespaced_map() {
        use Step::*;

        // Test 2 inputs across 2 workers.
        health_operator_runner(
            2,
            2,
            // testing this
            false,
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
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
                    input_index: 1,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: "stalled".to_string(),
                    error: Some("testscript: uhoh".to_string()),
                    errors: None,
                    ..Default::default()
                }]),
            ],
        )
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    fn test_health_operator_namespaces() {
        use Step::*;

        // Test 2 inputs across 2 workers.
        health_operator_runner(
            2,
            1,
            true,
            vec![
                AssertStatus(vec![
                    // Assert both inputs started.
                    StatusToAssert {
                        collection_index: 0,
                        status: "starting".to_string(),
                        ..Default::default()
                    },
                ]),
                // Assert that we merge namespaced errors correctly.
                //
                // Note that these all happen on the same worker id.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    error: Some("testscript: uhoh".to_string()),
                    errors: Some("testscript: uhoh".to_string()),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Kafka,
                    input_index: 0,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    error: Some("kafka: uhoh".to_string()),
                    errors: Some("testscript: uhoh, kafka: uhoh".to_string()),
                    ..Default::default()
                }]),
                // And that it can recover.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Kafka,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    error: Some("testscript: uhoh".to_string()),
                    errors: Some("testscript: uhoh".to_string()),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "running".to_string(),
                    ..Default::default()
                }]),
            ],
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    fn test_health_operator_namespace_side_channel() {
        use Step::*;

        health_operator_runner(
            2,
            1,
            true,
            vec![
                AssertStatus(vec![
                    // Assert both inputs started.
                    StatusToAssert {
                        collection_index: 0,
                        status: "starting".to_string(),
                        ..Default::default()
                    },
                ]),
                // Assert that sidechannel namespaces don't downgrade the status
                //
                // Note that these all happen on the same worker id.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Ssh,
                    input_index: 0,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    error: Some("ssh: uhoh".to_string()),
                    errors: Some("ssh: uhoh".to_string()),
                    ..Default::default()
                }]),
                // Note this is from a different work, but it merges as if its from
                // the same worker.
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::Ssh,
                    input_index: 0,
                    update: HealthStatusUpdate::stalled("uhoh2".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    error: Some("ssh: uhoh2".to_string()),
                    errors: Some("ssh: uhoh2".to_string()),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Ssh,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                // We haven't starting running yet, as a `Default` namespace hasn't told us.
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "starting".to_string(),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "running".to_string(),
                    ..Default::default()
                }]),
            ],
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    fn test_health_operator_hints() {
        use Step::*;

        health_operator_runner(
            2,
            1,
            true,
            vec![
                AssertStatus(vec![
                    // Assert both inputs started.
                    StatusToAssert {
                        collection_index: 0,
                        status: "starting".to_string(),
                        ..Default::default()
                    },
                ]),
                // Note that these all happen across worker ids.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::stalled(
                        "uhoh".to_string(),
                        Some("hint1".to_string()),
                    ),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    error: Some("testscript: uhoh".to_string()),
                    errors: Some("testscript: uhoh".to_string()),
                    hint: Some("hint1".to_string()),
                }]),
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::stalled(
                        "uhoh2".to_string(),
                        Some("hint2".to_string()),
                    ),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    // Note the error sorts later so we just use that.
                    error: Some("testscript: uhoh2".to_string()),
                    errors: Some("testscript: uhoh2".to_string()),
                    hint: Some("hint1, hint2".to_string()),
                }]),
                // Update one of the hints
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::stalled(
                        "uhoh2".to_string(),
                        Some("hint3".to_string()),
                    ),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    // Note the error sorts later so we just use that.
                    error: Some("testscript: uhoh2".to_string()),
                    errors: Some("testscript: uhoh2".to_string()),
                    hint: Some("hint1, hint3".to_string()),
                }]),
                // Assert recovery.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: "stalled".to_string(),
                    // Note the error sorts later so we just use that.
                    error: Some("testscript: uhoh2".to_string()),
                    errors: Some("testscript: uhoh2".to_string()),
                    hint: Some("hint3".to_string()),
                }]),
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::TestScript,
                    input_index: 0,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
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
        errors: Option<String>,
        hint: Option<String>,
    }

    /// An update to push into the operator.
    /// Can come from any worker, and from any input.
    #[derive(Debug, Clone)]
    struct TestUpdate {
        worker_id: u64,
        namespace: StatusNamespace,
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
            hints: &BTreeSet<String>,
            namespaced_errors: &BTreeMap<StatusNamespace, String>,
            _now: NowFn,
            _client: &PersistClient,
            _status_shard: ShardId,
            _relation_desc: &RelationDesc,
            write_namespaced_map: bool,
        ) {
            let _ = self.sender.send(StatusToAssert {
                collection_index: *self.input_mapping.get(&collection_id).unwrap(),
                status: new_status.to_string(),
                error: new_error.map(str::to_string),
                errors: if !namespaced_errors.is_empty() && write_namespaced_map {
                    Some(
                        namespaced_errors
                            .iter()
                            .map(|(ns, err)| format!("{}: {}", ns, err))
                            .join(", "),
                    )
                } else {
                    None
                },
                hint: if !hints.is_empty() {
                    Some(hints.iter().join(", "))
                } else {
                    None
                },
            });
        }

        async fn send_halt(
            &self,
            _id: GlobalId,
            _error: Option<(StatusNamespace, HealthStatusUpdate)>,
        ) {
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
    fn health_operator_runner(
        workers: usize,
        inputs: usize,
        write_namespaced_map: bool,
        steps: Vec<Step>,
    ) {
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
                                write_namespaced_map,
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
    ) -> Stream<G, HealthStatusMessage> {
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
                        (
                            element.worker_id,
                            element.input_index,
                            element.namespace,
                            element.update,
                        ),
                    )
                    .await;
            }

            capability.take();
        });

        let output = output.exchange(|d| d.0).map(|d| HealthStatusMessage {
            index: d.1,
            namespace: d.2,
            update: d.3,
        });

        output
    }
}
