// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthcheck common

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use differential_dataflow::Hashable;
use futures::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_repr::GlobalId;
use mz_storage_client::client::{Status, StatusUpdate};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use tracing::{error, info};

use crate::internal_control::{InternalCommandSender, InternalStorageCommand};

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
    Kafka,
    Postgres,
    MySql,
    SqlServer,
    Ssh,
    Upsert,
    Decode,
    Iceberg,
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
            Kafka => write!(f, "kafka"),
            Postgres => write!(f, "postgres"),
            MySql => write!(f, "mysql"),
            SqlServer => write!(f, "sql-server"),
            Ssh => write!(f, "ssh"),
            Upsert => write!(f, "upsert"),
            Decode => write!(f, "decode"),
            Internal => write!(f, "internal"),
            Iceberg => write!(f, "iceberg"),
        }
    }
}

#[derive(Debug)]
struct PerWorkerHealthStatus {
    pub(crate) errors_by_worker: Vec<BTreeMap<StatusNamespace, HealthStatusUpdate>>,
}

impl PerWorkerHealthStatus {
    fn merge_update(
        &mut self,
        worker: usize,
        namespace: StatusNamespace,
        update: HealthStatusUpdate,
        only_greater: bool,
    ) {
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
            for (ns, ns_status) in status.iter() {
                match ns_status {
                    // HealthStatusUpdate::Ceased is currently unused, so just
                    // treat it as if it were a normal error.
                    //
                    // TODO: redesign ceased status database-issues#7687
                    HealthStatusUpdate::Ceased { error } => {
                        if Some(error) > namespaced_errors.get(ns).as_deref() {
                            namespaced_errors.insert(*ns, error.to_string());
                        }
                    }
                    HealthStatusUpdate::Stalled { error, hint, .. } => {
                        if Some(error) > namespaced_errors.get(ns).as_deref() {
                            namespaced_errors.insert(*ns, error.to_string());
                        }

                        if let Some(hint) = hint {
                            hints.insert(hint.to_string());
                        }
                    }
                    HealthStatusUpdate::Running => {
                        if !ns.is_sidechannel() {
                            output_status = OverallStatus::Running;
                        }
                    }
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
    Ceased {
        error: String,
    },
}

impl OverallStatus {
    /// The user-readable error string, if there is one.
    pub(crate) fn error(&self) -> Option<&str> {
        match self {
            OverallStatus::Starting | OverallStatus::Running => None,
            OverallStatus::Stalled { error, .. } | OverallStatus::Ceased { error, .. } => {
                Some(error)
            }
        }
    }

    /// A set of namespaced errors, if there are any.
    pub(crate) fn errors(&self) -> Option<&BTreeMap<StatusNamespace, String>> {
        match self {
            OverallStatus::Starting | OverallStatus::Running | OverallStatus::Ceased { .. } => None,
            OverallStatus::Stalled {
                namespaced_errors, ..
            } => Some(namespaced_errors),
        }
    }

    /// A set of hints, if there are any.
    pub(crate) fn hints(&self) -> BTreeSet<String> {
        match self {
            OverallStatus::Starting | OverallStatus::Running | OverallStatus::Ceased { .. } => {
                BTreeSet::new()
            }
            OverallStatus::Stalled { hints, .. } => hints.clone(),
        }
    }
}

impl<'a> From<&'a OverallStatus> for Status {
    fn from(val: &'a OverallStatus) -> Self {
        match val {
            OverallStatus::Starting => Status::Starting,
            OverallStatus::Running => Status::Running,
            OverallStatus::Stalled { .. } => Status::Stalled,
            OverallStatus::Ceased { .. } => Status::Ceased,
        }
    }
}

#[derive(Debug)]
struct HealthState {
    healths: PerWorkerHealthStatus,
    last_reported_status: Option<OverallStatus>,
    halt_with: Option<(StatusNamespace, HealthStatusUpdate)>,
}

impl HealthState {
    fn new(worker_count: usize) -> HealthState {
        HealthState {
            healths: PerWorkerHealthStatus {
                errors_by_worker: vec![Default::default(); worker_count],
            },
            last_reported_status: None,
            halt_with: None,
        }
    }
}

/// A trait that lets a user configure the `health_operator` with custom
/// behavior. This is mostly useful for testing, and the [`DefaultWriter`]
/// should be the correct implementation for everyone.
pub trait HealthOperator {
    /// Record a new status.
    fn record_new_status(
        &self,
        collection_id: GlobalId,
        ts: DateTime<Utc>,
        new_status: Status,
        new_error: Option<&str>,
        hints: &BTreeSet<String>,
        namespaced_errors: &BTreeMap<StatusNamespace, String>,
        // TODO(guswynn): not urgent:
        // Ideally this would be entirely included in the `DefaultWriter`, but that
        // requires a fairly heavy change to the `health_operator`, which hardcodes
        // some use of persist. For now we just leave it and ignore it in tests.
        write_namespaced_map: bool,
    );
    fn send_halt(&self, id: GlobalId, error: Option<(StatusNamespace, HealthStatusUpdate)>);

    /// Optionally override the chosen worker index. Default is semi-random.
    /// Only useful for tests.
    fn chosen_worker(&self) -> Option<usize> {
        None
    }
}

/// A default `HealthOperator` for use in normal cases.
pub struct DefaultWriter {
    pub command_tx: InternalCommandSender,
    pub updates: Rc<RefCell<Vec<StatusUpdate>>>,
}

impl HealthOperator for DefaultWriter {
    fn record_new_status(
        &self,
        collection_id: GlobalId,
        ts: DateTime<Utc>,
        status: Status,
        new_error: Option<&str>,
        hints: &BTreeSet<String>,
        namespaced_errors: &BTreeMap<StatusNamespace, String>,
        write_namespaced_map: bool,
    ) {
        self.updates.borrow_mut().push(StatusUpdate {
            id: collection_id,
            timestamp: ts,
            status,
            error: new_error.map(|e| e.to_string()),
            hints: hints.clone(),
            namespaced_errors: if write_namespaced_map {
                namespaced_errors
                    .iter()
                    .map(|(ns, val)| (ns.to_string(), val.clone()))
                    .collect()
            } else {
                BTreeMap::new()
            },
            replica_id: None,
        });
    }

    fn send_halt(&self, id: GlobalId, error: Option<(StatusNamespace, HealthStatusUpdate)>) {
        self.command_tx
            .send(InternalStorageCommand::SuspendAndRestart {
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
    /// The object that this status message is about. When None, it refers to the entire ingestion
    /// as a whole. When Some, it refers to a specific subsource.
    pub id: Option<GlobalId>,
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
/// The `OutputIndex` values that come across `health_stream` must be a strict subset of those in
/// `configs`'s keys.
pub(crate) fn health_operator<G, P>(
    scope: &G,
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
    // An impl of `HealthOperator` that configures the output behavior of this operator.
    health_operator_impl: P,
    // Whether or not we should actually write namespaced errors in the `details` column.
    write_namespaced_map: bool,
    // How long to wait before initiating a `SuspendAndRestart` command, to
    // prevent hot restart loops.
    suspend_and_restart_delay: Duration,
) -> PressOnDropButton
where
    G: Scope,
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
        usize::cast_from(mark_starting.iter().next().hashed()) % worker_count
    };

    let is_active_worker = chosen_worker_id == healthcheck_worker_id;

    let operator_name = format!("healthcheck({})", healthcheck_worker_id);
    let mut health_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    let mut input = health_op.new_disconnected_input(
        &health_stream,
        Exchange::new(move |_| u64::cast_from(chosen_worker_id)),
    );

    let button = health_op.build(move |mut _capabilities| async move {
        let mut health_states: BTreeMap<_, _> = mark_starting
            .iter()
            .copied()
            .chain([halting_id])
            .map(|id| (id, HealthState::new(worker_count)))
            .collect();

        // Write the initial starting state to the status shard for all managed objects
        if is_active_worker {
            for (id, state) in health_states.iter_mut() {
                let status = OverallStatus::Starting;
                let timestamp = mz_ore::now::to_datetime(now());
                health_operator_impl.record_new_status(
                    *id,
                    timestamp,
                    (&status).into(),
                    status.error(),
                    &status.hints(),
                    status.errors().unwrap_or(&BTreeMap::new()),
                    write_namespaced_map,
                );

                state.last_reported_status = Some(status);
            }
        }

        let mut outputs_seen = BTreeMap::<GlobalId, BTreeSet<_>>::new();
        while let Some(event) = input.next().await {
            if let AsyncEvent::Data(_cap, rows) = event {
                for (worker_id, message) in rows {
                    let HealthStatusMessage {
                        id,
                        namespace: ns,
                        update: health_event,
                    } = message;
                    let id = id.unwrap_or(halting_id);
                    let HealthState {
                        healths, halt_with, ..
                    } = match health_states.get_mut(&id) {
                        Some(health) => health,
                        // This is a health status update for a sub-object_type that we did not request to
                        // be generated, which means it doesn't have a GlobalId and should not be
                        // propagated to the shard.
                        None => continue,
                    };

                    // Its important to track `new_round` per-namespace, so namespaces are reasoned
                    // about in `merge_update` independently.
                    let new_round = outputs_seen
                        .entry(id)
                        .or_insert_with(BTreeSet::new)
                        .insert(ns.clone());

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

                while let Some((id, _)) = outputs_seen.pop_first() {
                    let HealthState {
                        healths,
                        last_reported_status,
                        halt_with,
                    } = health_states.get_mut(&id).expect("known to exist");

                    let new_status = healths.decide_status();

                    if Some(&new_status) != last_reported_status.as_ref() {
                        info!(
                            "Health transition for {object_type} {id}: \
                                  {last_reported_status:?} -> {:?}",
                            Some(&new_status)
                        );

                        let timestamp = mz_ore::now::to_datetime(now());
                        health_operator_impl.record_new_status(
                            id,
                            timestamp,
                            (&new_status).into(),
                            new_status.error(),
                            &new_status.hints(),
                            new_status.errors().unwrap_or(&BTreeMap::new()),
                            write_namespaced_map,
                        );

                        *last_reported_status = Some(new_status.clone());
                    }

                    // Set halt with if None.
                    if halt_with_outer.is_none() && halt_with.is_some() {
                        halt_with_outer = Some((id, halt_with.clone()));
                    }
                }

                // TODO(aljoscha): Instead of threading through the
                // `should_halt` bit, we can give an internal command sender
                // directly to the places where `should_halt = true` originates.
                // We should definitely do that, but this is okay for a PoC.
                if let Some((id, halt_with)) = halt_with_outer {
                    mz_ore::soft_assert_or_log!(
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
                        halt_with, suspend_and_restart_delay
                    );
                    tokio::time::sleep(suspend_and_restart_delay).await;
                    health_operator_impl.send_halt(id, halt_with);
                }
            }
        }
    });

    button.press_on_drop()
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
    Ceased {
        error: String,
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

    // TODO: redesign ceased status database-issues#7687
    // Generates a ceasing [`HealthStatusUpdate`] with `update`.
    // pub(crate) fn ceasing(error: String) -> Self {
    //     HealthStatusUpdate::Ceased { error }
    // }

    /// Whether or not we should halt the dataflow instances and restart it.
    pub(crate) fn should_halt(&self) -> bool {
        match self {
            HealthStatusUpdate::Running |
            // HealthStatusUpdate::Ceased should never halt because it can occur
            // at the subsource level and should not cause the entire dataflow
            // to halt. Instead, the dataflow itself should handle shutting
            // itself down if need be.
            HealthStatusUpdate::Ceased { .. } => false,
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
                        status: Status::Starting,
                        ..Default::default()
                    },
                    StatusToAssert {
                        collection_index: 1,
                        status: Status::Starting,
                        ..Default::default()
                    },
                ]),
                // Update and assert one is running.
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Running,
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
                    namespace: StatusNamespace::Generator,
                    id: Some(GlobalId::User(1)),
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: Status::Running,
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: Some(GlobalId::User(1)),
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: Status::Stalled,
                    error: Some("generator: uhoh".to_string()),
                    errors: Some("generator: uhoh".to_string()),
                    ..Default::default()
                }]),
                // And that it can recover.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: Some(GlobalId::User(1)),
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: Status::Running,
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
                        status: Status::Starting,
                        ..Default::default()
                    },
                    StatusToAssert {
                        collection_index: 1,
                        status: Status::Starting,
                        ..Default::default()
                    },
                ]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: Some(GlobalId::User(1)),
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 1,
                    status: Status::Stalled,
                    error: Some("generator: uhoh".to_string()),
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
                        status: Status::Starting,
                        ..Default::default()
                    },
                ]),
                // Assert that we merge namespaced errors correctly.
                //
                // Note that these all happen on the same worker id.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    error: Some("generator: uhoh".to_string()),
                    errors: Some("generator: uhoh".to_string()),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Kafka,
                    id: None,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    error: Some("kafka: uhoh".to_string()),
                    errors: Some("generator: uhoh, kafka: uhoh".to_string()),
                    ..Default::default()
                }]),
                // And that it can recover.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Kafka,
                    id: None,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    error: Some("generator: uhoh".to_string()),
                    errors: Some("generator: uhoh".to_string()),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Running,
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
                        status: Status::Starting,
                        ..Default::default()
                    },
                ]),
                // Assert that sidechannel namespaces don't downgrade the status
                //
                // Note that these all happen on the same worker id.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Ssh,
                    id: None,
                    update: HealthStatusUpdate::stalled("uhoh".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    error: Some("ssh: uhoh".to_string()),
                    errors: Some("ssh: uhoh".to_string()),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Ssh,
                    id: None,
                    update: HealthStatusUpdate::stalled("uhoh2".to_string(), None),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    error: Some("ssh: uhoh2".to_string()),
                    errors: Some("ssh: uhoh2".to_string()),
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Ssh,
                    id: None,
                    update: HealthStatusUpdate::running(),
                }),
                // We haven't starting running yet, as a `Default` namespace hasn't told us.
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Starting,
                    ..Default::default()
                }]),
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Running,
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
                        status: Status::Starting,
                        ..Default::default()
                    },
                ]),
                // Note that these all happen across worker ids.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::stalled(
                        "uhoh".to_string(),
                        Some("hint1".to_string()),
                    ),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    error: Some("generator: uhoh".to_string()),
                    errors: Some("generator: uhoh".to_string()),
                    hint: Some("hint1".to_string()),
                }]),
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::stalled(
                        "uhoh2".to_string(),
                        Some("hint2".to_string()),
                    ),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    // Note the error sorts later so we just use that.
                    error: Some("generator: uhoh2".to_string()),
                    errors: Some("generator: uhoh2".to_string()),
                    hint: Some("hint1, hint2".to_string()),
                }]),
                // Update one of the hints
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::stalled(
                        "uhoh2".to_string(),
                        Some("hint3".to_string()),
                    ),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    // Note the error sorts later so we just use that.
                    error: Some("generator: uhoh2".to_string()),
                    errors: Some("generator: uhoh2".to_string()),
                    hint: Some("hint1, hint3".to_string()),
                }]),
                // Assert recovery.
                Update(TestUpdate {
                    worker_id: 0,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Stalled,
                    // Note the error sorts later so we just use that.
                    error: Some("generator: uhoh2".to_string()),
                    errors: Some("generator: uhoh2".to_string()),
                    hint: Some("hint3".to_string()),
                }]),
                Update(TestUpdate {
                    worker_id: 1,
                    namespace: StatusNamespace::Generator,
                    id: None,
                    update: HealthStatusUpdate::running(),
                }),
                AssertStatus(vec![StatusToAssert {
                    collection_index: 0,
                    status: Status::Running,
                    ..Default::default()
                }]),
            ],
        );
    }

    // The below is ALL test infrastructure for the above

    use mz_ore::assert_err;
    use timely::container::CapacityContainerBuilder;
    use timely::dataflow::Scope;
    use timely::dataflow::operators::Enter;
    use timely::dataflow::operators::exchange::Exchange;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

    /// A status to assert.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct StatusToAssert {
        collection_index: usize,
        status: Status,
        error: Option<String>,
        errors: Option<String>,
        hint: Option<String>,
    }

    impl Default for StatusToAssert {
        fn default() -> Self {
            StatusToAssert {
                collection_index: Default::default(),
                status: Status::Running,
                error: Default::default(),
                errors: Default::default(),
                hint: Default::default(),
            }
        }
    }

    /// An update to push into the operator.
    /// Can come from any worker, and from any input.
    #[derive(Debug, Clone)]
    struct TestUpdate {
        worker_id: u64,
        namespace: StatusNamespace,
        id: Option<GlobalId>,
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

    impl HealthOperator for TestWriter {
        fn record_new_status(
            &self,
            collection_id: GlobalId,
            _ts: DateTime<Utc>,
            status: Status,
            new_error: Option<&str>,
            hints: &BTreeSet<String>,
            namespaced_errors: &BTreeMap<StatusNamespace, String>,
            write_namespaced_map: bool,
        ) {
            let _ = self.sender.send(StatusToAssert {
                collection_index: *self.input_mapping.get(&collection_id).unwrap(),
                status,
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

        fn send_halt(&self, _id: GlobalId, _error: Option<(StatusNamespace, HealthStatusUpdate)>) {
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
                            let input = producer(root_scope.clone(), in_rx).enter(scope);
                            Box::leak(Box::new(health_operator(
                                scope,
                                mz_ore::now::SYSTEM_TIME.clone(),
                                inputs.keys().copied().collect(),
                                *inputs.first_key_value().unwrap().0,
                                "source_test",
                                &input,
                                TestWriter {
                                    sender: out_tx,
                                    input_mapping: inputs,
                                },
                                write_namespaced_map,
                                Duration::from_secs(5),
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
                    assert_err!(out_rx.try_recv());
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
        let (output_handle, output) = iterator.new_output::<CapacityContainerBuilder<Vec<_>>>();

        let index = scope.index();
        iterator.build(|mut caps| async move {
            // We input and assert outputs on the first worker.
            if index != 0 {
                return;
            }
            let mut capability = Some(caps.pop().unwrap());
            while let Some(element) = input.recv().await {
                output_handle.give(
                    capability.as_ref().unwrap(),
                    (
                        element.worker_id,
                        element.id,
                        element.namespace,
                        element.update,
                    ),
                );
            }

            capability.take();
        });

        let output = output.exchange(|d| d.0).map(|d| HealthStatusMessage {
            id: d.1,
            namespace: d.2,
            update: d.3,
        });

        output
    }
}
