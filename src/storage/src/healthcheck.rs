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
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Debug;
use std::rc::Rc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_persist_client::operators::shard_source::ErrorHandler;
use mz_repr::GlobalId;
use mz_storage_client::client::{Status, StatusUpdate};
use serde::{Deserialize, Serialize};
use timely::worker::Worker as TimelyWorker;
use tracing::{error, info};

use crate::event_log::{
    EventLogDataflow, EventLogger, aggregating_worker, event_logger, render_event_log,
};
use crate::internal_control::{InternalCommandSender, InternalStorageCommand};

/// The namespace of the update. The `Ord` impl matter here, later variants are
/// displayed over earlier ones.
///
/// Some namespaces (referred to as "sidechannels") can come from any worker_id,
/// and `Running` statuses from them do not mark the entire object as running.
///
/// Ensure you update `is_sidechannel` when adding variants.
#[derive(
    Copy,
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord
)]
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
    ) {
        self.errors_by_worker[worker].insert(namespace, update);
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
}

impl HealthState {
    fn new(worker_count: usize) -> HealthState {
        HealthState {
            healths: PerWorkerHealthStatus {
                errors_by_worker: vec![Default::default(); worker_count],
            },
            last_reported_status: None,
        }
    }
}

/// A trait that lets a user configure the health dataflow with custom
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
        // requires a fairly heavy change to the health dataflow, which hardcodes
        // some use of persist. For now we just leave it and ignore it in tests.
        write_namespaced_map: bool,
    );
    fn send_halt(&self, id: GlobalId, error: Option<(StatusNamespace, HealthStatusUpdate)>);
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

/// A health message reported through a [`HealthReporter`].
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

/// The name under which the health logger is registered with the timely worker.
const HEALTH_LOGGER_NAME: &str = "materialize/storage/health";

/// The kind of object a dataflow reports health about. Used in log lines.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HealthObjectType {
    Source,
    Sink,
}

impl fmt::Display for HealthObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthObjectType::Source => write!(f, "source"),
            HealthObjectType::Sink => write!(f, "sink"),
        }
    }
}

/// One worker's instance of a dataflow that reports health.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct HealthObject {
    /// The primary object of the dataflow. Messages without an explicit id are about it, and
    /// it is the only object that may halt the dataflow.
    id: GlobalId,
    /// The timely index of the dataflow instance.
    ///
    /// All workers render storage dataflows in the same order, so the index identifies the same
    /// instance on every worker, and a re-rendered dataflow has a larger index than its
    /// predecessor.
    generation: usize,
    /// The worker that logged the event.
    worker: usize,
}

/// Configuration of a dataflow instance's health reporting, fixed at rendering time.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthConfig {
    /// A description of the object type, used in log lines.
    pub object_type: HealthObjectType,
    /// Objects besides the primary object that report health, and are marked `Starting`
    /// when the dataflow instance starts. Messages about any other object are ignored.
    pub mark_starting: BTreeSet<GlobalId>,
    /// Whether to write namespaced errors in the `details` column.
    pub write_namespaced_map: bool,
    /// How long to wait before initiating a `SuspendAndRestart` command, to prevent hot restart
    /// loops.
    pub suspend_and_restart_delay: Duration,
}

/// An event in a dataflow instance's health reporting lifecycle.
///
/// Every worker logs `Register` before any `Update` for the same instance, and `Deregister`
/// after. Timely channels are FIFO per pair of workers, so the aggregating worker observes each
/// worker's events of an instance in that order, though it interleaves different workers'
/// events arbitrarily.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum HealthEvent {
    Register {
        object: HealthObject,
        config: HealthConfig,
    },
    Update {
        object: HealthObject,
        message: HealthStatusMessage,
    },
    Deregister {
        object: HealthObject,
    },
}

impl HealthEvent {
    fn object(&self) -> &HealthObject {
        match self {
            HealthEvent::Register { object, .. }
            | HealthEvent::Update { object, .. }
            | HealthEvent::Deregister { object } => object,
        }
    }
}

/// Reports health status messages of one worker's instance of a storage dataflow.
///
/// Reporting is local to the worker. The health dataflow moves messages to the worker that
/// aggregates the instance's status.
#[derive(Clone)]
pub struct HealthReporter {
    logger: EventLogger<HealthEvent>,
    object: HealthObject,
}

impl fmt::Debug for HealthReporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HealthReporter")
            .field("object", &self.object)
            .finish_non_exhaustive()
    }
}

impl HealthReporter {
    /// Registers health reporting for the dataflow instance that is rendered next on `worker`.
    ///
    /// Must be called on every worker, immediately before rendering the dataflow. Reporting for
    /// the instance ends when the returned token is dropped, which must happen on every worker.
    pub fn register(
        worker: &TimelyWorker,
        id: GlobalId,
        config: HealthConfig,
    ) -> (HealthReporter, HealthToken) {
        let logger = event_logger::<HealthEvent>(worker, HEALTH_LOGGER_NAME)
            .expect("health dataflow must be rendered");
        let object = HealthObject {
            id,
            generation: worker.next_dataflow_index(),
            worker: worker.index(),
        };
        logger.log(HealthEvent::Register { object, config });
        let reporter = HealthReporter { logger, object };
        let token = HealthToken {
            reporter: reporter.clone(),
        };
        (reporter, token)
    }

    /// Reports a health status message.
    pub fn report(&self, message: HealthStatusMessage) {
        self.logger.log(HealthEvent::Update {
            object: self.object,
            message,
        });
    }

    /// Returns an error handler that reports errors as halting health messages about the primary
    /// object, which suspends and restarts the dataflow instance.
    pub fn error_handler(&self, context: &'static str) -> ErrorHandler {
        let reporter = self.clone();
        ErrorHandler::signal(move |e| {
            reporter.report(HealthStatusMessage {
                id: None,
                namespace: StatusNamespace::Internal,
                update: HealthStatusUpdate::halting(format!("{context}: {e:#}"), None),
            })
        })
    }
}

/// Ends health reporting for a dataflow instance when dropped.
pub struct HealthToken {
    reporter: HealthReporter,
}

impl Drop for HealthToken {
    fn drop(&mut self) {
        self.reporter.logger.log(HealthEvent::Deregister {
            object: self.reporter.object,
        });
    }
}

/// The aggregated health of one dataflow instance.
struct InstanceHealth {
    generation: usize,
    config: HealthConfig,
    /// Health by object, including the primary object.
    states: BTreeMap<GlobalId, HealthState>,
    /// Workers that deregistered the instance. The instance reports no further status once any
    /// worker deregistered it, and is forgotten once all workers have.
    deregistered: BTreeSet<usize>,
    halt: Halt,
}

enum Halt {
    None,
    /// A halting message was received, and the halt of the primary object is sent at `at`.
    Pending {
        at: Instant,
        /// The object whose halting message is the reason.
        reason_id: GlobalId,
        reason: (StatusNamespace, HealthStatusUpdate),
    },
    /// The halt was sent. Each instance halts at most once, as the halt replaces it.
    Sent,
}

/// State of the health aggregation on one worker.
struct HealthAggregator<P> {
    now: NowFn,
    worker_count: usize,
    worker_index: usize,
    writer: P,
    instances: BTreeMap<GlobalId, InstanceHealth>,
}

impl<P: HealthOperator> HealthAggregator<P> {
    fn record_status(&self, id: GlobalId, status: &OverallStatus, write_namespaced_map: bool) {
        let timestamp = mz_ore::now::to_datetime((self.now)());
        self.writer.record_new_status(
            id,
            timestamp,
            status.into(),
            status.error(),
            &status.hints(),
            status.errors().unwrap_or(&BTreeMap::new()),
            write_namespaced_map,
        );
    }

    fn register(&mut self, object: HealthObject, config: HealthConfig) {
        if let Some(instance) = self.instances.get(&object.id) {
            // Either another worker registered this instance already, or the event belongs to
            // an instance that was replaced.
            if instance.generation >= object.generation {
                return;
            }
        }

        let mut ids = config.mark_starting.clone();
        ids.insert(object.id);
        let mut states = BTreeMap::new();
        for id in ids {
            let mut state = HealthState::new(self.worker_count);
            let status = OverallStatus::Starting;
            self.record_status(id, &status, config.write_namespaced_map);
            state.last_reported_status = Some(status);
            states.insert(id, state);
        }

        self.instances.insert(
            object.id,
            InstanceHealth {
                generation: object.generation,
                config,
                states,
                deregistered: BTreeSet::new(),
                halt: Halt::None,
            },
        );
    }

    fn deregister(&mut self, object: HealthObject) {
        let Some(instance) = self.instances.get_mut(&object.id) else {
            return;
        };
        if instance.generation != object.generation {
            return;
        }
        instance.deregistered.insert(object.worker);
        if instance.deregistered.len() == self.worker_count {
            self.instances.remove(&object.id);
        }
    }

    /// Applies events in the order they are passed, and records the resulting status
    /// transitions.
    ///
    /// The latest message per worker, object, and namespace replaces the previous status.
    fn apply(&mut self, events: impl IntoIterator<Item = HealthEvent>) {
        let mut changed = BTreeSet::new();
        for event in events {
            let object = *event.object();
            let expected_worker = aggregating_worker(object.id, self.worker_count);
            if expected_worker != self.worker_index {
                error!(
                    "Health event for {} passed to an unexpected worker: {}, expected {}",
                    object.id, self.worker_index, expected_worker
                );
            }

            match event {
                HealthEvent::Register { object, config } => self.register(object, config),
                HealthEvent::Deregister { object } => self.deregister(object),
                HealthEvent::Update { object, message } => {
                    let Some(instance) = self.instances.get_mut(&object.id) else {
                        continue;
                    };
                    if instance.generation != object.generation || !instance.deregistered.is_empty()
                    {
                        continue;
                    }
                    let HealthStatusMessage {
                        id,
                        namespace,
                        update,
                    } = message;
                    let id = id.unwrap_or(object.id);
                    // A message about an object that was not marked starting has no status
                    // collection to write to.
                    let Some(state) = instance.states.get_mut(&id) else {
                        continue;
                    };

                    if update.should_halt() {
                        match &mut instance.halt {
                            Halt::None => {
                                let delay = instance.config.suspend_and_restart_delay;
                                info!(
                                    "Scheduling suspend-and-restart of {} because of {update:?} \
                                     after {delay:?} delay",
                                    object.id,
                                );
                                instance.halt = Halt::Pending {
                                    at: Instant::now() + delay,
                                    reason_id: id,
                                    reason: (namespace, update.clone()),
                                };
                            }
                            // The latest halting message becomes the reason, but doesn't delay
                            // the halt. Producers report halting errors about subobjects
                            // alongside the primary object, so the primary's take precedence.
                            Halt::Pending {
                                reason_id, reason, ..
                            } => {
                                if id == object.id || *reason_id != object.id {
                                    *reason_id = id;
                                    *reason = (namespace, update.clone());
                                }
                            }
                            Halt::Sent => {}
                        }
                    }

                    state.healths.merge_update(object.worker, namespace, update);
                    changed.insert((object.id, id));
                }
            }
        }

        for (instance_id, id) in changed {
            let Some(instance) = self.instances.get_mut(&instance_id) else {
                continue;
            };
            if !instance.deregistered.is_empty() {
                continue;
            }
            let Some(state) = instance.states.get_mut(&id) else {
                continue;
            };
            let new_status = state.healths.decide_status();
            if Some(&new_status) != state.last_reported_status.as_ref() {
                info!(
                    "Health transition for {} {id}: {:?} -> {:?}",
                    instance.config.object_type,
                    state.last_reported_status,
                    Some(&new_status),
                );
                state.last_reported_status = Some(new_status.clone());
                let write_namespaced_map = instance.config.write_namespaced_map;
                self.record_status(id, &new_status, write_namespaced_map);
            }
        }
    }

    /// The earliest time a pending halt is due, if any.
    fn next_halt(&self) -> Option<Instant> {
        self.instances
            .values()
            .filter_map(|instance| match &instance.halt {
                Halt::Pending { at, .. } if instance.deregistered.is_empty() => Some(*at),
                _ => None,
            })
            .min()
    }

    /// Sends all halts that are due.
    fn send_due_halts(&mut self) {
        let now = Instant::now();

        for (instance_id, instance) in self.instances.iter_mut() {
            if !instance.deregistered.is_empty() {
                continue;
            }
            if let Halt::Pending { at, .. } = &instance.halt {
                if *at <= now {
                    let Halt::Pending {
                        reason_id, reason, ..
                    } = std::mem::replace(&mut instance.halt, Halt::Sent)
                    else {
                        unreachable!()
                    };
                    mz_ore::soft_assert_or_log!(
                        reason_id == *instance_id,
                        "sub{}s should not produce halting errors, however {:?} halted while \
                         primary {} is {:?}",
                        instance.config.object_type,
                        reason_id,
                        instance.config.object_type,
                        instance_id,
                    );
                    info!(
                        "Broadcasting suspend-and-restart command for {instance_id} because \
                         of {reason:?}",
                    );
                    self.writer.send_halt(*instance_id, Some(reason));
                }
            }
        }
    }
}

/// Renders the dataflow that aggregates health reported through [`HealthReporter`]s, and
/// registers the health logger with the worker.
///
/// Must be called once per worker, at the same point in the dataflow rendering order on every
/// worker, before any [`HealthReporter::register`]. The dataflow runs until the returned token
/// is dropped.
pub fn render_health_dataflow<P>(
    worker: &mut TimelyWorker,
    now: NowFn,
    writer: P,
) -> EventLogDataflow
where
    P: HealthOperator + 'static,
{
    let worker_count = worker.peers();
    let worker_index = worker.index();
    render_event_log(
        worker,
        "Dataflow: storage health",
        HEALTH_LOGGER_NAME,
        move |event: &HealthEvent| {
            u64::cast_from(aggregating_worker(event.object().id, worker_count))
        },
        {
            let mut aggregator = HealthAggregator {
                now,
                worker_count,
                worker_index,
                writer,
                instances: BTreeMap::new(),
            };
            // The deadline of the delayed activation requested last. Timely keeps every
            // requested activation, so only a new earliest deadline needs another.
            let mut armed: Option<Instant> = None;
            move |events, activator| {
                if !events.is_empty() {
                    aggregator.apply(events);
                }
                aggregator.send_due_halts();
                let now = Instant::now();
                if armed.is_some_and(|armed| armed <= now) {
                    armed = None;
                }
                if let Some(at) = aggregator.next_halt() {
                    if armed.is_none_or(|armed| at < armed) {
                        activator.activate_after(at.saturating_duration_since(now));
                        armed = Some(at);
                    }
                }
            }
        },
    )
}

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
mod tests;
