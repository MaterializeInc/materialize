// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use mz_ore::assert_err;

use super::*;

// Actual timely tests for the health dataflow.

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
fn test_health_basic() {
    use Step::*;

    // Test 2 inputs across 2 workers.
    health_runner(
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
fn test_health_write_namespaced_map() {
    use Step::*;

    // Test 2 inputs across 2 workers.
    health_runner(
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
fn test_health_namespaces() {
    use Step::*;

    // Test 2 inputs across 2 workers.
    health_runner(
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
fn test_health_namespace_side_channel() {
    use Step::*;

    health_runner(
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
fn test_health_hints() {
    use Step::*;

    health_runner(
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
                update: HealthStatusUpdate::stalled("uhoh".to_string(), Some("hint1".to_string())),
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
                update: HealthStatusUpdate::stalled("uhoh2".to_string(), Some("hint2".to_string())),
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
                update: HealthStatusUpdate::stalled("uhoh2".to_string(), Some("hint3".to_string())),
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

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
fn test_health_restart_ignores_stale_updates() {
    // As a suspend-and-restart does.
    restart_ignores_stale_updates(false);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
fn test_health_replacement_ignores_stale_updates() {
    // As re-rendering over an existing token does.
    restart_ignores_stale_updates(true);
}

fn restart_ignores_stale_updates(register_first: bool) {
    use Step::*;

    health_runner(
        2,
        1,
        true,
        vec![
            AssertStatus(vec![StatusToAssert {
                collection_index: 0,
                status: Status::Starting,
                ..Default::default()
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
            Restart { register_first },
            AssertStatus(vec![StatusToAssert {
                collection_index: 0,
                status: Status::Starting,
                ..Default::default()
            }]),
            // Reported by the replaced instance, and so ignored.
            StaleUpdate(TestUpdate {
                worker_id: 0,
                namespace: StatusNamespace::Generator,
                id: None,
                update: HealthStatusUpdate::stalled("stale".to_string(), None),
            }),
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
fn test_health_halts_once() {
    use Step::*;

    health_runner(
        2,
        1,
        true,
        vec![
            AssertStatus(vec![StatusToAssert {
                collection_index: 0,
                status: Status::Starting,
                ..Default::default()
            }]),
            Update(TestUpdate {
                worker_id: 0,
                namespace: StatusNamespace::Kafka,
                id: None,
                update: HealthStatusUpdate::halting("boom".to_string(), None),
            }),
            AssertStatus(vec![StatusToAssert {
                collection_index: 0,
                status: Status::Stalled,
                error: Some("kafka: boom".to_string()),
                errors: Some("kafka: boom".to_string()),
                ..Default::default()
            }]),
            Update(TestUpdate {
                worker_id: 1,
                namespace: StatusNamespace::Kafka,
                id: None,
                update: HealthStatusUpdate::halting("boom".to_string(), None),
            }),
            AssertHalt(0),
            // The instance already halted, so neither worker halts it again.
            Update(TestUpdate {
                worker_id: 1,
                namespace: StatusNamespace::Kafka,
                id: None,
                update: HealthStatusUpdate::halting("boom again".to_string(), None),
            }),
            Update(TestUpdate {
                worker_id: 0,
                namespace: StatusNamespace::Kafka,
                id: None,
                update: HealthStatusUpdate::halting("boom again".to_string(), None),
            }),
            AssertStatus(vec![StatusToAssert {
                collection_index: 0,
                status: Status::Stalled,
                error: Some("kafka: boom again".to_string()),
                errors: Some("kafka: boom again".to_string()),
                ..Default::default()
            }]),
        ],
    );
}

// Tests of the aggregator alone, which apply exactly the batches they construct.

const PRIMARY: GlobalId = GlobalId::User(0);

fn test_aggregator(worker_count: usize) -> (HealthAggregator<TestWriter>, Receiver<TestOutput>) {
    let (sender, receiver) = channel();
    let aggregator = HealthAggregator {
        now: mz_ore::now::SYSTEM_TIME.clone(),
        worker_count,
        worker_index: aggregating_worker(PRIMARY, worker_count),
        writer: TestWriter {
            sender,
            input_mapping: [(PRIMARY, 0)].into_iter().collect(),
        },
        instances: BTreeMap::new(),
    };
    (aggregator, receiver)
}

fn object(worker: usize) -> HealthObject {
    HealthObject {
        id: PRIMARY,
        generation: 0,
        worker,
    }
}

fn register(worker: usize) -> HealthEvent {
    HealthEvent::Register {
        object: object(worker),
        config: HealthConfig {
            object_type: HealthObjectType::Source,
            mark_starting: BTreeSet::new(),
            write_namespaced_map: false,
            suspend_and_restart_delay: Duration::ZERO,
        },
    }
}

fn update(worker: usize, update: HealthStatusUpdate) -> HealthEvent {
    HealthEvent::Update {
        object: object(worker),
        message: HealthStatusMessage {
            id: None,
            namespace: StatusNamespace::Kafka,
            update,
        },
    }
}

fn deregister(worker: usize) -> HealthEvent {
    HealthEvent::Deregister {
        object: object(worker),
    }
}

fn statuses(receiver: &Receiver<TestOutput>) -> Vec<Status> {
    receiver
        .try_iter()
        .map(|output| match output {
            TestOutput::Status(status) => status.status,
            TestOutput::Halt(_) => panic!("unexpected halt"),
        })
        .collect()
}

#[mz_ore::test]
fn test_aggregator_latest_update_wins_within_batch() {
    let (mut aggregator, receiver) = test_aggregator(1);
    aggregator.apply([
        register(0),
        update(0, HealthStatusUpdate::stalled("boom".to_string(), None)),
        update(0, HealthStatusUpdate::running()),
    ]);
    assert_eq!(statuses(&receiver), [Status::Starting, Status::Running]);
}

#[mz_ore::test]
fn test_aggregator_forgets_instance_after_all_workers_deregister() {
    let (mut aggregator, receiver) = test_aggregator(2);
    aggregator.apply([register(0), register(1), deregister(0)]);
    assert!(aggregator.instances.contains_key(&PRIMARY));
    // An instance that any worker deregistered reports nothing further.
    aggregator.apply([update(1, HealthStatusUpdate::running())]);
    aggregator.apply([deregister(1)]);
    assert!(aggregator.instances.is_empty());
    assert_eq!(statuses(&receiver), [Status::Starting]);
}

#[mz_ore::test]
fn test_aggregator_deregister_cancels_pending_halt() {
    let (mut aggregator, receiver) = test_aggregator(2);
    aggregator.apply([
        register(0),
        register(1),
        update(0, HealthStatusUpdate::halting("boom".to_string(), None)),
    ]);
    assert!(aggregator.next_halt().is_some());
    aggregator.apply([deregister(1)]);
    assert_eq!(aggregator.next_halt(), None);
    aggregator.send_due_halts();
    assert_eq!(statuses(&receiver), [Status::Starting, Status::Stalled]);
}

#[mz_ore::test]
fn test_aggregator_halts_with_latest_reason() {
    let (mut aggregator, receiver) = test_aggregator(1);
    aggregator.apply([
        register(0),
        update(0, HealthStatusUpdate::halting("first".to_string(), None)),
        update(0, HealthStatusUpdate::halting("second".to_string(), None)),
    ]);
    let Halt::Pending { reason, .. } = &aggregator.instances[&PRIMARY].halt else {
        panic!("expected a pending halt");
    };
    assert_eq!(
        reason.1,
        HealthStatusUpdate::halting("second".to_string(), None)
    );
    aggregator.send_due_halts();
    let outputs: Vec<_> = receiver.try_iter().collect();
    assert_eq!(outputs.last(), Some(&TestOutput::Halt(0)));
}

#[mz_ore::test]
fn test_aggregator_halts_for_subsources_alongside_primary() {
    const SUBSOURCE: GlobalId = GlobalId::User(1);
    let (mut aggregator, receiver) = test_aggregator(1);
    aggregator.writer.input_mapping.insert(SUBSOURCE, 1);
    let HealthEvent::Register {
        object: primary,
        mut config,
    } = register(0)
    else {
        unreachable!()
    };
    config.mark_starting.insert(SUBSOURCE);
    let halting = |id| HealthEvent::Update {
        object: object(0),
        message: HealthStatusMessage {
            id,
            namespace: StatusNamespace::Kafka,
            update: HealthStatusUpdate::halting("boom".to_string(), None),
        },
    };
    // As the Kafka reader reports a halting error, to the primary object and every output.
    aggregator.apply([
        HealthEvent::Register {
            object: primary,
            config,
        },
        halting(Some(SUBSOURCE)),
        halting(None),
        halting(Some(SUBSOURCE)),
    ]);
    let Halt::Pending { reason_id, .. } = &aggregator.instances[&PRIMARY].halt else {
        panic!("expected a pending halt");
    };
    assert_eq!(*reason_id, PRIMARY);
    aggregator.send_due_halts();
    let outputs: Vec<_> = receiver.try_iter().collect();
    assert_eq!(outputs.last(), Some(&TestOutput::Halt(0)));
}

// The below is ALL test infrastructure for the above

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

/// An update to report.
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
    /// Report a new health update.
    Update(TestUpdate),
    /// Report a health update through the reporter of the instance replaced by the last
    /// `Restart`.
    StaleUpdate(TestUpdate),
    /// Replace the instance on all workers. With `register_first`, the new instance
    /// registers before the old one deregisters.
    Restart { register_first: bool },
    /// Assert a set of outputs. Note that these should
    /// have unique `collection_index`'s
    AssertStatus(Vec<StatusToAssert>),
    /// Assert that exactly one halt was sent, for the given collection index.
    AssertHalt(usize),
}

/// A command from the driving worker to a worker.
#[derive(Debug)]
enum WorkerCommand {
    Update(TestUpdate),
    StaleUpdate(TestUpdate),
    Restart { register_first: bool },
}

#[derive(Debug, PartialEq, Eq)]
enum TestOutput {
    Status(StatusToAssert),
    Halt(usize),
}

struct TestWriter {
    sender: Sender<TestOutput>,
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
        let _ = self.sender.send(TestOutput::Status(StatusToAssert {
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
        }));
    }

    fn send_halt(&self, id: GlobalId, _error: Option<(StatusNamespace, HealthStatusUpdate)>) {
        let _ = self
            .sender
            .send(TestOutput::Halt(*self.input_mapping.get(&id).unwrap()));
    }
}

/// Runs the health dataflow with a set number of workers and inputs, and drives the steps
/// from the first worker.
///
/// The first input is the primary object. Every worker reports the updates addressed to it,
/// and all workers step until the first worker has run all steps.
fn health_runner(workers: usize, inputs: usize, write_namespaced_map: bool, steps: Vec<Step>) {
    let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
    let tokio_handle = tokio_runtime.handle().clone();

    let inputs: BTreeMap<GlobalId, usize> = (0..inputs)
        .map(|index| (GlobalId::User(u64::cast_from(index)), index))
        .collect();

    let (out_tx, out_rx) = channel();
    let out_rx = Arc::new(Mutex::new(Some(out_rx)));
    let (command_txs, command_rxs): (Vec<Sender<WorkerCommand>>, Vec<_>) =
        (0..workers).map(|_| channel()).unzip();
    let command_rxs: Arc<Mutex<Vec<Option<Receiver<WorkerCommand>>>>> =
        Arc::new(Mutex::new(command_rxs.into_iter().map(Some).collect()));
    let done = Arc::new(AtomicBool::new(false));

    timely::execute::execute(
        timely::execute::Config {
            communication: timely::CommunicationConfig::Process(workers),
            worker: Default::default(),
        },
        move |worker| {
            let _tokio_guard = tokio_handle.enter();
            let index = worker.index();
            let primary = *inputs.first_key_value().unwrap().0;
            let config = HealthConfig {
                object_type: HealthObjectType::Source,
                mark_starting: inputs.keys().copied().collect(),
                write_namespaced_map,
                suspend_and_restart_delay: Duration::from_millis(100),
            };

            let health_dataflow = render_health_dataflow(
                worker,
                mz_ore::now::SYSTEM_TIME.clone(),
                TestWriter {
                    sender: out_tx.clone(),
                    input_mapping: inputs.clone(),
                },
            );
            let (mut reporter, token) = HealthReporter::register(worker, primary, config.clone());
            let mut token = Some(token);
            let mut stale_reporter: Option<HealthReporter> = None;

            let commands = command_rxs.lock().unwrap()[index].take().unwrap();
            let mut apply_commands = |worker: &mut TimelyWorker| {
                while let Ok(command) = commands.try_recv() {
                    match command {
                        WorkerCommand::Update(update) => reporter.report(update.into()),
                        WorkerCommand::StaleUpdate(update) => stale_reporter
                            .as_ref()
                            .expect("stale update requires a restart")
                            .report(update.into()),
                        WorkerCommand::Restart { register_first } => {
                            if !register_first {
                                token = None;
                            }
                            // Stand-in for the replaced dataflow, so the new instance gets
                            // a new generation.
                            worker.dataflow::<(), _, _>(|_| {});
                            let (new_reporter, new_token) =
                                HealthReporter::register(worker, primary, config.clone());
                            token = Some(new_token);

                            stale_reporter = Some(std::mem::replace(&mut reporter, new_reporter));
                        }
                    }
                }
            };

            if index == 0 {
                let out_rx = out_rx.lock().unwrap().take().unwrap();
                let next_output =
                    |worker: &mut TimelyWorker, apply: &mut dyn FnMut(&mut TimelyWorker)| {
                        loop {
                            apply(worker);
                            match out_rx.try_recv() {
                                Ok(output) => return output,
                                Err(_) => {
                                    worker.step();
                                    // This makes testing easier.
                                    std::thread::sleep(Duration::from_millis(50));
                                }
                            }
                        }
                    };

                for step in steps.clone() {
                    match step {
                        Step::Update(update) => {
                            let target = usize::cast_from(update.worker_id);
                            command_txs[target]
                                .send(WorkerCommand::Update(update))
                                .unwrap();
                        }
                        Step::StaleUpdate(update) => {
                            let target = usize::cast_from(update.worker_id);
                            command_txs[target]
                                .send(WorkerCommand::StaleUpdate(update))
                                .unwrap();
                        }
                        Step::Restart { register_first } => {
                            for tx in &command_txs {
                                tx.send(WorkerCommand::Restart { register_first }).unwrap();
                            }
                        }
                        Step::AssertStatus(mut statuses) => {
                            while !statuses.is_empty() {
                                let TestOutput::Status(update) =
                                    next_output(worker, &mut apply_commands)
                                else {
                                    panic!("unexpected halt");
                                };
                                let pos = statuses
                                    .iter()
                                    .position(|s| s.collection_index == update.collection_index)
                                    .unwrap();
                                assert_eq!(&update, &statuses[pos]);
                                statuses.remove(pos);
                            }
                        }
                        Step::AssertHalt(collection_index) => {
                            let output = next_output(worker, &mut apply_commands);
                            assert_eq!(output, TestOutput::Halt(collection_index));
                        }
                    }
                }

                // Give a duplicate halt, if any, time to arrive.
                for _ in 0..10 {
                    apply_commands(worker);
                    worker.step();
                    std::thread::sleep(Duration::from_millis(50));
                }
                // Assert that nothing is left in the channel.
                assert_err!(out_rx.try_recv());
                done.store(true, Ordering::SeqCst);
            } else {
                while !done.load(Ordering::SeqCst) {
                    apply_commands(worker);
                    worker.step_or_park(Some(Duration::from_millis(10)));
                }
            }

            drop(token);
            drop(health_dataflow);
        },
    )
    .unwrap();
}

impl From<TestUpdate> for HealthStatusMessage {
    fn from(update: TestUpdate) -> Self {
        HealthStatusMessage {
            id: update.id,
            namespace: update.namespace,
            update: update.update,
        }
    }
}
