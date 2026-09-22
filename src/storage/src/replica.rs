// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Attempt-scoped observations and Kafka open admission for native execution.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::client::{StorageCommand, StorageResponse};
use mz_storage_types::sources::envelope::SourceEnvelope;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::progress::Antichain;

/// A response on the process-local maintained storage endpoint.
/// Only `ExecutionInput` describes read protection. Output uppers and drop
/// acknowledgements do not imply that an execution has finished reading.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicaStorageResponse {
    /// Revalidate current definition, eligibility, and input protection before
    /// this attempt opens a transactional Kafka producer.
    KafkaPreOpen {
        /// Unique, one-use request identity.
        request: uuid::Uuid,
        /// Run sequence that owns this request.
        execution: u64,
        /// Sink being opened.
        id: GlobalId,
    },
    /// The ordinary storage response, aggregated across global workers.
    Response(StorageResponse),
    /// Installation concluded on every worker, by rendering or fencing the
    /// attempt. Emitted once per admitted Run, including canceled starts. This
    /// is not hydration or read completion and releases no input protection.
    ExecutionStarted {
        /// Sequence of the Run whose installation concluded.
        execution: u64,
    },
    /// Actual input progress for one execution and one protected collection.
    ExecutionInput {
        /// Sequence returned by the command that admitted this execution.
        execution: u64,
        /// The collection being read, not the execution's output.
        input: GlobalId,
        /// No further reads at times strictly before this frontier.
        frontier: Antichain<Timestamp>,
    },
    /// Execution is suspended. The owner must authorize a fresh Run command.
    RestartRequested {
        /// The suspended execution's admission sequence.
        execution: u64,
        /// The ingestion or sink to reconstruct.
        id: GlobalId,
    },
}

/// Output generations disambiguate late drop acknowledgements from a Run that
/// reuses the same ID. They are independent of execution input protection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WorkerResponse {
    pub output_generation: Option<u64>,
    pub response: ReplicaStorageResponse,
}

impl From<ReplicaStorageResponse> for WorkerResponse {
    fn from(response: ReplicaStorageResponse) -> Self {
        Self {
            output_generation: None,
            response,
        }
    }
}

#[derive(Default)]
pub(crate) struct OutputGenerations(pub BTreeMap<GlobalId, u64>);

impl OutputGenerations {
    pub fn observe(&mut self, sequence: u64, command: &StorageCommand) -> Vec<(GlobalId, u64)> {
        let ids: Vec<_> = match command {
            StorageCommand::RunIngestion(run) => run.description.collection_ids().collect(),
            StorageCommand::RunSink(run) => vec![run.id],
            StorageCommand::AllowCompaction(id, frontier) if frontier.is_empty() => {
                self.0.remove(id);
                Vec::new()
            }
            _ => Vec::new(),
        };
        ids.into_iter()
            .map(|id| (id, *self.0.entry(id).or_insert(sequence)))
            .collect()
    }
}

pub(crate) struct OutputFrontier {
    workers: Vec<Option<Antichain<Timestamp>>>,
    reported: Antichain<Timestamp>,
}

impl OutputFrontier {
    pub fn new(peers: usize) -> Self {
        let minimum = Antichain::from_elem(Timestamp::MIN);
        Self {
            workers: vec![Some(minimum.clone()); peers],
            reported: minimum,
        }
    }

    pub fn update(
        &mut self,
        worker: usize,
        upper: Antichain<Timestamp>,
    ) -> Option<Antichain<Timestamp>> {
        let previous = self.workers[worker]
            .as_mut()
            .expect("output after worker drop");
        assert!(PartialOrder::less_equal(previous, &upper));
        *previous = upper;
        let frontier = self
            .workers
            .iter()
            .flatten()
            .flat_map(|f| f.iter().copied())
            .collect();
        if PartialOrder::less_than(&self.reported, &frontier) {
            self.reported = frontier;
            Some(self.reported.clone())
        } else {
            None
        }
    }

    pub fn drop_worker(&mut self, worker: usize) -> bool {
        assert!(self.workers[worker].take().is_some(), "double output drop");
        self.workers.iter().all(Option::is_none)
    }
}

pub(crate) fn inputs(command: &StorageCommand) -> Option<(GlobalId, Vec<GlobalId>)> {
    match command {
        StorageCommand::RunSink(run) => Some((run.id, vec![run.description.from])),
        StorageCommand::RunIngestion(run) => Some((
            run.id,
            std::iter::once(run.description.remap_collection_id)
                .chain(
                    run.description
                        .source_exports
                        .iter()
                        .filter_map(|(id, export)| {
                            matches!(export.data_config.envelope, SourceEnvelope::Upsert(_))
                                .then_some(*id)
                        }),
                )
                .collect(),
        )),
        _ => None,
    }
}

type Observation = Box<dyn Fn() -> Antichain<Timestamp>>;

/// Native endpoint ingress. This is not part of the controller wire protocol.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ReplicaCommand {
    /// Maintained storage command and its native ingress sequence.
    Storage(u64, StorageCommand),
    /// One-use response to a pre-open callback on its originating worker.
    KafkaPreOpenReply {
        /// Unique callback identity.
        request: uuid::Uuid,
        /// Owning Run sequence.
        execution: u64,
        /// Sink being opened.
        id: GlobalId,
        /// Maximum age since request start, or denial.
        max_age: Option<Duration>,
    },
}

struct PendingPreOpen {
    execution: u64,
    id: GlobalId,
    reply: tokio::sync::oneshot::Sender<Option<Duration>>,
}

struct PendingRequest(
    Rc<RefCell<BTreeMap<uuid::Uuid, PendingPreOpen>>>,
    uuid::Uuid,
);

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.0.borrow_mut().remove(&self.1);
    }
}

pub(crate) struct KafkaPreOpen {
    execution: u64,
    id: GlobalId,
    current: Arc<AtomicBool>,
    pending: Rc<RefCell<BTreeMap<uuid::Uuid, PendingPreOpen>>>,
    commands: crate::internal_control::InternalCommandSender,
}

/// Consumed inside the blocking closure, immediately before the external call.
/// The clock starts before requesting admission so queues never extend its age.
/// This bounds the open decision, not the external call's duration. Kafka's
/// transactional fencing still governs a call that is already in flight.
pub(crate) struct KafkaOpenApproval {
    started: Instant,
    max_age: Duration,
    current: Arc<AtomicBool>,
}

impl KafkaOpenApproval {
    pub fn check(self) -> anyhow::Result<()> {
        anyhow::ensure!(self.current.load(Ordering::SeqCst), "Kafka attempt retired");
        anyhow::ensure!(
            self.started.elapsed() < self.max_age,
            "Kafka admission expired"
        );
        Ok(())
    }
}

impl KafkaPreOpen {
    pub async fn request(self) -> anyhow::Result<KafkaOpenApproval> {
        let started = Instant::now();
        anyhow::ensure!(self.current.load(Ordering::SeqCst), "Kafka attempt retired");
        let request = uuid::Uuid::new_v4();
        let (reply, response) = tokio::sync::oneshot::channel();
        self.pending.borrow_mut().insert(
            request,
            PendingPreOpen {
                execution: self.execution,
                id: self.id,
                reply,
            },
        );
        // Cancellation must remove the local callback even if no reply arrives.
        let _cleanup = PendingRequest(Rc::clone(&self.pending), request);
        self.commands.send(
            crate::internal_control::InternalStorageCommand::KafkaPreOpen {
                request,
                execution: self.execution,
                id: self.id,
            },
        );
        let max_age = response
            .await?
            .ok_or_else(|| anyhow::anyhow!("Kafka admission denied"))?;
        Ok(KafkaOpenApproval {
            started,
            max_age,
            current: self.current,
        })
    }
}

struct Input {
    readers: Vec<Observation>,
    reported: Antichain<Timestamp>,
}

struct Attempt {
    // Remains true across async frontier calculation AND global delivery of
    // the rendering command. A gap between readers must never look complete.
    starting: bool,
    reported_start: bool,
    inputs: BTreeMap<GlobalId, Input>,
}

#[derive(Default)]
pub(crate) struct Executions {
    // Mutate through start/retire so blocking Kafka opens are revoked too.
    pub current: BTreeMap<GlobalId, u64>,
    pub outputs: OutputGenerations,
    pub dropped_outputs: Vec<(GlobalId, u64)>,
    attempts: RefCell<BTreeMap<u64, Attempt>>,
    gates: BTreeMap<GlobalId, Arc<AtomicBool>>,
    preopens: Rc<RefCell<BTreeMap<uuid::Uuid, PendingPreOpen>>>,
}

impl Drop for Executions {
    fn drop(&mut self) {
        for gate in self.gates.values() {
            gate.store(false, Ordering::SeqCst);
        }
        self.preopens.borrow_mut().clear();
    }
}

impl Executions {
    pub fn retire(&mut self, id: GlobalId) {
        self.current.remove(&id);
        if let Some(gate) = self.gates.remove(&id) {
            gate.store(false, Ordering::SeqCst);
        }
        self.preopens
            .borrow_mut()
            .retain(|_, pending| pending.id != id);
    }

    pub fn kafka_pre_open(
        &self,
        id: GlobalId,
        commands: crate::internal_control::InternalCommandSender,
    ) -> KafkaPreOpen {
        KafkaPreOpen {
            execution: self.current[&id],
            id,
            current: Arc::clone(&self.gates[&id]),
            pending: Rc::clone(&self.preopens),
            commands,
        }
    }

    pub fn kafka_pre_open_reply(
        &self,
        request: uuid::Uuid,
        execution: u64,
        id: GlobalId,
        max_age: Option<Duration>,
    ) {
        let mut pending = self.preopens.borrow_mut();
        if pending
            .get(&request)
            .is_some_and(|p| p.execution == execution && p.id == id)
        {
            let pending = pending.remove(&request).expect("checked request");
            let max_age = max_age.filter(|_| self.current.get(&id) == Some(&execution));
            let _ = pending.reply.send(max_age);
        }
    }

    pub fn start(&mut self, execution: u64, id: GlobalId, inputs: Vec<GlobalId>) {
        self.retire(id);
        self.gates.insert(id, Arc::new(AtomicBool::new(true)));
        self.current.insert(id, execution);
        assert!(
            self.attempts
                .get_mut()
                .insert(
                    execution,
                    Attempt {
                        starting: true,
                        reported_start: false,
                        inputs: inputs
                            .into_iter()
                            .map(|input| (
                                input,
                                Input {
                                    readers: Vec::new(),
                                    reported: Antichain::from_elem(Timestamp::MIN),
                                }
                            ))
                            .collect(),
                    }
                )
                .is_none()
        );
    }

    pub fn observe(&self, id: GlobalId, input: GlobalId, reader: Observation) {
        let execution = self.current[&id];
        let mut attempts = self.attempts.borrow_mut();
        let attempt = attempts.get_mut(&execution).expect("starting execution");
        assert!(
            attempt.starting,
            "readers must register before startup completes"
        );
        attempt
            .inputs
            .get_mut(&input)
            .expect("declared input")
            .readers
            .push(reader);
    }

    pub fn started(&mut self, execution: u64) {
        self.attempts
            .get_mut()
            .get_mut(&execution)
            .expect("pending startup")
            .starting = false;
    }

    pub fn report(&mut self) -> Vec<ReplicaStorageResponse> {
        let mut responses = Vec::new();
        self.attempts.get_mut().retain(|execution, attempt| {
            if attempt.starting {
                return true;
            }
            if !attempt.reported_start {
                attempt.reported_start = true;
                responses.push(ReplicaStorageResponse::ExecutionStarted {
                    execution: *execution,
                });
            }
            for (input, state) in &mut attempt.inputs {
                let frontier = state
                    .readers
                    .iter()
                    .flat_map(|read| read().into_iter())
                    .collect();
                assert!(
                    PartialOrder::less_equal(&state.reported, &frontier),
                    "input regressed"
                );
                if state.reported != frontier {
                    state.reported = frontier;
                    responses.push(ReplicaStorageResponse::ExecutionInput {
                        execution: *execution,
                        input: *input,
                        frontier: state.reported.clone(),
                    });
                }
            }
            // Probes and frontier cells own no readers, capabilities, or tokens.
            // Keep old attempts until their actual readers finish, even on DROP.
            !attempt
                .inputs
                .values()
                .all(|input| input.reported.is_empty())
        });
        responses
    }
}

/// Owned by the actual reader future, never by its cancellation button.
/// Observations run on the same worker, after synchronous destruction of the
/// future and its in-flight reads, not concurrently with its drop glue.
pub(crate) struct ReadProgress<T: timely::progress::Timestamp>(Rc<RefCell<Antichain<T>>>);

impl<T: timely::progress::Timestamp> ReadProgress<T> {
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(Antichain::from_elem(T::minimum()))))
    }

    pub fn frontier(&self) -> Rc<RefCell<Antichain<T>>> {
        Rc::clone(&self.0)
    }

    pub fn advance(&self, frontier: Antichain<T>) {
        assert!(PartialOrder::less_equal(&*self.0.borrow(), &frontier));
        *self.0.borrow_mut() = frontier;
    }
}

impl<T: timely::progress::Timestamp> Drop for ReadProgress<T> {
    fn drop(&mut self) {
        self.0.borrow_mut().clear();
    }
}
