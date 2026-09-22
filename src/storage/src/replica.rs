// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Passive, attempt-scoped observations for native maintained execution.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

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
    pub current: BTreeMap<GlobalId, u64>,
    pub outputs: OutputGenerations,
    pub dropped_outputs: Vec<(GlobalId, u64)>,
    attempts: RefCell<BTreeMap<u64, Attempt>>,
}

impl Executions {
    pub fn start(&mut self, execution: u64, id: GlobalId, inputs: Vec<GlobalId>) {
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
