// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime-owned transport of maintained compute responses to global worker zero.
//! This transports responses without interpreting or aggregating their frontiers.

use std::collections::BTreeMap;
use std::sync::mpsc::{self, TryRecvError};

use mz_compute_client::protocol::response::ComputeResponse;
use mz_timely_util::scope_label::ScopeExt;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::generic::source;
use timely::scheduling::SyncActivator;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// The runtime's sender for one global worker's maintained responses.
pub(crate) struct Sender {
    tx: mpsc::Sender<ComputeResponse>,
    activator: SyncActivator,
}

impl Sender {
    /// Enqueues a response by value and wakes its Timely source.
    ///
    /// Calls on this sender preserve their enqueue order. Concurrent calls have no
    /// specified order. Panics on channel or activation failure. The runtime must
    /// treat this panic as fatal, not catch it and continue with missing progress.
    pub(crate) fn send(&self, response: ComputeResponse) {
        self.tx.send(response).expect("replica progress input lost");
        self.activator
            .activate()
            .expect("replica progress activation lost");
    }
}

/// Renders a persistent control-plane gather on every worker in the runtime.
///
/// Call once, in the same dataflow construction order on every worker. Only the
/// receiver on global worker zero receives responses. Other receivers may be
/// dropped. Keep each sender and worker zero's receiver until runtime teardown,
/// and keep stepping Timely independently of external client connections.
///
/// Delivery preserves each worker's enqueue order, not a total order across
/// workers. Channel loss while running panics and must terminate the runtime.
/// Queues are in-memory and unbounded, so the owner must continuously drain the
/// output. There is no replay or delivery guarantee across runtime failure.
pub(crate) fn render(
    timely_worker: &mut TimelyWorker,
) -> (Sender, UnboundedReceiver<(usize, ComputeResponse)>) {
    let (input_tx, input_rx) = mpsc::channel();
    let (output_tx, output_rx) = unbounded_channel();

    let activator = timely_worker.dataflow_named::<(), _, _>("replica_progress", |scope| {
        let scope = scope.with_label();
        let worker_id = scope.index();
        let mut activator = None;
        let stream = source(scope, "replica_progress::source", |cap, info| {
            activator = Some(scope.worker().sync_activator_for(info.address.to_vec()));
            let mut next = 0u64;
            move |output| {
                // Retain the capability for the runtime lifetime, including idle
                // periods. Sequence numbers, not timestamps, establish order.
                let mut session = output.session(&cap);
                loop {
                    match input_rx.try_recv() {
                        Ok(response) => {
                            let sequence = next;
                            next = next
                                .checked_add(1)
                                .expect("replica progress sequence overflow");
                            session.give((worker_id, sequence, response));
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            panic!("replica progress sender lost")
                        }
                    }
                }
            }
        });

        // This non-key exchange is intentional: a single runtime-owned consumer
        // needs all worker partitions for existing response aggregation. Routing
        // by collection key would split that consumer's inputs across workers.
        // Only control messages traverse this edge, never user data collections.
        let origins = if worker_id == 0 { scope.peers() } else { 0 };
        let mut ordering: Vec<_> = (0..origins).map(|_| OrderedResponses::default()).collect();
        stream.sink(
            Exchange::new(|_| 0),
            "replica_progress::sink",
            move |(input, _)| {
                input.for_each(|_time, data| {
                    for (origin, sequence, response) in data.drain(..) {
                        assert_eq!(worker_id, 0);
                        ordering[origin].push(sequence, response, |response| {
                            output_tx
                                .send((origin, response))
                                .expect("replica progress output lost");
                        });
                    }
                });
            },
        );
        activator.expect("replica progress source constructed")
    });

    (
        Sender {
            tx: input_tx,
            activator,
        },
        output_rx,
    )
}

/// Timely channels need not preserve arrival order. Buffer only responses behind
/// a gap in this worker's sequence, without blocking responses from other workers.
#[derive(Default)]
struct OrderedResponses {
    next: u64,
    pending: BTreeMap<u64, ComputeResponse>,
}

impl OrderedResponses {
    fn push(
        &mut self,
        sequence: u64,
        response: ComputeResponse,
        mut emit: impl FnMut(ComputeResponse),
    ) {
        assert!(sequence >= self.next, "replica progress sequence repeated");
        assert!(
            self.pending.insert(sequence, response).is_none(),
            "replica progress sequence repeated"
        );
        while let Some(response) = self.pending.remove(&self.next) {
            self.next = self
                .next
                .checked_add(1)
                .expect("replica progress sequence overflow");
            emit(response);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use mz_compute_client::protocol::response::FrontiersResponse;
    use mz_repr::{GlobalId, Timestamp};
    use timely::progress::Antichain;

    use super::*;

    fn response(worker: usize, sequence: u64) -> ComputeResponse {
        ComputeResponse::Frontiers(
            GlobalId::User(u64::try_from(worker).unwrap()),
            FrontiersResponse {
                write_frontier: Some(Antichain::from_elem(Timestamp::from(sequence))),
                ..Default::default()
            },
        )
    }

    #[mz_ore::test]
    fn multiworker_delivery_preserves_each_workers_order() {
        const WORKERS: usize = 4;
        const RESPONSES: u64 = 128;
        let done = Arc::new(AtomicBool::new(false));
        let guards = timely::execute(timely::Config::process(WORKERS), move |worker| {
            let (sender, mut receiver) = render(worker);
            let mut received = [0u64; WORKERS];
            let deadline = Instant::now() + Duration::from_secs(10);
            for sequence in 0..RESPONSES {
                sender.send(response(worker.index(), sequence));
                worker.step();
            }
            while !done.load(Ordering::Acquire) && Instant::now() < deadline {
                worker.step_or_park(Some(Duration::from_millis(1)));
                while let Ok((origin, actual)) = receiver.try_recv() {
                    assert_eq!(worker.index(), 0, "only global worker zero receives");
                    assert_eq!(actual, response(origin, received[origin]));
                    received[origin] += 1;
                    assert!(received[origin] <= RESPONSES);
                }
                if worker.index() == 0 && received.iter().all(|n| *n == RESPONSES) {
                    done.store(true, Ordering::Release);
                }
            }
            assert!(done.load(Ordering::Acquire), "progress delivery timed out");
            assert!(receiver.try_recv().is_err());
            // This persistent dataflow only ends with runtime teardown. Drop it
            // before the sender so execute's final stepping cannot observe loss.
            worker.drop_dataflow(0);
        })
        .unwrap();
        for result in guards.join() {
            result.unwrap();
        }
    }

    #[mz_ore::test]
    fn reordered_arrivals_wait_for_gaps_without_blocking_other_workers() {
        let mut ordering = [OrderedResponses::default(), OrderedResponses::default()];
        let mut delivered = Vec::new();
        for (worker, sequence) in [(0, 2), (1, 0), (0, 1), (1, 2), (0, 0), (1, 1)] {
            ordering[worker].push(sequence, response(worker, sequence), |response| {
                delivered.push((worker, response));
            });
            if worker == 1 && sequence == 0 {
                assert_eq!(delivered, vec![(1, response(1, 0))]);
            }
        }
        let expected = [(1, 0), (0, 0), (0, 1), (0, 2), (1, 1), (1, 2)]
            .into_iter()
            .map(|(worker, sequence)| (worker, response(worker, sequence)))
            .collect::<Vec<_>>();
        assert_eq!(delivered, expected);
    }

    #[mz_ore::test]
    #[should_panic(expected = "replica progress output lost")]
    fn output_loss_is_fatal() {
        timely::execute_directly(|worker| {
            let (sender, receiver) = render(worker);
            drop(receiver);
            sender.send(response(0, 0));
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                worker.step();
            }
            worker.drop_dataflow(0);
            panic!("output loss went undetected");
        });
    }

    #[mz_ore::test]
    #[should_panic(expected = "replica progress input lost")]
    fn sending_after_dataflow_loss_is_fatal() {
        timely::execute_directly(|worker| {
            let (sender, _receiver) = render(worker);
            worker.drop_dataflow(0);
            sender.send(response(0, 0));
        });
    }
}
