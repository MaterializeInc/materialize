// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute specialization of the runtime-owned ordered progress transport.

use mz_compute_client::protocol::response::ComputeResponse;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc::UnboundedReceiver;

pub(crate) type Sender = mz_cluster::replica_progress::Sender<ComputeResponse>;

pub(crate) fn render(
    worker: &mut TimelyWorker,
) -> (Sender, UnboundedReceiver<(usize, ComputeResponse)>) {
    mz_cluster::replica_progress::render(worker)
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
