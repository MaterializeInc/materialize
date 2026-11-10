// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::mpsc;

use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle;
use timely::progress::frontier::AntichainRef;
use timely::worker::Worker;

use super::progress_link;

/// Bound on worker steps a frontier change may take to reach the probe.
const MAX_STEPS: usize = 100;

fn probe_frontier(probe: &Handle<u64>) -> Vec<u64> {
    probe.with_frontier(|frontier| frontier.to_vec())
}

/// Parks the worker until the probe reports `expected`, failing after [`MAX_STEPS`] wakeups.
fn park_until(worker: &mut Worker, probe: &Handle<u64>, expected: &[u64]) {
    for _ in 0..MAX_STEPS {
        if probe_frontier(probe) == expected {
            return;
        }
        worker.step_or_park(None);
    }
    assert_eq!(
        probe_frontier(probe),
        expected,
        "probe did not reach frontier"
    );
}

#[mz_ore::test]
fn a_replayed_frontier_follows_the_pusher_across_threads() {
    let (pusher, source) = progress_link::<u64>();
    timely::execute_directly(move |worker| {
        let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
            let (stream, token) = source.replay(scope, "progress_link");
            (stream.probe().0, token)
        });
        // Let the replay run its first scheduling and settle at the minimum.
        park_until(worker, &probe, &[0]);

        let (step_tx, step_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let mut pusher = pusher;
            pusher.advance_to(AntichainRef::new(&[3]));
            step_rx.recv().expect("worker alive");
            pusher.advance_to(AntichainRef::new(&[7]));
            pusher
        });

        park_until(worker, &probe, &[3]);
        step_tx.send(()).expect("pusher alive");
        park_until(worker, &probe, &[7]);
        let _pusher = thread.join().expect("pusher thread panicked");
    });
}

#[mz_ore::test]
fn a_push_before_the_replay_is_built_is_observed() {
    let (mut pusher, source) = progress_link::<u64>();
    pusher.advance_to(AntichainRef::new(&[5]));
    timely::execute_directly(move |worker| {
        let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
            let (stream, token) = source.replay(scope, "progress_link");
            (stream.probe().0, token)
        });
        park_until(worker, &probe, &[5]);
        drop(pusher);
    });
}

#[mz_ore::test]
fn an_empty_advance_completes_the_replayed_stream() {
    let (mut pusher, source) = progress_link::<u64>();
    timely::execute_directly(move |worker| {
        let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
            let (stream, token) = source.replay(scope, "progress_link");
            (stream.probe().0, token)
        });
        pusher.advance_to(AntichainRef::new(&[]));
        park_until(worker, &probe, &[]);
        assert!(probe.done());
    });
}
