// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Measures the same-thread cost of a shareable trace: cursor acquisition on a local handle, and
//! the maintenance CPU of arranging a stream of small batches with periodic compaction.
//!
//! Usage: `own_agent_cost <cursor|maintain> <unpublished|published>`.
//!
//! Only the `api` module differs between the trace implementations compared. Everything else is
//! the same source on both sides.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::input::Input;
use differential_dataflow::trace::{Cursor, TraceReader};
use mz_timely_util::shared_trace::Shared;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::Probe;
use timely::progress::Antichain;

type Update = ((u64, u64), u64, isize);

// BEGIN API
mod api {
    use std::sync::Arc;

    use differential_dataflow::operators::arrange::Arranged;
    use differential_dataflow::operators::arrange::arrangement::arrange_core_with_agent;
    use differential_dataflow::trace::TraceReader;
    use differential_dataflow::trace::implementations::{
        ContainerChunker, ValBatcher, ValBuilder, ValSpine,
    };
    use mz_timely_util::shared_trace::{Shared, SharedAgent};
    use timely::dataflow::Stream;
    use timely::dataflow::channels::pact::Pipeline;
    use timely::worker::Worker;

    pub type Spine = ValSpine<u64, u64, u64, isize>;
    pub type Agent = SharedAgent<Spine>;
    pub type Batch = <Spine as TraceReader>::Batch;
    pub const NAME: &str = "own";

    pub fn arrange<'s>(stream: Stream<'s, u64, Vec<super::Update>>) -> Arranged<'s, Agent> {
        arrange_core_with_agent::<
            _,
            _,
            ContainerChunker<Vec<super::Update>>,
            ValBatcher<u64, u64, u64, isize>,
            ValBuilder<u64, u64, u64, isize>,
            Spine,
            Agent,
        >(stream, Pipeline, "arrange")
    }

    pub fn publish(agent: &Agent, worker: &Worker, shared: Arc<Shared<Batch>>) {
        let activator = worker.sync_activator_for(agent.operator().address.to_vec());
        agent.publish(shared, activator, || {});
    }
}
// END API

fn cpu_ns() -> u64 {
    // The first field of the thread group leader's schedstat is its on-CPU time in nanoseconds.
    // `execute_directly` runs the worker on this thread.
    let stat = std::fs::read_to_string("/proc/self/schedstat").expect("schedstat");
    stat.split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .expect("on-cpu ns")
}

fn key(i: u64) -> u64 {
    i.wrapping_mul(2654435761) % 1_000_000
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).cloned().unwrap_or_else(|| "cursor".into());
    let published = args.get(2).map(|s| s == "published").unwrap_or(false);
    let label = format!(
        "{} {}",
        api::NAME,
        if published { "published" } else { "unpublished" }
    );

    timely::execute_directly(move |worker| {
        let probe = ProbeHandle::new();
        let (mut input, mut keep) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(u64, u64), isize>();
            let arranged = api::arrange(collection.inner);
            arranged.stream.probe_with(&probe);
            (input, arranged.trace)
        });
        // The registry's slot and a peer's hold, which keep a published point reachable and held.
        let point: Arc<Shared<api::Batch>> = Arc::new(Shared::new());
        let mut peer = None;
        if published {
            api::publish(&keep, worker, Arc::clone(&point));
            peer = Some(point.reader_at_least(&Antichain::from_elem(0)));
        }

        match mode.as_str() {
            "cursor" => {
                // 1M updates over 100 times, compacted to the last, so the spine holds a few
                // merged batches.
                let mut i = 0u64;
                for t in 0..100u64 {
                    for _ in 0..10_000 {
                        input.update((key(i), i % 7), 1);
                        i += 1;
                    }
                    input.advance_to(t + 1);
                    input.flush();
                    while probe.less_than(&(t + 1)) {
                        worker.step();
                    }
                    let frontier = Antichain::from_elem(t + 1);
                    keep.set_physical_compaction(frontier.borrow());
                }
                let frontier = Antichain::from_elem(100u64);
                keep.set_logical_compaction(frontier.borrow());
                if let Some(peer) = peer.as_mut() {
                    peer.set_logical_compaction(frontier.borrow());
                }
                for _ in 0..100 {
                    worker.step();
                }
                let mut batches = 0;
                keep.map_batches(|_| batches += 1);

                let iterations = 200_000;
                let mut samples = Vec::with_capacity(iterations);
                let mut handle = keep.clone();
                for _ in 0..iterations {
                    let start = Instant::now();
                    let (mut cursor, storage) = handle.cursor();
                    let mut walked = 0;
                    while walked < 16 && cursor.key_valid(&storage) {
                        black_box(cursor.key(&storage));
                        cursor.step_key(&storage);
                        walked += 1;
                    }
                    black_box(walked);
                    samples.push(u64::try_from(start.elapsed().as_nanos()).expect("fits"));
                }
                samples.sort_unstable();
                let p = |percent: usize| samples[(samples.len() - 1) * percent / 100];
                println!(
                    "cursor\t{label}\tbatches={batches}\tp50_ns={}\tp99_ns={}",
                    p(50),
                    p(99)
                );
            }
            "maintain" => {
                let wall = Instant::now();
                let cpu = cpu_ns();
                let mut i = 0u64;
                let rounds = 10_000u64;
                for t in 0..rounds {
                    for _ in 0..1_000 {
                        input.update((key(i), i % 7), 1);
                        i += 1;
                    }
                    input.advance_to(t + 1);
                    input.flush();
                    while probe.less_than(&(t + 1)) {
                        worker.step();
                    }
                    // The trace manager's maintenance lets the spine merge up to its upper, and
                    // the controller compacts periodically, which the peer follows.
                    let frontier = Antichain::from_elem(t + 1);
                    keep.set_physical_compaction(frontier.borrow());
                    if t % 10 == 0 {
                        keep.set_logical_compaction(frontier.borrow());
                        if let Some(peer) = peer.as_mut() {
                            peer.set_logical_compaction(frontier.borrow());
                        }
                    }
                }
                let cpu = cpu_ns() - cpu;
                let wall = wall.elapsed();
                println!(
                    "maintain\t{label}\twall_ms={}\tcpu_ms={}",
                    wall.as_millis(),
                    cpu / 1_000_000
                );
            }
            other => panic!("unknown mode {other}"),
        }
        drop(peer);
        drop(input);
        drop(keep);
        drop(point);
    });
}
