//! Minimal SCC benchmark, no diagnostics or logging.
//!
//! Usage: scc [timely args] [nodes [edges [batch [rounds]]]]
//!
//! Mirrors `diagnostics/examples/scc-bench.rs` minus the logging dataflow,
//! for a clean baseline to compare against `interactive/examples/ddir_col`.

use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::algorithms::graphs::scc::strongly_connected;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn hash_to_u64<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn edge_for(index: usize, nodes: usize) -> (usize, usize) {
    let h1 = hash_to_u64(&index);
    let h2 = hash_to_u64(&h1);
    ((h1 as usize) % nodes, (h2 as usize) % nodes)
}

fn main() {
    let timer = std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {
        let positional: Vec<String> = std::env::args()
            .skip(1)
            .filter(|a| !a.starts_with('-'))
            .collect();
        let nodes: usize = positional.get(0).and_then(|s| s.parse().ok()).unwrap_or(100_000);
        let edges: usize = positional.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
        let batch: usize = positional.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000);
        let rounds: usize = positional.get(3).and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);

        if worker.index() == 0 {
            println!("nodes: {nodes}, edges: {edges}, batch: {batch}, rounds: {}, workers: {}",
                if rounds == usize::MAX { "∞".to_string() } else { rounds.to_string() },
                worker.peers());
        }

        let mut probe = Handle::new();
        let mut input = worker.dataflow(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), isize>();
            let _scc = strongly_connected(graph).probe_with(&mut probe);
            input
        });

        let index = worker.index();
        let peers = worker.peers();

        // Load initial edges (partitioned across workers).
        let timer_load = std::time::Instant::now();
        for i in (0..edges).filter(|i| i % peers == index) {
            input.insert(edge_for(i, nodes));
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        if index == 0 {
            println!("{:?}\t{:?}\tloaded {edges} edges", timer.elapsed(), timer_load.elapsed());
        }

        // Apply changes in rounds.
        for round in 0..rounds {
            let timer_round = std::time::Instant::now();
            for i in (0..batch).filter(|i| i % peers == index) {
                input.remove(edge_for(round * batch + i, nodes));
                input.insert(edge_for(edges + round * batch + i, nodes));
            }
            input.advance_to(round + 2);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            if index == 0 {
                println!("{:?}\t{:?}\tround {round} ({} changes)",
                    timer.elapsed(), timer_round.elapsed(), batch * 2);
            }
        }
    }).unwrap();

    println!("{:?}\tshut down", timer.elapsed());
}
