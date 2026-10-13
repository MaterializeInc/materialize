//! Test harness for the model-derived `reference` reduce tactic. Four things:
//!
//! * `differential_*` — run the default (cursor) reduce and the `reference` reduce on the SAME random
//!   input and `assert_eq` their outputs at every time. The drift detector: if either tactic changes
//!   behavior, it fails. Inputs use `Pair<u64,u64>` (product / partial order, so joins and synthetic
//!   times occur) with diffs ±1 (so updates cancel — the corner where the value-blind reference could
//!   over-derive).
//!
//! * `oracle_*` — run one tactic and check its output against a from-scratch recomputation of
//!   `f(accumulated input)` at many times. The correctness check: it does not rely on the two tactics
//!   agreeing, so it catches bugs in code they share (the driver, `ValueHistory`, …). It is the
//!   executable form of `formal/Differential/Model.lean`'s stream-correctness statement.
//!
//! * `bfs_*` — an iterative BFS (product time via iteration; a real computation shape) computed both
//!   ways over a graph that grows and shrinks; another differential check.
//!
//! * `scc_*` — strongly connected components: two nested product-timed fixpoints (edge trimming around
//!   iterative label propagation), so the reduce runs at three nested time coordinates. This depth is
//!   what exposed the phase-2 catch-up walk stepping the partial-order `less_equal` set instead of the
//!   total-`Ord` prefix (see the note at that walk in `operators/reduce.rs`); the two-coordinate shapes
//!   above never reached it.

use std::collections::BTreeMap;

use rand::{Rng, SeedableRng, StdRng};

use timely::PartialOrder;
use timely::dataflow::operators::capture::{Capture, Extract};
use timely::dataflow::operators::vec::unordered_input::UnorderedInput;

use differential_dataflow::{AsCollection, Data, ExchangeData, Hashable};
use differential_dataflow::VecCollection;
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::reduce_trace_reference;
use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};

use pair::Pair;
type Time = Pair<u64, u64>;

// ---- the reduce logics under test (plain `fn`s, so they are `Copy` and can feed both tactics) ----

fn count(_key: &u64, input: &[(&u64, isize)], output: &mut Vec<(i64, isize)>) {
    output.push((input.len() as i64, 1));
}
fn sum(_key: &u64, input: &[(&u64, isize)], output: &mut Vec<(i64, isize)>) {
    let s: i64 = input.iter().map(|(v, d)| **v as i64 * *d as i64).sum();
    output.push((s, 1));
}
fn min(_key: &u64, input: &[(&u64, isize)], output: &mut Vec<(i64, isize)>) {
    if let Some(m) = input.iter().map(|(v, _)| **v).min() {
        output.push((m as i64, 1));
    }
}

type Logic = fn(&u64, &[(&u64, isize)], &mut Vec<(i64, isize)>);

// Stand-alone equivalent of the default `reduce`, driven by the model-derived reference tactic. The
// library exposes only the low-level, doc-hidden `reduce_trace_reference`; this helper supplies the
// same arrange / abelian-negate / as_collection glue `reduce_named` uses, so the tests compare the
// two tactics without the reference living on the public `Collection`/`Arranged` API.
fn reduce_reference<'scope, Ti, K, V, R, V2, R2, L>(
    collection: VecCollection<'scope, Ti, (K, V), R>,
    mut logic: L,
) -> VecCollection<'scope, Ti, (K, V2), R2>
where
    Ti: timely::progress::Timestamp + Lattice + Ord,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: ExchangeData + Semigroup,
    V2: Data,
    R2: Ord + Abelian + 'static,
    L: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>) + 'static,
{
    // Bind the input arrangement to an explicitly-typed local so `Tr1` is concrete going into
    // `reduce_trace_reference` (otherwise its output-key bound is checked before the argument pins it).
    let arranged: Arranged<'scope, TraceAgent<ValSpine<K, V, Ti, R>>> =
        collection.arrange_by_key_named("Arrange: ReduceReference");

    // The closure params are annotated to the concrete `&K` / `&V` / owned-output types (rather than
    // left to infer the cursor's `Key<'_>` projection) so the coercion to the user's `&K` logic
    // normalizes here, exactly as the default `reduce_named` gets for free by passing its logic through.
    reduce_trace_reference::<TraceAgent<ValSpine<K, V, Ti, R>>, ValBuilder<K, V2, Ti, R2>, ValSpine<K, V2, Ti, R2>, _, _>(
        arranged,
        "ReduceReference",
        move |key: &K, input: &[(&V, R)], output: &mut Vec<(V2, R2)>, change: &mut Vec<(V2, R2)>| {
            if !input.is_empty() { logic(key, input, change); }
            change.extend(output.drain(..).map(|(x, mut d)| { d.negate(); (x, d) }));
            differential_dataflow::consolidation::consolidate(change);
        },
        |vec, key: &K, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r))); },
    )
    .as_collection(|k, v| (k.clone(), v.clone()))
}

// A random input: `((key, val), Pair(a, b), diff)`, diffs ±1 so updates cancel.
fn random_input(seed: usize, n: usize, keys: u64, vals: u64, span: u64) -> Vec<((u64, u64), Time, isize)> {
    let s: &[usize] = &[seed, 7, 13, 29];
    let mut rng: StdRng = SeedableRng::from_seed(s);
    (0..n).map(|_| {
        let k = rng.gen_range(0, keys);
        let v = rng.gen_range(0, vals);
        let t = Pair::new(rng.gen_range(0, span), rng.gen_range(0, span));
        let d = if rng.gen::<bool>() { 1 } else { -1 };
        ((k, v), t, d)
    }).collect()
}

// ======================= differential: cursor vs reference, all times =======================

#[test] fn differential_count() { for s in 0..25 { differential(s, count); } }
#[test] fn differential_sum()   { for s in 0..25 { differential(s, sum); } }
#[test] fn differential_min()   { for s in 0..25 { differential(s, min); } }

fn differential(seed: usize, logic: Logic) {
    let span = 6u64;
    let updates = random_input(seed, 400, 12, 8, span);
    timely::execute_directly(move |worker| {
        let (mut input, capability) = worker.dataflow::<Time, _, _>(|scope| {
            let ((input, capability), stream) = scope.new_unordered_input::<((u64, u64), Time, isize)>();
            let collection = stream.as_collection();
            let via_cursor    = collection.clone().reduce(logic);
            let via_reference = reduce_reference(collection, logic);
            via_cursor.assert_eq(via_reference);
            (input, capability)
        });
        for u in updates { input.activate().session(&capability).give(u); }
        drop(capability); // release the input frontier so everything finalizes
    });
}

// ======================= oracle: a tactic vs from-scratch f(acc input) =======================

#[test] fn oracle_cursor_count()    { for s in 0..15 { oracle(s, false, count); } }
#[test] fn oracle_reference_count() { for s in 0..15 { oracle(s, true,  count); } }
#[test] fn oracle_reference_sum()   { for s in 0..15 { oracle(s, true,  sum); } }
#[test] fn oracle_reference_min()   { for s in 0..15 { oracle(s, true,  min); } }

fn oracle(seed: usize, reference: bool, logic: Logic) {
    let span = 6u64;
    let updates = random_input(seed, 300, 10, 8, span);

    // Capture the reduce output as `((key, out_val), time, diff)` updates.
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));
    let drive = updates.clone();
    timely::execute_directly(move |worker| {
        let send = send.lock().unwrap().clone();
        let (mut input, capability) = worker.dataflow::<Time, _, _>(|scope| {
            let ((input, capability), stream) = scope.new_unordered_input::<((u64, u64), Time, isize)>();
            let collection = stream.as_collection();
            let out = if reference { reduce_reference(collection, logic) } else { collection.reduce(logic) };
            out.inner.capture_into(send);
            (input, capability)
        });
        for u in drive { input.activate().session(&capability).give(u); }
        drop(capability);
    });
    let output: Vec<((u64, i64), Time, isize)> =
        recv.extract().into_iter().flat_map(|(_, batch)| batch).collect();

    // Everything is finalized, so `acc(output, q) == f(acc(input, q))` must hold at every `q`. Check
    // at a rich set: all input times, all output times, and their pairwise joins (where reduce's
    // output can change).
    let mut times: Vec<Time> = Vec::new();
    times.extend(updates.iter().map(|(_, t, _)| t.clone()));
    times.extend(output.iter().map(|(_, t, _)| t.clone()));
    let base = times.clone();
    for a in &base { for b in &base { times.push(a.join(b)); } }
    times.sort(); times.dedup();

    for q in &times {
        // expected: per key, f applied to the accumulated input at `q`.
        let mut expected: BTreeMap<(u64, i64), i64> = BTreeMap::new();
        let mut per_key: BTreeMap<u64, BTreeMap<u64, isize>> = BTreeMap::new();
        for ((k, v), t, d) in &updates {
            if t.less_equal(q) { *per_key.entry(*k).or_default().entry(*v).or_default() += *d; }
        }
        for (k, vals) in &per_key {
            let acc: Vec<(&u64, isize)> = vals.iter().filter(|(_, d)| **d != 0).map(|(v, d)| (v, *d)).collect();
            if acc.is_empty() { continue; }
            let mut out = Vec::new();
            logic(k, &acc, &mut out);
            for (ov, od) in out { *expected.entry((*k, ov)).or_default() += od as i64; }
        }
        expected.retain(|_, d| *d != 0);

        // actual: the accumulated reduce output at `q`.
        let mut actual: BTreeMap<(u64, i64), i64> = BTreeMap::new();
        for ((k, ov), t, d) in &output {
            if t.less_equal(q) { *actual.entry((*k, *ov)).or_default() += *d as i64; }
        }
        actual.retain(|_, d| *d != 0);

        assert_eq!(expected, actual, "seed {seed} reference {reference} at time {q:?}");
    }
}

// ======================= iterative BFS differential (a real shape) =======================

#[test] fn bfs_tiny_a()   { bfs_differential(6, 10, 4, &[1, 2, 3, 4]); }
#[test] fn bfs_tiny_b()   { bfs_differential(5, 8, 5, &[5, 6, 7, 8]); }
#[test] fn bfs_30_60_20() { bfs_differential(30, 60, 20, &[1, 2, 3, 4]); }
#[test] fn bfs_15_40_30() { bfs_differential(15, 40, 30, &[5, 6, 7, 8]); }
#[test] fn bfs_sweep() {
    for s in 0u8 .. 12 { bfs_differential(40, 120, 30, &[s as usize, (s as usize) + 7, 13, 29]); }
    for s in 0u8 .. 6  { bfs_differential(60, 200, 25, &[3, (s as usize) + 1, (s as usize) * 5 + 2, 11]); }
}

type Node = usize;
type Edge = (Node, Node);

fn bfs_differential(nodes: usize, edges: usize, rounds: usize, seed: &[usize]) {
    let mut rng1: StdRng = SeedableRng::from_seed(seed);
    let mut rng2: StdRng = SeedableRng::from_seed(seed);
    let mut edge_list: Vec<((usize, usize), usize, isize)> = Vec::new();
    for _ in 0 .. edges { edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1)); }
    for round in 1 .. rounds {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
        edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round, -1));
    }
    let root_list = vec![(1usize, 0usize, 1isize)];

    timely::execute_directly(move |worker| {
        let mut roots_list = root_list.clone();
        let mut edges_list = edge_list.clone();
        let (mut roots, mut edges) = worker.dataflow::<usize, _, _>(|scope| {
            let (root_input, roots) = scope.new_collection();
            let (edge_input, edges) = scope.new_collection();
            let via_cursor    = bfs(edges.clone(), roots.clone(), false);
            let via_reference = bfs(edges, roots, true);
            via_cursor.assert_eq(via_reference);
            (root_input, edge_input)
        });
        roots_list.sort_by(|x, y| y.1.cmp(&x.1));
        edges_list.sort_by(|x, y| y.1.cmp(&x.1));
        let mut round = 0;
        while !roots_list.is_empty() || !edges_list.is_empty() {
            while roots_list.last().map(|x| x.1) == Some(round) { let (n, _, d) = roots_list.pop().unwrap(); roots.update(n, d); }
            while edges_list.last().map(|x| x.1) == Some(round) { let ((s, t), _, d) = edges_list.pop().unwrap(); edges.update((s, t), d); }
            round += 1;
            roots.advance_to(round);
            edges.advance_to(round);
        }
    });
}

fn bfs<'scope, T>(edges: VecCollection<'scope, T, Edge>, roots: VecCollection<'scope, T, Node>, reference: bool) -> VecCollection<'scope, T, (Node, usize)>
where
    T: timely::progress::Timestamp + Lattice + Ord,
{
    let nodes = roots.map(|x| (x, 0));
    nodes.clone().iterate(|scope, inner| {
        let edges = edges.enter(scope);
        let nodes = nodes.enter(scope);
        let combined = inner.join_map(edges, |_k, l, d| (*d, l + 1)).concat(nodes);
        if reference {
            reduce_reference(combined, |_, s, t| t.push((*s[0].0, 1)))
        } else {
            combined.reduce(|_, s, t| t.push((*s[0].0, 1)))
        }
    })
}

// ===================== iterative SCC differential (a deeper shape) =====================
// SCC nests two Product-timed fixpoints: an outer edge-trimming loop and, inside each trim, an
// iterative label propagation whose `reduce` is the tactic under test. The doubly-nested product
// time produces far more synthetic interesting times than BFS, over joins / concats / filters.

#[test] fn scc_tiny_a() { scc_differential(6, 12, 4, &[1, 2, 3, 4]); }
#[test] fn scc_tiny_b() { scc_differential(8, 16, 4, &[5, 6, 7, 8]); }
#[test] fn scc_12_24()  { scc_differential(12, 24, 5, &[2, 4, 6, 8]); }
#[test] fn scc_sweep()  { for s in 0u8 .. 6 { scc_differential(15, 30, 5, &[s as usize, (s as usize) + 7, 13, 29]); } }

fn scc_differential(nodes: usize, edges: usize, rounds: usize, seed: &[usize]) {
    let mut rng1: StdRng = SeedableRng::from_seed(seed);
    let mut rng2: StdRng = SeedableRng::from_seed(seed);
    let mut edge_list: Vec<((usize, usize), usize, isize)> = Vec::new();
    for _ in 0 .. edges { edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1)); }
    for round in 1 .. rounds {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
        edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round, -1));
    }

    timely::execute_directly(move |worker| {
        let mut edges_list = edge_list.clone();
        let mut edges = worker.dataflow::<usize, _, _>(|scope| {
            let (edge_input, edges) = scope.new_collection();
            let via_cursor    = scc(edges.clone(), false);
            let via_reference = scc(edges, true);
            via_cursor.assert_eq(via_reference);
            edge_input
        });
        edges_list.sort_by(|x, y| y.1.cmp(&x.1));
        let mut round = 0;
        while !edges_list.is_empty() {
            while edges_list.last().map(|x| x.1) == Some(round) { let ((s, t), _, d) = edges_list.pop().unwrap(); edges.update((s, t), d); }
            round += 1;
            edges.advance_to(round);
        }
    });
}

// The subset of edges within a strongly connected component, mirroring
// `algorithms::graphs::scc::strongly_connected` but with the propagation `reduce` swappable.
fn scc<'scope, T>(graph: VecCollection<'scope, T, Edge>, reference: bool) -> VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + Lattice + Ord,
{
    let graph2 = graph.clone();
    graph.iterate(move |scope, inner| {
        let edges = graph2.enter(scope);
        let trans = edges.clone().map(|(a, b)| (b, a));
        let trimmed = trim_edges(inner, edges, reference);
        trim_edges(trimmed, trans, reference)
    })
}

// Keep only edges whose endpoints propagate to the same label; run twice (forward then transposed)
// this converges on the SCC edges.
fn trim_edges<'scope, T>(cycle: VecCollection<'scope, T, Edge>, edges: VecCollection<'scope, T, Edge>, reference: bool) -> VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + Lattice + Ord,
{
    let nodes = edges.clone().map(|(_a, b)| (b, b));
    let labels = propagate(cycle, nodes, reference);
    edges.join_map(labels.clone(), |&src, &dst, &l1| (dst, (src, l1)))
         .join_map(labels, |&dst, &(src, l1), &l2| ((src, dst), (l1, l2)))
         .filter(|(_, (l1, l2))| l1 == l2)
         .map(|((src, dst), _)| (dst, src))
}

// Forward min-label propagation along `edges` from per-node seed labels; the inner `reduce` (min
// label per node) is the tactic under test.
fn propagate<'scope, T>(edges: VecCollection<'scope, T, Edge>, nodes: VecCollection<'scope, T, (Node, Node)>, reference: bool) -> VecCollection<'scope, T, (Node, Node)>
where
    T: timely::progress::Timestamp + Lattice + Ord,
{
    let seed = nodes.clone();
    nodes.filter(|_| false).iterate(move |scope, inner| {
        let edges = edges.enter(scope);
        let seed = seed.enter(scope);
        let combined = inner.join_map(edges, |&_n, &l, &d| (d, l)).concat(seed);
        if reference {
            reduce_reference(combined, |_, s, t| t.push((*s[0].0, 1)))
        } else {
            combined.reduce(|_, s, t| t.push((*s[0].0, 1)))
        }
    })
}

/// A minimal product-order (partially ordered) timestamp, so the harness exercises the synthetic
/// interesting times determination is about. Copied from `examples/multitemporal.rs`.
mod pair {

    #[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct Pair<S, T> { pub first: S, pub second: T }

    impl<S, T> Pair<S, T> {
        pub fn new(first: S, second: T) -> Self { Pair { first, second } }
    }

    use timely::order::PartialOrder;
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    use timely::progress::timestamp::Refines;
    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Self::minimum() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    use timely::progress::PathSummary;
    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> { Some(timestamp.clone()) }
        fn followed_by(&self, other: &Self) -> Option<Self> { Some(other.clone()) }
    }

    use timely::progress::Timestamp;
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() }}
        type Summary = ();
    }

    use differential_dataflow::lattice::Lattice;
    impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
        fn join(&self, other: &Self) -> Self {
            Pair { first: self.first.join(&other.first), second: self.second.join(&other.second) }
        }
        fn meet(&self, other: &Self) -> Self {
            Pair { first: self.first.meet(&other.first), second: self.second.meet(&other.second) }
        }
    }

    use std::fmt::{Formatter, Error, Debug};
    use serde::{Deserialize, Serialize};
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }
}
