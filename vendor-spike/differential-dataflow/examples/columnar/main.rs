//! Columnar reachability example for differential dataflow.
//!
//! Demonstrates columnar-backed arrangements in an iterative scope,
//! exercising Enter, Leave, Negate, ResultsIn on RecordedUpdates,
//! and Push on UpdatesTyped for the reduce builder path.

use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;

use differential_dataflow::columnar::collection::Builder as ValColBuilder;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap_or("100".into()).parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap_or("300".into()).parse().unwrap();

    let timer = ::std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {

        type EdgeUpdate = (u32, u32, u64, i64);
        type NodeUpdate = (u32, (), u64, i64);
        type EdgeBuilder = ValColBuilder<EdgeUpdate>;
        type NodeBuilder = ValColBuilder<NodeUpdate>;

        let mut edge_input = <InputHandle<_, EdgeBuilder>>::new_with_builder();
        let mut root_input = <InputHandle<_, NodeBuilder>>::new_with_builder();
        let mut probe = ProbeHandle::new();

        worker.dataflow::<u64, _, _>(|scope| {
            use differential_dataflow::AsCollection;
            use timely::dataflow::operators::Probe;

            let edges = edge_input.to_stream(scope).as_collection();
            let roots = root_input.to_stream(scope).as_collection();

            reachability::reach(edges, roots)
                .inner
                .probe_with(&mut probe);
        });

        // Generate a small random graph.
        let mut edge_builder = EdgeBuilder::default();
        let mut node_builder = NodeBuilder::default();

        if worker.index() == 0 {
            // Simple deterministic "random" edges.
            let mut src: u32 = 0;
            for _ in 0..edges {
                let dst = (src.wrapping_mul(7).wrapping_add(13)) % nodes;
                edge_builder.push_into((src, dst, 0u64, 1i64));
                src = (src + 1) % nodes;
            }
            // Root: node 0.
            node_builder.push_into((0u32, (), 0u64, 1i64));
        }

        while let Some(container) = edge_builder.finish() {
            edge_input.send_batch(container);
        }
        while let Some(container) = node_builder.finish() {
            root_input.send_batch(container);
        }

        edge_input.advance_to(1);
        root_input.advance_to(1);
        edge_input.flush();
        root_input.flush();

        while probe.less_than(edge_input.time()) {
            worker.step_or_park(None);
        }

        println!("{:?}\treachability complete ({} nodes, {} edges)", timer.elapsed(), nodes, edges);

    }).unwrap();
    println!("{:?}\tshut down", timer.elapsed());
}

/// Reachability on a random directed graph using columnar containers.
///
/// This module exercises the container traits needed for iterative columnar
/// computation: Enter, Leave, Negate, ResultsIn on RecordedUpdates, and
/// Push on UpdatesTyped for the reduce builder path.
mod reachability {

    use timely::container::PushInto;
    use timely::order::Product;
    use differential_dataflow::Collection;
    use differential_dataflow::AsCollection;
    use differential_dataflow::operators::iterate::Variable;
    use differential_dataflow::operators::arrange::arrangement::arrange_core;
    use differential_dataflow::operators::join::join_traces;

    use differential_dataflow::columnar::trace::{Batcher as ValBatcher, Builder as ValBuilder, Chunker as ValChunker, Spine as ValSpine};
    use differential_dataflow::columnar::collection::{Builder as ValColBuilder, Pact as ValPact, RecordedUpdates, as_recorded_updates};

    type Node = u32;
    type Time = u64;
    type Diff = i64;
    type IterTime = Product<Time, u64>;

    /// Compute the set of nodes reachable from `roots` along directed `edges`.
    ///
    /// Returns `(node, ())` for each reachable node.
    pub fn reach<'scope>(
        edges: Collection<'scope, Time, RecordedUpdates<(Node, Node, Time, Diff)>>,
        roots: Collection<'scope, Time, RecordedUpdates<(Node, (), Time, Diff)>>,
    ) -> Collection<'scope, Time, RecordedUpdates<(Node, (), Time, Diff)>>
    {
        let outer = edges.inner.scope();

        outer.iterative::<u64, _, _>(|nested| {
            let summary = Product::new(Time::default(), 1);

            let roots_inner = roots.enter(nested);
            let (variable, reach) = Variable::new_from(roots_inner.clone(), summary);
            let edges_inner = edges.enter(nested);

            // Arrange both collections into columnar spines for joining.
            let edges_pact = ValPact { hashfunc: |k: columnar::Ref<'_, Node>| *k as u64 };
            let reach_pact = ValPact { hashfunc: |k: columnar::Ref<'_, Node>| *k as u64 };

            let edges_arr = arrange_core::<_, _,
                ValChunker<(Node, Node, IterTime, Diff)>,
                ValBatcher<Node, Node, IterTime, Diff>,
                ValBuilder<Node, Node, IterTime, Diff>,
                ValSpine<Node, Node, IterTime, Diff>,
            >(edges_inner.inner, edges_pact, "Edges");

            let reach_arr = arrange_core::<_, _,
                ValChunker<(Node, (), IterTime, Diff)>,
                ValBatcher<Node, (), IterTime, Diff>,
                ValBuilder<Node, (), IterTime, Diff>,
                ValSpine<Node, (), IterTime, Diff>,
            >(reach.inner, reach_pact, "Reach");

            // join_traces with ValColBuilder: produces Stream<_, RecordedUpdates<...>>.
            let proposed =
                join_traces::<_, _, _, _, ValColBuilder<(Node, (), IterTime, Diff)>>(
                    edges_arr,
                    reach_arr,
                    |_src, dst, (), time, d1, d2, session| {
                        use differential_dataflow::difference::Multiply;
                        let dst: Node = *dst;
                        let diff: Diff = d1.clone().multiply(d2);
                        session.push_into((dst, (), time.clone(), diff));
                    },
                ).as_collection();

            // concat: both sides are now Collection<_, RecordedUpdates<...>>.
            let combined = proposed.concat(roots_inner);

            // Arrange for reduce.
            let combined_pact = ValPact { hashfunc: |k: columnar::Ref<'_, Node>| *k as u64 };
            let combined_arr = arrange_core::<_, _,
                ValChunker<(Node, (), IterTime, Diff)>,
                ValBatcher<Node, (), IterTime, Diff>,
                ValBuilder<Node, (), IterTime, Diff>,
                ValSpine<Node, (), IterTime, Diff>,
            >(combined.inner, combined_pact, "Combined");

            // reduce_abelian on the columnar arrangement.
            let result = combined_arr.reduce_abelian::<_,
                ValBuilder<Node, (), IterTime, Diff>,
                ValSpine<Node, (), IterTime, Diff>,
                _,
                _,
            >("Distinct", |_node, _input, output| { output.push(((), 1)); },
            |col, key, upds| {
                use columnar::Push;
                use differential_dataflow::columnar::updates::UpdatesTyped;
                // `col` is now a `ColChunk` (the trie behind an `Rc`); build this
                // key's run in a fresh trie and install it consolidated.
                let mut trie = UpdatesTyped::default();
                for (val, time, diff) in upds.drain(..) { trie.push((key, &val, &time, &diff)); }
                *col.updates_mut() = trie.consolidate();
            });

            // Extract RecordedUpdates from the Arranged's batch stream.
            let result_col = as_recorded_updates::<(Node, (), IterTime, Diff)>(result);

            variable.set(result_col.clone());

            // Leave the iterative scope.
            result_col.leave(outer)
        })
    }
}
