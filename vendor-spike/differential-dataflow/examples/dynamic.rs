use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::VecCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let rounds: u32 = std::env::args().nth(4).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(5).unwrap() == "inspect";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut roots, mut graph) = worker.dataflow(|scope| {

            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            let mut result = bfs(graph, roots);

            if !inspect {
                result = result.filter(|_| false);
            }

            result.map(|(_,l)| l)
                  .consolidate()
                  .inspect(|x| println!("\t{:?}", x))
                  .probe_with(&mut probe);

            (root_input, edge_input)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        roots.insert(0);
        roots.close();

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }
        }

        println!("{:?}\tloaded", timer.elapsed());

        graph.advance_to(1);
        graph.flush();
        worker.step_or_park_while(None, || probe.less_than(graph.time()));

        println!("{:?}\tstable", timer.elapsed());

        for round in 0 .. rounds {
            for element in 0 .. batch {
                if worker.index() == 0 {
                    graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    graph.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                }
                graph.advance_to(2 + round * batch + element);
            }
            graph.flush();

            let timer2 = ::std::time::Instant::now();
            worker.step_or_park_while(None, || probe.less_than(&graph.time()));

            if worker.index() == 0 {
                let elapsed = timer2.elapsed();
                println!("{:?}\t{:?}:\t{}", timer.elapsed(), round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<'scope, T>(edges: VecCollection<'scope, T, Edge>, roots: VecCollection<'scope, T, Node>) -> VecCollection<'scope, T, (Node, u32)>
where
    T: timely::progress::Timestamp + Lattice + Ord,
{
    use timely::order::Product;
    use iterate::Variable;
    use differential_dataflow::dynamic::{feedback_summary, pointstamp::PointStamp};

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    let outer = nodes.scope();
    outer.iterative::<PointStamp<usize>, _, _>(|inner| {

        // These enter the statically bound scope, rather than any iterative scopes.
        // We do not *need* to enter them into the dynamic scope, as they are static
        // within that scope.
        let edges = edges.enter(inner);
        let nodes = nodes.enter(inner);

        // Create a variable for label iteration.
        let inner = feedback_summary::<usize>(1, 1);
        let (label_bind, label) = Variable::new_from(nodes.clone(), Product { outer: Default::default(), inner });

        let next =
        label
            .join_map(edges, |_k,l,d| (*d, l+1))
            .concat(nodes)
            .reduce(|_, s, t| t.push((*s[0].0, 1)))
            ;

        label_bind.set(next.clone());
        // Leave the dynamic iteration, stripping off the last timestamp coordinate.
        next.leave_dynamic(1)
            .inspect(|x| println!("{:?}", x))
            .leave(outer)
    })

}
