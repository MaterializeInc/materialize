//! Bi-directional Dijkstra distance labeling.

use std::hash::Hash;

use timely::order::Product;
use timely::progress::Timestamp;

use crate::{VecCollection, ExchangeData};
use crate::lattice::Lattice;
use crate::operators::iterate::Variable;

/// Returns the subset of `goals` that can reach each other in `edges`, with distance.
///
/// This method performs bidirectional search, from both ends of each goal in forward
/// and reverse direction, for the sources and targets respectively. Each search can
/// examine a fraction of the graph before meeting, and multiple searches can be managed
/// concurrently.
///
/// Goals that cannot reach from the source to the target are relatively expensive, as
/// the entire graph must be explored to confirm this. A graph connectivity pre-filter
/// could be good insurance here.
pub fn bidijkstra<'scope, T, N>(edges: VecCollection<'scope, T, (N,N)>, goals: VecCollection<'scope, T, (N,N)>) -> VecCollection<'scope, T, ((N,N), u32)>
where
    T: Timestamp + Lattice,
    N: ExchangeData+Hash,
{
    let forward = edges.clone().arrange_by_key();
    let reverse = edges.map(|(x,y)| (y,x)).arrange_by_key();
    bidijkstra_arranged(forward, reverse, goals)
}

use crate::trace::{BatchCursor, Cursor, Navigable, TraceReader};
use crate::operators::arrange::Arranged;

/// Bi-directional Dijkstra search using arranged forward and reverse edge collections.
pub fn bidijkstra_arranged<'scope, N, Tr>(
    forward: Arranged<'scope, Tr>,
    reverse: Arranged<'scope, Tr>,
    goals: VecCollection<'scope, Tr::Time, (N,N)>
) -> VecCollection<'scope, Tr::Time, ((N,N), u32)>
where
    N: ExchangeData+Hash,
    Tr: TraceReader<Batch: Navigable>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a>=&'a N, Val<'a>=&'a N, Time=Tr::Time, Diff=isize>,
{
    let outer = forward.stream.scope();
    outer.iterative::<u64,_,_>(|inner| {

            let forward_edges = forward.enter(inner);
            let reverse_edges = reverse.enter(inner);

        // Our plan is to start evolving distances from both sources and destinations.
        // The evolution from a source or destination should continue as long as there
        // is a corresponding destination or source that has not yet been reached.

        // forward and reverse (node, (root, dist))
        let (forward_bind, forward) = Variable::new_from(goals.clone().map(|(x,_)| (x.clone(),(x.clone(),0))).enter(inner), Product::new(Default::default(), 1));
        let (reverse_bind, reverse) = Variable::new_from(goals.clone().map(|(_,y)| (y.clone(),(y.clone(),0))).enter(inner), Product::new(Default::default(), 1));

        forward.clone().map(|_| ()).consolidate().inspect(|x| println!("forward: {:?}", x));
        reverse.clone().map(|_| ()).consolidate().inspect(|x| println!("reverse: {:?}", x));

        let goals = goals.enter(inner);
        // let edges = edges.enter(inner);

        // Let's determine which (src, dst) pairs are ready to return.
        //
        //   done(src, dst) := forward(src, med), reverse(dst, med), goal(src, dst).
        //
        // This is a cyclic join, which should scare us a bunch.
        let reached =
        forward
            .clone()
            .join_map(reverse.clone(), |_, (src,d1), (dst,d2)| ((src.clone(), dst.clone()), *d1 + *d2))
            .reduce(|_key, s, t| t.push((*s[0].0, 1)))
            .semijoin(goals.clone());

        let active =
        reached
            .clone()
            .negate()
            .map(|(srcdst,_)| srcdst)
            .concat(goals)
            .consolidate();

        // Let's expand out forward queries that are active.
        let forward_active = active.clone().map(|(x,_y)| x).distinct();
        let forward_next =
        forward
            .clone()
            .map(|(med, (src, dist))| (src, (med, dist)))
            .semijoin(forward_active)
            .map(|(src, (med, dist))| (med, (src, dist)))
            .join_core(forward_edges, |_med, (src, dist), next| Some((next.clone(), (src.clone(), *dist+1))))
            .concat(forward)
            .map(|(next, (src, dist))| ((next, src), dist))
            .reduce(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next, src), dist)| (next, (src, dist)));

        forward_next.clone().map(|_| ()).consolidate().inspect(|x| println!("forward_next: {:?}", x));

        forward_bind.set(forward_next);

        // Let's expand out reverse queries that are active.
        let reverse_active = active.map(|(_x,y)| y).distinct();
        let reverse_next =
        reverse
            .clone()
            .map(|(med, (rev, dist))| (rev, (med, dist)))
            .semijoin(reverse_active)
            .map(|(rev, (med, dist))| (med, (rev, dist)))
            .join_core(reverse_edges, |_med, (rev, dist), next| Some((next.clone(), (rev.clone(), *dist+1))))
            .concat(reverse)
            .map(|(next, (rev, dist))| ((next, rev), dist))
            .reduce(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next,rev), dist)| (next, (rev, dist)));

        reverse_next.clone().map(|_| ()).consolidate().inspect(|x| println!("reverse_next: {:?}", x));

        reverse_bind.set(reverse_next);

        reached.leave(outer)
    })
}
