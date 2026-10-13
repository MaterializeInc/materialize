//! Breadth-first distance labeling.

use std::hash::Hash;

use timely::progress::Timestamp;

use crate::{VecCollection, ExchangeData};
use crate::operators::*;
use crate::lattice::Lattice;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs<'scope, T, N>(edges: VecCollection<'scope, T, (N,N)>, roots: VecCollection<'scope, T, N>) -> VecCollection<'scope, T, (N,u32)>
where
    T: Timestamp + Lattice,
    N: ExchangeData+Hash,
{
    let edges = edges.arrange_by_key();
    bfs_arranged(edges, roots)
}

use crate::trace::{BatchCursor, Cursor, Navigable, TraceReader};
use crate::operators::arrange::Arranged;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs_arranged<'scope, N, Tr>(edges: Arranged<'scope, Tr>, roots: VecCollection<'scope, Tr::Time, N>) -> VecCollection<'scope, Tr::Time, (N, u32)>
where
    N: ExchangeData+Hash,
    Tr: TraceReader<Batch: Navigable>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a>=&'a N, Val<'a>=&'a N, Time=Tr::Time, Diff=isize>,
{
    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.clone().iterate(|scope, inner| {

        let edges = edges.enter(scope);
        let nodes = nodes.enter(scope);

        inner.join_core(edges, |_k,l,d| Some((d.clone(), l+1)))
             .concat(nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1)))
     })
}
