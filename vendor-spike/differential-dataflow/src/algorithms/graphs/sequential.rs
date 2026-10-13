//! Sequential (non-concurrent) graph algorithms.

use std::hash::Hash;

use timely::progress::Timestamp;

use crate::{VecCollection, ExchangeData};
use crate::lattice::Lattice;
use crate::operators::*;
use crate::hashable::Hashable;

fn _color<'scope, T, N>(edges: VecCollection<'scope, T, (N,N)>) -> VecCollection<'scope, T,(N,Option<u32>)>
where
    T: Timestamp + Lattice + Hash,
    N: ExchangeData+Hash,
{
    // need some bogus initial values.
    let start = edges.clone()
                     .map(|(x,_y)| (x,u32::MAX))
                     .distinct();

    // repeatedly apply color-picking logic.
    sequence(start, edges, |_node, vals| {

        // look for the first absent positive integer.
        // start at 1 in case we ever use NonZero<u32>.

        (1u32 ..)
            .find(|&i| vals.get(i as usize - 1).map(|x| *x.0) != Some(i))
            .unwrap()
    })
}

/// Applies `logic` to nodes sequentially, in order of node identifiers.
///
/// The `logic` function updates a node's state as a function of its
/// neighbor states. It will only be called on complete input.
///
/// Internally, this method performs a fixed-point computation in which
/// a node "fires" once all of its neighbors with lower identifier have
/// fired, and we apply `logic` to the new state of lower neighbors and
/// the old state (input) of higher neighbors.
pub fn sequence<'scope, T, N, V, F>(
    state: VecCollection<'scope, T, (N,V)>,
    edges: VecCollection<'scope, T, (N,N)>,
    logic: F) -> VecCollection<'scope, T, (N,Option<V>)>
where
    T: Timestamp + Lattice + Hash,
    N: ExchangeData+Hashable,
    V: ExchangeData,
    F: Fn(&N, &[(&V, isize)])->V+'static
{

    // start iteration with None messages for all.
    state
        .clone()
        .map(|(node, _state)| (node, None))
        .iterate(|scope, new_state| {
            // immutable content: edges and initial state.
            let edges = edges.enter(scope);
            let old_state = state.enter(scope);
                                 // .map(|x| (x.0, Some(x.1)));

            // break edges into forward and reverse directions.
            let forward = edges.clone().filter(|edge| edge.0 < edge.1);
            let reverse = edges.filter(|edge| edge.0 > edge.1);

            // new state goes along forward edges, old state along reverse edges
            let new_messages = new_state.join_map(forward, |_k,v,d| (d.clone(),v.clone()));

            let incomplete = new_messages.clone().filter(|x| x.1.is_none()).map(|x| x.0).distinct();
            let new_messages = new_messages.filter(|x| x.1.is_some()).map(|x| (x.0, x.1.unwrap()));

            let old_messages = old_state.join_map(reverse, |_k,v,d| (d.clone(),v.clone()));

            let messages = new_messages.concat(old_messages).antijoin(incomplete.clone());

            // // determine who has incoming `None` messages, and suppress all of them.
            // let incomplete = new_messages.filter(|x| x.1.is_none()).map(|x| x.0).distinct();

            // merge messages; suppress computation if not all inputs available yet.
            messages
                // .concat(old_messages)  // /-- possibly too clever: None if any inputs None.
                // .antijoin(incomplete)
                .reduce(move |k, vs, t| t.push((Some(logic(k,vs)),1)))
                .concat(incomplete.map(|x| (x, None)))
        })
}
