//! Directed label reachability.

use std::hash::Hash;

use timely::progress::Timestamp;

use crate::{VecCollection, ExchangeData};
use crate::lattice::Lattice;
use crate::difference::{Abelian, Multiply};

/// Propagates labels forward, retaining the minimum label.
///
/// This algorithm naively propagates all labels at once, much like standard label propagation.
/// To more carefully control the label propagation, consider `propagate_core` which supports a
/// method to limit the introduction of labels.
pub fn propagate<'scope, T, N, L, R>(edges: VecCollection<'scope, T, (N,N), R>, nodes: VecCollection<'scope, T,(N,L),R>) -> VecCollection<'scope, T,(N,L),R>
where
    T: Timestamp + Lattice + Hash,
    N: ExchangeData+Hash,
    R: ExchangeData+Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>,
    L: ExchangeData,
{
    propagate_core(edges.arrange_by_key(), nodes, |_label| 0)
}

/// Propagates labels forward, retaining the minimum label.
///
/// This algorithm naively propagates all labels at once, much like standard label propagation.
/// To more carefully control the label propagation, consider `propagate_core` which supports a
/// method to limit the introduction of labels.
pub fn propagate_at<'scope, T, N, L, F, R>(edges: VecCollection<'scope, T, (N,N), R>, nodes: VecCollection<'scope, T,(N,L),R>, logic: F) -> VecCollection<'scope, T,(N,L),R>
where
    T: Timestamp + Lattice + Hash,
    N: ExchangeData+Hash,
    R: ExchangeData+Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>,
    L: ExchangeData,
    F: Fn(&L)->u64+Clone+'static,
{
    propagate_core(edges.arrange_by_key(), nodes, logic)
}

use crate::trace::{BatchCursor, Cursor, Navigable, TraceReader};
use crate::operators::arrange::arrangement::Arranged;

/// Propagates labels forward, retaining the minimum label.
///
/// This variant takes a pre-arranged edge collection, to facilitate re-use, and allows
/// a method `logic` to specify the rounds in which we introduce various labels. The output
/// of `logic should be a number in the interval \[0,64\],
pub fn propagate_core<'scope, N, L, Tr, F, R>(edges: Arranged<'scope, Tr>, nodes: VecCollection<'scope, Tr::Time,(N,L),R>, logic: F) -> VecCollection<'scope, Tr::Time,(N,L),R>
where
    N: ExchangeData+Hash,
    R: ExchangeData+Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>,
    L: ExchangeData,
    Tr: TraceReader<Batch: Navigable, Time: Hash>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a>=&'a N, Val<'a>=&'a N, Time=Tr::Time, Diff=R>,
    F: Fn(&L)->u64+Clone+'static,
{
    // Morally the code performs the following iterative computation. However, in the interest of a simplified
    // dataflow graph and reduced memory footprint we instead have a wordier version below. The core differences
    // between the two are that 1. the former filters its input and pretends to perform non-monotonic computation,
    // whereas the latter creates an initially empty monotonic iteration variable, and 2. the latter rotates the
    // iterative computation so that the arrangement produced by `reduce` can be re-used.

    // nodes.filter(|_| false)
    //      .iterate(|scope, inner| {
    //          let edges = edges.enter(scope);
    //          let nodes = nodes.enter_at(scope, move |r| 256 * (64 - (logic(&r.1)).leading_zeros() as u64));
    //          inner.join_map(edges, |_k,l,d| (d.clone(),l.clone()))
    //               .concat(nodes)
    //               .reduce(|_, s, t| t.push((s[0].0.clone(), 1)))
    //      })

    use timely::order::Product;
    let outer = nodes.scope();
    outer.scoped::<Product<_, usize>,_,_>("Propagate", |scope| {

        use crate::operators::iterate::Variable;
        use crate::trace::implementations::{ValBuilder, ValSpine};

        use timely::order::Product;

        let edges = edges.enter(scope);
        let nodes = nodes.enter_at(scope, move |r| 256 * (64 - (logic(&r.1)).leading_zeros() as usize));

        let (proposals_bind, proposals) = Variable::new(scope, Product::new(Default::default(), 1usize));

        let labels =
        proposals
            .concat(nodes)
            .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>("Propagate", |_, s, t| t.push((s[0].0.clone(), R::from(1_i8))));

        let propagate: VecCollection<_, (N, L), R> =
        labels
            .clone()
            .join_core(edges, |_k, l: &L, d| Some((d.clone(), l.clone())));

        proposals_bind.set(propagate);

        labels
            .as_collection(|k,v| (k.clone(), v.clone()))
            .leave(outer)
    })
}
