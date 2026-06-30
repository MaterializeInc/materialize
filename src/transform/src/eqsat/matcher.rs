// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The payload algebra for rule instantiation.
//!
//! A rule's matching and side-condition checking happen on the e-graph (see
//! [`crate::eqsat::egraph`]); this module supplies the value layer those use: the
//! [`Payload`] a metavariable binds, and the payload-combining operations
//! (concatenating, shifting, or remapping columns) the compiled rule matchers
//! (`crate::eqsat::rules`) call when instantiating a right-hand side.
//!
//! Since SP4a, an `ENode`'s scalar payloads are scalar e-class [`Id`]s, not
//! inline [`EScalar`]s, so a [`Payload`] that carries scalars carries their
//! `Id`s. The column-reading helpers resolve `Id`→`EScalar` through the combined
//! graph's `escalar` cache (`data.escalar(id)`), and the column-rewriting helpers
//! intern the permuted expression back into the graph (`graph.intern_scalar`),
//! obtaining a fresh `Id`. Column remapping is still the real
//! `MirScalarExpr::permute`, so a remapped predicate is a faithful expression.

use std::collections::BTreeMap;

use mz_expr::{AggregateExpr, Columns, TableFunc};
use mz_ore::cast::CastFrom;

use crate::eqsat::core::Id;
use crate::eqsat::egraph::CombinedData;
use crate::eqsat::egraph::view::ApplyGraph;
use crate::eqsat::ir::{Col, EScalar};

/// The payload captured by a metavariable. The variant records which operator
/// the payload came from so the template can rebuild a well-typed operator.
///
/// Scalar-bearing variants hold scalar e-class [`Id`]s (resolved to [`EScalar`]
/// via the combined graph's cache); non-scalar variants (`Outputs`,
/// `Aggregates`, `FlatMapFunc`) carry their payload verbatim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    Predicates(Vec<Id>),
    Scalars(Vec<Id>),
    Outputs(Vec<Col>),
    Equivalences(Vec<Vec<Id>>),
    GroupKey(Vec<Id>),
    Aggregates(Vec<AggregateExpr>),
    /// The opaque table-function payload of a `FlatMap` operator.
    FlatMapFunc(TableFunc),
    /// The opaque argument-scalar payload of a `FlatMap` operator (scalar ids).
    FlatMapExprs(Vec<Id>),
}

impl Payload {
    /// The number of list elements in the payload.
    pub fn len(&self) -> usize {
        match self {
            Payload::Predicates(s) | Payload::Scalars(s) | Payload::GroupKey(s) => s.len(),
            Payload::Aggregates(a) => a.len(),
            Payload::Outputs(o) => o.len(),
            Payload::Equivalences(c) => c.len(),
            Payload::FlatMapExprs(s) => s.len(),
            Payload::FlatMapFunc(_) => 1,
        }
    }

    /// Whether the payload list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The underlying scalar-id list, for the scalar-bearing payload kinds.
    /// Aggregates are `AggregateExpr`, not scalars, so they are excluded, as are
    /// the opaque `FlatMap` payloads (matching the pre-SP4a `scalars()`).
    pub fn scalar_ids(&self) -> Option<&[Id]> {
        match self {
            Payload::Predicates(s) | Payload::Scalars(s) | Payload::GroupKey(s) => Some(s),
            Payload::Aggregates(_)
            | Payload::Outputs(_)
            | Payload::Equivalences(_)
            | Payload::FlatMapFunc(_)
            | Payload::FlatMapExprs(_) => None,
        }
    }

    /// All columns referenced by this payload (used by side conditions). Scalar
    /// ids are resolved to their cached [`EScalar`] to read their column support.
    pub fn columns(&self, data: &CombinedData) -> Vec<Col> {
        match self {
            Payload::Predicates(s) | Payload::Scalars(s) | Payload::GroupKey(s) => {
                s.iter().flat_map(|id| data.escalar(*id).cols()).collect()
            }
            Payload::Aggregates(a) => a.iter().flat_map(|x| x.expr.support()).collect(),
            Payload::Outputs(o) => o.clone(),
            Payload::Equivalences(classes) => classes
                .iter()
                .flat_map(|c| c.iter().flat_map(|id| data.escalar(*id).cols()))
                .collect(),
            // FlatMap payloads are opaque; the rule never inspects their columns.
            Payload::FlatMapFunc(_) | Payload::FlatMapExprs(_) => Vec::new(),
        }
    }
}

/// `iota(n)`: the identity projection `[0, 1, …, n-1]`. Builds the leading
/// "keep all input columns" part of a projection.
pub(crate) fn iota_payload(n: i64) -> Result<Payload, String> {
    if n < 0 {
        return Err("iota of negative length".into());
    }
    Ok(Payload::Outputs((0..n as usize).collect()))
}

/// Turn a payload of bare column references into a projection (`Outputs`). The
/// scalar ids are resolved via `escalar` to read whether each is a bare column.
pub(crate) fn cols_of_payload(escalar: &dyn Fn(Id) -> EScalar, p: Payload) -> Result<Payload, String> {
    let scalars = match p {
        Payload::GroupKey(s) | Payload::Predicates(s) | Payload::Scalars(s) => s,
        Payload::Outputs(o) => return Ok(Payload::Outputs(o)),
        _ => return Err("cols_of expects a list of scalars".into()),
    };
    let cols = scalars
        .iter()
        .map(|id| {
            let e = escalar(*id);
            e.is_col()
                .ok_or_else(|| format!("cols_of: `{}` is not a bare column reference", e.expr))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Payload::Outputs(cols))
}

/// Apply a column-index function to an aggregate (rewriting the columns of its
/// inner expression). The aggregate function and flags are untouched.
fn map_aggregate_cols(
    a: &AggregateExpr,
    f: &mut dyn FnMut(Col) -> i64,
) -> Result<AggregateExpr, String> {
    let mut map: BTreeMap<usize, usize> = BTreeMap::new();
    for c in a.expr.support() {
        let nc = f(c);
        if nc < 0 {
            return Err(format!(
                "column shift produced negative index for aggregate `{}`",
                a.expr
            ));
        }
        map.insert(
            c,
            usize::try_from(nc).expect("non-negative index fits usize"),
        );
    }
    let mut out = a.clone();
    out.expr.permute_map(&map);
    Ok(out)
}

/// Resolve each scalar `id` to its cached [`EScalar`], permute its columns with
/// `f`, and intern the result, returning the fresh scalar ids. A no-op `f`
/// re-interns the identical expr, which hash-conses back to the same id, so this
/// is byte-identical to the pre-SP4a inline permutation.
fn map_scalar_ids<G: ApplyGraph + ?Sized>(
    g: &mut G,
    ids: &[Id],
    f: &mut dyn FnMut(Col) -> i64,
) -> Result<Vec<Id>, String> {
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        // `g.escalar(id)` returns an owned `EScalar`, so the shared borrow ends
        // before the `&mut` `intern_scalar` borrow begins.
        let escalar = g.escalar(id);
        let permuted = escalar.permute_cols(&mut *f)?;
        out.push(g.intern_scalar(&permuted));
    }
    Ok(out)
}

/// Apply a column-index function to every column of `p`, interning any rewritten
/// scalars back into `g`.
fn map_payload_cols<G: ApplyGraph + ?Sized>(
    g: &mut G,
    p: Payload,
    mut f: impl FnMut(Col) -> i64,
) -> Result<Payload, String> {
    Ok(match p {
        Payload::Predicates(ids) => Payload::Predicates(map_scalar_ids(g, &ids, &mut f)?),
        Payload::Scalars(ids) => Payload::Scalars(map_scalar_ids(g, &ids, &mut f)?),
        Payload::GroupKey(ids) => Payload::GroupKey(map_scalar_ids(g, &ids, &mut f)?),
        Payload::FlatMapExprs(ids) => Payload::FlatMapExprs(map_scalar_ids(g, &ids, &mut f)?),
        Payload::Aggregates(xs) => Payload::Aggregates(
            xs.iter()
                .map(|a| map_aggregate_cols(a, &mut f))
                .collect::<Result<_, _>>()?,
        ),
        Payload::Outputs(o) => {
            let mut out = Vec::with_capacity(o.len());
            for c in o {
                let nc = f(c);
                if nc < 0 {
                    return Err("column shift produced negative index".into());
                }
                out.push(usize::try_from(nc).expect("non-negative index fits usize"));
            }
            Payload::Outputs(out)
        }
        Payload::Equivalences(classes) => {
            let mut out = Vec::with_capacity(classes.len());
            for class in classes {
                out.push(map_scalar_ids(g, &class, &mut f)?);
            }
            Payload::Equivalences(out)
        }
        // FlatMap function payloads are opaque captures; column remapping does
        // not apply.
        Payload::FlatMapFunc(_) => {
            return Err("column remapping is not supported for FlatMap opaque payloads".into());
        }
    })
}

/// Shift every column index in `p` by `k`.
pub(crate) fn shift_payload<G: ApplyGraph + ?Sized>(g: &mut G, p: Payload, k: i64) -> Result<Payload, String> {
    map_payload_cols(g, p, |c| c as i64 + k)
}

/// Remap every column index `c` of `p` to `outs[c]` (inverting a projection).
pub(crate) fn remap_payload<G: ApplyGraph + ?Sized>(
    g: &mut G,
    p: Payload,
    outs: &[usize],
) -> Result<Payload, String> {
    map_payload_cols(g, p, |c| match outs.get(c) {
        Some(&nc) => nc as i64,
        None => -1, // out of range ⇒ surfaced as an error by the callers
    })
}

pub(crate) fn concat_payload(a: Payload, c: Payload) -> Result<Payload, String> {
    use Payload::*;
    Ok(match (a, c) {
        (Predicates(mut x), Predicates(y)) => {
            x.extend(y);
            Predicates(x)
        }
        (Scalars(mut x), Scalars(y)) => {
            x.extend(y);
            Scalars(x)
        }
        (Outputs(mut x), Outputs(y)) => {
            x.extend(y);
            Outputs(x)
        }
        (Equivalences(mut x), Equivalences(y)) => {
            x.extend(y);
            Equivalences(x)
        }
        _ => return Err("concat of mismatched payload kinds".into()),
    })
}

/// Equivalence classes fully contained in columns [0, boundary): the
/// constraints a binary inner join over the first two inputs can enforce.
pub(crate) fn equivs_inner(
    escalar: &dyn Fn(Id) -> EScalar,
    e: Payload,
    boundary: i64,
) -> Result<Payload, String> {
    let Payload::Equivalences(classes) = e else {
        return Err("equivs_inner expects an equivalences payload".into());
    };
    let b = usize::cast_from(u64::try_from(boundary).map_err(|_| "negative boundary")?);
    let kept = classes
        .into_iter()
        .filter(|class| {
            class
                .iter()
                .all(|id| escalar(*id).cols().iter().all(|&c| c < b))
        })
        .collect();
    Ok(Payload::Equivalences(kept))
}

/// Complement of `equivs_inner`: classes touching a column >= boundary.
pub(crate) fn equivs_outer(
    escalar: &dyn Fn(Id) -> EScalar,
    e: Payload,
    boundary: i64,
) -> Result<Payload, String> {
    let Payload::Equivalences(classes) = e else {
        return Err("equivs_outer expects an equivalences payload".into());
    };
    let b = usize::cast_from(u64::try_from(boundary).map_err(|_| "negative boundary")?);
    let kept = classes
        .into_iter()
        .filter(|class| {
            class
                .iter()
                .any(|id| escalar(*id).cols().iter().any(|&c| c >= b))
        })
        .collect();
    Ok(Payload::Equivalences(kept))
}

/// The projection that restores the original column order after swapping the
/// first two join inputs (arities a, b). The commuted `Join(b, a)` outputs
/// columns [b | a]; this maps them back to [a | b], so the projected result
/// has the same schema as the original `Join(a, b)`.
pub(crate) fn swap_projection(arity_a: i64, arity_b: i64) -> Result<Payload, String> {
    let a = usize::cast_from(u64::try_from(arity_a).map_err(|_| "negative arity_a")?);
    let b = usize::cast_from(u64::try_from(arity_b).map_err(|_| "negative arity_b")?);
    // The commuted join outputs [b | a]. To restore [a | b]:
    //   position 0..a in the original maps to b..b+a in the commuted output.
    //   position a..a+b in the original maps to 0..b in the commuted output.
    let outputs = (b..b + a).chain(0..b).collect();
    Ok(Payload::Outputs(outputs))
}

/// Remap equivalences for swapping the first two inputs (arities a, b):
/// columns [0,a) -> +b, [a,a+b) -> -a, columns >= a+b unchanged.
pub(crate) fn swap_equivs<G: ApplyGraph + ?Sized>(
    g: &mut G,
    e: Payload,
    arity_a: i64,
    arity_b: i64,
) -> Result<Payload, String> {
    let Payload::Equivalences(classes) = e else {
        return Err("swap_equivs expects an equivalences payload".into());
    };
    let a = usize::cast_from(u64::try_from(arity_a).map_err(|_| "negative arity")?);
    let b = usize::cast_from(u64::try_from(arity_b).map_err(|_| "negative arity")?);
    let mut remap = |c: usize| -> i64 {
        if c < a {
            i64::try_from(c + b).expect("column fits i64")
        } else if c < a + b {
            i64::try_from(c - a).expect("column fits i64")
        } else {
            i64::try_from(c).expect("column fits i64")
        }
    };
    let mut out = Vec::with_capacity(classes.len());
    for class in classes {
        out.push(map_scalar_ids(g, &class, &mut remap)?);
    }
    Ok(Payload::Equivalences(out))
}

/// `compose(outer, inner)`: the projection that first applies `inner` then
/// `outer`, i.e. `result[i] = inner[outer[i]]`.
pub(crate) fn compose_payload(outer: Payload, inner: Payload) -> Result<Payload, String> {
    match (outer, inner) {
        (Payload::Outputs(o), Payload::Outputs(i)) => {
            let mut out = Vec::with_capacity(o.len());
            for idx in o {
                let mapped = *i
                    .get(idx)
                    .ok_or_else(|| format!("compose index {idx} out of range"))?;
                out.push(mapped);
            }
            Ok(Payload::Outputs(out))
        }
        _ => Err("compose expects two projection lists".into()),
    }
}

impl Payload {
    pub fn into_predicates(self) -> Result<Vec<Id>, String> {
        match self {
            Payload::Predicates(s) => Ok(s),
            _ => Err("expected a predicate payload".into()),
        }
    }
    pub fn into_scalars(self) -> Result<Vec<Id>, String> {
        match self {
            Payload::Scalars(s) => Ok(s),
            _ => Err("expected a scalar payload".into()),
        }
    }
    pub fn into_outputs(self) -> Result<Vec<Col>, String> {
        match self {
            Payload::Outputs(o) => Ok(o),
            _ => Err("expected a projection payload".into()),
        }
    }
    pub fn into_equivalences(self) -> Result<Vec<Vec<Id>>, String> {
        match self {
            Payload::Equivalences(e) => Ok(e),
            _ => Err("expected an equivalences payload".into()),
        }
    }
    pub fn into_group_key(self) -> Result<Vec<Id>, String> {
        match self {
            Payload::GroupKey(s) => Ok(s),
            _ => Err("expected a group-key payload".into()),
        }
    }
    pub fn into_aggregates(self) -> Result<Vec<AggregateExpr>, String> {
        match self {
            Payload::Aggregates(s) => Ok(s),
            _ => Err("expected an aggregates payload".into()),
        }
    }
    pub fn into_flatmap_func(self) -> Result<TableFunc, String> {
        match self {
            Payload::FlatMapFunc(f) => Ok(f),
            _ => Err("expected a FlatMap function payload".into()),
        }
    }
    pub fn into_flatmap_exprs(self) -> Result<Vec<Id>, String> {
        match self {
            Payload::FlatMapExprs(s) => Ok(s),
            _ => Err("expected a FlatMap exprs payload".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::egraph::EGraph;
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;

    /// Intern a list of bare-column predicates into `eg`, returning a
    /// `Payload::Predicates` of their scalar ids.
    fn pred(eg: &mut EGraph, cols: &[Col]) -> Payload {
        Payload::Predicates(
            cols.iter()
                .map(|&c| eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(c))))
                .collect(),
        )
    }

    /// Resolve a `Payload::Predicates`/`Scalars` to the bare column each scalar
    /// references, for assertions.
    fn pred_cols(eg: &EGraph, p: &Payload) -> Vec<Option<Col>> {
        p.scalar_ids()
            .expect("scalar payload")
            .iter()
            .map(|id| eg.data().escalar(*id).is_col())
            .collect()
    }

    #[mz_ore::test]
    fn shift_rewrites_columns() {
        // p reads #2,#3 ; shifting by -arity(a) with arity(a)=2 yields #0,#1.
        let mut eg = EGraph::new();
        let p = pred(&mut eg, &[2, 3]);
        let out = shift_payload(&mut eg, p, -2).unwrap();
        assert_eq!(pred_cols(&eg, &out), vec![Some(0), Some(1)]);
    }

    #[mz_ore::test]
    fn remap_inverts_a_projection() {
        // p reads projected positions #0,#1 ; project outputs = [5, 7] ; so the
        // underlying columns are #5,#7.
        let mut eg = EGraph::new();
        let p = pred(&mut eg, &[0, 1]);
        let out = remap_payload(&mut eg, p, &[5, 7]).unwrap();
        assert_eq!(pred_cols(&eg, &out), vec![Some(5), Some(7)]);
    }

    #[mz_ore::test]
    fn swap_projection_restores_column_order() {
        // a-arity 2, b-arity 1: commuted [b | a] = [#0_b, #0_a, #1_a]; restore maps
        // back to [a | b] = positions [1, 2, 0].
        assert_eq!(
            swap_projection(2, 1).unwrap(),
            Payload::Outputs(vec![1, 2, 0]),
        );
    }

    #[mz_ore::test]
    fn shift_via_apply_graph_trait() {
        use crate::eqsat::egraph::view::ApplyGraph;
        let mut eg = EGraph::new();
        let p = pred(&mut eg, &[2, 3]);
        let out = shift_payload(&mut eg as &mut dyn ApplyGraph, p, -2).unwrap();
        assert_eq!(pred_cols(&eg, &out), vec![Some(0), Some(1)]);
    }

    #[mz_ore::test]
    fn equivs_split_and_swap() {
        let mut eg = EGraph::new();
        let col =
            |eg: &mut EGraph, c| eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(c)));
        // Classes: {#0,#2} fully in [0,3); {#1,#4} crosses boundary 3.
        let c0 = col(&mut eg, 0);
        let c2 = col(&mut eg, 2);
        let c1 = col(&mut eg, 1);
        let c4 = col(&mut eg, 4);
        let e = Payload::Equivalences(vec![vec![c0, c2], vec![c1, c4]]);
        let inner = equivs_inner(&|id| eg.data().escalar(id).clone(), e.clone(), 3).unwrap();
        let outer = equivs_outer(&|id| eg.data().escalar(id).clone(), e.clone(), 3).unwrap();
        assert_eq!(inner, Payload::Equivalences(vec![vec![c0, c2]]));
        assert_eq!(outer, Payload::Equivalences(vec![vec![c1, c4]]));
        // Swap inputs of arities 2 and 1: #0->#1, #1->#2, #2->#0.
        let swapped =
            swap_equivs(&mut eg, Payload::Equivalences(vec![vec![c0, c2]]), 2, 1).unwrap();
        let Payload::Equivalences(classes) = &swapped else {
            panic!("expected equivalences");
        };
        let cols: Vec<Option<Col>> = classes[0]
            .iter()
            .map(|id| eg.data().escalar(*id).is_col())
            .collect();
        assert_eq!(cols, vec![Some(1), Some(0)]);
    }
}
