// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Constant-column analysis: per e-class, the output columns proven to hold a
//! fixed scalar value on every row.

use std::collections::BTreeMap;

use mz_expr::{BinaryFunc, Columns, MirScalarExpr};

use crate::eqsat::core::{Analysis, Id};
use crate::eqsat::egraph::{CNode, CombinedLang, ENode};
use crate::eqsat::ir::EScalar;

use super::RelCtx;

/// Map from an output column index to the constant scalar value it is known to
/// hold on every row of the relation.
pub type ConstCols = BTreeMap<usize, EScalar>;

/// Constant-column analysis: per e-class, the output columns proven to hold a
/// fixed scalar value on every row.
///
/// The value is stored as a full [`EScalar`], not a bare `Datum`, so that a
/// later generalization from "column equals literal" to "column equals an
/// arbitrary scalar expression" needs no change to the domain.
///
/// Conservative and sound: a column is recorded constant only where an operator
/// forces it (a `Filter` predicate `#i = literal`, a `Map`/`Reduce` key that is
/// itself a literal or a column already known constant). `merge` and `Union`
/// intersect on `(column, value)`, keeping a fact only where both forms agree.
///
/// `locals` carries facts for `LocalGet` references, mirroring [`NonNeg`] and
/// [`Keys`]; within a fragment a recursive reference contributes nothing.
///
/// [`NonNeg`]: super::nonneg::NonNeg
/// [`Keys`]: super::keys::Keys
#[derive(Default)]
pub struct ConstantColumns {
    pub locals: BTreeMap<usize, ConstCols>,
}

/// If `pred` is `#i = <literal>` or `<literal> = #i`, return `(i, literal)`.
///
/// The literal is returned as the [`EScalar`] for that side, so the value the
/// analysis stores is the scalar payload itself (preserving its `lit` fact).
fn col_eq_literal(pred: &EScalar) -> Option<(usize, EScalar)> {
    let MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq(_),
        expr1,
        expr2,
    } = &pred.expr
    else {
        return None;
    };
    // One side a bare column reference, the other an `Ok` literal.
    match (expr1.as_column(), expr2.as_column()) {
        (Some(i), None) if expr2.is_literal_ok() => Some((i, EScalar::plain((**expr2).clone()))),
        (None, Some(i)) if expr1.is_literal_ok() => Some((i, EScalar::plain((**expr1).clone()))),
        _ => None,
    }
}

/// Intersect two constant-column maps on `(column, value)`: keep a column only
/// where both maps agree it is constant and agree on the value. This is the
/// sound combination for a `Union` (a column is constant only if every branch
/// agrees on the same value).
fn intersect_const_cols(a: ConstCols, b: &ConstCols) -> ConstCols {
    a.into_iter().filter(|(k, v)| b.get(k) == Some(v)).collect()
}

/// Join two constant-column maps proven of the SAME relation (two e-node forms
/// of one e-class): keep every column EITHER form proves constant. This moves
/// toward more precision, matching the other e-class analyses (`NonNeg`'s `||`,
/// `Keys`' set union), and makes the empty map a left/right identity so the
/// `run_analysis` fold from `bottom` does not annihilate facts. A value conflict
/// means the two forms disagree on a column they both pin, so the relation is
/// contradictory hence empty: drop that column (any value is then vacuously
/// true, and dropping is the safe choice).
fn union_const_cols(mut a: ConstCols, b: &ConstCols) -> ConstCols {
    for (k, v) in b {
        match a.get(k) {
            None => {
                a.insert(*k, v.clone());
            }
            Some(existing) if existing == v => {}
            Some(_) => {
                a.remove(k);
            }
        }
    }
    a
}

impl Analysis<CombinedLang> for ConstantColumns {
    type Domain = ConstCols;
    type Ctx<'a> = RelCtx<'a>;

    fn bottom(&self) -> ConstCols {
        ConstCols::new()
    }

    fn make(&self, node: &CNode, get: &dyn Fn(Id) -> ConstCols, ctx: Self::Ctx<'_>) -> ConstCols {
        let CNode::Rel(node) = node else {
            return self.bottom();
        };
        let arity = ctx.arity;
        let data = ctx.data;
        match node {
            // Opaque leaves: no column is known constant.
            ENode::Constant { .. } | ENode::Get { .. } | ENode::Opaque(_) => ConstCols::new(),

            // A recursive reference: use the Rel-level recursion fact if we have
            // one, else assume nothing (missing knowledge, never wrong).
            ENode::LocalGet { id, .. } => self.locals.get(id).cloned().unwrap_or_default(),

            // Filter: the input's constants survive, plus any column pinned to a
            // literal by an equality predicate. `IndexedFilter` is the same filter
            // and pins the same columns.
            ENode::Filter { input, predicates }
            | ENode::IndexedFilter {
                input, predicates, ..
            } => {
                let mut cols = get(*input);
                for &pred in predicates {
                    if let Some((i, lit)) = col_eq_literal(data.escalar(pred)) {
                        cols.insert(i, lit);
                    }
                }
                cols
            }

            // Map: input columns keep their indices; each appended scalar lands
            // at `input_arity + pos`. A scalar that is itself a literal is
            // constant; a bare column reference inherits the input column's
            // constant (if any).
            ENode::Map { input, scalars } => {
                let cols = get(*input);
                let input_arity = arity(*input);
                let mut out = cols.clone();
                for (pos, &e) in scalars.iter().enumerate() {
                    let e = data.escalar(e);
                    let out_col = input_arity + pos;
                    if e.expr.is_literal_ok() {
                        out.insert(out_col, e.clone());
                    } else if let Some(j) = e.is_col() {
                        if let Some(v) = cols.get(&j) {
                            out.insert(out_col, v.clone());
                        }
                    }
                }
                out
            }

            // Project: remap input-column constants to their output positions. A
            // repeated source column independently pins each output position.
            ENode::Project { input, outputs } => {
                let cols = get(*input);
                let mut out = ConstCols::new();
                for (new, &old) in outputs.iter().enumerate() {
                    if let Some(v) = cols.get(&old) {
                        out.insert(new, v.clone());
                    }
                }
                out
            }

            // Join: the output columns are the concatenation of the inputs', so
            // shift each input's constants by its running column offset. A join
            // equivalence that links a column to a literal also pins that column.
            ENode::Join {
                inputs,
                equivalences: join_equivs,
            }
            | ENode::WcoJoin {
                inputs,
                equivalences: join_equivs,
            } => {
                let mut out = ConstCols::new();
                let mut offset = 0usize;
                for &inp in inputs.iter() {
                    let input_arity = arity(inp);
                    for (col, v) in get(inp) {
                        out.insert(offset + col, v);
                    }
                    offset += input_arity;
                }
                // Forward pass over join equivalences: a class that contains a
                // literal pins every bare-column member of that class.
                for class in join_equivs {
                    let lit = class
                        .iter()
                        .map(|&s| data.escalar(s))
                        .find(|s| s.expr.is_literal_ok());
                    if let Some(lit) = lit {
                        for &s in class {
                            if let Some(col) = data.escalar(s).is_col() {
                                out.insert(col, EScalar::plain(lit.expr.clone()));
                            }
                        }
                    }
                }
                out
            }

            // Reduce: the output columns `0..group_key.len()` are the group-key
            // expressions. A key that is a literal, or a bare column reference to
            // an input column already known constant, is constant in the output.
            // Aggregate columns (after the key) are generally not constant.
            ENode::Reduce {
                input, group_key, ..
            } => {
                let cols = get(*input);
                let mut out = ConstCols::new();
                for (pos, &gk) in group_key.iter().enumerate() {
                    let gk = data.escalar(gk);
                    if gk.expr.is_literal_ok() {
                        out.insert(pos, gk.clone());
                    } else if let Some(j) = gk.is_col() {
                        if let Some(v) = cols.get(&j) {
                            out.insert(pos, v.clone());
                        }
                    }
                }
                out
            }

            // FlatMap: input column constants survive (same indices). The appended
            // output columns are computed by the table function and are generally
            // not constant, so only the input constants are propagated.
            ENode::FlatMap { input, .. } => get(*input),

            // Passthrough: these change multiplicities or row presence but not
            // the column values of surviving rows (ArrangeBy and ArrangeByMany
            // are multiset identities).
            ENode::Negate { input }
            | ENode::Threshold { input }
            | ENode::ArrangeBy { input, .. }
            | ENode::ArrangeByMany { input, .. }
            | ENode::TopK { input, .. } => get(*input),

            // Union: a column is constant only where every branch agrees on its
            // value. Intersect across branches. Empty input yields no constants.
            ENode::Union { inputs } => {
                let mut iter = inputs.iter().map(|&inp| get(inp));
                let Some(first) = iter.next() else {
                    return ConstCols::new();
                };
                iter.fold(first, |acc, next| intersect_const_cols(acc, &next))
            }
        }
    }

    fn merge(&self, a: ConstCols, b: ConstCols) -> ConstCols {
        // The two e-nodes denote the same relation, so a column proven constant
        // in EITHER form is constant in the relation: join both forms' facts.
        // This is a meet toward more precision (like the other analyses), and
        // crucially makes `bottom` the identity so the fold from `bottom` in
        // `run_analysis` preserves facts rather than annihilating them. A value
        // conflict means an empty relation, so that column is dropped. The map
        // grows monotonically and is bounded by the relation's arity, so the
        // fixpoint terminates.
        union_const_cols(a, &b)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::{BinaryFunc, MirScalarExpr};
    use mz_repr::{Datum, ReprScalarType};

    use crate::eqsat::core::{Analysis, Id};
    use crate::eqsat::egraph::{CNode, CombinedLang, EGraph, ENode};
    use crate::eqsat::ir::EScalar;

    use super::{ConstCols, ConstantColumns, RelCtx};

    /// Intern an `EScalar` into `eg`, returning its scalar e-class id.
    fn intern(eg: &mut EGraph, e: &EScalar) -> Id {
        eg.intern_scalar(e)
    }

    /// Run an analysis `make` over a relational `node`, resolving its scalar ids
    /// through `eg`'s cache and using the given `get`/`arity` providers.
    fn run_make<A>(
        eg: &EGraph,
        analysis: &A,
        node: ENode,
        get: &dyn Fn(Id) -> A::Domain,
        arity: &dyn Fn(Id) -> usize,
    ) -> A::Domain
    where
        A: for<'a> Analysis<CombinedLang, Ctx<'a> = RelCtx<'a>>,
    {
        let ctx = RelCtx {
            arity,
            data: eg.data(),
        };
        analysis.make(&CNode::Rel(node), get, ctx)
    }

    // --- ConstantColumns analysis tests --------------------------------------

    /// A literal `1` as an `EScalar` (for building predicates and group keys).
    fn lit1() -> EScalar {
        EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(1),
            ReprScalarType::Int64,
        ))
    }

    /// The predicate `#col = 1`.
    fn col_eq_one(col: usize) -> EScalar {
        EScalar::plain(MirScalarExpr::column(col).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
            BinaryFunc::Eq(mz_expr::func::Eq),
        ))
    }

    fn cc() -> ConstantColumns {
        ConstantColumns {
            locals: BTreeMap::new(),
        }
    }

    /// `Filter[#1 = 1]` pins output column 1 to the literal `1`; other columns
    /// stay unknown.
    #[mz_ore::test]
    fn constant_columns_filter_pins_column() {
        let analysis = cc();
        let mut eg = EGraph::new();
        let p = intern(&mut eg, &col_eq_one(1));
        let node = ENode::Filter {
            input: 0,
            predicates: vec![p],
        };
        let get = |_: Id| analysis.bottom();
        let arity = |_: Id| 2usize;
        let result = run_make(&eg, &analysis, node, &get, &arity);
        assert_eq!(result.get(&1), Some(&lit1()));
        assert_eq!(result.get(&0), None);
    }

    /// A non-equality predicate, or an equality between two columns, pins
    /// nothing.
    #[mz_ore::test]
    fn constant_columns_filter_ignores_non_literal_equality() {
        let analysis = cc();
        let mut eg = EGraph::new();
        // #0 = #1: no literal side.
        let pred = intern(
            &mut eg,
            &EScalar::plain(
                MirScalarExpr::column(0)
                    .call_binary(MirScalarExpr::column(1), BinaryFunc::Eq(mz_expr::func::Eq)),
            ),
        );
        let node = ENode::Filter {
            input: 0,
            predicates: vec![pred],
        };
        let get = |_: Id| analysis.bottom();
        let arity = |_: Id| 2usize;
        assert!(run_make(&eg, &analysis, node, &get, &arity).is_empty());
    }

    /// `Map` appends a literal at `input_arity + pos` and propagates a constant
    /// through a bare column reference.
    #[mz_ore::test]
    fn constant_columns_map_literal_and_column_ref() {
        let analysis = cc();
        let mut eg = EGraph::new();
        // Input arity 2, with column 0 known constant `1`.
        let mut input_cols = ConstCols::new();
        input_cols.insert(0, lit1());
        // Scalars: [literal 1, #0]. Output columns 2 and 3.
        let s0 = intern(&mut eg, &lit1());
        let s1 = intern(&mut eg, &EScalar::plain(MirScalarExpr::column(0)));
        let node = ENode::Map {
            input: 0,
            scalars: vec![s0, s1],
        };
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 2usize;
        let result = run_make(&eg, &analysis, node, &get, &arity);
        // Input column 0 stays constant.
        assert_eq!(result.get(&0), Some(&lit1()));
        // Appended literal at column 2.
        assert_eq!(result.get(&2), Some(&lit1()));
        // Appended #0 inherits column 0's constant at column 3.
        assert_eq!(result.get(&3), Some(&lit1()));
    }

    /// `Project` remaps input-column constants to output positions, including a
    /// repeated source column.
    #[mz_ore::test]
    fn constant_columns_project_remaps() {
        let analysis = cc();
        let eg = EGraph::new();
        let mut input_cols = ConstCols::new();
        input_cols.insert(2, lit1());
        // outputs = [2, 0, 2]: positions 0 and 2 both come from source column 2.
        let node = ENode::Project {
            input: 0,
            outputs: vec![2, 0, 2],
        };
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 3usize;
        let result = run_make(&eg, &analysis, node, &get, &arity);
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&1), None);
        assert_eq!(result.get(&2), Some(&lit1()));
    }

    /// `Join` shifts each input's constants by its running column offset.
    #[mz_ore::test]
    fn constant_columns_join_offsets_inputs() {
        let analysis = cc();
        let eg = EGraph::new();
        // Two inputs, each arity 2. Each input has column 0 constant `1`.
        let mut per_input = ConstCols::new();
        per_input.insert(0, lit1());
        let node = ENode::Join {
            inputs: vec![0, 1],
            equivalences: vec![],
        };
        let get = |_: Id| per_input.clone();
        let arity = |_: Id| 2usize;
        let result = run_make(&eg, &analysis, node, &get, &arity);
        // Left input column 0 -> output 0; right input column 0 -> output 2.
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&2), Some(&lit1()));
        assert_eq!(result.get(&1), None);
        assert_eq!(result.get(&3), None);
    }

    /// A join equivalence class containing a literal pins every bare-column
    /// member of that class.
    #[mz_ore::test]
    fn constant_columns_join_equivalence_literal() {
        let analysis = cc();
        let mut eg = EGraph::new();
        // Class: [#0, literal 1]: column 0 must equal 1.
        let c0 = intern(&mut eg, &EScalar::plain(MirScalarExpr::column(0)));
        let l1 = intern(&mut eg, &lit1());
        let node = ENode::Join {
            inputs: vec![0, 1],
            equivalences: vec![vec![c0, l1]],
        };
        let get = |_: Id| ConstCols::new();
        let arity = |_: Id| 1usize;
        let result = run_make(&eg, &analysis, node, &get, &arity);
        assert_eq!(result.get(&0), Some(&lit1()));
    }

    /// `Reduce` pins a group-key position that is a literal or a column already
    /// constant in the input.
    #[mz_ore::test]
    fn constant_columns_reduce_group_key() {
        let analysis = cc();
        let mut eg = EGraph::new();
        let mut input_cols = ConstCols::new();
        input_cols.insert(3, lit1());
        // group_key = [literal 1, #3]: output positions 0 and 1 are constant.
        let g0 = intern(&mut eg, &lit1());
        let g1 = intern(&mut eg, &EScalar::plain(MirScalarExpr::column(3)));
        let node = ENode::Reduce {
            input: 0,
            group_key: vec![g0, g1],
            aggregates: vec![],
            monotonic: false,
            expected_group_size: None,
        };
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 4usize;
        let result = run_make(&eg, &analysis, node, &get, &arity);
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&1), Some(&lit1()));
    }

    /// Passthrough operators carry the input's constants unchanged.
    #[mz_ore::test]
    fn constant_columns_passthrough() {
        let analysis = cc();
        let eg = EGraph::new();
        let mut input_cols = ConstCols::new();
        input_cols.insert(0, lit1());
        let get = |_: Id| input_cols.clone();
        let arity = |_: Id| 2usize;
        for node in [ENode::Negate { input: 0 }, ENode::Threshold { input: 0 }] {
            let result = run_make(&eg, &analysis, node, &get, &arity);
            assert_eq!(result.get(&0), Some(&lit1()));
        }
    }

    /// `Union` keeps a column constant only where every branch agrees on the
    /// value.
    #[mz_ore::test]
    fn constant_columns_union_intersects() {
        let analysis = cc();
        // Branch 0 (id 0): cols {0:1, 1:1}. Branch 1 (id 1): cols {0:1, 1:2}.
        let lit2 = EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(2),
            ReprScalarType::Int64,
        ));
        let mut b0 = ConstCols::new();
        b0.insert(0, lit1());
        b0.insert(1, lit1());
        let mut b1 = ConstCols::new();
        b1.insert(0, lit1());
        b1.insert(1, lit2.clone());
        let eg = EGraph::new();
        let node = ENode::Union { inputs: vec![0, 1] };
        let get = |id: Id| if id == 0 { b0.clone() } else { b1.clone() };
        let arity = |_: Id| 2usize;
        let result = run_make(&eg, &analysis, node, &get, &arity);
        // Column 0 agrees (both 1); column 1 disagrees (1 vs 2) so it drops.
        assert_eq!(result.get(&0), Some(&lit1()));
        assert_eq!(result.get(&1), None);
    }

    /// `merge` joins two forms of the same relation: a column proven constant in
    /// EITHER form survives, a value conflict drops the column, and the empty map
    /// is the identity (so folding from `bottom` does not annihilate facts).
    #[mz_ore::test]
    fn constant_columns_merge_unions() {
        let analysis = cc();
        let lit2 = EScalar::plain(MirScalarExpr::literal_ok(
            Datum::Int64(2),
            ReprScalarType::Int64,
        ));
        let mut a = ConstCols::new();
        a.insert(0, lit1());
        a.insert(1, lit1());
        let mut b = ConstCols::new();
        b.insert(0, lit1());
        b.insert(1, lit2);
        b.insert(2, lit1());
        let merged = analysis.merge(a, b);
        // Column 0 agrees (kept); 1 disagrees (dropped); 2 is in only one form
        // (kept: a fact proven by either form holds of the relation).
        assert_eq!(merged.get(&0), Some(&lit1()));
        assert_eq!(merged.get(&1), None);
        assert_eq!(merged.get(&2), Some(&lit1()));
    }

    /// Regression guard for the `run_analysis` fold `d = merge(bottom, make(n))`:
    /// `bottom` (the empty map) must be the identity for `merge`, otherwise every
    /// class collapses to empty and the analysis produces no facts. An opaque
    /// `LocalGet` contributing the empty map must likewise not poison a class it
    /// is unioned into.
    #[mz_ore::test]
    fn constant_columns_merge_bottom_is_identity() {
        let analysis = cc();
        let mut x = ConstCols::new();
        x.insert(0, lit1());
        x.insert(5, lit1());
        assert_eq!(analysis.merge(ConstCols::new(), x.clone()), x);
        assert_eq!(analysis.merge(x.clone(), ConstCols::new()), x);
    }
}
