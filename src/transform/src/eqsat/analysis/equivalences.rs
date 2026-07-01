// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Equivalence-class analysis: per e-class, the scalar-expression equivalences
//! known to hold over the relation the class denotes.

use std::collections::BTreeMap;

use mz_expr::MirScalarExpr;
use mz_repr::{Datum, ReprScalarType};

use crate::analysis::equivalences::{EquivalenceClasses, aggregate_is_input};
use crate::eqsat::core::{Analysis, Id};
use crate::eqsat::egraph::{CNode, CombinedLang, ENode};

use super::RelCtx;

/// Equivalence-class analysis: per e-class, the scalar-expression equivalences
/// known to hold over the relation the class denotes.
///
/// Reuses Materialize's `EquivalenceClasses` (the same type the production
/// `Equivalences` analysis produces), built here by mirroring that analysis's
/// per-operator derivation in `analysis/equivalences.rs`.
///
/// `None` means the relation is empty (vacuously all expressions equivalent),
/// the top of the lattice. `Some(default)` means no equivalences are known, the
/// bottom. `merge` combines the facts of two e-nodes that denote the same
/// relation, so it asserts all equivalences of both and re-minimizes.
pub struct Equivalences {
    pub locals: BTreeMap<usize, Option<EquivalenceClasses>>,
}

impl Analysis<CombinedLang> for Equivalences {
    type Domain = Option<EquivalenceClasses>;
    type Ctx<'a> = RelCtx<'a>;

    fn bottom(&self) -> Self::Domain {
        Some(EquivalenceClasses::default())
    }

    fn make(
        &self,
        node: &CNode,
        get: &dyn Fn(Id) -> Self::Domain,
        ctx: Self::Ctx<'_>,
    ) -> Self::Domain {
        let CNode::Rel(node) = node else {
            return self.bottom();
        };
        let arity = ctx.arity;
        let data = ctx.data;
        // Mirror `Equivalences::derive` in analysis/equivalences.rs, arm by arm.
        // Scalar payloads are scalar `Id`s; resolve them to `EScalar` via the
        // cache (`data.escalar(id).expr`) to get the `MirScalarExpr`.
        match node {
            // Leaves with no useful row data: emit empty equivalences (bottom).
            // The eqsat engine bails Constant and Get to opaque leaves; trawling
            // rows or type schemas is not available here.
            ENode::Constant { .. } | ENode::Get { .. } | ENode::Opaque(_) => {
                Some(EquivalenceClasses::default())
            }

            // A recursive reference: use the Rel-level recursion fact if we have
            // one, else assume nothing (conservative).
            ENode::LocalGet { id, .. } => self
                .locals
                .get(id)
                .cloned()
                .unwrap_or_else(|| Some(EquivalenceClasses::default())),

            // Project: restrict equivalences to the surviving columns, introducing
            // equivalences for repeated output positions.
            ENode::Project { input, outputs } => {
                let mut equivalences = get(*input);
                equivalences
                    .as_mut()
                    .map(|e| e.project(outputs.iter().cloned()));
                equivalences
            }

            // Map: for each new scalar at position (input_arity + pos), record
            // the equivalence [column(input_arity+pos), scalar_expr].
            ENode::Map { input, scalars } => {
                let mut equivalences = get(*input);
                if let Some(equivalences) = &mut equivalences {
                    let input_arity = arity(*input);
                    for (pos, &e) in scalars.iter().enumerate() {
                        equivalences.classes.push(vec![
                            MirScalarExpr::column(input_arity + pos),
                            data.escalar(e).expr.clone(),
                        ]);
                    }
                }
                equivalences
            }

            // FlatMap: pass through the input's equivalences. The func may produce
            // multiple output rows per input row and appends new columns; the
            // input equivalences survive (same column indices), but no new
            // equivalences are introduced (the output columns are opaque).
            // Mirrors production's `Equivalences::derive` which returns the input
            // equivalences unchanged for FlatMap.
            ENode::FlatMap { input, .. } => get(*input),

            // Filter: add a class that equates all predicates with literal true.
            // `IndexedFilter` is semantically the same filter, so it derives the
            // same equivalences from its predicates.
            ENode::Filter { input, predicates }
            | ENode::IndexedFilter {
                input, predicates, ..
            } => {
                let mut equivalences = get(*input);
                if let Some(equivalences) = &mut equivalences {
                    let mut class: Vec<MirScalarExpr> = predicates
                        .iter()
                        .map(|&p| data.escalar(p).expr.clone())
                        .collect();
                    class.push(MirScalarExpr::literal_ok(Datum::True, ReprScalarType::Bool));
                    equivalences.classes.push(class);
                }
                equivalences
            }

            // Join: permute each input's equivalences to the join's column
            // layout, then extend with the join's own equivalence scalars.
            // If any input is None (empty), the whole join is None.
            ENode::Join {
                inputs,
                equivalences: join_equivs,
            }
            | ENode::WcoJoin {
                inputs,
                equivalences: join_equivs,
            } => {
                let mut result = Some(EquivalenceClasses::default());
                let mut columns: usize = 0;
                for &inp in inputs.iter() {
                    let input_arity = arity(inp);
                    let child_equivs = get(inp);
                    if let Some(mut child_equivs) = child_equivs {
                        let permutation: Vec<usize> = (columns..(columns + input_arity)).collect();
                        child_equivs.permute(&permutation);
                        result
                            .as_mut()
                            .map(|e| e.classes.extend(child_equivs.classes));
                    } else {
                        result = None;
                    }
                    columns += input_arity;
                }
                // Fold the join's own equivalence scalars (each vec is one class).
                result.as_mut().map(|e| {
                    e.classes.extend(join_equivs.iter().map(|class| {
                        class
                            .iter()
                            .map(|&s| data.escalar(s).expr.clone())
                            .collect()
                    }))
                });
                result
            }

            // Reduce: mirror lines 204-252 of derive.
            // Add group-key column equivalences as if a Map, minimize, project
            // to key columns, then handle input-passthrough aggregates.
            ENode::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                let input_arity = arity(*input);
                let mut equivalences = get(*input);
                if let Some(equivalences) = &mut equivalences {
                    // Introduce key-column equivalences at positions input_arity + pos.
                    for (pos, &expr) in group_key.iter().enumerate() {
                        equivalences.classes.push(vec![
                            MirScalarExpr::column(input_arity + pos),
                            data.escalar(expr).expr.clone(),
                        ]);
                    }
                    // Minimize before projecting so cross-class information is folded.
                    equivalences.minimize(None);

                    // Keep a copy for aggregate reasoning before narrowing.
                    let extended = equivalences.clone();

                    // Project down to the group-key output columns.
                    equivalences.project(input_arity..(input_arity + group_key.len()));

                    // For aggregates that pass through an input datum (MIN/MAX/ANY/ALL),
                    // propagate their equivalences into the output.
                    for (index, aggregate) in aggregates.iter().enumerate() {
                        if aggregate_is_input(&aggregate.func) {
                            let mut temp_equivs = extended.clone();
                            temp_equivs.classes.push(vec![
                                MirScalarExpr::column(input_arity + group_key.len()),
                                aggregate.expr.clone(),
                            ]);
                            temp_equivs.minimize(None);
                            temp_equivs.project(input_arity..(input_arity + group_key.len() + 1));
                            let columns: Vec<usize> = (0..group_key.len())
                                .chain(std::iter::once(group_key.len() + index))
                                .collect();
                            temp_equivs.permute(&columns[..]);
                            equivalences.classes.extend(temp_equivs.classes);
                        }
                    }
                }
                equivalences
            }

            // Passthrough: these operators do not change which rows are present
            // (Negate flips signs but not values; TopK/Threshold filter rows but
            // do not change column values of surviving rows; ArrangeBy and
            // ArrangeByMany are multiset identities).
            ENode::Negate { input }
            | ENode::Threshold { input }
            | ENode::ArrangeBy { input, .. }
            | ENode::ArrangeByMany { input, .. }
            | ENode::TopK { input, .. } => get(*input),

            // Union: intersection of equivalences across all non-empty branches.
            // Mirrors derive's `flat_map(|c| &results[c])`: None children (empty
            // relations) are vacuously skipped because an empty branch cannot
            // constrain the union's equivalences. If all branches are None (all
            // empty), the union is also None.
            ENode::Union { inputs } => {
                // Collect only the Some values; None (empty relation) is skipped.
                let mut some_iter = inputs.iter().filter_map(|&inp| get(inp));
                let Some(first) = some_iter.next() else {
                    // All children were None (empty): union of empty relations is empty.
                    return None;
                };
                Some(first.union_many(some_iter.collect::<Vec<_>>().iter()))
            }
        }
    }

    fn merge(&self, a: Self::Domain, b: Self::Domain) -> Self::Domain {
        match (a, b) {
            // None = empty relation = absorbing top (vacuously all equivalences hold).
            (None, _) | (_, None) => None,
            (Some(mut a), Some(b)) => {
                a.classes.extend(b.classes);
                // The e-graph can force-equate arbitrary expressions via Union
                // nodes, so a single merge's minimize is bounded to prevent
                // non-termination. Stopping early is a sound under-approximation:
                // fewer known equivalences, never incorrect ones.
                a.minimize_bounded(None, 100);
                Some(a)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::MirScalarExpr;

    use crate::analysis::equivalences::EquivalenceClasses;
    use crate::eqsat::core::Analysis;
    use crate::eqsat::core::Id;
    use crate::eqsat::egraph::{CNode, CombinedLang, EGraph, ENode};
    use crate::eqsat::ir::EScalar;

    use super::{Equivalences, RelCtx};

    /// Intern an `EScalar` into `eg`, returning its scalar e-class id. The
    /// e-node `make` tests below build their relational nodes from these ids and
    /// resolve them back through `eg.data()` via the `RelCtx`.
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

    // --- Equivalences analysis tests ------------------------------------------

    /// Check that a Filter over a bottom input (no equivalences known on the
    /// input) with predicate `#0 = #1` yields a class containing
    /// `{#0, #1, true}` after minimize. The equality predicate is pushed into a
    /// class together with literal true, so minimize unpacks `Eq(#0,#1) = true`
    /// into the class `[#0, #1]`.
    #[mz_ore::test]
    fn equivalences_filter_equality_class() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };
        let mut eg = EGraph::new();
        // Input: 2 columns (arity 2), no known equivalences.
        let input_id: Id = 0;
        let input_arity = 2usize;

        // Predicate: #0 = #1
        let pred = intern(
            &mut eg,
            &EScalar::plain(MirScalarExpr::column(0).call_binary(
                MirScalarExpr::column(1),
                mz_expr::BinaryFunc::Eq(mz_expr::func::Eq),
            )),
        );

        let filter_node = ENode::Filter {
            input: input_id,
            predicates: vec![pred],
        };

        // get(input_id) returns bottom (no equivalences).
        let get = |_: Id| analysis.bottom();
        let arity = |_: Id| input_arity;

        let result = run_make(&eg, &analysis, filter_node, &get, &arity);
        let mut result = result.expect("Filter over non-empty input must be Some");
        result.minimize(None);

        // After minimize, the class [Eq(#0,#1), true] is unpacked:
        // minimize_once detects Eq(x,y)=true and adds class [x, y], so
        // columns 0 and 1 must be in the same equivalence class.
        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let reducer = result.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        assert_eq!(
            canon0, canon1,
            "Filter[#0=#1]: columns 0 and 1 must be equivalent; classes={:?}",
            result.classes
        );
    }

    /// A Join of two arity-1 inputs with join equivalence [#0, #1] (equating
    /// the first column of each input at the join's combined layout) must yield
    /// a class equating columns 0 and 1 of the joined output.
    #[mz_ore::test]
    fn equivalences_join_equiv_offsets() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };
        let mut eg = EGraph::new();
        // Two inputs each with arity 1 at ids 0 and 1.
        let left_id: Id = 0;
        let right_id: Id = 1;

        // Join equivalence: column 0 (from left, offset 0) = column 1 (from right, offset 1).
        let e0 = intern(&mut eg, &EScalar::plain(MirScalarExpr::column(0)));
        let e1 = intern(&mut eg, &EScalar::plain(MirScalarExpr::column(1)));
        let join_node = ENode::Join {
            inputs: vec![left_id, right_id],
            equivalences: vec![vec![e0, e1]],
        };

        let get = |_: Id| Some(EquivalenceClasses::default());
        let arity = |_: Id| 1usize;

        let result = run_make(&eg, &analysis, join_node, &get, &arity);
        let mut result = result.expect("Join of non-empty inputs must be Some");
        result.minimize(None);

        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let reducer = result.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        assert_eq!(
            canon0, canon1,
            "Join[#0=#1]: columns 0 and 1 must be equivalent; classes={:?}",
            result.classes
        );
    }

    /// `merge(Some(default), x) == x` (bottom is the identity) and
    /// `merge(None, x) == None` (None is absorbing).
    #[mz_ore::test]
    fn equivalences_merge_identity_and_absorbing() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };
        let bottom = analysis.bottom();
        let top: Option<EquivalenceClasses> = None;

        // Build a non-trivial Some value: {#0, #1} in one class.
        let mut ec = EquivalenceClasses::default();
        ec.classes
            .push(vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]);

        // bottom is identity for merge.
        let merged = analysis.merge(bottom.clone(), Some(ec.clone()));
        let merged = merged.expect("merge(bottom, Some(_)) must be Some");
        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let reducer = merged.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        assert_eq!(
            canon0, canon1,
            "merge(bottom, Some([#0,#1])): columns 0 and 1 must be equivalent"
        );

        // None is absorbing.
        assert_eq!(
            analysis.merge(top.clone(), Some(ec.clone())),
            None,
            "merge(None, x) must be None"
        );
        assert_eq!(
            analysis.merge(Some(ec.clone()), top),
            None,
            "merge(x, None) must be None"
        );
    }

    /// `merge` of `{#0=#1}` and `{#1=#2}` yields a single class `{#0,#1,#2}`
    /// after minimize (transitivity closure).
    #[mz_ore::test]
    fn equivalences_merge_transitive_closure() {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };

        let mut ec1 = EquivalenceClasses::default();
        ec1.classes
            .push(vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]);

        let mut ec2 = EquivalenceClasses::default();
        ec2.classes
            .push(vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]);

        let merged = analysis.merge(Some(ec1), Some(ec2));
        let merged = merged.expect("merge of two Some must be Some");

        let col0 = MirScalarExpr::column(0);
        let col1 = MirScalarExpr::column(1);
        let col2 = MirScalarExpr::column(2);
        let reducer = merged.reducer();
        let canon0 = reducer.get(&col0).unwrap_or(&col0);
        let canon1 = reducer.get(&col1).unwrap_or(&col1);
        let canon2 = reducer.get(&col2).unwrap_or(&col2);
        assert_eq!(
            canon0, canon1,
            "merge of [#0=#1] and [#1=#2]: 0 and 1 must be equivalent; classes={:?}",
            merged.classes
        );
        assert_eq!(
            canon1, canon2,
            "merge of [#0=#1] and [#1=#2]: 1 and 2 must be equivalent; classes={:?}",
            merged.classes
        );
        assert_eq!(
            canon0, canon2,
            "merge of [#0=#1] and [#1=#2]: 0 and 2 must be equivalent (transitivity); classes={:?}",
            merged.classes
        );
    }
}
