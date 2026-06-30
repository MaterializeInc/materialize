// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Rewrite rules for the scalar e-graph saturation loop.
//!
//! Each rule inspects an e-node and its children's analyses, builds any
//! intermediate nodes it needs via `eg.add(..)`, and returns zero or more
//! e-class ids to union into the source node's class. The saturation loop in
//! [`super::egraph`] snapshots (class, node) pairs read-only first, then calls
//! rules with mutable access so rules can construct nested terms.
//!
//! Phase 1 defines constant folding plus the boolean-algebra and NOT rules
//! ported from `MirScalarExpr::reduce`.

use std::collections::HashSet;

use mz_expr::{Eval, EvalError, MirScalarExpr, UnaryFunc, VariadicFunc};
use mz_repr::{Datum, ReprScalarType, RowArena};

use crate::eqsat::scalar::egraph::{Id, ScalarEGraph};
use crate::eqsat::scalar::node::SNode;
use crate::eqsat::scalar::raise;

/// A local rewrite rule. Given one e-node `node` from a class, returns e-class
/// ids that are semantically equal to `node` and should be unioned into that
/// class. A rule may call `eg.add(..)` to hash-cons new intermediate nodes and
/// return their ids. Returning an empty vec means the rule does not fire.
pub type Rule = fn(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id>;

/// The active rule set. Phase 1 grows this list; the infrastructure does not
/// change between phases.
pub fn rules() -> &'static [Rule] {
    &[
        const_fold,
        and_or_dedup,
        and_or_single,
        and_or_empty,
        and_or_short_circuit,
        and_or_drop_unit,
        flatten_assoc,
        factor_and_or,
        absorb_and_or,
        not_not,
        not_binary_negate,
        not_demorgan,
        if_true,
        if_false_or_null,
        if_same_branches,
        if_err_cond,
        null_prop_binary,
        null_prop_variadic,
        err_prop_binary,
        err_prop_variadic,
        isnull_fold,
    ]
}

/// Whether `func` is `And` or `Or`. The boolean-algebra rules apply uniformly to
/// both, parameterized by the connective's unit and zero.
fn is_and_or(func: &VariadicFunc) -> bool {
    matches!(func, VariadicFunc::And(_) | VariadicFunc::Or(_))
}

/// The non-null boolean literal of class `id`, if it carries one.
///
/// Returns `Some(true)`/`Some(false)` only for a class whose literal analysis is
/// `Ok(true)`/`Ok(false)`. A null literal, an error literal, a non-boolean
/// literal, or a non-literal class returns `None`. The short-circuit and
/// drop-unit rules use this so a null operand never matches the true/false arms,
/// matching reduce, which compares against the concrete `true`/`false` literals.
fn lit_bool(eg: &ScalarEGraph, id: Id) -> Option<bool> {
    let (row, _col_type) = eg.analysis(id).literal.as_ref()?;
    let row = row.as_ref().ok()?;
    match row.unpack_first() {
        Datum::True => Some(true),
        Datum::False => Some(false),
        _ => None,
    }
}

/// Build the `SNode::Literal` for the connective's unit (`true` for `And`,
/// `false` for `Or`).
fn unit_node(func: &VariadicFunc) -> SNode {
    destructure_literal(func.unit_of_and_or())
}

/// Destructure a `MirScalarExpr::Literal` into an `SNode::Literal` leaf.
fn destructure_literal(expr: MirScalarExpr) -> SNode {
    let MirScalarExpr::Literal(row, col_type) = expr else {
        unreachable!("expected a Literal variant")
    };
    SNode::Literal(row, col_type)
}

/// Fold a call whose operands are all literals into the literal it evaluates to.
///
/// Sound because evaluation IS the meaning of the call: if every operand is a
/// compile-time constant, the result is too. Non-destructive: the original call
/// node remains in the class; extraction prefers the cheaper literal node.
fn const_fold(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    // Only call nodes can fold; leaves have nothing to fold.
    match node {
        SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(..) => return vec![],
        _ => {}
    }

    // Never evaluate `mz_panic` at optimization time. Folding a literal-argument
    // `Panic` would run the call here (`call.eval` below), aborting the optimizer
    // instead of letting the panic surface at runtime where it belongs. `reduce`
    // makes the same exclusion in `reduce/unary.rs`, and this rule must match it.
    if let SNode::CallUnary { func, .. } = node {
        if matches!(func, UnaryFunc::Panic(_)) {
            return vec![];
        }
    }

    // Every child class must carry a literal analysis for the fold to fire.
    let children = node.children();
    let child_lits: Vec<_> = children
        .iter()
        .map(|&c| eg.analysis(c).literal.clone())
        .collect();
    if child_lits.iter().any(|l| l.is_none()) {
        return vec![];
    }
    // Unwrap is safe: all are Some.
    let child_lits: Vec<_> = child_lits.into_iter().map(|l| l.unwrap()).collect();

    // Reconstruct child MirScalarExprs as literal nodes so we can evaluate
    // the parent without Column refs. Empty col_types is sound here because
    // a fully-literal subtree has no Column references.
    let child_exprs: Vec<MirScalarExpr> = child_lits
        .into_iter()
        .map(|(row, col_type)| MirScalarExpr::Literal(row, col_type))
        .collect();

    // Assemble the parent call with its literal child exprs.
    let call = match node {
        SNode::CallUnary { func, .. } => MirScalarExpr::CallUnary {
            func: func.clone(),
            expr: Box::new(child_exprs[0].clone()),
        },
        SNode::CallBinary { func, .. } => MirScalarExpr::CallBinary {
            func: func.clone(),
            expr1: Box::new(child_exprs[0].clone()),
            expr2: Box::new(child_exprs[1].clone()),
        },
        SNode::CallVariadic { func, .. } => MirScalarExpr::CallVariadic {
            func: func.clone(),
            exprs: child_exprs,
        },
        SNode::If { .. } => MirScalarExpr::If {
            cond: Box::new(child_exprs[0].clone()),
            then: Box::new(child_exprs[1].clone()),
            els: Box::new(child_exprs[2].clone()),
        },
        // Leaves were already handled above.
        SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(..) => {
            unreachable!("leaves filtered above")
        }
    };

    // Evaluate using empty datums and column types: no Column refs exist, so
    // both are unused.
    let temp = RowArena::new();
    // `MirScalarExpr::literal` takes Result<Datum, EvalError>; eval returns that.
    let folded = MirScalarExpr::literal(call.eval(&[], &temp), call.typ(&[]).scalar_type);

    // Destructure the resulting Literal into an SNode leaf and add it.
    vec![eg.add(destructure_literal(folded))]
}

/// `x AND/OR x -> x`: drop duplicate operands of an AND/OR.
///
/// Mirrors reduce's `exprs.dedup()` after a sort. The e-graph already canonicalizes
/// children to class ids, so duplicates are equal ids regardless of source
/// syntax. We dedup by canonical id and keep the first occurrence, which is the
/// stronger (order-independent) form of reduce's sort-then-dedup.
fn and_or_dedup(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    if !is_and_or(func) {
        return vec![];
    }
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for &e in exprs {
        let canon = eg.find(e);
        // insert returns false when the id was already present, so a duplicate
        // is skipped in O(1) rather than the O(n) of a Vec membership scan.
        if !seen.insert(canon) {
            continue;
        }
        deduped.push(e);
    }
    if deduped.len() == exprs.len() {
        // No duplicates: the rule does not fire.
        return vec![];
    }
    let func = func.clone();
    vec![eg.add(SNode::CallVariadic {
        func,
        exprs: deduped,
    })]
}

/// `AND/OR(x) -> x`: a one-argument AND/OR equals its sole argument.
///
/// Mirrors reduce's `exprs.len() == 1` arm. Unions the AND/OR class with the
/// argument's class by returning the argument class's id.
fn and_or_single(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    if !is_and_or(func) || exprs.len() != 1 {
        return vec![];
    }
    vec![eg.find(exprs[0])]
}

/// `AND() -> true`, `OR() -> false`: a zero-argument AND/OR is its unit.
///
/// Mirrors reduce's `exprs.len() == 0` arm via `func.unit_of_and_or()`.
fn and_or_empty(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    if !is_and_or(func) || !exprs.is_empty() {
        return vec![];
    }
    vec![eg.add(unit_node(func))]
}

/// `AND(.., false, ..) -> false`, `OR(.., true, ..) -> true`: short-circuit to
/// the connective's zero.
///
/// Mirrors reduce's `exprs.iter().any(|e| *e == func.zero_of_and_or())` arm. The
/// zero is `false` for `And` and `true` for `Or`. Sound under three-valued logic
/// and under errors: `AND` returns `false` as soon as any operand is `false`,
/// even if another operand would error, so collapsing to `false` preserves
/// evaluation. The property test exercises this with a `1/0` operand.
fn and_or_short_circuit(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    let zero = match func {
        // The zero of AND is false; the zero of OR is true.
        VariadicFunc::And(_) => false,
        VariadicFunc::Or(_) => true,
        _ => return vec![],
    };
    if exprs.iter().any(|&e| lit_bool(eg, e) == Some(zero)) {
        let zero_node = destructure_literal(func.zero_of_and_or());
        return vec![eg.add(zero_node)];
    }
    vec![]
}

/// `AND(.., true, ..) -> AND(..)`, `OR(.., false, ..) -> OR(..)`: drop operands
/// equal to the connective's unit.
///
/// Mirrors reduce's `exprs.retain(|e| *e != func.unit_of_and_or())` arm. Unit is
/// `true` for `And` and `false` for `Or`. We only drop the units; if dropping
/// leaves one or zero operands, `and_or_single` / `and_or_empty` collapse it on
/// a later iteration. Returning a multi-operand result when nothing was dropped
/// would be a no-op union, so we fire only when at least one unit was removed.
fn and_or_drop_unit(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    let unit = match func {
        // The unit of AND is true; the unit of OR is false.
        VariadicFunc::And(_) => true,
        VariadicFunc::Or(_) => false,
        _ => return vec![],
    };
    let kept: Vec<Id> = exprs
        .iter()
        .copied()
        .filter(|&e| lit_bool(eg, e) != Some(unit))
        .collect();
    if kept.len() == exprs.len() {
        // No unit operand: the rule does not fire.
        return vec![];
    }
    let func = func.clone();
    vec![eg.add(SNode::CallVariadic { func, exprs: kept })]
}

/// Hard cap on the operand-vector length produced by [`flatten_assoc`].
///
/// Chosen to be comfortably above any realistic predicate width and above
/// `MAX_ENODES` (600 at the time of writing) so that the cap never triggers
/// on legitimate inputs while still bounding transitive-cycle blowup.
const FLATTEN_MAX_OPERANDS: usize = 4096;

/// `AND(a, AND(b, c)) -> AND(a, b, c)` and `OR(a, OR(b, c)) -> OR(a, b, c)`:
/// splice nested same-connective variadic operands one level up.
///
/// For any associative variadic (And, Or, and any other func with
/// `func.is_associative()` == true), a nested call with the same func can be
/// dissolved: the outer and inner calls are interchangeable by associativity, so
/// the flat form evaluates identically for all inputs.
///
/// UNCONDITIONAL: no `could_error` gate, no nullability gate. Associativity is
/// order-independent over the And/Or join semilattice, including error handling
/// (And/Or accumulate errors via `max` and surface the max error after all
/// non-short-circuit operands, regardless of evaluation order). Short-circuit is
/// also order-independent: a false in And (or true in Or) dominates regardless of
/// nesting. So `And(a, And(b, c))` and `And(a, b, c)` evaluate identically for
/// ALL inputs, including error-producing ones. Mirrors `reduce`'s
/// `flatten_associative` (`src/expr/src/scalar.rs`), which is also ungated. The
/// unconditional-soundness argument generalizes to every `is_associative`
/// variadic (Coalesce, Greatest, Least) by the same reduce-parity reasoning.
///
/// The flat form is structurally cheaper (one fewer interior variadic node per
/// spliced level), so min-cost extraction prefers it. This delivers the flat
/// conjunction that `canonicalize_predicates` relies on.
///
/// Fires only when at least one operand was actually spliced; otherwise returns
/// `vec![]` to avoid a no-op union. Saturation handles deeper nesting by
/// re-applying on the newly flat node (which has no same-func child and returns
/// `vec![]` immediately).
///
/// Caps the produced operand vector at [`FLATTEN_MAX_OPERANDS`] as defense-in-depth
/// against transitive same-func cycles. The one-hop cycle guard below covers every
/// cycle the current rule set can produce (direct self-loops from `and_or_single`);
/// this cap bounds the worst case should a future rule introduce a two-class
/// transitive cycle that the one-hop guard misses. Legitimate flat conjunctions are
/// far below the cap.
fn flatten_assoc(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    if !func.is_associative() {
        return vec![];
    }
    // Read all sibling nodes into owned data before any eg.add (borrow discipline).
    // Build the flat operand list by splicing any same-func child.
    let mut new_exprs: Vec<Id> = Vec::with_capacity(exprs.len());
    let mut spliced = false;
    for &op_id in exprs {
        let canon = eg.find(op_id);
        // eg.nodes() returns an owned Vec<SNode>, releasing the eg borrow immediately.
        let siblings = eg.nodes(canon);
        let mut inner: Option<Vec<Id>> = None;
        for sibling in &siblings {
            if let SNode::CallVariadic {
                func: inner_func,
                exprs: inner_exprs,
            } = sibling
            {
                if inner_func != func {
                    continue;
                }
                // Canonicalize the inner children before checking for circular
                // references. After rules like `and_or_single` collapse
                // `OR(c0)` into c0's class, c0's class contains OR nodes
                // whose children point back to `canon` itself. Splicing such
                // nodes would replace one copy of `c0` with N copies of
                // `c0`, producing exponential growth across iterations. Skip
                // any inner node whose children are all in the same class as
                // the current operand.
                let inner_canons: Vec<Id> = inner_exprs.iter().map(|&e| eg.find(e)).collect();
                if inner_canons.contains(&canon) {
                    continue;
                }
                // First non-circular matching node is fine; congruence makes
                // any such node's operand list semantically equal.
                inner = Some(inner_canons);
                break;
            }
        }
        if let Some(inner_ids) = inner {
            new_exprs.extend(inner_ids);
            spliced = true;
        } else {
            new_exprs.push(op_id);
        }
    }
    if !spliced {
        return vec![];
    }
    // NOTE: Defense-in-depth against transitive same-func cycles. The one-hop
    // guard above eliminates every cycle the current rules produce, but a future
    // rule proving X=f(..,Y,..) and Y=f(..,X,..) for distinct classes would
    // create a two-class cycle that the one-hop guard misses, and neither
    // MAX_ENODES nor MAX_ITERS bounds operand-vector growth in that case.
    // Declining the splice when the vector exceeds FLATTEN_MAX_OPERANDS bounds
    // the worst case regardless of cycle shape. Declining only means less
    // flattening, never a wrong node.
    if new_exprs.len() > FLATTEN_MAX_OPERANDS {
        return vec![];
    }
    let func = func.clone();
    vec![eg.add(SNode::CallVariadic {
        func,
        exprs: new_exprs,
    })]
}

/// For each operand of an AND/OR, its inner-operand set as canonical, sorted,
/// unique class ids under the dual connective `inner_func`.
///
/// If an operand's class holds an `inner_func` variadic node, that node's operand
/// ids are the set; otherwise the operand is a singleton `{find(operand)}`
/// (reduce's 1-arg view). A class can hold several nodes. The first `inner_func`
/// node found is fine, congruence makes any such node's operand set equal. Shared
/// by [`factor_and_or`] and [`absorb_and_or`], which both reason over these
/// inner-sets by canonical id.
fn inner_sets(eg: &ScalarEGraph, operands: &[Id], inner_func: &VariadicFunc) -> Vec<Vec<Id>> {
    let mut sets: Vec<Vec<Id>> = Vec::with_capacity(operands.len());
    for &operand in operands {
        let canon = eg.find(operand);
        let mut inner: Option<Vec<Id>> = None;
        for sibling in eg.nodes(canon) {
            if let SNode::CallVariadic { func, exprs } = sibling {
                if &func == inner_func {
                    let mut ids: Vec<Id> = exprs.iter().map(|&e| eg.find(e)).collect();
                    ids.sort();
                    ids.dedup();
                    inner = Some(ids);
                    break;
                }
            }
        }
        sets.push(inner.unwrap_or_else(|| vec![canon]));
    }
    sets
}

/// Undistribute a common factor out of an AND/OR, residual-error gated.
///
/// ```text
/// undistribute-OR  (outer Or, inner And):  (a∧b)∨(a∧c) → a∧(b∨c)
/// undistribute-AND (outer And, inner Or):  (a∨b)∧(a∨c) → a∨(b∧c)
/// ```
///
/// Generalized to the FULL common-factor intersection across all outer operands
/// in one step. With outer connective `outer_func` and inner `inner_func =
/// outer_func.switch_and_or()`, this builds
/// `inner_func(factor.., outer_func(residual_1, .., residual_n))`, where `factor`
/// is the set of inner operands common to every branch and `residual_i` is branch
/// `i`'s remaining inner operands. It mirrors the `!intersection.is_empty()` path
/// of `MirScalarExpr::undistribute_and_or` (`src/expr/src/scalar.rs`), but
/// non-destructively: it unions the factored form into the source class rather
/// than mutating operands, so no fixpoint loop and no subset heuristic are needed
/// (those exist in reduce only because it rewrites in place).
///
/// RESIDUAL-ERROR GATE. Factoring is NOT unconditionally sound under exact
/// evaluation: `And`/`Or` surface an operand error only when no short-circuit
/// value masks it, so regrouping can turn an error into a null when the common
/// factor is null and a residual both masks an error on one branch and not
/// another (see the design note's worked counterexample). The conservative,
/// exact-eval-sound gate is to fire only when EVERY residual operand has
/// `could_error == false`. The common factor MAY error and is deliberately NOT
/// gated, because it is never dropped or relocated past a masking value. That is
/// the precise narrowing of reduce's over-broad whole-expression
/// `self.could_error()`: in CLU-137 the fallible cast lives in the common factor,
/// while the residuals (`s IS NULL`, `s = ''`) cannot error, so factoring fires
/// and lifts the temporal conjunct to the top level.
///
/// NON-EMPTY-RESIDUAL CONDITION. This rule fires only when every branch has a
/// non-empty residual. An empty residual is the absorption case (a branch that
/// WAS exactly the intersection, e.g. `(a∧b)∨(a∧b∧c)`); it belongs to the
/// separately gated absorption rule. Producing it here would build an empty inner
/// call that Phase 1 collapses to the connective's unit, which the short-circuit
/// and drop-unit rules would then turn into an UNGATED absorption, reintroducing
/// the CLU-137 class of bug. The non-empty-residual condition keeps factoring and
/// absorption cleanly distinct.
///
/// SCOPE: full-intersection factoring only. Subset factoring (reduce's
/// `indexes_to_undistribute` path) and the alternative "common factor cannot be
/// null" gate disjunct are deferred.
fn factor_and_or(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic {
        func: outer_func,
        exprs: outer_operands,
    } = node
    else {
        return vec![];
    };
    if !is_and_or(outer_func) {
        return vec![];
    }
    // Factoring needs at least two branches to find something common.
    if outer_operands.len() < 2 {
        return vec![];
    }
    let inner_func = outer_func.switch_and_or();

    // For each outer operand, its inner-operand set (the operand's `inner_func`
    // operands, or a singleton if it is not such a call). See [`inner_sets`].
    let branch_sets = inner_sets(eg, outer_operands, &inner_func);

    // The intersection common to every branch. Built from the first branch's
    // (already sorted, unique) set, so it stays sorted and unique.
    let mut intersection = branch_sets[0].clone();
    for set in &branch_sets[1..] {
        intersection.retain(|id| set.contains(id));
    }
    if intersection.is_empty() {
        // Nothing common to all branches; full-intersection factoring does not
        // apply. Subset factoring is deferred.
        return vec![];
    }

    // Each branch's residual: its inner operands not in the intersection.
    let mut residuals: Vec<Vec<Id>> = Vec::with_capacity(branch_sets.len());
    for set in &branch_sets {
        let residual: Vec<Id> = set
            .iter()
            .copied()
            .filter(|id| !intersection.contains(id))
            .collect();
        if residual.is_empty() {
            // Absorption case (this branch was exactly the intersection); leave
            // it to the separately gated absorption rule. See the non-empty
            // residual condition in the doc comment.
            return vec![];
        }
        residuals.push(residual);
    }

    // Residual-error gate: every residual operand must be provably error-free.
    // The common factor is intentionally exempt; see the doc comment.
    for residual in &residuals {
        if residual.iter().any(|&id| eg.analysis(id).could_error) {
            return vec![];
        }
    }

    // Build the factored form. All operand sets are owned class ids already, so
    // no borrow of `eg` is held across the `eg.add` calls.
    let outer_func = outer_func.clone();
    let mut branch_ids: Vec<Id> = residuals
        .into_iter()
        .map(|residual| {
            if residual.len() == 1 {
                residual[0]
            } else {
                eg.add(SNode::CallVariadic {
                    func: inner_func.clone(),
                    exprs: residual,
                })
            }
        })
        .collect();
    // Sort the branch ids so the constructed combination node is stable across
    // re-firing (hashcons keys on the operand vector).
    branch_ids.sort();
    let residual_combination = eg.add(SNode::CallVariadic {
        func: outer_func,
        exprs: branch_ids,
    });
    let mut factored_exprs = intersection;
    factored_exprs.push(residual_combination);
    vec![eg.add(SNode::CallVariadic {
        func: inner_func,
        exprs: factored_exprs,
    })]
}

/// Absorption: drop an AND/OR operand whose inner-set is a superset of another's.
///
/// ```text
/// absorb-OR  (outer Or, inner And):  a ∨ (a ∧ c) → a
/// absorb-AND (outer And, inner Or):  a ∧ (a ∨ c) → a
/// ```
///
/// Generalized over inner-sets. With outer connective `outer_func` and inner
/// `inner_func = outer_func.switch_and_or()`, each outer operand maps to its set
/// of inner class ids (see [`inner_sets`]). When one operand `Q`'s inner-set is a
/// proper SUPERSET of another operand `P`'s inner-set, `Q` is redundant and is
/// dropped: for an outer `Or`, `Q = AND(S_Q)` has more conjuncts than
/// `P = AND(S_P)`, so `Q` implies `P` and `P ∨ Q = P` (dual for outer `And`).
/// Mirrors the Absorb-OR / Absorb-AND special case folded into
/// `MirScalarExpr::undistribute_and_or` (`src/expr/src/scalar.rs`), but
/// non-destructively: it unions the absorbed form into the source class rather
/// than mutating operands.
///
/// DROPPED-EXTRA GATE. This is the gated counterpart to [`factor_and_or`]: the
/// CLU-137 split gates only the operands whose error behavior the rewrite would
/// change. For absorption that is the EXTRA inner operands dropped along with
/// `Q`, namely `inner-set(Q) \ inner-set(P)`. `a ∨ (a ∧ c) → a` is unsound when
/// `a` can be null and `c` errors: the original `null ∨ (null ∧ err)` evaluates
/// to `err`, but the result `a` is `null`. The error came from `c`, a dropped
/// extra. So this rule fires only when every id in `inner-set(Q) \ inner-set(P)`
/// has `could_error == false`. The retained operand `P` is deliberately NOT
/// gated: it stays in the result, so its own error still surfaces (verified over
/// the cube for `a ∈ {true, false, null, error}`). Gating `P`, or the whole
/// expression as reduce's over-broad `self.could_error()` does, would needlessly
/// block sound absorptions.
///
/// SCOPE: the alternative "retained operand `P` cannot be null" gate disjunct
/// (which would make absorption sound even when the extras can error) needs a
/// nullability analysis we lack and is deferred.
fn absorb_and_or(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic {
        func: outer_func,
        exprs: outer_operands,
    } = node
    else {
        return vec![];
    };
    if !is_and_or(outer_func) {
        return vec![];
    }
    // Absorption needs at least two operands: one to drop, one to retain.
    if outer_operands.len() < 2 {
        return vec![];
    }
    let inner_func = outer_func.switch_and_or();
    let sets = inner_sets(eg, outer_operands, &inner_func);

    // Find the first operand `Q` (by index) that some distinct operand `P` proper-
    // subsumes (`inner-set(P) ⊊ inner-set(Q)`) and whose dropped extras are all
    // error-free. Iterating by index makes the choice deterministic. The result
    // (drop `Q`, keep the rest) does not depend on which `P` qualified, so finding
    // any one qualifying `P` suffices.
    let mut drop_index: Option<usize> = None;
    'search: for q in 0..sets.len() {
        for p in 0..sets.len() {
            if p == q {
                continue;
            }
            // `P ⊊ Q`: sets are unique, so a strictly smaller `P` fully contained
            // in `Q` is a proper subset. Equal sets (duplicates) are left to
            // `and_or_dedup`, not absorbed here.
            if sets[p].len() >= sets[q].len() {
                continue;
            }
            if !sets[p].iter().all(|id| sets[q].contains(id)) {
                continue;
            }
            // Gate the dropped extras `inner-set(Q) \ inner-set(P)`.
            let extras_can_error = sets[q]
                .iter()
                .filter(|id| !sets[p].contains(id))
                .any(|&id| eg.analysis(id).could_error);
            if extras_can_error {
                continue;
            }
            drop_index = Some(q);
            break 'search;
        }
    }
    let Some(q) = drop_index else {
        return vec![];
    };

    // Build the outer call of all operands except `Q`. The kept ids are owned and
    // sorted so the constructed node is stable across re-firing (hashcons keys on
    // the operand vector). `outer_operands` borrows `node`, not `eg`, so the
    // borrow does not conflict with the `eg.add` below.
    let mut kept: Vec<Id> = outer_operands
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != q)
        .map(|(_, &id)| id)
        .collect();
    kept.sort();
    if kept.len() == 1 {
        // One operand remains; union directly with its class.
        return vec![eg.find(kept[0])];
    }
    let outer_func = outer_func.clone();
    vec![eg.add(SNode::CallVariadic {
        func: outer_func,
        exprs: kept,
    })]
}

/// `Not(Not(x)) -> x`: double-negation elimination.
///
/// Mirrors reduce's inner `Not` arm. Looks for a `Not` e-node in the child class
/// and unions this `Not` class with the grandchild's class. Sound under
/// three-valued logic: `Not(Not(null)) == null`.
fn not_not(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallUnary { func, expr } = node else {
        return vec![];
    };
    if !matches!(func, UnaryFunc::Not(_)) {
        return vec![];
    }
    let mut out = Vec::new();
    for child in eg.nodes(*expr) {
        if let SNode::CallUnary {
            func: UnaryFunc::Not(_),
            expr: grandchild,
        } = child
        {
            // Union with the grandchild class by returning its canonical id.
            out.push(eg.find(grandchild));
        }
    }
    out
}

/// `Not(a bf b) -> a negate(bf) b` when `bf.negate()` exists.
///
/// Mirrors reduce's binary `Not` arm. The negated function over the same two
/// operand classes is an equivalent term, e.g. `Not(a = b) -> a != b`. The
/// operands `e1`, `e2` are existing class ids, so no nested new nodes are needed.
fn not_binary_negate(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallUnary { func, expr } = node else {
        return vec![];
    };
    if !matches!(func, UnaryFunc::Not(_)) {
        return vec![];
    }
    let mut out = Vec::new();
    for child in eg.nodes(*expr) {
        if let SNode::CallBinary {
            func: bf,
            expr1,
            expr2,
        } = child
        {
            if let Some(neg) = bf.negate() {
                out.push(eg.add(SNode::CallBinary {
                    func: neg,
                    expr1,
                    expr2,
                }));
            }
        }
    }
    out
}

/// `NOT(AND(a, b, ...)) -> OR(NOT a, NOT b, ...)` and
/// `NOT(OR(a, b, ...)) -> AND(NOT a, NOT b, ...)`.
///
/// Structurally equivalent in Kleene three-valued logic and under error
/// semantics: operand order is preserved and `switch_and_or` flips the
/// short-circuit value in lockstep with the outer `Not`, so the same operand
/// decides the result on both sides. The error a variadic And/Or surfaces is
/// the order-INDEPENDENT `max` over its operands' errors (`func/variadic.rs:89`:
/// `err = max(err.take(), Some(this_err))`), not the first error under
/// left-to-right evaluation, so wrapping each operand in `Not` and switching the
/// connective cannot change which error wins. No `could_error` gate is needed.
/// Mirrors `reduce::demorgans` (ungated).
fn not_demorgan(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallUnary { func, expr } = node else {
        return vec![];
    };
    if !matches!(func, UnaryFunc::Not(_)) {
        return vec![];
    }
    // eg.nodes() returns an owned Vec<SNode>, releasing the borrow before the
    // eg.add() calls below.
    let child_nodes = eg.nodes(*expr);
    let mut out = Vec::new();
    for child in child_nodes {
        let SNode::CallVariadic { func: vf, exprs } = child else {
            continue;
        };
        if !is_and_or(&vf) {
            continue;
        }
        // Clone the flipped connective and the Not func before mutating eg.
        let new_func = vf.switch_and_or();
        let not_func = func.clone();
        // Build NOT(operand) for each operand, then build the flipped variadic.
        let not_ids: Vec<Id> = exprs
            .into_iter()
            .map(|op| {
                eg.add(SNode::CallUnary {
                    func: not_func.clone(),
                    expr: op,
                })
            })
            .collect();
        out.push(eg.add(SNode::CallVariadic {
            func: new_func,
            exprs: not_ids,
        }));
    }
    out
}

/// The boolean-or-null literal of class `id`, distinguishing all three cases.
///
/// Returns `Some(Some(true))` for a `true` literal, `Some(Some(false))` for a
/// `false` literal, `Some(None)` for a `null` literal, and `None` for anything
/// else (non-literal, error literal, non-boolean, or multi-datum rows).
///
/// An error-literal condition returns `None` so the If rules leave it alone.
/// That is the deferred Err arm of `reduce_if`, which needs branch types.
fn lit_bool_or_null(eg: &ScalarEGraph, id: Id) -> Option<Option<bool>> {
    let (row, _col_type) = eg.analysis(id).literal.as_ref()?;
    let row = row.as_ref().ok()?;
    match row.unpack_first() {
        Datum::True => Some(Some(true)),
        Datum::False => Some(Some(false)),
        Datum::Null => Some(None),
        _ => None,
    }
}

/// `if(true, a, b) -> a`: constant-true condition resolves to the then branch.
///
/// Mirrors `reduce_if`'s `Ok(Datum::True)` arm. The rule fires only when the
/// condition class carries a `true` literal; error literals are excluded (those
/// belong to a deferred col_types task). Unions the If class with the then
/// class by returning the then class's id.
fn if_true(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::If { cond, then, .. } = node else {
        return vec![];
    };
    if lit_bool_or_null(eg, *cond) != Some(Some(true)) {
        return vec![];
    }
    vec![eg.find(*then)]
}

/// `if(false, a, b) -> b` and `if(null, a, b) -> b`: constant-false or null
/// condition resolves to the else branch.
///
/// Mirrors `reduce_if`'s `Ok(Datum::False) | Ok(Datum::Null)` arm. A null
/// condition takes the else branch per SQL semantics. Error literals are
/// excluded (deferred). Unions the If class with the els class.
fn if_false_or_null(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::If { cond, els, .. } = node else {
        return vec![];
    };
    match lit_bool_or_null(eg, *cond) {
        Some(Some(false)) | Some(None) => vec![eg.find(*els)],
        _ => vec![],
    }
}

/// `if(c, x, x) -> x` when `c` cannot error: identical branches collapse.
///
/// Uses canonical class ids to detect branch equivalence, which is strictly
/// stronger than syntactic equality and covers cases where distinct source
/// expressions were proven equal by prior rules. Unions the If class with the
/// (shared) branch class.
///
/// DELIBERATE DEVIATION FROM REDUCE, gated on `could_error`. reduce's `then ==
/// els` arm (`reduce/if_then.rs`) collapses `if(c, x, x)` to `x` UNGATED on the
/// condition. `If` evaluates the condition first and short-circuits on a
/// condition error (the `Eval` impl runs `cond.eval(..)?` before either branch),
/// so when `c` can error `if(c, x, x)` is NOT equal to `x`: the input surfaces
/// `c`'s error while `x` does not. Firing only when the condition class has
/// `could_error == false` keeps the rewrite eval-exact, the same gate and
/// rationale as the err/null-prop rules. This is strictly more conservative than
/// reduce.
fn if_same_branches(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::If { cond, then, els } = node else {
        return vec![];
    };
    let then_id = eg.find(*then);
    let els_id = eg.find(*els);
    if then_id != els_id {
        return vec![];
    }
    if eg.analysis(*cond).could_error {
        return vec![];
    }
    vec![then_id]
}

/// The scalar type a call node evaluates to, computed from its children's types.
///
/// Rules that build a typed literal (a null or an error) need the call's output
/// type. We reconstruct the call as a `MirScalarExpr` by raising each child to
/// its cheapest representative and asking `typ`, which is what reduce does with
/// `e.typ(column_types).scalar_type`. The raised children may carry columns, so
/// `eg.col_types()` is required to type them.
fn call_scalar_type(eg: &ScalarEGraph, node: &SNode) -> ReprScalarType {
    let raised: Vec<MirScalarExpr> = node
        .children()
        .iter()
        .map(|&c| raise::raise(eg, c))
        .collect();
    let assembled = match node {
        SNode::CallUnary { func, .. } => MirScalarExpr::CallUnary {
            func: func.clone(),
            expr: Box::new(raised[0].clone()),
        },
        SNode::CallBinary { func, .. } => MirScalarExpr::CallBinary {
            func: func.clone(),
            expr1: Box::new(raised[0].clone()),
            expr2: Box::new(raised[1].clone()),
        },
        SNode::CallVariadic { func, .. } => MirScalarExpr::CallVariadic {
            func: func.clone(),
            exprs: raised,
        },
        SNode::If { .. } => MirScalarExpr::If {
            cond: Box::new(raised[0].clone()),
            then: Box::new(raised[1].clone()),
            els: Box::new(raised[2].clone()),
        },
        SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(..) => {
            unreachable!("call_scalar_type is only called on call nodes")
        }
    };
    assembled.typ(eg.col_types()).scalar_type
}

/// Whether class `id` is a literal `null`.
///
/// True iff the class's literal analysis is `Some((Ok(row), _))` whose single
/// datum is `Datum::Null`. An error literal, a non-null literal, or a
/// non-literal class returns false.
fn is_literal_null(eg: &ScalarEGraph, id: Id) -> bool {
    let Some((Ok(row), _col_type)) = eg.analysis(id).literal.as_ref() else {
        return false;
    };
    row.unpack_first() == Datum::Null
}

/// The `EvalError` when class `id` is a literal error, else `None`.
///
/// Returns `Some(e)` iff the class's literal analysis is `Some((Err(e), _))`.
/// An ok literal, a non-literal class, or a class with no literal returns `None`.
fn literal_err(eg: &ScalarEGraph, id: Id) -> Option<EvalError> {
    let (row, _col_type) = eg.analysis(id).literal.as_ref()?;
    match row {
        Err(e) => Some(e.clone()),
        Ok(_) => None,
    }
}

/// `if(err_cond, then, els) -> err_cond` when the condition is a literal error.
///
/// Mirrors `reduce_if`'s `Err(err)` arm exactly. This rule needs NO could_error
/// gate: `If` evaluates the condition first via `?` and short-circuits on it
/// (see `scalar.rs` Eval: `match cond.eval(datums, temp)? { ... }`), so a
/// literal-error condition makes the entire `If` evaluate to that error before
/// `then` or `els` are ever touched. The rewrite is therefore unconditionally
/// sound regardless of what the branches are or whether they could error.
///
/// The result type is the union of the two branch types, matching reduce, which
/// computes `then.typ(column_types).union(&els.typ(column_types))`. If the union
/// fails (incompatible scalar types, which should not occur in well-typed
/// input), the rule conservatively does not fire rather than unwrap-panic.
fn if_err_cond(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::If { cond, then, els } = node else {
        return vec![];
    };
    let Some(err) = literal_err(eg, *cond) else {
        return vec![];
    };
    let then_ty = raise::raise(eg, *then).typ(eg.col_types());
    let els_ty = raise::raise(eg, *els).typ(eg.col_types());
    let Ok(result_ty) = then_ty.union(&els_ty) else {
        // Incompatible branch types should not occur in a well-typed expression.
        // Be conservative and do not fire rather than unwrap-panic like reduce.
        return vec![];
    };
    vec![eg.add(SNode::Literal(Err(err), result_ty))]
}

/// `f(null, b) -> null` and `f(a, null) -> null` for a binary `f` that
/// propagates nulls, GATED on the other operand being error-free.
///
/// DELIBERATE DEVIATION FROM REDUCE. reduce's binary null-prop
/// (`src/expr/src/scalar/reduce/binary.rs`) fires whenever an operand is a
/// literal null and `func.propagates_nulls()`, UNGATED on the other operand. A
/// probe established that Materialize's eval returns the ERROR, not null, when
/// the other operand errors: `eval(AddInt64(null, 1/0))` is
/// `Err(DivisionByZero)`, not `Null`. So rewriting `f(null, x)` to null when `x`
/// can error would turn `err` into `null`, an unsound rewrite. In the e-graph it
/// is worse: it would union a null class with the error-producing class.
///
/// Therefore this rule fires only when `func.propagates_nulls()` and the OTHER
/// operand's class has `could_error == false`. This is strictly more
/// conservative than reduce. It never changes `err` to `null`, and on the
/// all-literal case it agrees with `const_fold`: an error-literal operand has
/// `could_error == true`, so the gate blocks null-prop and lets `const_fold`
/// produce the error, so the two rules never union disagreeing values.
fn null_prop_binary(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallBinary { func, expr1, expr2 } = node else {
        return vec![];
    };
    if !func.propagates_nulls() {
        return vec![];
    }
    // Determine the non-null-literal operand whose error-freedom we must check.
    // If both operands are literal null the call is fully literal and
    // `const_fold` handles it, but the gate below still holds (a null literal
    // has `could_error == false`).
    let other = if is_literal_null(eg, *expr1) {
        *expr2
    } else if is_literal_null(eg, *expr2) {
        *expr1
    } else {
        return vec![];
    };
    if eg.analysis(other).could_error {
        return vec![];
    }
    let ty = call_scalar_type(eg, node);
    vec![eg.add(destructure_literal(MirScalarExpr::literal_null(ty)))]
}

/// `f(.., null, ..) -> null` for a variadic `f` that propagates nulls, GATED on
/// every other operand being error-free.
///
/// Same deviation from reduce as [`null_prop_binary`]: reduce's variadic
/// null-prop (`src/expr/src/scalar/reduce/variadic.rs`) is ungated on the other
/// operands, which would change `err` into `null` for an error-producing
/// operand. The probe result (`eval(AddInt64(null, 1/0))` is
/// `Err(DivisionByZero)`, not `Null`) shows error wins over null in eval, so we
/// fire only when every non-null-literal operand has `could_error == false`.
fn null_prop_variadic(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    if !func.propagates_nulls() {
        return vec![];
    }
    if !exprs.iter().any(|&e| is_literal_null(eg, e)) {
        return vec![];
    }
    // Every operand that is not itself a literal null must be error-free.
    // A literal-null operand is the null we propagate; the others must not be
    // able to turn the result into an error instead.
    let other_can_error = exprs
        .iter()
        .any(|&e| !is_literal_null(eg, e) && eg.analysis(e).could_error);
    if other_can_error {
        return vec![];
    }
    let ty = call_scalar_type(eg, node);
    vec![eg.add(destructure_literal(MirScalarExpr::literal_null(ty)))]
}

/// `f(err_lit, b) -> err_lit` and `f(a, err_lit) -> err_lit` for a binary `f`,
/// GATED on the other operand being error-free.
///
/// DELIBERATE DEVIATION FROM REDUCE. reduce's binary err-prop
/// (`src/expr/src/scalar/reduce/binary.rs`) fires whenever an operand is a
/// literal error, UNGATED on the other operand. Eval is left-to-right via `?`:
/// `eval(f(x, err_lit))` returns `x`'s error if `x` errors first, not the
/// literal error. Rewriting without a gate would substitute one error class for
/// another, which (a) fails the exact-eval differential and (b) in the e-graph
/// would union two different error classes.
///
/// Therefore this rule fires on a literal-error operand only when EVERY OTHER
/// operand has `could_error == false`. This is the same gate and rationale as
/// [`null_prop_binary`]. No `propagates_nulls` gate is applied (matching reduce's
/// binary err-prop, which is unconditional on the function).
fn err_prop_binary(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallBinary { expr1, expr2, .. } = node else {
        return vec![];
    };
    // Try expr1 as the error operand: expr2 must be error-free.
    if let Some(err) = literal_err(eg, *expr1) {
        if !eg.analysis(*expr2).could_error {
            let ty = call_scalar_type(eg, node);
            return vec![eg.add(destructure_literal(MirScalarExpr::literal(Err(err), ty)))];
        }
    }
    // Try expr2 as the error operand: expr1 must be error-free.
    if let Some(err) = literal_err(eg, *expr2) {
        if !eg.analysis(*expr1).could_error {
            let ty = call_scalar_type(eg, node);
            return vec![eg.add(destructure_literal(MirScalarExpr::literal(Err(err), ty)))];
        }
    }
    vec![]
}

/// `f(.., err_lit, ..) -> err_lit` for a variadic `f` that propagates nulls,
/// GATED on every other operand being error-free.
///
/// Same deviation from reduce as [`err_prop_binary`]: without the gate, an
/// operand that errors first (left-to-right eval) would be silently replaced by
/// the literal error, changing one error class into a different one. The gate
/// ensures the literal error is the only possible error, so the call's result is
/// EXACTLY that error regardless of argument-evaluation order.
///
/// The `propagates_nulls` gate mirrors reduce's variadic err-prop
/// (`src/expr/src/scalar/reduce/variadic.rs`), which also requires it. Functions
/// that do not propagate nulls may short-circuit before reaching the error
/// operand, so we conservatively skip them.
fn err_prop_variadic(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallVariadic { func, exprs } = node else {
        return vec![];
    };
    if !func.propagates_nulls() {
        return vec![];
    }
    // Find the first literal-error operand, if any.
    let Some(err) = exprs.iter().find_map(|&e| literal_err(eg, e)) else {
        return vec![];
    };
    // Every other operand must be error-free. The `literal_err(..).is_none()`
    // filter excludes the error-literal operands themselves from the "other" set,
    // so a second literal-error operand does not count as "other can error".
    // That means a call with multiple literal-error operands still fires, using
    // the first error found above. That is sound: eval is left-to-right via `?`,
    // so the first error is the one a full evaluation would surface, and
    // const_fold (which also fires on the all-literal case) agrees.
    let other_can_error = exprs
        .iter()
        .any(|&e| literal_err(eg, e).is_none() && eg.analysis(e).could_error);
    if other_can_error {
        return vec![];
    }
    let ty = call_scalar_type(eg, node);
    vec![eg.add(destructure_literal(MirScalarExpr::literal(Err(err), ty)))]
}

/// `IsNull(x) -> false` when x is non-nullable and error-free.
///
/// Mirrors the non-nullable arm of `reduce_pre`'s IsNull rewrite
/// (`src/expr/src/scalar/reduce.rs`), which folds `IsNull(non-nullable) ->
/// false`. Nullability comes from the same `raise + typ` machinery
/// [`call_scalar_type`] uses: `raise::raise(eg, x).typ(eg.col_types()).nullable`.
/// Reading `col_types` is what lets a column made non-nullable by an upstream
/// equivalence (the CLU-137-adjacent regression case) be seen as non-nullable.
///
/// COULD_ERROR GATE, a deliberate deviation from reduce. `IsNull` evaluates its
/// operand first and propagates an operand error: `IsNull(err)` is `err`, not
/// `false`. So folding to `false` when the operand can error is not exact-eval
/// sound. At a row where the operand errors the input is `err` while `false` is
/// not. reduce folds this arm ungated on errors. We hold the conservative
/// exact-eval contract (guarded by the differential test) and fire only when x
/// is non-nullable AND `eg.analysis(x).could_error == false`. When both hold, x
/// is never null and never errors, so `IsNull(x)` is always `false`. This is
/// strictly more conservative than reduce.
///
/// SCOPE: only the non-nullable fold. `decompose_is_null` (reduce's nullable ->
/// disjunction-of-IsNulls rewrite) is deferred to a later task.
fn isnull_fold(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id> {
    let SNode::CallUnary { func, expr } = node else {
        return vec![];
    };
    if !matches!(func, UnaryFunc::IsNull(_)) {
        return vec![];
    }
    // Gate: an operand that can error makes IsNull propagate that error, so the
    // fold to false would be unsound on the erroring rows.
    if eg.analysis(*expr).could_error {
        return vec![];
    }
    // Nullability via the col_types the e-graph was built with, mirroring reduce.
    // This read finishes (returns an owned bool) before the eg.add below.
    let nullable = raise::raise(eg, *expr).typ(eg.col_types()).nullable;
    if nullable {
        return vec![];
    }
    vec![eg.add(destructure_literal(MirScalarExpr::literal_false()))]
}

#[cfg(test)]
mod tests {
    use mz_expr::{BinaryFunc, Eval, MirScalarExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc};
    use mz_repr::{Datum, ReprScalarType, Row, RowArena};

    use crate::eqsat::scalar::canonicalize;
    use crate::eqsat::scalar::egraph::ScalarEGraph;
    use crate::eqsat::scalar::lower::lower;

    fn lit_int(v: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
    }

    fn col(i: usize) -> MirScalarExpr {
        MirScalarExpr::column(i)
    }

    fn add64() -> BinaryFunc {
        BinaryFunc::AddInt64(mz_expr::func::AddInt64)
    }

    fn mul64() -> BinaryFunc {
        BinaryFunc::MulInt64(mz_expr::func::MulInt64)
    }

    fn neg64() -> UnaryFunc {
        UnaryFunc::NegInt64(mz_expr::func::NegInt64)
    }

    fn div64() -> BinaryFunc {
        BinaryFunc::DivInt64(mz_expr::func::DivInt64)
    }

    fn not() -> UnaryFunc {
        UnaryFunc::Not(mz_expr::func::Not)
    }

    fn and() -> VariadicFunc {
        VariadicFunc::And(mz_expr::func::variadic::And)
    }

    fn or() -> VariadicFunc {
        VariadicFunc::Or(mz_expr::func::variadic::Or)
    }

    fn eq() -> BinaryFunc {
        BinaryFunc::Eq(mz_expr::func::Eq)
    }

    fn neq() -> BinaryFunc {
        BinaryFunc::NotEq(mz_expr::func::NotEq)
    }

    fn variadic(func: VariadicFunc, exprs: Vec<MirScalarExpr>) -> MirScalarExpr {
        MirScalarExpr::CallVariadic { func, exprs }
    }

    // ===================================================================
    // Soundness gate: eval-differential over the {true, false, null} cube.
    // ===================================================================

    /// Evaluate `expr` over `row`, returning an owned, comparable result. We
    /// canonicalize the `Datum` into an owned `Row` so the borrow on the arena
    /// does not escape, and so two evaluations compare by value.
    fn eval_owned(expr: &MirScalarExpr, datums: &[Datum]) -> Result<Row, mz_expr::EvalError> {
        let temp = RowArena::new();
        expr.eval(datums, &temp).map(|d| Row::pack_slice(&[d]))
    }

    /// The {true, false, null} assignment cube over `n` boolean columns.
    ///
    /// Returns every length-`n` vector of `Datum`s drawn from
    /// `{True, False, Null}`. With `3^n` rows this is the full three-valued-logic
    /// truth table, which is what proves a boolean rewrite preserves null
    /// semantics, not only the two-valued cases.
    fn bool_cube(n: usize) -> Vec<Vec<Datum<'static>>> {
        let choices = [Datum::True, Datum::False, Datum::Null];
        let mut rows = vec![vec![]];
        for _ in 0..n {
            let mut next = Vec::new();
            for row in &rows {
                for c in &choices {
                    let mut r = row.clone();
                    r.push(*c);
                    next.push(r);
                }
            }
            rows = next;
        }
        rows
    }

    /// Assert that `canonicalize(expr)` is observationally equal to `expr` over
    /// the full {true, false, null} cube on `n` boolean columns. This is the
    /// required soundness gate for every boolean rule: it differentials the
    /// rewritten term against the input under evaluation, including `Null` and
    /// `Err` results.
    fn assert_eval_equiv(expr: &MirScalarExpr, n: usize) {
        // `&[]` is sound here: every boolean rule is type-agnostic and never
        // types a column. The null-prop tests, which do need types, call
        // `canonicalize` directly with the right `col_types`.
        let got = canonicalize(expr, &[]);
        for row in bool_cube(n) {
            let want = eval_owned(expr, &row);
            let have = eval_owned(&got, &row);
            assert_eq!(
                want, have,
                "eval mismatch on row {row:?}: input {expr:?} -> {want:?}, \
                 canonicalized {got:?} -> {have:?}"
            );
        }
    }

    // ===================================================================
    // Rule 1: and_or_dedup.
    // ===================================================================

    #[mz_ore::test]
    fn test_and_or_dedup() {
        // AND(c0, c0, c1) and OR(c0, c0).
        let and_dup = variadic(and(), vec![col(0), col(0), col(1)]);
        assert_eval_equiv(&and_dup, 2);
        // The rule must actually fire: the canonical form has the duplicate
        // dropped. Without this structural check the eval-differential would
        // pass trivially (the original term is observationally equal to itself).
        assert_eq!(
            canonicalize(&and_dup, &[]),
            variadic(and(), vec![col(0), col(1)]),
            "AND(c0, c0, c1) must dedup to AND(c0, c1)"
        );

        let or_dup = variadic(or(), vec![col(0), col(0)]);
        assert_eval_equiv(&or_dup, 1);
        // OR(c0, c0) dedups to OR(c0), which then collapses to c0.
        assert_eq!(
            canonicalize(&or_dup, &[]),
            col(0),
            "OR(c0, c0) must dedup then collapse to c0"
        );
    }

    // ===================================================================
    // Rule 2: and_or_single.
    // ===================================================================

    #[mz_ore::test]
    fn test_and_or_single() {
        // AND(c0) collapses to c0; OR(c0) collapses to c0.
        let and_one = variadic(and(), vec![col(0)]);
        assert_eval_equiv(&and_one, 1);
        assert_eq!(
            canonicalize(&and_one, &[]),
            col(0),
            "AND(c0) must collapse to c0"
        );

        let or_one = variadic(or(), vec![col(0)]);
        assert_eval_equiv(&or_one, 1);
        assert_eq!(
            canonicalize(&or_one, &[]),
            col(0),
            "OR(c0) must collapse to c0"
        );
    }

    // ===================================================================
    // Rule 3: and_or_empty.
    // ===================================================================

    #[mz_ore::test]
    fn test_and_or_empty() {
        // AND() -> true, OR() -> false.
        //
        // NOTE: a zero-argument AND/OR is inherently fully constant, so there is
        // no non-literal input that would force `and_or_empty` to fire rather
        // than `const_fold`. This test therefore checks the result is correct,
        // not that this specific rule produced it. The rule still mirrors reduce
        // and is kept for faithfulness; its redundancy with const_fold on this
        // input is expected, not a coverage hole that a different input closes.
        let and_zero = variadic(and(), vec![]);
        assert_eval_equiv(&and_zero, 0);
        assert_eq!(
            canonicalize(&and_zero, &[]),
            MirScalarExpr::literal_true(),
            "AND() must become true"
        );

        let or_zero = variadic(or(), vec![]);
        assert_eval_equiv(&or_zero, 0);
        assert_eq!(
            canonicalize(&or_zero, &[]),
            MirScalarExpr::literal_false(),
            "OR() must become false"
        );
    }

    // ===================================================================
    // Rule 4: and_or_short_circuit, including an error operand.
    // ===================================================================

    #[mz_ore::test]
    fn test_and_or_short_circuit() {
        // AND(c0, false) -> false over the cube.
        let and_zero = variadic(and(), vec![col(0), MirScalarExpr::literal_false()]);
        assert_eval_equiv(&and_zero, 1);
        assert_eq!(
            canonicalize(&and_zero, &[]),
            MirScalarExpr::literal_false(),
            "AND(c0, false) must short-circuit to false"
        );

        // OR(c0, true) -> true over the cube.
        let or_one = variadic(or(), vec![col(0), MirScalarExpr::literal_true()]);
        assert_eval_equiv(&or_one, 1);
        assert_eq!(
            canonicalize(&or_one, &[]),
            MirScalarExpr::literal_true(),
            "OR(c0, true) must short-circuit to true"
        );
    }

    #[mz_ore::test]
    fn test_and_or_short_circuit_with_error() {
        // AND(1/0 = 1/0, false): the error operand must not change the result.
        // `And` returns false as soon as any operand is false, ignoring the
        // error. The differential confirms the rewrite preserves this.
        let div_by_zero = lit_int(1).call_binary(lit_int(0), div64());
        let erroring = div_by_zero.clone().call_binary(div_by_zero, eq());
        let expr = variadic(and(), vec![erroring, MirScalarExpr::literal_false()]);
        // No columns: the cube is a single empty row, but eval still exercises
        // the error path.
        assert_eval_equiv(&expr, 0);
        assert_eq!(
            canonicalize(&expr, &[]),
            MirScalarExpr::literal_false(),
            "AND(err, false) must short-circuit to false"
        );
    }

    #[mz_ore::test]
    fn test_short_circuit_drops_nonliteral_error() {
        // The CLU-137-class soundness case: short-circuit must drop a
        // NON-LITERAL erroring operand. The prior test uses a fully-literal
        // error, so `const_fold` collapses the AND and the short-circuit rule is
        // never exercised. Here `1 / c0 = 5` is boolean, non-literal (so
        // const_fold cannot fire), and errors at c0 == 0. Only the short-circuit
        // rule can collapse the AND, and the differential against eval proves
        // `And` short-circuits the error rather than propagating it.
        let erroring = lit_int(1)
            .call_binary(col(0), div64())
            .call_binary(lit_int(5), eq());
        let expr = variadic(and(), vec![erroring, MirScalarExpr::literal_false()]);
        // The short-circuit rule, not const_fold, must produce false.
        assert_eq!(
            canonicalize(&expr, &[]),
            MirScalarExpr::literal_false(),
            "AND(non-literal-erroring, false) must short-circuit to false"
        );
        // Differential including the c0 == 0 error row. If `And` did not
        // short-circuit on false, eval would return the error here and the
        // rewrite to false would be unsound.
        for c0 in [Datum::Int64(0), Datum::Int64(1), Datum::Int64(2)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&canonicalize(&expr, &[]), &[c0]);
            assert_eq!(want, have, "short-circuit error mismatch at c0={c0:?}");
        }
    }

    // ===================================================================
    // Rule 5: and_or_drop_unit.
    // ===================================================================

    #[mz_ore::test]
    fn test_and_or_drop_unit() {
        // AND(c0, true) equals c0.
        let and_unit = variadic(and(), vec![col(0), MirScalarExpr::literal_true()]);
        assert_eval_equiv(&and_unit, 1);
        assert_eq!(
            canonicalize(&and_unit, &[]),
            col(0),
            "AND(c0, true) must collapse to c0"
        );

        // OR(c0, false) equals c0.
        let or_unit = variadic(or(), vec![col(0), MirScalarExpr::literal_false()]);
        assert_eval_equiv(&or_unit, 1);
        assert_eq!(
            canonicalize(&or_unit, &[]),
            col(0),
            "OR(c0, false) must collapse to c0"
        );
    }

    #[mz_ore::test]
    fn test_drop_unit_preserves_nonliteral_error() {
        // Drop-unit must keep a NON-LITERAL erroring operand intact while it
        // drops the unit. As with short-circuit, a literal operand would let
        // const_fold collapse the whole AND, so use `1 / c0 = 5` (non-literal,
        // errors at c0 == 0). drop_unit removes the literal true, single then
        // collapses AND(erroring) to erroring, and the differential including
        // the c0 == 0 error row proves the remaining operand's error behavior is
        // preserved.
        let erroring = lit_int(1)
            .call_binary(col(0), div64())
            .call_binary(lit_int(5), eq());
        let expr = variadic(and(), vec![erroring.clone(), MirScalarExpr::literal_true()]);
        assert_eq!(
            canonicalize(&expr, &[]),
            erroring,
            "AND(non-literal-erroring, true) must drop the unit and collapse to the operand"
        );
        for c0 in [Datum::Int64(0), Datum::Int64(1), Datum::Int64(2)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&canonicalize(&expr, &[]), &[c0]);
            assert_eq!(want, have, "drop-unit error mismatch at c0={c0:?}");
        }
    }

    // ===================================================================
    // Rule: factor_and_or (undistribute, residual-error gated).
    // ===================================================================

    // ===================================================================
    // Rule: flatten_assoc (AND/OR associativity flattening).
    // ===================================================================

    #[mz_ore::test]
    fn test_flatten_assoc_firing_and() {
        // AND(c0, AND(c1, c2)) must flatten to AND(c0, c1, c2): three column
        // operands at the top level, no nested AND.
        let nested = variadic(and(), vec![col(0), variadic(and(), vec![col(1), col(2)])]);
        let got = canonicalize(&nested, &[]);
        // Assert flat: the outer AND has exactly three operands, none of which
        // is itself a nested AND.
        let MirScalarExpr::CallVariadic { func, exprs } = &got else {
            panic!("AND(c0, AND(c1, c2)) must canonicalize to a CallVariadic And, got {got:?}");
        };
        assert!(
            matches!(func, VariadicFunc::And(_)),
            "outer connective must be And, got {func:?}"
        );
        assert_eq!(
            exprs.len(),
            3,
            "flat AND must have 3 operands, got {}: {got:?}",
            exprs.len()
        );
        for e in exprs {
            assert!(
                !matches!(e, MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::And(_))),
                "flat AND must not contain a nested And operand, found one in {got:?}"
            );
        }
        // Soundness: identical evaluation over the 3-column boolean cube.
        assert_eval_equiv(&nested, 3);
    }

    #[mz_ore::test]
    fn test_flatten_assoc_firing_or() {
        // OR(c0, OR(c1, c2)) must flatten to OR(c0, c1, c2).
        let nested = variadic(or(), vec![col(0), variadic(or(), vec![col(1), col(2)])]);
        let got = canonicalize(&nested, &[]);
        let MirScalarExpr::CallVariadic { func, exprs } = &got else {
            panic!("OR(c0, OR(c1, c2)) must canonicalize to a CallVariadic Or, got {got:?}");
        };
        assert!(
            matches!(func, VariadicFunc::Or(_)),
            "outer connective must be Or, got {func:?}"
        );
        assert_eq!(
            exprs.len(),
            3,
            "flat OR must have 3 operands, got {}: {got:?}",
            exprs.len()
        );
        for e in exprs {
            assert!(
                !matches!(e, MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::Or(_))),
                "flat OR must not contain a nested Or operand, found one in {got:?}"
            );
        }
        assert_eval_equiv(&nested, 3);
    }

    #[mz_ore::test]
    fn test_flatten_assoc_deep_nesting() {
        // AND(c0, AND(c1, AND(c2, c3))) flattens fully to AND(c0, c1, c2, c3)
        // via saturation re-applying the rule on each newly flat intermediate.
        let nested = variadic(
            and(),
            vec![
                col(0),
                variadic(and(), vec![col(1), variadic(and(), vec![col(2), col(3)])]),
            ],
        );
        let got = canonicalize(&nested, &[]);
        let MirScalarExpr::CallVariadic { func, exprs } = &got else {
            panic!("deeply nested AND must canonicalize to a CallVariadic And, got {got:?}");
        };
        assert!(
            matches!(func, VariadicFunc::And(_)),
            "outer connective must be And after deep flattening, got {func:?}"
        );
        assert_eq!(
            exprs.len(),
            4,
            "fully flat AND(c0,c1,c2,c3) must have 4 operands, got {}: {got:?}",
            exprs.len()
        );
        for e in exprs {
            assert!(
                !matches!(e, MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::And(_))),
                "fully flat AND must have no nested And, found one in {got:?}"
            );
        }
        assert_eval_equiv(&nested, 4);
    }

    #[mz_ore::test]
    fn test_flatten_assoc_error_operand_preservation() {
        // AND(c0, AND((1/c1 = 5), c2)): the nested AND contains a potentially
        // erroring operand (1/c1 = 5 errors at c1 == 0). Flattening must
        // produce AND(c0, (1/c1 = 5), c2) and preserve exact evaluation
        // including the error at c1 == 0.
        let erroring = lit_int(1)
            .call_binary(col(1), div64())
            .call_binary(lit_int(5), eq());
        let nested = variadic(and(), vec![col(0), variadic(and(), vec![erroring, col(2)])]);
        let got = canonicalize(&nested, &[]);

        // Differential over c0 in {true, false, null}, c1 in {0, 1, 2}
        // (c1 == 0 fires the error), c2 in {true, false, null}.
        for c0 in [Datum::True, Datum::False, Datum::Null] {
            for c1 in [Datum::Int64(0), Datum::Int64(1), Datum::Int64(2)] {
                for c2 in [Datum::True, Datum::False, Datum::Null] {
                    let row = vec![c0, c1, c2];
                    let want = eval_owned(&nested, &row);
                    let have = eval_owned(&got, &row);
                    assert_eq!(
                        want, have,
                        "error-preservation mismatch at c0={c0:?} c1={c1:?} c2={c2:?}"
                    );
                }
            }
        }
        // The error path is live: at c0=true, c1=0, c2=true the nested AND errors
        // because (1/0 = 5) errors and c0=true does not short-circuit it.
        assert!(
            eval_owned(&nested, &[Datum::True, Datum::Int64(0), Datum::True]).is_err(),
            "input must eval to Err at c0=true, c1=0, c2=true"
        );
    }

    #[mz_ore::test]
    fn test_flatten_assoc_noop() {
        // An already-flat AND(c0, c1) must not change: the rule must not fire
        // and the output must equal the input.
        let flat = variadic(and(), vec![col(0), col(1)]);
        let got = canonicalize(&flat, &[]);
        assert_eq!(
            got, flat,
            "already-flat AND(c0, c1) must be unchanged, got {got:?}"
        );

        // A mixed AND(c0, OR(c1, c2)) must NOT splice the Or into the And:
        // different connectives are not associative with each other.
        let mixed = variadic(and(), vec![col(0), variadic(or(), vec![col(1), col(2)])]);
        let got_mixed = canonicalize(&mixed, &[]);
        // The outer must remain an And and the Or operand must not be dissolved.
        let MirScalarExpr::CallVariadic {
            func: outer,
            exprs: outer_exprs,
        } = &got_mixed
        else {
            panic!("AND(c0, OR(c1, c2)) must remain a CallVariadic, got {got_mixed:?}");
        };
        assert!(
            matches!(outer, VariadicFunc::And(_)),
            "outer connective must remain And in mixed case, got {outer:?}"
        );
        let has_or = outer_exprs.iter().any(|e| {
            matches!(
                e,
                MirScalarExpr::CallVariadic { func, .. }
                    if matches!(func, VariadicFunc::Or(_))
            )
        });
        assert!(
            has_or,
            "AND(c0, OR(c1, c2)) must keep the Or as a direct child, \
             not splice its operands into the And; got {got_mixed:?}"
        );
    }

    /// Assert that `expr` has no same-func nested operand at the top level.
    ///
    /// For an outer `CallVariadic { func: outer, exprs }`, none of the direct
    /// children may be a `CallVariadic` with the same func. This is the
    /// structural flatness condition that `flatten_assoc` enforces.
    fn assert_flat(expr: &MirScalarExpr) {
        let MirScalarExpr::CallVariadic { func: outer, exprs } = expr else {
            return;
        };
        for child in exprs {
            if let MirScalarExpr::CallVariadic { func: inner, .. } = child {
                assert_ne!(
                    inner, outer,
                    "flat form must not contain a nested same-func child; got {expr:?}"
                );
            }
        }
    }

    // Regression test for the one-hop cycle guard: OR(OR(c0, c0), c1).
    //
    // The inner OR(c0, c0) deduplicates to OR(c0), then and_or_single collapses
    // it into c0's class. That class then contains self-referential OR nodes
    // (children pointing back at the class itself). Without the cycle guard,
    // flatten_assoc would splice those back into ever-growing operand vectors.
    // This test asserts the rule terminates quickly and yields a flat, correct result.
    #[mz_ore::test]
    fn test_flatten_assoc_cycle_or_terminates() {
        // OR(OR(c0, c0), c1): the inner OR(c0, c0) creates the self-referential
        // class after dedup + single collapse.
        let inner = variadic(or(), vec![col(0), col(0)]);
        let outer = variadic(or(), vec![inner, col(1)]);
        let got = canonicalize(&outer, &[]);
        // Must terminate and yield a flat OR: no nested OR at the top level.
        assert_flat(&got);
        // Soundness: identical evaluation over the 2-column boolean cube.
        assert_eval_equiv(&outer, 2);
    }

    // Dual regression test with AND(AND(c0, c0), c1).
    #[mz_ore::test]
    fn test_flatten_assoc_cycle_and_terminates() {
        let inner = variadic(and(), vec![col(0), col(0)]);
        let outer = variadic(and(), vec![inner, col(1)]);
        let got = canonicalize(&outer, &[]);
        assert_flat(&got);
        assert_eval_equiv(&outer, 2);
    }

    /// Recursively sort the operands of every AND/OR so two terms that differ
    /// only in commutative operand order compare equal. Factoring builds its
    /// operand lists from a sorted-id intersection, but extraction can still
    /// emit operands in any order, so the firing tests compare normalized forms.
    fn normalize(e: &MirScalarExpr) -> MirScalarExpr {
        match e {
            MirScalarExpr::CallVariadic { func, exprs }
                if matches!(func, VariadicFunc::And(_) | VariadicFunc::Or(_)) =>
            {
                let mut norm: Vec<MirScalarExpr> = exprs.iter().map(normalize).collect();
                norm.sort();
                MirScalarExpr::CallVariadic {
                    func: func.clone(),
                    exprs: norm,
                }
            }
            MirScalarExpr::CallUnary { func, expr } => MirScalarExpr::CallUnary {
                func: func.clone(),
                expr: Box::new(normalize(expr)),
            },
            MirScalarExpr::CallBinary { func, expr1, expr2 } => MirScalarExpr::CallBinary {
                func: func.clone(),
                expr1: Box::new(normalize(expr1)),
                expr2: Box::new(normalize(expr2)),
            },
            MirScalarExpr::CallVariadic { func, exprs } => MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: exprs.iter().map(normalize).collect(),
            },
            MirScalarExpr::If { cond, then, els } => MirScalarExpr::If {
                cond: Box::new(normalize(cond)),
                then: Box::new(normalize(then)),
                els: Box::new(normalize(els)),
            },
            other => other.clone(),
        }
    }

    #[mz_ore::test]
    fn test_factor_and_or_soundness() {
        // Both duals over the {true, false, null} cube. Residuals are error-free
        // columns so the rule fires; this proves it preserves 3VL evaluation.
        let or_of_and = variadic(
            or(),
            vec![
                variadic(and(), vec![col(0), col(1)]),
                variadic(and(), vec![col(0), col(2)]),
            ],
        );
        assert_eval_equiv(&or_of_and, 3);

        let and_of_or = variadic(
            and(),
            vec![
                variadic(or(), vec![col(0), col(1)]),
                variadic(or(), vec![col(0), col(2)]),
            ],
        );
        assert_eval_equiv(&and_of_or, 3);
    }

    #[mz_ore::test]
    fn test_factor_and_or_fires() {
        // (c0∧c1)∨(c0∧c2) factors to c0∧(c1∨c2). The factored form has ~5 e-nodes
        // vs ~7, so min-cost extraction picks it. This proves the rule fired.
        let input = variadic(
            or(),
            vec![
                variadic(and(), vec![col(0), col(1)]),
                variadic(and(), vec![col(0), col(2)]),
            ],
        );
        let got = canonicalize(&input, &[]);
        let expected = variadic(and(), vec![col(0), variadic(or(), vec![col(1), col(2)])]);
        assert_eq!(
            normalize(&got),
            normalize(&expected),
            "(c0∧c1)∨(c0∧c2) must factor to c0∧(c1∨c2), got {got:?}"
        );

        // Dual: (c0∨c1)∧(c0∨c2) factors to c0∨(c1∧c2).
        let input = variadic(
            and(),
            vec![
                variadic(or(), vec![col(0), col(1)]),
                variadic(or(), vec![col(0), col(2)]),
            ],
        );
        let got = canonicalize(&input, &[]);
        let expected = variadic(or(), vec![col(0), variadic(and(), vec![col(1), col(2)])]);
        assert_eq!(
            normalize(&got),
            normalize(&expected),
            "(c0∨c1)∧(c0∨c2) must factor to c0∨(c1∧c2), got {got:?}"
        );
    }

    #[mz_ore::test]
    fn test_factor_and_or_erroring_common_factor() {
        // The CLU-137 property: full-intersection factoring with an erroring
        // COMMON FACTOR. g = (1 / c0 = 0) is boolean, non-literal, and errors at
        // c0 == 0; c2, c3 are error-free bool columns. (g∧c2)∨(g∧c3) must factor
        // to g∧(c2∨c3): the erroring factor g is pulled out, the gate fires
        // because the RESIDUALS (c2, c3) cannot error.
        let ct = vec![
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Bool.nullable(true),
            ReprScalarType::Bool.nullable(true),
        ];
        let g = lit_int(1)
            .call_binary(col(0), div64())
            .call_binary(lit_int(0), eq());
        let input = variadic(
            or(),
            vec![
                variadic(and(), vec![g.clone(), col(2)]),
                variadic(and(), vec![g.clone(), col(3)]),
            ],
        );
        let got = canonicalize(&input, &ct);
        let expected = variadic(and(), vec![g.clone(), variadic(or(), vec![col(2), col(3)])]);
        assert_eq!(
            normalize(&got),
            normalize(&expected),
            "(g∧c2)∨(g∧c3) must factor to g∧(c2∨c3) with an erroring common factor, got {got:?}"
        );

        // Differential over rows including c0 == 0 (g errors) and c2, c3 over
        // {true, false, null}. The erroring common factor must stay exact-eval
        // equal after factoring.
        for c0 in [Datum::Int64(0), Datum::Int64(1), Datum::Int64(2)] {
            for c2 in [Datum::True, Datum::False, Datum::Null] {
                for c3 in [Datum::True, Datum::False, Datum::Null] {
                    let row = vec![c0, Datum::Null, c2, c3];
                    let want = eval_owned(&input, &row);
                    let have = eval_owned(&got, &row);
                    assert_eq!(
                        want, have,
                        "erroring-factor mismatch at c0={c0:?} c2={c2:?} c3={c3:?}"
                    );
                }
            }
        }
        // The erroring-factor path is live: at c0 == 0 the input errors.
        assert!(
            eval_owned(
                &input,
                &[Datum::Int64(0), Datum::Null, Datum::True, Datum::True]
            )
            .is_err(),
            "input must eval to Err at c0 == 0 (the common factor g errors)"
        );
    }

    #[mz_ore::test]
    fn test_factor_and_or_gate_blocks_erroring_residual() {
        // CLU-137-class soundness: the gate must BLOCK when a RESIDUAL can error.
        // r = (1 / c1 = 5) errors at c1 == 0. (c0∧r)∨(c0∧c2) has intersection
        // {c0} and residuals {r}, {c2}; r can error, so factoring must not fire.
        let ct = vec![
            ReprScalarType::Bool.nullable(true),
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Bool.nullable(true),
        ];
        let r = lit_int(1)
            .call_binary(col(1), div64())
            .call_binary(lit_int(5), eq());
        let input = variadic(
            or(),
            vec![
                variadic(and(), vec![col(0), r.clone()]),
                variadic(and(), vec![col(0), col(2)]),
            ],
        );
        let got = canonicalize(&input, &ct);

        // The factored form must NOT be the extraction: the output keeps the
        // outer Or shape and is not c0∧(r∨c2).
        let unsound = variadic(and(), vec![col(0), variadic(or(), vec![r.clone(), col(2)])]);
        assert_ne!(
            normalize(&got),
            normalize(&unsound),
            "gate must block factoring an erroring residual; got the unsound form {got:?}"
        );
        let outer_is_or = matches!(
            &got,
            MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::Or(_))
        );
        assert!(
            outer_is_or,
            "blocked factoring must leave the outer Or intact, got {got:?}"
        );

        // Differential: canonicalize must not change observable evaluation. The
        // row c0=null, c1=0 (r errors), c2=true is the witness that the gate is
        // needed: the input errors there, but the unsound factored form would
        // mask the error to null.
        for c0 in [Datum::True, Datum::False, Datum::Null] {
            for c1 in [Datum::Int64(0), Datum::Int64(2)] {
                for c2 in [Datum::True, Datum::False, Datum::Null] {
                    let row = vec![c0, c1, c2];
                    let want = eval_owned(&input, &row);
                    let have = eval_owned(&got, &row);
                    assert_eq!(
                        want, have,
                        "blocked-residual mismatch at c0={c0:?} c1={c1:?} c2={c2:?}"
                    );
                }
            }
        }
        // Make the hazard explicit: at c0=null, c1=0, c2=true the input errors
        // (null∧err = err in the first branch, Or surfaces it), while the unsound
        // factored form would short-circuit (r∨true = true) and yield null.
        let witness = vec![Datum::Null, Datum::Int64(0), Datum::True];
        assert!(
            eval_owned(&input, &witness).is_err(),
            "input must eval to Err at the witness row (error not masked)"
        );
        assert!(
            !eval_owned(&unsound, &witness).is_err(),
            "the unsound factored form would mask the error to a non-error (gate is needed)"
        );
    }

    #[mz_ore::test]
    fn test_factor_hands_empty_residual_to_absorption() {
        // The 2a/2b boundary on the empty-residual case. (c0∧c1)∨(c0∧c1∧c2) has
        // intersection {c0,c1} and one EMPTY residual, so `factor_and_or`
        // deliberately declines it (factoring needs non-empty residuals on every
        // branch). `absorb_and_or` is the rule that handles it: the And operand's
        // inner-set {c0,c1,c2} is a proper superset of {c0,c1}, the dropped extra
        // c2 is error-free, so it absorbs to c0∧c1.
        let input = variadic(
            or(),
            vec![
                variadic(and(), vec![col(0), col(1)]),
                variadic(and(), vec![col(0), col(1), col(2)]),
            ],
        );
        let got = canonicalize(&input, &[]);
        let absorbed = variadic(and(), vec![col(0), col(1)]);
        assert_eq!(
            normalize(&got),
            normalize(&absorbed),
            "(c0∧c1)∨(c0∧c1∧c2) must absorb to c0∧c1, got {got:?}"
        );
    }

    // ===================================================================
    // CLU-137 regression: temporal predicate factors to top level.
    // ===================================================================

    #[mz_ore::test]
    fn test_clu137_temporal_factors_to_top_level() {
        // Regression for CLU-137: a temporal filter predicate shared by all
        // branches of a DNF OR must be lifted to a top-level AND conjunct so
        // downstream temporal-filter detection can recognize it.
        //
        // Column layout:
        //   col(0): timestamptz nullable  (ts)
        //   col(1): text nullable         (s)
        //   col(2), col(3), col(4): bool nullable  (a, b, c)
        //
        // Input (two-branch DNF, top-level OR):
        //   (a AND b AND c AND (s IS NULL) AND (mz_now() < cast(ts)))
        //   OR
        //   (a AND b AND c AND (s = '')   AND (mz_now() < cast(ts)))
        //
        // factor_and_or fires: intersection is {a, b, c, mz_now() < cast(ts)},
        // residuals are {s IS NULL} and {s = ''}, both error-free. The gated
        // common factor mz_now() < cast(ts) is exempt from the residual-error
        // gate (it errors, but stays in the result). The factored form is:
        //   AND(a, b, c, mz_now() < cast(ts), OR(s IS NULL, s = ''))
        //
        // The factored form has fewer tree nodes than the original OR, so
        // size-only extraction prefers it.
        let col_types = vec![
            ReprScalarType::TimestampTz.nullable(true),
            ReprScalarType::String.nullable(true),
            ReprScalarType::Bool.nullable(true),
            ReprScalarType::Bool.nullable(true),
            ReprScalarType::Bool.nullable(true),
        ];

        let mz_now = MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow);
        let cast_ts = col(0).call_unary(UnaryFunc::CastTimestampTzToMzTimestamp(
            mz_expr::func::CastTimestampTzToMzTimestamp,
        ));
        let temporal = mz_now.call_binary(cast_ts, BinaryFunc::Lt(mz_expr::func::Lt));
        let s_is_null = col(1).call_unary(UnaryFunc::IsNull(mz_expr::func::IsNull));
        let s_eq_empty = col(1).call_binary(
            MirScalarExpr::literal_ok(Datum::String(""), ReprScalarType::String),
            eq(),
        );

        let input = variadic(
            or(),
            vec![
                variadic(
                    and(),
                    vec![col(2), col(3), col(4), s_is_null.clone(), temporal.clone()],
                ),
                variadic(
                    and(),
                    vec![col(2), col(3), col(4), s_eq_empty.clone(), temporal.clone()],
                ),
            ],
        );

        let got = canonicalize(&input, &col_types);

        // The canonical form must be an outer AND.
        let MirScalarExpr::CallVariadic {
            func: outer_func,
            exprs: outer_exprs,
        } = &got
        else {
            panic!("CLU-137: canonical form must be a CallVariadic And, got {got:?}");
        };
        assert!(
            matches!(outer_func, VariadicFunc::And(_)),
            "CLU-137: outer connective must be And, got {outer_func:?} in {got:?}"
        );

        // The temporal predicate must appear as a DIRECT operand of the outer
        // And, not inside an Or. We look for a binary comparison that has
        // MzNow on one side and CastTimestampTzToMzTimestamp on the other,
        // tolerating either Lt(mz_now, cast) or Gt(cast, mz_now) (commuted).
        let has_temporal = outer_exprs.iter().any(|e| {
            if let MirScalarExpr::CallBinary { func, expr1, expr2 } = e {
                let is_lt_or_gt =
                    matches!(func, BinaryFunc::Lt(_)) || matches!(func, BinaryFunc::Gt(_));
                if !is_lt_or_gt {
                    return false;
                }
                let has_mz_now = matches!(
                    expr1.as_ref(),
                    MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
                ) || matches!(
                    expr2.as_ref(),
                    MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
                );
                let has_cast = matches!(
                    expr1.as_ref(),
                    MirScalarExpr::CallUnary {
                        func: UnaryFunc::CastTimestampTzToMzTimestamp(_),
                        ..
                    }
                ) || matches!(
                    expr2.as_ref(),
                    MirScalarExpr::CallUnary {
                        func: UnaryFunc::CastTimestampTzToMzTimestamp(_),
                        ..
                    }
                );
                has_mz_now && has_cast
            } else {
                false
            }
        });
        assert!(
            has_temporal,
            "CLU-137: mz_now() < cast(ts) must be a top-level conjunct of the \
             outer And; got {got:?}"
        );

        // The residual OR must also appear as a direct conjunct of the outer And.
        let has_residual_or = outer_exprs.iter().any(|e| {
            matches!(
                e,
                MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::Or(_))
            )
        });
        assert!(
            has_residual_or,
            "CLU-137: residual OR(s IS NULL, s = '') must be a direct conjunct \
             of the outer And; got {got:?}"
        );
    }

    // ===================================================================
    // Rule: absorb_and_or (absorption, dropped-extra gated).
    // ===================================================================

    #[mz_ore::test]
    fn test_absorb_and_or_soundness() {
        // Both duals over the {true, false, null} cube. The dropped extra (c1) is
        // an error-free column so the rule fires; this proves absorption preserves
        // 3VL evaluation, including null rows.
        let or_absorb = variadic(or(), vec![col(0), variadic(and(), vec![col(0), col(1)])]);
        assert_eval_equiv(&or_absorb, 2);

        let and_absorb = variadic(and(), vec![col(0), variadic(or(), vec![col(0), col(1)])]);
        assert_eval_equiv(&and_absorb, 2);
    }

    #[mz_ore::test]
    fn test_absorb_and_or_fires() {
        // c0 ∨ (c0 ∧ c1) absorbs to c0 (the subset operand); the And operand is a
        // proper superset and is dropped. c0 is the cheapest member of the class,
        // so extraction picks it, proving the rule fired.
        let or_absorb = variadic(or(), vec![col(0), variadic(and(), vec![col(0), col(1)])]);
        assert_eq!(
            canonicalize(&or_absorb, &[]),
            col(0),
            "c0 ∨ (c0 ∧ c1) must absorb to c0, got {:?}",
            canonicalize(&or_absorb, &[])
        );

        // Dual: c0 ∧ (c0 ∨ c1) absorbs to c0.
        let and_absorb = variadic(and(), vec![col(0), variadic(or(), vec![col(0), col(1)])]);
        assert_eq!(
            canonicalize(&and_absorb, &[]),
            col(0),
            "c0 ∧ (c0 ∨ c1) must absorb to c0, got {:?}",
            canonicalize(&and_absorb, &[])
        );
    }

    #[mz_ore::test]
    fn test_absorb_and_or_gate_allows_retained_error() {
        // The retained operand may error: g = (1 / c0 = 5) is boolean, non-literal,
        // and errors at c0 == 0; c1 is an error-free bool column. g ∨ (g ∧ c1) must
        // absorb to g. The dropped extra is c1 (error-free), so the gate fires; g
        // is RETAINED, so its error is preserved and absorption is sound.
        let ct = vec![
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Bool.nullable(true),
        ];
        let g = lit_int(1)
            .call_binary(col(0), div64())
            .call_binary(lit_int(5), eq());
        let input = variadic(
            or(),
            vec![g.clone(), variadic(and(), vec![g.clone(), col(1)])],
        );
        let got = canonicalize(&input, &ct);
        assert_eq!(
            normalize(&got),
            normalize(&g),
            "g ∨ (g ∧ c1) must absorb to g even though g errors (g is retained), got {got:?}"
        );

        // Differential over c0 (g errors at c0 == 0) and c1 over {true, false,
        // null}. The retained g still surfaces its error after absorption.
        for c0 in [
            Datum::Int64(0),
            Datum::Int64(1),
            Datum::Int64(2),
            Datum::Int64(5),
        ] {
            for c1 in [Datum::True, Datum::False, Datum::Null] {
                let row = vec![c0, c1];
                let want = eval_owned(&input, &row);
                let have = eval_owned(&got, &row);
                assert_eq!(
                    want, have,
                    "retained-error absorption mismatch at c0={c0:?} c1={c1:?}"
                );
            }
        }
        // The retained-error path is live: at c0 == 0 the input errors.
        assert!(
            eval_owned(&input, &[Datum::Int64(0), Datum::True]).is_err(),
            "input must eval to Err at c0 == 0 (the retained operand g errors)"
        );
    }

    #[mz_ore::test]
    fn test_absorb_and_or_gate_blocks_dropped_extra_error() {
        // CLU-137-class soundness: the gate must BLOCK when a DROPPED extra can
        // error. r = (1 / c1 = 5) errors at c1 == 0. In c0 ∨ (c0 ∧ r) the And
        // operand's inner-set {c0, r} is a proper superset of {c0}; the extra r
        // can error, so absorption must NOT drop it.
        let ct = vec![
            ReprScalarType::Bool.nullable(true),
            ReprScalarType::Int64.nullable(true),
        ];
        let r = lit_int(1)
            .call_binary(col(1), div64())
            .call_binary(lit_int(5), eq());
        let input = variadic(or(), vec![col(0), variadic(and(), vec![col(0), r.clone()])]);
        let got = canonicalize(&input, &ct);

        // The blocked rule must not collapse to the absorbed form c0; the outer Or
        // stays intact.
        assert_ne!(
            normalize(&got),
            normalize(&col(0)),
            "gate must block absorbing an erroring extra; got the unsound c0, {got:?}"
        );
        let outer_is_or = matches!(
            &got,
            MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::Or(_))
        );
        assert!(
            outer_is_or,
            "blocked absorption must leave the outer Or intact, got {got:?}"
        );

        // Differential: canonicalize must not change observable evaluation over
        // c0 ∈ {true, false, null} and c1 including the r-erroring row c1 == 0.
        for c0 in [Datum::True, Datum::False, Datum::Null] {
            for c1 in [Datum::Int64(0), Datum::Int64(2)] {
                let row = vec![c0, c1];
                let want = eval_owned(&input, &row);
                let have = eval_owned(&got, &row);
                assert_eq!(want, have, "blocked-extra mismatch at c0={c0:?} c1={c1:?}");
            }
        }
        // Make the hazard explicit: at c0=null, c1=0 (r errors) the input is Err
        // (null ∧ err = err in the And, the Or surfaces it), while the unsound
        // absorbed c0 would be null. Without the gate the differential would fail.
        let witness = vec![Datum::Null, Datum::Int64(0)];
        assert!(
            eval_owned(&input, &witness).is_err(),
            "input must eval to Err at the witness row (error not masked)"
        );
        assert!(
            !eval_owned(&col(0), &witness).is_err(),
            "the unsound absorbed c0 would mask the error to null (gate is needed)"
        );
    }

    // ===================================================================
    // Rule 6: not_not.
    // ===================================================================

    #[mz_ore::test]
    fn test_not_not() {
        // Not(Not(c0)) equals c0 over {true, false, null}.
        let expr = col(0).call_unary(not()).call_unary(not());
        assert_eval_equiv(&expr, 1);
        assert_eq!(
            canonicalize(&expr, &[]),
            col(0),
            "Not(Not(c0)) must collapse to c0"
        );
    }

    // ===================================================================
    // Rule 7: not_binary_negate.
    // ===================================================================

    #[mz_ore::test]
    fn test_not_binary_negate() {
        // Not(c0 = c1) equals c0 != c1 over the cube.
        let expr = col(0).call_binary(col(1), eq()).call_unary(not());
        assert_eval_equiv(&expr, 2);
        let neq = BinaryFunc::NotEq(mz_expr::func::NotEq);
        assert_eq!(
            canonicalize(&expr, &[]),
            col(0).call_binary(col(1), neq),
            "Not(c0 = c1) must become c0 != c1"
        );
    }

    // ===================================================================
    // Rule 8: not_demorgan.
    // ===================================================================

    #[mz_ore::test]
    fn test_not_demorgan_soundness() {
        // NOT(AND(c0, c1)) and NOT(OR(c0, c1)) over the {true, false, null} cube.
        // Proves De Morgan holds in three-valued logic, including under null.
        let not_and = variadic(and(), vec![col(0), col(1)]).call_unary(not());
        assert_eval_equiv(&not_and, 2);
        let not_or = variadic(or(), vec![col(0), col(1)]).call_unary(not());
        assert_eval_equiv(&not_or, 2);
    }

    #[mz_ore::test]
    fn test_not_demorgan_fires_via_not_not() {
        // De Morgan rewrites NOT(AND(NOT c0, NOT c1)) to OR(NOT NOT c0, NOT NOT c1),
        // then not_not collapses each double negation to OR(c0, c1). OR(c0, c1)
        // has cost 3 vs the input's cost 6, so extraction picks OR(c0, c1).
        // This can only happen if not_demorgan fired.
        let not_and = variadic(
            and(),
            vec![col(0).call_unary(not()), col(1).call_unary(not())],
        )
        .call_unary(not());
        assert_eval_equiv(&not_and, 2);
        assert_eq!(
            canonicalize(&not_and, &[]),
            variadic(or(), vec![col(0), col(1)]),
            "NOT(AND(NOT c0, NOT c1)) must canonicalize to OR(c0, c1)"
        );

        // NOT(OR(NOT c0, NOT c1)) -> AND(NOT NOT c0, NOT NOT c1) -> AND(c0, c1).
        let not_or = variadic(
            or(),
            vec![col(0).call_unary(not()), col(1).call_unary(not())],
        )
        .call_unary(not());
        assert_eval_equiv(&not_or, 2);
        assert_eq!(
            canonicalize(&not_or, &[]),
            variadic(and(), vec![col(0), col(1)]),
            "NOT(OR(NOT c0, NOT c1)) must canonicalize to AND(c0, c1)"
        );
    }

    #[mz_ore::test]
    fn test_not_demorgan_error_preservation() {
        // d = (1 / c0 == 5): boolean, non-literal, errors at c0 == 0.
        // NOT(AND(NOT d, NOT c1)) simplifies via De Morgan + not_not to OR(d, c1),
        // which has cost 7 vs the input's cost 10, so extraction picks OR(d, c1).
        // The differential including the c0 == 0 error row proves error and
        // short-circuit behavior is preserved by the rewrite.
        let d = lit_int(1)
            .call_binary(col(0), div64())
            .call_binary(lit_int(5), eq());
        let expr = variadic(
            and(),
            vec![d.clone().call_unary(not()), col(1).call_unary(not())],
        )
        .call_unary(not());

        // De Morgan + not_not must simplify to OR(c1, d). Extraction sorts
        // And/Or operands by MirScalarExpr::Ord (matching reduce). Column(1)
        // precedes CallBinary in the enum discriminant order, so c1 sorts first.
        let got = canonicalize(&expr, &[]);
        assert_eq!(
            got,
            variadic(or(), vec![col(1), d.clone()]),
            "NOT(AND(NOT d, NOT c1)) must canonicalize to OR(c1, d) (sorted order)"
        );

        // Differential over c0 in {0, 5} x c1 in {true, false, null}.
        for c0 in [Datum::Int64(0), Datum::Int64(5)] {
            for c1 in [Datum::True, Datum::False, Datum::Null] {
                let row = vec![c0, c1];
                let want = eval_owned(&expr, &row);
                let have = eval_owned(&got, &row);
                assert_eq!(
                    want, have,
                    "De Morgan error mismatch at c0={c0:?} c1={c1:?}: \
                     input={want:?}, canonical={have:?}"
                );
            }
        }

        // At c0 == 0 with c1 == false: NOT d = Err, NOT c1 = true, AND(Err, true)
        // evaluates to Err (true does not short-circuit past the error), then
        // NOT(Err) = Err. Proves the differential covers a live error row and
        // that error behavior is preserved by the rewrite.
        assert!(
            eval_owned(&expr, &[Datum::Int64(0), Datum::False]).is_err(),
            "input must eval to Err at c0 == 0, c1 == false (error in AND is not masked)"
        );
    }

    // ===================================================================
    // If-resolution rules: if_true, if_false_or_null, if_same_branches.
    // ===================================================================

    fn if_expr(cond: MirScalarExpr, then: MirScalarExpr, els: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::If {
            cond: Box::new(cond),
            then: Box::new(then),
            els: Box::new(els),
        }
    }

    #[mz_ore::test]
    fn test_if_true() {
        // if(true, c0, c1) must resolve to c0.
        // The condition is a literal so if_true (not const_fold) fires.
        // Branches are columns so const_fold cannot collapse the whole If.
        let expr = if_expr(MirScalarExpr::literal_true(), col(0), col(1));
        assert_eval_equiv(&expr, 2);
        assert_eq!(
            canonicalize(&expr, &[]),
            col(0),
            "if(true, c0, c1) must canonicalize to c0"
        );
    }

    #[mz_ore::test]
    fn test_if_false() {
        // if(false, c0, c1) must resolve to c1.
        let expr = if_expr(MirScalarExpr::literal_false(), col(0), col(1));
        assert_eval_equiv(&expr, 2);
        assert_eq!(
            canonicalize(&expr, &[]),
            col(1),
            "if(false, c0, c1) must canonicalize to c1"
        );
    }

    #[mz_ore::test]
    fn test_if_null() {
        // if(null::bool, c0, c1) must resolve to c1. A null condition takes
        // the else branch per SQL semantics, matching reduce_if's
        // `Ok(Datum::Null) -> els` arm.
        use mz_repr::ReprScalarType;
        let null_cond = MirScalarExpr::literal_null(ReprScalarType::Bool);
        let expr = if_expr(null_cond, col(0), col(1));
        assert_eval_equiv(&expr, 2);
        assert_eq!(
            canonicalize(&expr, &[]),
            col(1),
            "if(null, c0, c1) must canonicalize to c1"
        );
    }

    #[mz_ore::test]
    fn test_if_same_branches() {
        // if(c0, c1, c1) must resolve to c1. The condition is a column so
        // const_fold cannot fire; only if_same_branches can collapse it.
        let expr = if_expr(col(0), col(1), col(1));
        assert_eval_equiv(&expr, 2);
        assert_eq!(
            canonicalize(&expr, &[]),
            col(1),
            "if(c0, c1, c1) must canonicalize to c1"
        );
    }

    // ===================================================================
    // Regression: const folding and round-trip still hold.
    // ===================================================================

    #[mz_ore::test]
    fn test_const_fold_binary() {
        let expr = lit_int(2).call_binary(lit_int(3), add64());
        let result = canonicalize(&expr, &[]);
        assert_eq!(result, lit_int(5), "2 + 3 must fold to 5");
        assert!(
            matches!(result, MirScalarExpr::Literal(..)),
            "result must be a Literal"
        );
    }

    #[mz_ore::test]
    fn test_const_fold_nested() {
        let inner = lit_int(2).call_binary(lit_int(3), add64());
        let expr = inner.call_binary(lit_int(4), mul64());
        let result = canonicalize(&expr, &[]);
        assert_eq!(result, lit_int(20), "(2 + 3) * 4 must fold to 20");
        assert!(
            matches!(result, MirScalarExpr::Literal(..)),
            "result must be a Literal"
        );
    }

    #[mz_ore::test]
    fn test_const_fold_error() {
        let expr = lit_int(1).call_binary(lit_int(0), div64());
        let result = canonicalize(&expr, &[]);
        let temp = RowArena::new();
        let expected = MirScalarExpr::literal(expr.eval(&[], &temp), expr.typ(&[]).scalar_type);
        assert_eq!(
            result, expected,
            "div-by-zero must fold to an error literal"
        );
        assert!(
            matches!(&result, MirScalarExpr::Literal(Err(_), _)),
            "folded result must be an Err literal"
        );
    }

    #[mz_ore::test]
    fn test_no_fold_with_column() {
        let expr = col(0).call_binary(lit_int(1), add64());
        let result = canonicalize(&expr, &[]);
        assert!(
            matches!(result, MirScalarExpr::CallBinary { .. }),
            "col + lit must not fold; expected CallBinary, got {result:?}"
        );
    }

    #[mz_ore::test]
    fn test_no_fold_panic() {
        // `mz_panic` must never run during optimization. `const_fold` folds calls
        // whose operands are all literals by evaluating them, which for a
        // literal-argument `Panic` would abort the optimizer. The guard mirrors
        // `reduce/unary.rs`. Reaching this assertion (rather than a panic) is the
        // test: canonicalize must leave the call intact, not evaluate it.
        let panic = MirScalarExpr::literal_ok(Datum::String("boom"), ReprScalarType::String)
            .call_unary(UnaryFunc::Panic(mz_expr::func::Panic));
        let result = canonicalize(&panic, &[]);
        assert_eq!(result, panic, "Panic(literal) must not be folded");

        // The same guard must hold when `Panic` is a subexpression, the shape a
        // real predicate such as `WHERE mz_panic('boom') = 'x'` takes. Saturation
        // visits the inner `Panic` node directly, and folding it there would
        // equally crash the optimizer. The surrounding `Eq` is not foldable (one
        // operand is the non-literal `Panic`), so the expression must round-trip.
        let nested = panic.clone().call_binary(
            MirScalarExpr::literal_ok(Datum::String("x"), ReprScalarType::String),
            eq(),
        );
        let result = canonicalize(&nested, &[]);
        assert_eq!(
            result, nested,
            "Eq(Panic(literal), 'x') must not evaluate the inner Panic"
        );
    }

    #[mz_ore::test]
    fn test_round_trip_regression_no_rules() {
        let expr = col(0).call_binary(col(1), add64());
        let result = canonicalize(&expr, &[]);
        assert_eq!(result, expr, "non-foldable expression must round-trip");
    }

    #[mz_ore::test]
    fn test_fold_terminates() {
        let expr = lit_int(2).call_binary(lit_int(3), add64());
        let mut eg = ScalarEGraph::new();
        let root = lower(&mut eg, &expr);
        let iters = crate::eqsat::scalar::egraph::saturate(&mut eg);
        assert!(
            iters <= 10,
            "saturation must terminate quickly; got {iters} iters"
        );
        let _ = root;
    }

    #[mz_ore::test]
    fn test_fold_cycle_could_error_is_conservative() {
        let expr = lit_int(0).call_unary(neg64());
        let mut eg = ScalarEGraph::new();
        let root = lower(&mut eg, &expr);
        crate::eqsat::scalar::egraph::saturate(&mut eg);
        let a = eg.analysis(root);
        assert!(
            a.literal.is_some(),
            "neg(0) class must carry the folded literal"
        );
        assert!(
            a.could_error,
            "fold-induced self-cycle must keep could_error == true (conservative)"
        );
    }

    // ===================================================================
    // Null propagation, could_error-gated (deviation from reduce).
    // ===================================================================

    fn null_int() -> MirScalarExpr {
        MirScalarExpr::literal_null(ReprScalarType::Int64)
    }

    // A single nullable Int64 column type for the c0-based null-prop tests.
    fn int_col_types() -> Vec<mz_repr::ReprColumnType> {
        vec![ReprScalarType::Int64.nullable(true)]
    }

    #[mz_ore::test]
    fn test_null_prop_binary_fires_on_safe_operand() {
        // AddInt64(null, c0): c0 cannot error, so the gate permits null-prop and
        // the call collapses to a typed null. AddInt64's result type is Int64.
        let ct = int_col_types();
        let expr = null_int().call_binary(col(0), add64());
        let got = canonicalize(&expr, &ct);
        assert_eq!(
            got,
            MirScalarExpr::literal_null(ReprScalarType::Int64),
            "AddInt64(null, c0) must propagate to a typed Int64 null"
        );
        // Differential: input and output eval identically for several c0 values
        // and for a null c0. `Add` propagates nulls and c0 cannot error, so both
        // are null everywhere.
        for c0 in [
            Datum::Int64(0),
            Datum::Int64(7),
            Datum::Int64(-3),
            Datum::Null,
        ] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(want, have, "null-prop eval mismatch at c0={c0:?}");
        }
    }

    #[mz_ore::test]
    fn test_null_prop_binary_blocked_when_other_can_error() {
        // AddInt64(null, 1 / c0): `1 / c0` has could_error == true (division by
        // zero), so the gate BLOCKS null-prop. This is the proof that the gate
        // prevents the unsound err->null. The result must remain the call, not a
        // null literal.
        let ct = int_col_types();
        let dividing = lit_int(1).call_binary(col(0), div64());
        let expr = null_int().call_binary(dividing, add64());
        let got = canonicalize(&expr, &ct);
        assert_ne!(
            got,
            MirScalarExpr::literal_null(ReprScalarType::Int64),
            "null-prop must NOT fire when the other operand can error"
        );
        assert!(
            matches!(got, MirScalarExpr::CallBinary { .. }),
            "blocked null-prop must leave the call intact, got {got:?}"
        );
        // Differential including the c0 == 0 error row: `eval(input)` is an Err
        // there (error wins over null in eval), and the unchanged output evals to
        // the same Err. The c0 == 5 row evals to null on both sides.
        for c0 in [Datum::Int64(0), Datum::Int64(5)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(want, have, "blocked null-prop eval mismatch at c0={c0:?}");
        }
        // Make the err->null hazard explicit: at c0 == 0 the input is an Err,
        // never null. A naive ungated null-prop would have produced null here.
        assert!(
            eval_owned(&expr, &[Datum::Int64(0)]).is_err(),
            "input must eval to Err at c0 == 0 (error wins over null)"
        );
    }

    // A propagates_nulls variadic over numeric inputs: makets takes five i64
    // and one f64, all non-nullable, so `propagates_nulls()` is true.
    fn make_timestamp() -> VariadicFunc {
        VariadicFunc::MakeTimestamp(mz_expr::func::variadic::MakeTimestamp)
    }

    fn lit_f64(v: f64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(
            Datum::Float64(ordered_float::OrderedFloat(v)),
            ReprScalarType::Float64,
        )
    }

    #[mz_ore::test]
    fn test_null_prop_variadic_fires_on_safe_operands() {
        assert!(
            make_timestamp().propagates_nulls(),
            "makets must propagate nulls for this test to be meaningful"
        );
        // makets(2024, c0, 1, 0, 0, 0.0) with a null year argument. Every other
        // operand is a safe column or literal, so the gate permits null-prop.
        let ct = int_col_types();
        let exprs = vec![
            null_int(),
            col(0),
            lit_int(1),
            lit_int(0),
            lit_int(0),
            lit_f64(0.0),
        ];
        let expr = variadic(make_timestamp(), exprs);
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(got, MirScalarExpr::Literal(Ok(_), _)),
            "variadic null-prop must produce a typed null literal, got {got:?}"
        );
        let want_ty = expr.typ(&ct).scalar_type;
        assert_eq!(
            got,
            MirScalarExpr::literal_null(want_ty),
            "makets(null, ..) must propagate to a typed null"
        );
        // Differential over safe c0 values and a null c0.
        for c0 in [Datum::Int64(1), Datum::Int64(12), Datum::Null] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(want, have, "variadic null-prop eval mismatch at c0={c0:?}");
        }
    }

    #[mz_ore::test]
    fn test_null_prop_variadic_blocked_when_other_can_error() {
        // makets(null, 1 / c0, 1, 0, 0, 0.0): the `1 / c0` operand can error, so
        // the gate BLOCKS null-prop. The call must remain.
        let ct = int_col_types();
        let dividing = lit_int(1).call_binary(col(0), div64());
        let exprs = vec![
            null_int(),
            dividing,
            lit_int(1),
            lit_int(0),
            lit_int(0),
            lit_f64(0.0),
        ];
        let expr = variadic(make_timestamp(), exprs);
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(got, MirScalarExpr::CallVariadic { .. }),
            "blocked variadic null-prop must leave the call intact, got {got:?}"
        );
        // Differential including the c0 == 0 error row: input is Err there, and
        // the unchanged output evals to the same Err.
        for c0 in [Datum::Int64(0), Datum::Int64(5)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(
                want, have,
                "blocked variadic null-prop eval mismatch at c0={c0:?}"
            );
        }
        assert!(
            eval_owned(&expr, &[Datum::Int64(0)]).is_err(),
            "input must eval to Err at c0 == 0 (error wins over null)"
        );
    }

    // ===================================================================
    // Error propagation, could_error-gated (deviation from reduce).
    // ===================================================================

    // A literal DivisionByZero error of type Int64: `1 / 0` const-folds to this.
    fn div_by_zero_err() -> MirScalarExpr {
        lit_int(1).call_binary(lit_int(0), div64())
    }

    #[mz_ore::test]
    fn test_err_prop_binary_fires_first_position() {
        // AddInt64(1/0, c0) with c0 a non-erroring column: the gate permits
        // err-prop and the call folds to the DivisionByZero error literal.
        let ct = int_col_types();
        let err_lit = div_by_zero_err();
        let expr = err_lit.call_binary(col(0), add64());
        let got = canonicalize(&expr, &ct);
        // The canonical form must be a Literal(Err(DivisionByZero), _).
        assert!(
            matches!(&got, MirScalarExpr::Literal(Err(_), _)),
            "err-prop must fold AddInt64(err, c0) to an error literal, got {got:?}"
        );
        // Exact-error differential: every c0 row evaluates to DivisionByZero.
        for c0 in [Datum::Int64(0), Datum::Int64(7), Datum::Int64(-3)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(
                want, have,
                "err-prop eval mismatch at c0={c0:?}: input={want:?}, canonical={have:?}"
            );
        }
    }

    #[mz_ore::test]
    fn test_err_prop_binary_fires_second_position() {
        // AddInt64(c0, 1/0) with c0 a non-erroring column: same as first
        // position — gate permits err-prop from the second operand.
        let ct = int_col_types();
        let err_lit = div_by_zero_err();
        let expr = col(0).call_binary(err_lit, add64());
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(&got, MirScalarExpr::Literal(Err(_), _)),
            "err-prop must fold AddInt64(c0, err) to an error literal, got {got:?}"
        );
        for c0 in [Datum::Int64(0), Datum::Int64(7), Datum::Int64(-3)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(
                want, have,
                "err-prop eval mismatch at c0={c0:?}: input={want:?}, canonical={have:?}"
            );
        }
    }

    #[mz_ore::test]
    fn test_err_prop_binary_blocked_when_other_can_error() {
        // AddInt64(1/c0, 1/0): `1/c0` has could_error == true, so the gate BLOCKS
        // err-prop. This proves the gate preserves the exact error: at c0 == 0,
        // `1/c0` errors first (left-to-right eval), not the literal `1/0`.
        let ct = int_col_types();
        let dividing = lit_int(1).call_binary(col(0), div64());
        let err_lit = div_by_zero_err();
        let expr = dividing.call_binary(err_lit, add64());
        let got = canonicalize(&expr, &ct);
        // Must remain a CallBinary, not a bare error literal.
        assert!(
            matches!(got, MirScalarExpr::CallBinary { .. }),
            "blocked err-prop must leave AddInt64(1/c0, err) as a call, got {got:?}"
        );
        // Differential at c0 == 0 (1/c0 errors first) and c0 == 5 (1/0 errors).
        for c0 in [Datum::Int64(0), Datum::Int64(5)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(
                want, have,
                "blocked err-prop eval mismatch at c0={c0:?}: input={want:?}, canonical={have:?}"
            );
        }
        // At c0 == 0 the input IS an error (proving the gate is needed).
        assert!(
            eval_owned(&expr, &[Datum::Int64(0)]).is_err(),
            "input must eval to Err at c0 == 0 (1/c0 errors first)"
        );
    }

    #[mz_ore::test]
    fn test_err_prop_variadic_fires() {
        // makets(1/0, c0, 1, 0, 0, 0.0): the first operand is a literal error;
        // all other operands have could_error == false. Gate permits err-prop.
        assert!(
            make_timestamp().propagates_nulls(),
            "makets must propagate nulls for the variadic err-prop gate to apply"
        );
        let ct = int_col_types();
        let err_lit = div_by_zero_err();
        let exprs = vec![
            err_lit,
            col(0),
            lit_int(1),
            lit_int(0),
            lit_int(0),
            lit_f64(0.0),
        ];
        let expr = variadic(make_timestamp(), exprs);
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(&got, MirScalarExpr::Literal(Err(_), _)),
            "variadic err-prop must fold makets(err, ..) to an error literal, got {got:?}"
        );
        // Exact-error differential over safe c0 values.
        for c0 in [Datum::Int64(1), Datum::Int64(12)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(
                want, have,
                "variadic err-prop eval mismatch at c0={c0:?}: input={want:?}, canonical={have:?}"
            );
        }
    }

    #[mz_ore::test]
    fn test_err_prop_variadic_blocked_when_other_can_error() {
        // makets(1/0, 1/c0, 1, 0, 0, 0.0): `1/c0` can error, so the gate BLOCKS
        // err-prop. At c0 == 0, `1/c0` errors; at c0 == 5, `1/0` errors.
        let ct = int_col_types();
        let err_lit = div_by_zero_err();
        let dividing = lit_int(1).call_binary(col(0), div64());
        let exprs = vec![
            err_lit,
            dividing,
            lit_int(1),
            lit_int(0),
            lit_int(0),
            lit_f64(0.0),
        ];
        let expr = variadic(make_timestamp(), exprs);
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(got, MirScalarExpr::CallVariadic { .. }),
            "blocked variadic err-prop must leave the call intact, got {got:?}"
        );
        for c0 in [Datum::Int64(0), Datum::Int64(5)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(
                want, have,
                "blocked variadic err-prop eval mismatch at c0={c0:?}"
            );
        }
        assert!(
            eval_owned(&expr, &[Datum::Int64(0)]).is_err(),
            "input must eval to Err at c0 == 0"
        );
    }

    // ===================================================================
    // Rule: isnull_fold, could_error-gated (deviation from reduce).
    // ===================================================================

    #[mz_ore::test]
    fn test_isnull_fold_fires_on_non_nullable_col() {
        // IsNull(c0) with c0 non-nullable and error-free: folds to false.
        let ct = vec![ReprScalarType::Int64.nullable(false)];
        let expr = col(0).call_is_null();
        let got = canonicalize(&expr, &ct);
        assert_eq!(
            got,
            MirScalarExpr::literal_false(),
            "IsNull(non-nullable col) must fold to false"
        );
        // Differential over non-null int rows (the column type forbids null): both
        // sides are false everywhere.
        for c0 in [Datum::Int64(0), Datum::Int64(7), Datum::Int64(-3)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(want, have, "isnull_fold eval mismatch at c0={c0:?}");
        }
    }

    #[mz_ore::test]
    fn test_isnull_fold_blocked_on_nullable_col() {
        // IsNull(c0) with c0 nullable: must NOT fold, the column can be null.
        let ct = int_col_types();
        let expr = col(0).call_is_null();
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(
                got,
                MirScalarExpr::CallUnary {
                    func: UnaryFunc::IsNull(_),
                    ..
                }
            ),
            "IsNull(nullable col) must remain an IsNull call, got {got:?}"
        );
        // Differential over a null and a non-null row: IsNull genuinely differs
        // between them, so a fold to false would be observable here.
        for c0 in [Datum::Int64(5), Datum::Null] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(want, have, "blocked isnull_fold eval mismatch at c0={c0:?}");
        }
    }

    #[mz_ore::test]
    fn test_isnull_fold_blocked_when_operand_can_error() {
        // IsNull(1/c0) with c0 non-nullable: `1/c0` is non-nullable-typed (both
        // operands non-nullable, DivInt64 propagates nulls) but has could_error ==
        // true, so the gate BLOCKS the fold. This is the deviation from reduce: a
        // pure-nullability fold would turn IsNull(1/c0) into false, which is
        // unsound at c0 == 0 where the operand errors and IsNull propagates it.
        let ct = vec![ReprScalarType::Int64.nullable(false)];
        let expr = lit_int(1).call_binary(col(0), div64()).call_is_null();
        let got = canonicalize(&expr, &ct);
        assert_ne!(
            got,
            MirScalarExpr::literal_false(),
            "isnull_fold must NOT fire when the operand can error"
        );
        assert!(
            matches!(
                got,
                MirScalarExpr::CallUnary {
                    func: UnaryFunc::IsNull(_),
                    ..
                }
            ),
            "blocked isnull_fold must leave the IsNull call intact, got {got:?}"
        );
        // Differential including the c0 == 0 error row: `eval(input)` is an Err
        // there (IsNull propagates the operand error), and the unchanged output
        // evals to the same Err. At c0 == 5 both sides are false.
        for c0 in [Datum::Int64(0), Datum::Int64(5)] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(want, have, "blocked isnull_fold eval mismatch at c0={c0:?}");
        }
        // Make the hazard explicit: at c0 == 0 the input is an Err, never false.
        assert!(
            eval_owned(&expr, &[Datum::Int64(0)]).is_err(),
            "input must eval to Err at c0 == 0 (IsNull propagates operand error)"
        );
    }

    #[mz_ore::test]
    fn test_isnull_fold_composition_not_is_not_null() {
        // NOT(IsNull(c0)) with c0 non-nullable error-free: isnull_fold turns the
        // IsNull into false, then not + const-fold collapse NOT(false) to true.
        // Mirrors the regression predicate `(#1) IS NOT NULL` collapsing.
        let ct = vec![ReprScalarType::Bool.nullable(false)];
        let expr = col(0).call_is_null().call_unary(not());
        let got = canonicalize(&expr, &ct);
        assert_eq!(
            got,
            MirScalarExpr::literal_true(),
            "NOT(IsNull(non-nullable col)) must collapse to true"
        );
        for c0 in [Datum::True, Datum::False] {
            let want = eval_owned(&expr, &[c0]);
            let have = eval_owned(&got, &[c0]);
            assert_eq!(want, have, "composition eval mismatch at c0={c0:?}");
        }
    }

    // ===================================================================
    // Rule: if_err_cond.
    // ===================================================================

    // Two nullable Int64 column types for if_err_cond tests (c0 and c1).
    fn two_int_col_types() -> Vec<mz_repr::ReprColumnType> {
        vec![
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Int64.nullable(true),
        ]
    }

    #[mz_ore::test]
    fn test_if_err_cond_fires() {
        // if(1/0, c0, c1): `1/0` const-folds to a literal DivisionByZero error,
        // so if_err_cond must fire and replace the whole If with that error.
        // The branches are columns so const_fold cannot collapse the If directly.
        let ct = two_int_col_types();
        let err_cond = div_by_zero_err();
        let expr = if_expr(err_cond, col(0), col(1));
        let got = canonicalize(&expr, &ct);

        // The canonical form must be a Literal(Err(DivisionByZero), _).
        assert!(
            matches!(&got, MirScalarExpr::Literal(Err(_), _)),
            "if(err, c0, c1) must canonicalize to an error literal, got {got:?}"
        );
        let MirScalarExpr::Literal(Err(err), _) = &got else {
            unreachable!()
        };
        assert!(
            matches!(err, mz_expr::EvalError::DivisionByZero),
            "error must be DivisionByZero, got {err:?}"
        );

        // Differential: for several int rows the condition errors first,
        // so eval(input) == eval(output) == Err(DivisionByZero) regardless of
        // the branch values.
        for (c0, c1) in [
            (Datum::Int64(0), Datum::Int64(1)),
            (Datum::Int64(42), Datum::Int64(-7)),
            (Datum::Null, Datum::Int64(3)),
        ] {
            let want = eval_owned(&expr, &[c0, c1]);
            let have = eval_owned(&got, &[c0, c1]);
            assert_eq!(
                want, have,
                "if_err_cond eval mismatch at c0={c0:?} c1={c1:?}: input={want:?}, canonical={have:?}"
            );
            assert!(
                want.is_err(),
                "input must eval to Err at c0={c0:?} c1={c1:?} (condition errors first)"
            );
        }
    }

    #[mz_ore::test]
    fn test_if_err_cond_branch_type_union() {
        // if(1/0, c0::Int64 nullable, 5::Int64 non-null): the then-branch is
        // nullable, the else-branch is not. The union must yield nullable Int64.
        // The main check is no panic and the result is an error literal.
        let ct = vec![ReprScalarType::Int64.nullable(true)];
        let err_cond = div_by_zero_err();
        // c0 is nullable Int64; 5 is non-null Int64.
        let expr = if_expr(err_cond, col(0), lit_int(5));
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(&got, MirScalarExpr::Literal(Err(_), _)),
            "if(err, nullable_col, non_null_lit) must yield an error literal, got {got:?}"
        );
        // The result type must be nullable (union of nullable and non-null).
        let MirScalarExpr::Literal(_, col_type) = &got else {
            unreachable!()
        };
        assert!(
            col_type.nullable,
            "union of nullable and non-null branch must yield nullable result type"
        );
    }

    #[mz_ore::test]
    fn test_if_non_err_cond_not_affected() {
        // if_err_cond must NOT fire when the condition is a column (not a literal
        // error). The If must be left as a call (or resolved by other rules).
        let ct = two_int_col_types();
        // c0 is not a literal error, so if_err_cond must not fire.
        let expr = if_expr(col(0), col(1), col(1));
        // if_same_branches fires here (both branches are c1), but if_err_cond must
        // not: the result must NOT be an error literal.
        let got = canonicalize(&expr, &ct);
        assert!(
            !matches!(&got, MirScalarExpr::Literal(Err(_), _)),
            "if(column, c1, c1) must not produce an error literal, got {got:?}"
        );
    }

    #[mz_ore::test]
    fn test_if_err_cond_silent_on_distinct_branches() {
        // if(c0, c1, c2): the condition is a non-error column and the branches
        // are DISTINCT, so neither if_err_cond nor if_same_branches can fire and
        // the If must survive as a call. The companion test above uses identical
        // branches (if(c0, c1, c1)), where if_same_branches collapses the If and
        // could mask whether if_err_cond stayed silent. Distinct branches
        // isolate if_err_cond: only its (absent) firing could turn this into an
        // error literal or otherwise collapse the If.
        let ct = vec![
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Int64.nullable(true),
        ];
        let expr = if_expr(col(0), col(1), col(2));
        let got = canonicalize(&expr, &ct);
        assert!(
            matches!(&got, MirScalarExpr::If { .. }),
            "if(c0, c1, c2) must remain an If; if_err_cond must not fire, got {got:?}"
        );
        assert_eq!(got, expr, "if(c0, c1, c2) must round-trip unchanged");
    }

    #[mz_ore::test]
    fn test_if_true_false_rules_still_fire_regression() {
        // Adding if_err_cond must not break the 1d rules. Verify that if(true,
        // c0, c1) and if(false, c0, c1) still resolve correctly.
        let ct = two_int_col_types();
        let true_expr = if_expr(MirScalarExpr::literal_true(), col(0), col(1));
        assert_eq!(
            canonicalize(&true_expr, &ct),
            col(0),
            "if(true, c0, c1) regression: must still resolve to c0"
        );
        let false_expr = if_expr(MirScalarExpr::literal_false(), col(0), col(1));
        assert_eq!(
            canonicalize(&false_expr, &ct),
            col(1),
            "if(false, c0, c1) regression: must still resolve to c1"
        );
    }

    // ===================================================================
    // Canonical operand ordering at extraction (Phase 3).
    //
    // Extraction sorts And/Or operands by MirScalarExpr::Ord, mirroring
    // reduce's exprs.sort() in reduce_and_canonicalize_and_or. Tests assert
    // the sorted order, soundness, determinism, and that non-And/Or operand
    // order is preserved.
    // ===================================================================

    #[mz_ore::test]
    fn test_ordering_and() {
        // AND(c1, c0): the natural lowering order is [c1, c0], which is NOT
        // the sorted order. Extraction must emit [c0, c1] (Column(0) < Column(1)
        // in Ord). This makes the sort observable: the test fails if no sort
        // happens.
        let input = variadic(and(), vec![col(1), col(0)]);
        let got = canonicalize(&input, &[]);
        assert_eq!(
            got,
            variadic(and(), vec![col(0), col(1)]),
            "AND(c1, c0) must extract as AND(c0, c1) in sorted order"
        );
    }

    #[mz_ore::test]
    fn test_ordering_or() {
        // OR(c1, c0): dual of the And case. Sorted order is [c0, c1].
        let input = variadic(or(), vec![col(1), col(0)]);
        let got = canonicalize(&input, &[]);
        assert_eq!(
            got,
            variadic(or(), vec![col(0), col(1)]),
            "OR(c1, c0) must extract as OR(c0, c1) in sorted order"
        );
    }

    #[mz_ore::test]
    fn test_ordering_deterministic() {
        // The extracted And/Or operand order is deterministic across two
        // canonicalize calls on the same input. A plain equality suffices: the
        // sort makes the order total, so no HashSet-based nondeterminism can
        // surface.
        let input = variadic(and(), vec![col(1), col(0)]);
        let first = canonicalize(&input, &[]);
        let second = canonicalize(&input, &[]);
        assert_eq!(first, second, "canonicalize must be deterministic");
    }

    #[mz_ore::test]
    fn test_ordering_soundness() {
        // Sorting And/Or operands must not change observable evaluation.
        // AND(c1, c0) and OR(c1, c0) are checked over the full {true, false,
        // null} cube on 2 boolean columns.
        let and_input = variadic(and(), vec![col(1), col(0)]);
        assert_eval_equiv(&and_input, 2);

        let or_input = variadic(or(), vec![col(1), col(0)]);
        assert_eval_equiv(&or_input, 2);
    }

    #[mz_ore::test]
    fn test_ordering_non_and_or_unchanged() {
        // A non-And/Or variadic (MakeTimestamp) must NOT have its operand
        // order changed: it is order-significant. The operands here include a
        // column (non-foldable), so no other rule collapses the call. The
        // extraction must return exactly the input operand order.
        let ct = int_col_types();
        let mk_ts = make_timestamp();
        // Put c0 in month position (index 1): year=2024, month=c0, day=1, ...
        // The order of arguments is part of the function's meaning.
        let input = variadic(
            mk_ts,
            vec![
                lit_int(2024),
                col(0),
                lit_int(1),
                lit_int(0),
                lit_int(0),
                lit_f64(0.0),
            ],
        );
        let got = canonicalize(&input, &ct);
        let MirScalarExpr::CallVariadic { exprs, .. } = &got else {
            // If it const-folded, the order test is moot. Only assert when the
            // call survives.
            return;
        };
        assert_eq!(
            exprs[0],
            lit_int(2024),
            "MakeTimestamp first operand (year) must remain first"
        );
        assert_eq!(
            exprs[1],
            col(0),
            "MakeTimestamp second operand (month col) must remain second"
        );
    }

    // ===================================================================
    // Phase-level differential: canonicalize preserves evaluation over
    // arbitrary nested expressions, including error-producing rows.
    //
    // The per-rule tests above cannot catch an emergent bad interaction over a
    // deeply nested term. This generates a large corpus of bounded-random,
    // well-typed expressions and asserts that the canonical form evaluates
    // identically to the input on every row of a datum cube that fires
    // division-by-zero and overflow errors. Results are `Result`s, so errors and
    // nulls compare by value: an unsound rule that turned an error into a null,
    // a different error, or a wrong value would fail here.
    // ===================================================================

    /// A deterministic xorshift64 RNG. Seeded explicitly so the generated corpus
    /// is reproducible across runs and in CI. We deliberately use neither system
    /// time nor an unseeded RNG.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            // A zero state would make xorshift64 stick at zero forever.
            Rng(if seed == 0 {
                0x_DEAD_BEEF_CAFE_F00D
            } else {
                seed
            })
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        /// A uniformly distributed index in `0..n`. `n` must be non-zero.
        fn below(&mut self, n: usize) -> usize {
            let n64 = u64::try_from(n).expect("bound fits in u64");
            usize::try_from(self.next_u64() % n64).expect("remainder fits in usize")
        }
    }

    /// The two scalar types the generator produces. Restricting the alphabet to
    /// Int64 and Bool keeps every generated term well-typed, which both
    /// `canonicalize` (it types columns) and `eval` require.
    #[derive(Clone, Copy)]
    enum GenTy {
        Int,
        Bool,
    }

    // Column layout for the generated corpus: c0, c1 are Int64; c2, c3 are Bool.
    fn diff_col_types() -> Vec<mz_repr::ReprColumnType> {
        vec![
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Bool.nullable(true),
            ReprScalarType::Bool.nullable(true),
        ]
    }

    /// A leaf of the requested type: a column, a small literal, or a typed null.
    /// Int leaves include `0` so a `DivInt64` denominator can be the literal
    /// zero in addition to the zero-valued column rows.
    fn gen_leaf(ty: GenTy, rng: &mut Rng) -> MirScalarExpr {
        match ty {
            GenTy::Int => match rng.below(6) {
                0 => col(0),
                1 => col(1),
                2 => lit_int(0),
                3 => lit_int(1),
                4 => lit_int(2),
                _ => MirScalarExpr::literal_null(ReprScalarType::Int64),
            },
            GenTy::Bool => match rng.below(5) {
                0 => col(2),
                1 => col(3),
                2 => MirScalarExpr::literal_true(),
                3 => MirScalarExpr::literal_false(),
                _ => MirScalarExpr::literal_null(ReprScalarType::Bool),
            },
        }
    }

    /// A bounded-random well-typed expression of type `ty`. At depth 0, or with
    /// a small probability, it stops at a leaf. Otherwise it builds a nested
    /// call so the corpus is dominated by non-trivial terms that exercise the
    /// rules. Boolean-context operands are bool-typed, arithmetic operands are
    /// int-typed, and `If` keeps the condition bool and both branches the shared
    /// result type, so the result is always well-typed.
    fn gen_expr(ty: GenTy, depth: usize, rng: &mut Rng) -> MirScalarExpr {
        if depth == 0 || rng.below(4) == 0 {
            return gen_leaf(ty, rng);
        }
        let d = depth - 1;
        match ty {
            GenTy::Int => {
                match rng.below(4) {
                    0 => gen_expr(GenTy::Int, d, rng)
                        .call_binary(gen_expr(GenTy::Int, d, rng), add64()),
                    1 => gen_expr(GenTy::Int, d, rng)
                        .call_binary(gen_expr(GenTy::Int, d, rng), mul64()),
                    2 => gen_expr(GenTy::Int, d, rng)
                        .call_binary(gen_expr(GenTy::Int, d, rng), div64()),
                    _ => if_expr(
                        gen_expr(GenTy::Bool, d, rng),
                        gen_expr(GenTy::Int, d, rng),
                        gen_expr(GenTy::Int, d, rng),
                    ),
                }
            }
            GenTy::Bool => match rng.below(6) {
                0 => {
                    // Eq/NotEq over a randomly chosen (shared) operand type.
                    let op_ty = if rng.below(2) == 0 {
                        GenTy::Int
                    } else {
                        GenTy::Bool
                    };
                    let f = if rng.below(2) == 0 { eq() } else { neq() };
                    gen_expr(op_ty, d, rng).call_binary(gen_expr(op_ty, d, rng), f)
                }
                1 => gen_expr(GenTy::Bool, d, rng).call_unary(not()),
                2 | 3 => {
                    let f = if rng.below(2) == 0 { and() } else { or() };
                    let k = 2 + rng.below(2);
                    let exprs = (0..k).map(|_| gen_expr(GenTy::Bool, d, rng)).collect();
                    variadic(f, exprs)
                }
                _ => if_expr(
                    gen_expr(GenTy::Bool, d, rng),
                    gen_expr(GenTy::Bool, d, rng),
                    gen_expr(GenTy::Bool, d, rng),
                ),
            },
        }
    }

    /// The datum cube the corpus is differentialed over. Int columns hold `0`
    /// (so `DivInt64` errors fire), `i64::MAX` (so `AddInt64`/`MulInt64` overflow
    /// errors fire), small values, and `Null`; bool columns range over the full
    /// `{true, false, null}` three-valued set.
    fn diff_cube() -> Vec<Vec<Datum<'static>>> {
        let ints = [
            Datum::Int64(0),
            Datum::Int64(1),
            Datum::Int64(2),
            Datum::Int64(i64::MAX),
            Datum::Null,
        ];
        let bools = [Datum::True, Datum::False, Datum::Null];
        let mut rows = Vec::new();
        for &c0 in &ints {
            for &c1 in &ints {
                for &c2 in &bools {
                    for &c3 in &bools {
                        rows.push(vec![c0, c1, c2, c3]);
                    }
                }
            }
        }
        rows
    }

    #[mz_ore::test]
    fn test_canonicalize_eval_differential() {
        let col_types = diff_col_types();
        let rows = diff_cube();

        // Fixed seeds keep the corpus reproducible. Each seed contributes
        // EXPRS_PER_SEED bounded-depth expressions.
        const SEEDS: [u64; 3] = [
            0x9E37_79B9_7F4A_7C15,
            0xD1B5_4A32_D192_ED03,
            0x2545_F491_4F6C_DD1D,
        ];
        const EXPRS_PER_SEED: usize = 100;

        let mut cases = 0;
        for seed in SEEDS {
            let mut rng = Rng::new(seed);
            for _ in 0..EXPRS_PER_SEED {
                let ty = if rng.below(2) == 0 {
                    GenTy::Int
                } else {
                    GenTy::Bool
                };
                // Depth in 2..=4: deep enough to nest several rules, cheap
                // enough for a large corpus.
                let depth = 2 + rng.below(3);
                let expr = gen_expr(ty, depth, &mut rng);
                let canon = canonicalize(&expr, &col_types);
                for row in &rows {
                    let want = eval_owned(&expr, row);
                    let have = eval_owned(&canon, row);
                    assert_eq!(
                        want, have,
                        "differential mismatch on row {row:?}:\n  input: {expr:?}\n  canon: {canon:?}\n  want {want:?}, have {have:?}"
                    );
                }
                cases += 1;
            }
        }
        // Guard the corpus size so an accidental change to the loop bounds that
        // silently shrinks coverage is caught.
        assert_eq!(cases, SEEDS.len() * EXPRS_PER_SEED, "expected 300 cases");
    }
}
