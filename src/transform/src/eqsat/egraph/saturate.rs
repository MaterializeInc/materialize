// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Equality saturation, side conditions, and analysis driver for [`EGraph`].

use std::collections::{BTreeMap, HashMap};

use mz_expr::{BinaryFunc, Columns, MirScalarExpr};
use mz_ore::cast::CastFrom;

use crate::eqsat::analysis::{ConstCols, KeySet, Keys, LocalFacts, Monotonic, NonNeg, is_superkey};
use crate::eqsat::core::Id;
use crate::eqsat::ir::EScalar;
use crate::eqsat::matcher::Payload;
use crate::eqsat::rules::CompiledRuleSet;

use super::combined::{CombinedData, EGraph};
use super::node::ENode;

// Constants used in saturation (defined in the root egraph.rs but accessed here
// as `super::MAX_ENODES` etc.).
use super::{INITIAL_BAN_LEN, MATCH_LIMIT, MAX_ENODES};

// --- compiled-rule bindings -----------------------------------------------

/// All e-graph bindings produced by matching a rule's left-hand side.
#[derive(Clone, Debug, Default)]
pub struct EBindings {
    pub rels: HashMap<&'static str, Id>,
    pub payloads: BTreeMap<&'static str, Payload>,
    pub rests: HashMap<&'static str, Vec<Id>>,
    /// Func-metavar bindings, e.g. the `BinaryFunc` bound by a `Pat::SBinaryVar`
    /// match. A function symbol has neither an e-class id (like `rels`) nor a
    /// payload list (like `payloads`), so it gets its own store. See
    /// `bind_binary_func`/`binary_func`.
    binary_funcs: HashMap<&'static str, BinaryFunc>,
    /// The class at which the pattern's root matched.
    pub root: Id,
}

impl EBindings {
    /// Bind `name` to `func`, called by a compiled `find` when a `Pat::SBinaryVar`
    /// pattern matches.
    pub fn bind_binary_func(&mut self, name: &'static str, func: BinaryFunc) {
        self.binary_funcs.insert(name, func);
    }

    /// The `BinaryFunc` bound to metavariable `name`.
    ///
    /// Panics if unbound: a compiled `apply` only calls this for a name its
    /// rule's `find` is statically guaranteed to have bound.
    pub fn binary_func(&self, name: &str) -> BinaryFunc {
        self.binary_funcs
            .get(name)
            .unwrap_or_else(|| panic!("unbound func metavariable `{name}`"))
            .clone()
    }
}

// --- side conditions ------------------------------------------------------
//
// Each function mirrors a `Cond` arm of the former AST interpreter. The
// compiled `find` matchers (see [`crate::eqsat::rules`]) call these directly
// instead of walking a `Cond` list.

/// `uses_only_input`: every column referenced by the payload is an output
/// column of the bound relation (which has arity `rel_arity`).
pub(crate) fn cond_uses_only_input(data: &CombinedData, p: &Payload, rel_arity: usize) -> bool {
    p.columns(data).into_iter().all(|c| c < rel_arity)
}

/// `identity_projection`: the projection payload is exactly `[0, 1, ..., n-1]`
/// where `n = rel_arity`. Used to drop `Project[id](r) = r`. Reads only the
/// `Outputs` projection list, so it needs no cache.
pub(crate) fn cond_identity_projection(p: &Payload, rel_arity: usize) -> bool {
    match p {
        Payload::Outputs(o) => o.len() == rel_arity && o.iter().enumerate().all(|(i, &c)| c == i),
        _ => false,
    }
}

/// `cols_in_range`: every column referenced by the payload lies in `[lo, hi)`.
pub(crate) fn cond_cols_in_range(data: &CombinedData, p: &Payload, lo: i64, hi: i64) -> bool {
    p.columns(data)
        .into_iter()
        .all(|c| (c as i64) >= lo && (c as i64) < hi)
}

/// `all_columns`: every scalar in the payload is a bare column reference.
pub(crate) fn cond_all_columns(data: &CombinedData, p: &Payload) -> bool {
    p.scalar_ids()
        .is_some_and(|s| s.iter().all(|&x| data.escalar(x).is_col().is_some()))
}

/// `any_false`: some scalar constant-folds to the literal `false`.
pub(crate) fn cond_any_false(data: &CombinedData, p: &Payload) -> bool {
    p.scalar_ids()
        .is_some_and(|s| s.iter().any(|&x| data.escalar(x).lit == Some(false)))
}

/// `no_false`: no scalar in the payload is a known-false literal (vacuously
/// true for an empty list).
pub(crate) fn cond_no_false(data: &CombinedData, p: &Payload) -> bool {
    p.scalar_ids()
        .is_some_and(|s| s.iter().all(|&x| data.escalar(x).lit != Some(false)))
}

/// `no_error`: no scalar in the payload might produce a runtime error
/// (vacuously true for an empty list, and for the non-scalar payload kinds).
///
/// Reads the original `MirScalarExpr` rather than the precomputed `lit` fact:
/// an `error(..)` literal or an `error_if_null(..)` call folds to neither
/// `true` nor `false`, so `lit` is `None` for it and cannot distinguish an
/// error-bearing predicate from an ordinary opaque one.
pub(crate) fn cond_no_error(data: &CombinedData, p: &Payload) -> bool {
    p.scalar_ids()
        .is_some_and(|s| s.iter().all(|&x| !data.escalar(x).expr.might_error()))
}

/// `all_true`: every scalar constant-folds to the literal `true`.
pub(crate) fn cond_all_true(data: &CombinedData, p: &Payload) -> bool {
    p.scalar_ids()
        .is_some_and(|s| s.iter().all(|&x| data.escalar(x).lit == Some(true)))
}

impl EGraph {
    /// `non_negative`: the bound relation has non-negative multiplicities.
    pub(crate) fn cond_non_negative(&self, an: &Analyses, id: Id) -> bool {
        an.nn.get(&self.find(id)).copied().unwrap_or(false)
    }

    /// `monotonic`: the bound relation is insert-only. No rule currently uses
    /// this condition, but the analysis and check are kept for the physical
    /// monotonic rewrites the rule file is expected to grow.
    #[allow(dead_code)]
    pub(crate) fn cond_monotonic(&self, an: &Analyses, id: Id) -> bool {
        an.mono.get(&self.find(id)).copied().unwrap_or(false)
    }

    /// `is_unique_key`: the payload's columns form a unique key of the relation.
    pub(crate) fn cond_is_unique_key(&self, an: &Analyses, p: &Payload, id: Id) -> bool {
        let cand = p.columns(self.data()).into_iter().collect();
        an.keys
            .get(&self.find(id))
            .is_some_and(|ks| is_superkey(ks, &cand))
    }

    /// `produces_key`: the relation already produces an arrangement keyed by
    /// `key`, so an `ArrangeBy[key]` on top of it is redundant.
    ///
    /// True when the relation's e-class contains either an `ArrangeBy` with the
    /// same key, or a `Reduce` whose output is arranged by its group key (the
    /// requested key is exactly the leading group-key columns `[#0, .., #n-1]`).
    /// This is a physical-property fact (the collection is already materialized
    /// in this order), not a linearity claim, so it is sound for any operator.
    /// The `TopK` and indexed-`Get` cases are intentionally omitted for now.
    pub(crate) fn cond_produces_key(&self, _an: &Analyses, rel: Id, key: &Payload) -> bool {
        let Some(key) = key.scalar_ids() else {
            return false;
        };
        // Scalar ids are byte-identical for structurally-equal exprs (hash-cons),
        // so comparing the requested key's ids against a node's key ids is
        // equivalent to the pre-SP4a `EScalar`-content comparison.
        self.rel_class_nodes(rel).iter().any(|n| match n {
            // Already arranged by exactly this key.
            ENode::ArrangeBy { key: k2, .. } => k2.as_slice() == key,
            // A multi-key arrangement covers the requested key if any of its
            // key lists matches.
            ENode::ArrangeByMany { keys, .. } => keys.iter().any(|k2| k2.as_slice() == key),
            // A Reduce emits its group key as the leading output columns and
            // maintains its output arranged by them, so an arrangement on those
            // leading columns is redundant.
            ENode::Reduce { group_key, .. } => {
                key.len() == group_key.len()
                    && key
                        .iter()
                        .enumerate()
                        .all(|(i, &s)| self.data().escalar(s).is_col() == Some(i))
            }
            _ => false,
        })
    }

    /// `is_rel_empty`: the relation has a zero-row `Constant` in its e-class.
    pub(crate) fn cond_is_rel_empty(&self, id: Id) -> bool {
        self.rel_class_nodes(id)
            .iter()
            .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
    }

    /// `not_rel_empty`: the relation has no zero-row `Constant` in its e-class.
    pub(crate) fn cond_not_rel_empty(&self, id: Id) -> bool {
        !self
            .rel_class_nodes(id)
            .iter()
            .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
    }

    /// `join_is_cyclic`: the root e-class has a `Join` whose constraint
    /// hypergraph is cyclic. `root` is the class the rule's root matched.
    pub(crate) fn cond_join_is_cyclic(&self, root: Id) -> bool {
        self.rel_class_nodes(root).iter().any(|n| match n {
            ENode::Join {
                inputs,
                equivalences,
            } => {
                let arities: Vec<usize> = inputs.iter().map(|&c| self.arity(c)).collect();
                let equivalences = self.resolve_equivalences(equivalences);
                crate::eqsat::cost::join_is_cyclic(&arities, &equivalences)
            }
            _ => false,
        })
    }

    /// `has_three_or_more_inputs`: the root e-class contains a `Join` with at
    /// least three inputs. Guards `binarize_join_first` so the rule does not fire
    /// on an already-binary join, which would trigger an infinite rewrite loop.
    pub(crate) fn cond_has_three_or_more_inputs(&self, root: Id) -> bool {
        self.rel_class_nodes(root).iter().any(|n| match n {
            ENode::Join { inputs, .. } => inputs.len() >= 3,
            _ => false,
        })
    }

    /// `is_binary_join`: the root e-class contains a `Join` with exactly two
    /// inputs. Guards `commute_binary_join` so the commutativity rewrite fires
    /// only on binary joins (n-ary joins are handled by `binarize_join_first`
    /// first, and WcoJoin has its own separate rule).
    pub(crate) fn cond_is_binary_join(&self, root: Id) -> bool {
        self.rel_class_nodes(root).iter().any(|n| match n {
            ENode::Join { inputs, .. } => inputs.len() == 2,
            _ => false,
        })
    }

    /// `has_inner_equiv`: the equivalences payload contains at least one class
    /// all of whose columns are < `boundary`. Equivalently, `equivs_inner(p,
    /// boundary)` would be non-empty.
    ///
    /// Guards `binarize_join_first` so binarization fires only when the first
    /// two inputs share a fully-contained join equivalence. Without this guard,
    /// clique/star joins (where every equivalence class spans all inputs) produce
    /// an empty `equivs_inner`, turning the inner join into a cross product.
    pub(crate) fn cond_has_inner_equiv(&self, p: &Payload, boundary: i64) -> bool {
        let Payload::Equivalences(classes) = p else {
            return false;
        };
        // A non-negative boundary is guaranteed by the rule; a negative value
        // cannot match any column, so the condition is conservatively false.
        let Ok(b_u64) = u64::try_from(boundary) else {
            return false;
        };
        let b = usize::cast_from(b_u64);
        classes.iter().any(|class| {
            class
                .iter()
                .all(|&s| self.data().escalar(s).cols().iter().all(|&c| c < b))
        })
    }

    /// `reads_indexed_global`: the relation, after stripping column- and
    /// row-preserving wrappers (`Project`/`Filter`/`Map`/`ArrangeBy`/
    /// `ArrangeByMany`), reaches an `Opaque` global `Get` covered by a
    /// maintained index. Mirror of `cse::reads_index_backed_get`, but it walks
    /// e-classes (a class may hold several nodes) with a visited set to bound
    /// cyclic congruence. Gates the pull-up rule so it fires only where exposing
    /// the bare `Get` unlocks an existing index.
    pub(crate) fn cond_reads_indexed_global(&self, id: Id) -> bool {
        use mz_expr::MirRelationExpr;
        use std::collections::HashSet;
        let mut stack = vec![self.find(id)];
        let mut seen = HashSet::new();
        while let Some(cls) = stack.pop() {
            if !seen.insert(cls) {
                continue;
            }
            for n in self.rel_class_nodes(cls) {
                match n {
                    ENode::Opaque(m) => {
                        if let MirRelationExpr::Get {
                            id: mz_expr::Id::Global(gid),
                            ..
                        } = m
                        {
                            if self
                                .data()
                                .rel
                                .available
                                .get(gid)
                                .is_some_and(|keys| !keys.is_empty())
                            {
                                return true;
                            }
                        }
                    }
                    ENode::Project { input, .. }
                    | ENode::Filter { input, .. }
                    | ENode::Map { input, .. }
                    | ENode::ArrangeBy { input, .. }
                    | ENode::ArrangeByMany { input, .. } => stack.push(self.find(*input)),
                    _ => {}
                }
            }
        }
        false
    }
}

// --- saturation -----------------------------------------------------------

/// Per-class analysis results computed once per saturation round and consulted
/// by analysis-backed side conditions.
#[derive(Default)]
pub(crate) struct Analyses {
    pub(crate) nn: BTreeMap<Id, bool>,
    pub(crate) keys: BTreeMap<Id, KeySet>,
    // Read only by `cond_monotonic`, which no rule uses yet; kept for the
    // monotonic physical rewrites the rule file is expected to grow.
    #[allow(dead_code)]
    pub(crate) mono: BTreeMap<Id, bool>,
    // Consumed by Cond::ScalarEquiv (Task 2); unread for now.
    #[allow(dead_code)]
    pub(crate) cc: BTreeMap<Id, ConstCols>,
}

impl EGraph {
    /// Apply all `rules` everywhere they match, to a fixpoint (or until
    /// `max_iters` is reached). This is equality saturation; it never removes
    /// information, so it cannot get stuck in a local minimum.
    ///
    /// Each rule's matching, side-condition checking, and right-hand-side
    /// instantiation are compiled functions generated at build time from the
    /// rule file (see [`crate::eqsat::rules`]); this loop only drives them.
    pub fn saturate(
        &mut self,
        rules: &CompiledRuleSet,
        max_iters: usize,
        locals: &LocalFacts,
    ) -> usize {
        let compiled = rules.rules();
        // Recompute an analysis each round only when some active rule reads it.
        // The unread ones are otherwise pure per-round waste.
        let needs = rules.needed_analyses();

        // Per-rule backoff state: the iteration index up to which a rule is
        // banned, and its current ban length (doubles on each re-offense). A
        // rule is banned when its match enumeration hits `MATCH_LIMIT`, so an
        // explosive rule is throttled while the rest keep firing.
        let mut banned_until = vec![0usize; compiled.len()];
        let mut ban_len = vec![INITIAL_BAN_LEN; compiled.len()];

        let mut iters = 0;
        for iter in 0..max_iters {
            iters += 1;
            self.rebuild();
            // Bound runaway e-graph growth: equality saturation can explode
            // combinatorially, and an unbounded e-graph costs seconds to
            // saturate and extract. Once the e-node count crosses the budget,
            // stop growing and extract from what we have (a sound, if
            // incomplete, saturation).
            let n_nodes: usize = self.classes.values().map(|ns| ns.len()).sum();
            if n_nodes > MAX_ENODES {
                break;
            }
            let index = self.rel_index();

            // Build the per-run analysis context once per round: an arity provider
            // plus the scalar-fact cache the analyses resolve `Id`→`EScalar`
            // through. The closure, `self.data()`, and the `run_analysis` calls
            // all borrow `self` immutably, which is allowed (shared borrows).
            let arity = |c: Id| self.arity(c);
            let arity_fn: &dyn Fn(Id) -> usize = &arity;
            let ctx = crate::eqsat::analysis::RelCtx {
                arity: arity_fn,
                data: self.data(),
            };

            // Phase 1 (read-only): collect every rewrite to apply. Compute each
            // analysis only when some active rule reads it; the rest are unread
            // per-round waste.
            let analyses = Analyses {
                nn: if needs.nonneg {
                    self.run_analysis(
                        &NonNeg {
                            locals: locals.nonneg.clone(),
                        },
                        ctx,
                    )
                } else {
                    BTreeMap::new()
                },
                keys: if needs.keys {
                    self.run_analysis(
                        &Keys {
                            locals: locals.keys.clone(),
                        },
                        ctx,
                    )
                } else {
                    BTreeMap::new()
                },
                mono: if needs.monotonic {
                    self.run_analysis(
                        &Monotonic {
                            locals: locals.monotonic.clone(),
                        },
                        ctx,
                    )
                } else {
                    BTreeMap::new()
                },
                // ConstantColumns has no rule consumer yet (no `ScalarEquiv`
                // condition exists), so computing it every round is pure waste.
                // Re-wire through `AnalysisNeeds` when a rule first reads it.
                cc: BTreeMap::new(),
            };
            let mut pending: Vec<(usize, EBindings)> = Vec::new();
            {
                // Phase 1 (read-only): build the immutable view once for the
                // round and collect all rule matches before any mutation.
                let scalar_index = crate::eqsat::egraph::combined::ScalarIndex::new();
                let view = crate::eqsat::egraph::view::BaseView {
                    eg: self,
                    index: &index,
                    scalar_index: &scalar_index,
                    an: &analyses,
                };
                for (qi, rule) in compiled.iter().enumerate() {
                    // Skip rules currently serving a ban.
                    if iter < banned_until[qi] {
                        continue;
                    }
                    // Enumerate at most `MATCH_LIMIT` matches. Asking for one
                    // extra lets the matcher report that it hit the cap
                    // (explosive) so we ban it for a growing number of
                    // iterations.
                    let (matches, hit_limit) = (rule.find)(&view, &analyses, MATCH_LIMIT + 1);
                    if hit_limit {
                        banned_until[qi] = iter + ban_len[qi];
                        ban_len[qi] = ban_len[qi].saturating_mul(2);
                    }
                    for b in matches.into_iter().take(MATCH_LIMIT) {
                        pending.push((qi, b));
                    }
                }
            } // view (and its immutable borrow of self) is dropped here

            // Phase 2 (mutate, compiled rule application): instantiate
            // right-hand sides and union.
            let mut changed = false;

            // The e-node budget is rechecked here because this phase can add many
            // new nodes in one pass. Stopping mid-pass when the budget is reached
            // is sound: already-applied rewrites are unioned, skipped ones are
            // conservatively omitted (same semantics as the outer MAX_ENODES guard).
            for (qi, b) in pending {
                if let Ok(new_id) = (compiled[qi].apply)(self, &b) {
                    if self.union(new_id, b.root) {
                        changed = true;
                    }
                }
                let n_nodes: usize = self.classes.values().map(|ns| ns.len()).sum();
                if n_nodes > MAX_ENODES {
                    break;
                }
            }

            if !changed {
                break;
            }
        }
        self.rebuild();
        iters
    }

    /// Arities of the bound relation metavariables.
    ///
    /// `b.rels` also carries scalar metavariables bound inside a scalar
    /// pattern (the grammar has no separate binding kind for them), so this
    /// uses `try_arity` and skips entries with no well-defined arity rather
    /// than `arity`'s panic. A template that queries a skipped name via
    /// `IxExpr::Arity` gets the generated `apply` function's "unbound
    /// relation" error, not a panic.
    pub(crate) fn binding_arities(&self, b: &EBindings) -> BTreeMap<&'static str, usize> {
        b.rels
            .iter()
            .filter_map(|(&n, &id)| self.try_arity(id).map(|a| (n, a)))
            .collect()
    }
}

/// Apply a `BTreeMap` reducer to `expr` using an **outermost-first** strategy:
/// try the whole-expression replacement to a fixpoint BEFORE recursing into
/// children. After any child changes, re-try the whole-expression replacement
/// at the parent to a fixpoint.
///
/// This is the eqsat-local counterpart to [`ExpressionReducer::reduce_expr`] in
/// `analysis/equivalences.rs`. The shared trait uses a children-first order that
/// short-circuits the parent `replace` call if any child is rewritten; that is
/// correct for the standalone equivalence-propagation pass but silently drops
/// composite rewrites (e.g. `Eq(#0,#1) → true` under `{#0=#1}`) in eqsat's
/// contextual reduction. This function is eqsat-local and must NOT be added to
/// the shared trait.
///
/// `max_col` is the canonicalization validity bound: only accept an individual
/// whole-expression replacement if every column reference in the result is
/// strictly less than `max_col`. If a replacement would produce an out-of-range
/// column, that specific step is skipped (the loop stops attempting whole-expr
/// replaces at this node) and the function falls through to child recursion
/// instead. This per-step guard ensures that a valid child rewrite is not lost
/// when the whole-expr rewrite would be out of range.
///
/// Child-traversal mirrors `reduce_child` exactly: `CallBinary`, `CallUnary`,
/// `CallVariadic`, `If` (then/els only — not cond).
fn reduce_outermost(
    reducer: &BTreeMap<MirScalarExpr, MirScalarExpr>,
    expr: &mut MirScalarExpr,
    max_col: usize,
) -> bool {
    let mut changed = false;
    // Phase 1: try whole-expr replacement to a fixpoint first, accepting each
    // step only if the result keeps all column references < max_col.
    loop {
        let Some(replacement) = reducer.get(expr) else {
            break;
        };
        if replacement == expr {
            break; // identity entry: guard against infinite loop
        }
        if replacement.support().into_iter().all(|c| c < max_col) {
            expr.clone_from(replacement);
            changed = true;
        } else {
            // Out-of-range: stop whole-expr replaces at this node; child
            // recursion below may still find valid sub-expression rewrites.
            break;
        }
    }
    // Phase 2: recurse into children (mirroring `reduce_child` arms exactly).
    let child_changed = match expr {
        MirScalarExpr::CallBinary { expr1, expr2, .. } => {
            let c1 = reduce_outermost(reducer, expr1, max_col);
            let c2 = reduce_outermost(reducer, expr2, max_col);
            c1 || c2
        }
        MirScalarExpr::CallUnary { expr: inner, .. } => reduce_outermost(reducer, inner, max_col),
        MirScalarExpr::CallVariadic { exprs, .. } => exprs
            .iter_mut()
            .fold(false, |acc, e| reduce_outermost(reducer, e, max_col) || acc),
        MirScalarExpr::If { cond: _, then, els } => {
            // Do not simplify `cond` (matches `reduce_child`'s comment and behavior).
            let c1 = reduce_outermost(reducer, then, max_col);
            let c2 = reduce_outermost(reducer, els, max_col);
            c1 || c2
        }
        _ => false,
    };
    if child_changed {
        changed = true;
        // Phase 3: after children changed, re-try the parent-level replacement
        // to a fixpoint (a composite rewrite may now apply), still guarded
        // per-step.
        loop {
            let Some(replacement) = reducer.get(expr) else {
                break;
            };
            if replacement == expr {
                break;
            }
            if replacement.support().into_iter().all(|c| c < max_col) {
                expr.clone_from(replacement);
            } else {
                break;
            }
        }
    }
    changed
}

/// Apply an equivalence-class reducer to a single `EScalar`, returning
/// `(changed, new_escalar)`. Rejects the rewrite (keeps the original) if the
/// result references any column index `>= max_col`.
///
/// The `max_col` guard enforces the canonicalization validity invariant: a
/// rewritten scalar is accepted only if every column reference it contains is
/// strictly less than the column index valid in the scalar's evaluation context
/// (e.g. a `Map` scalar at position `pos` may reference columns
/// `< input_arity + pos`, never its own output column). A rewrite that would
/// produce an out-of-range column reference is silently dropped (the original is
/// kept), which is always sound: fewer rewrites mean fewer canonicalizations,
/// never an incorrect plan.
///
/// Used by the colored-derivation pass ([`crate::eqsat::colored_derive`]) to
/// reduce Filter predicates / Map scalars to their contextual spellings.
pub(crate) fn reduce_escalar(
    escalar: &EScalar,
    reducer: &BTreeMap<MirScalarExpr, MirScalarExpr>,
    max_col: usize,
) -> (bool, EScalar) {
    let mut expr = escalar.expr.clone();
    let changed = reduce_outermost(reducer, &mut expr, max_col);
    if changed {
        // Reject the rewrite if it produces a column reference that is out
        // of range for the scalar's evaluation context (see invariant above).
        if expr.support().into_iter().all(|c| c < max_col) {
            // Re-derive `lit` from the rewritten expr (the canonical function the
            // cache requires) rather than clearing it. A rewrite can fold a
            // predicate to the literal `true`/`false`, which hash-conses to the
            // shared literal scalar class; a cleared `lit: None` would clobber
            // that class's genuine fact when re-interned. (`intern_scalar` also
            // re-derives `lit`, so this is belt-and-suspenders, but it keeps the
            // returned `EScalar` self-consistent.)
            let lit = EScalar::lit_of(&expr);
            (true, EScalar::new(expr, lit))
        } else {
            (false, escalar.clone())
        }
    } else {
        (false, escalar.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::{BinaryFunc, MirScalarExpr, func};

    use crate::eqsat::ir::EScalar;

    use super::{reduce_escalar, reduce_outermost};

    // --- helpers -------------------------------------------------------------

    fn col(i: usize) -> MirScalarExpr {
        MirScalarExpr::column(i)
    }

    fn lit_true() -> MirScalarExpr {
        MirScalarExpr::literal_true()
    }

    fn eq_expr(lhs: MirScalarExpr, rhs: MirScalarExpr) -> MirScalarExpr {
        lhs.call_binary(rhs, BinaryFunc::Eq(func::Eq))
    }

    // --- reduce_outermost unit tests -----------------------------------------

    /// The key regression: composite replacement `Eq(#0,#1) → true` plus
    /// child-level `#1 → #0`. With the old children-first strategy, `#1` is
    /// rewritten to `#0` first, then `Eq(#0,#0) → true` has no entry and the
    /// result is the wrong `Eq(#0,#0)`. With outermost-first, `Eq(#0,#1) → true`
    /// fires immediately before any child is touched.
    #[mz_ore::test]
    fn reduce_outermost_composite_replace_fires_before_children() {
        let mut reducer: BTreeMap<MirScalarExpr, MirScalarExpr> = BTreeMap::new();
        reducer.insert(eq_expr(col(0), col(1)), lit_true());
        reducer.insert(col(1), col(0));

        let mut expr = eq_expr(col(0), col(1));
        // max_col=usize::MAX: no column is out of range; guard does not interfere.
        let changed = reduce_outermost(&reducer, &mut expr, usize::MAX);

        assert!(changed, "reduction must report a change");
        assert_eq!(
            expr,
            lit_true(),
            "Eq(#0,#1) must fold to `true`, not Eq(#0,#0)"
        );
    }

    /// Bare column substitution still works: `#1 → #0` on a plain `#1`.
    /// This confirms the common path is not broken by the outermost-first change.
    #[mz_ore::test]
    fn reduce_outermost_plain_column_substitution() {
        let mut reducer: BTreeMap<MirScalarExpr, MirScalarExpr> = BTreeMap::new();
        reducer.insert(col(1), col(0));

        let mut expr = col(1);
        // max_col=usize::MAX: no column is out of range; guard does not interfere.
        let changed = reduce_outermost(&reducer, &mut expr, usize::MAX);

        assert!(changed, "column substitution must report a change");
        assert_eq!(expr, col(0), "#1 must be rewritten to #0");
    }

    /// An expression with no matching entry is unchanged.
    #[mz_ore::test]
    fn reduce_outermost_no_match_unchanged() {
        let reducer: BTreeMap<MirScalarExpr, MirScalarExpr> = BTreeMap::new();
        let mut expr = eq_expr(col(0), col(1));
        // max_col=usize::MAX: no column is out of range; guard does not interfere.
        let changed = reduce_outermost(&reducer, &mut expr, usize::MAX);
        assert!(!changed, "no-op reduction must return false");
        assert_eq!(expr, eq_expr(col(0), col(1)));
    }

    // --- reduce_escalar integration tests ------------------------------------

    /// Whole-expression fold: `Eq(#0,#1)` under reducer `{Eq(#0,#1)→true, #1→#0}`
    /// must produce `(true, EScalar{lit: Some(true)})`, not `Eq(#0,#0)`.
    #[mz_ore::test]
    fn reduce_escalar_tautology_folds_to_true() {
        let escalar = EScalar::plain(eq_expr(col(0), col(1)));
        let mut reducer: BTreeMap<MirScalarExpr, MirScalarExpr> = BTreeMap::new();
        reducer.insert(eq_expr(col(0), col(1)), lit_true());
        reducer.insert(col(1), col(0));

        let (changed, result) = reduce_escalar(&escalar, &reducer, 2);

        assert!(changed, "escalar must report a change");
        assert_eq!(result.expr, lit_true(), "must fold to `true`");
        assert_eq!(result.lit, Some(true), "lit field must be Some(true)");
    }

    /// Regression (T11b-2): `add(#1, 1)` with reducer `{add(#1,1)→#2, #1→#0}` and
    /// `max_col=2` must yield `add(#0, 1)` — the whole-expr rewrite to out-of-range
    /// `#2` must be rejected at the per-step level so the child rewrite `#1→#0` can
    /// still fire. Before the fix, `reduce_escalar` rejected the entire reduction
    /// (keeping the original `add(#1,1)`) when the whole-expr rewrite was out of range.
    #[mz_ore::test]
    fn reduce_escalar_per_step_max_col_guard_allows_child_rewrite() {
        use mz_repr::{Datum, ReprScalarType};

        let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let add_col1_1 = col(1).call_binary(one.clone(), BinaryFunc::AddInt64(func::AddInt64));
        let add_col0_1 = col(0).call_binary(one.clone(), BinaryFunc::AddInt64(func::AddInt64));

        let mut reducer: BTreeMap<MirScalarExpr, MirScalarExpr> = BTreeMap::new();
        // Whole-expr rewrite to out-of-range col(2) (col 2 is NOT < max_col=2).
        reducer.insert(add_col1_1.clone(), col(2));
        // Child rewrite: #1 → #0 (in range).
        reducer.insert(col(1), col(0));

        let escalar = EScalar::plain(add_col1_1);
        let (changed, result) = reduce_escalar(&escalar, &reducer, 2);

        assert!(
            changed,
            "child rewrite #1→#0 must be accepted and report a change"
        );
        assert_eq!(
            result.expr, add_col0_1,
            "add(#1,1) must become add(#0,1), not col(2) and not the original add(#1,1)"
        );
    }

    // --- EBindings func-metavar binding ---------------------------------------

    #[mz_ore::test]
    fn ebindings_binary_func_round_trips() {
        let mut b = super::EBindings::default();
        b.bind_binary_func("f", BinaryFunc::AddInt64(func::AddInt64));
        assert_eq!(b.binary_func("f"), BinaryFunc::AddInt64(func::AddInt64));
    }

    #[mz_ore::test]
    #[should_panic(expected = "unbound func metavariable `f`")]
    fn ebindings_binary_func_panics_when_unbound() {
        let b = super::EBindings::default();
        b.binary_func("f");
    }
}
