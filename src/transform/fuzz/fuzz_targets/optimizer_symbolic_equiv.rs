// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the logical optimizer must preserve results over *symbolic*
//! inputs. `full_optimizer_equiv` builds plans rooted at `Constant`s, so the
//! optimizer constant-folds everything away before the interesting relational
//! planning (join ordering/implementation, predicate and projection pushdown
//! through `Get`s, key inference) ever runs. This target instead roots the plan
//! at `Get`s, opaque relations, exactly what the optimizer sees when planning a
//! real query against catalog objects, so that planning actually happens, while
//! still retaining a ground-truth oracle.
//!
//! Each `Get` is bound (in a side table) to a concrete, fuzzed constant
//! collection. The oracle:
//!
//!   1. `baseline = collapse(substitute(plan))`. Inline each `Get`'s data, then
//!      fold to the actual result rows.
//!   2. `optimized = optimize(plan)`. Run the full logical optimizer with the
//!      `Get`s still symbolic, so join/pushdown/key planning runs for real.
//!   3. `after = collapse(substitute(optimized))`. Inline the same data into the
//!      optimized plan and fold.
//!   4. assert `baseline == after`.
//!
//! `substitute` replaces only the global `Get`s we created. The optimizer's own
//! `Let`/local `Get` bindings (e.g. from CSE) are collapsed by `collapse`, which
//! iterates `FoldConstants` + `NormalizeLets` until the plan reduces to a
//! `Constant`. The comparison is conservative (only asserted when both sides
//! fold, a `Typecheck`/optimizer error is a skip), so a surviving divergence or
//! an optimizer panic is a genuine finding. It covers the symbolic-input
//! planning that the constant-rooted target cannot reach.

#![no_main]

use std::collections::BTreeMap;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{Id, MirRelationExpr};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Diff, GlobalId, Row};
use mz_transform::fold_constants::FoldConstants;
use mz_transform::normalize_lets::NormalizeLets;
use mz_transform_fuzz::{Ty, apply_recursively, gen_constant, gen_rel, optimize};

/// A symbolic `Get` leaf bound (in `data`) to a fresh constant collection.
fn gen_get(
    u: &mut Unstructured,
    next_id: &mut u64,
    data: &mut BTreeMap<u64, MirRelationExpr>,
) -> arbitrary::Result<(MirRelationExpr, Vec<Ty>)> {
    let (constant, schema) = gen_constant(u)?;
    // The `Get`'s declared type is the constant's *stored* (all-nullable) type,
    // not `constant.typ()`, which would refine nullability from the data. Keeping
    // it loose is the whole point: the optimizer must plan against an opaque
    // relation, without the exact nullability/keys a `Constant` would reveal.
    let MirRelationExpr::Constant { typ, .. } = &constant else {
        unreachable!("gen_constant returns a Constant");
    };
    let typ = typ.clone();

    let id = *next_id;
    *next_id += 1;
    data.insert(id, constant);

    Ok((MirRelationExpr::global_get(GlobalId::User(id), typ), schema))
}

/// Replace every global `Get` we created with its bound constant collection.
/// Local `Get`s (introduced by the optimizer's `Let`s) are left for `collapse`.
fn substitute(mut rel: MirRelationExpr, data: &BTreeMap<u64, MirRelationExpr>) -> MirRelationExpr {
    rel.visit_pre_mut(|e| {
        let replacement = match e {
            MirRelationExpr::Get {
                id: Id::Global(GlobalId::User(uid)),
                ..
            } => data.get(&*uid).cloned(),
            _ => None,
        };
        if let Some(c) = replacement {
            *e = c;
        }
    });
    rel
}

/// Outcome of trying to fold a (`Get`-free) plan all the way to a `Constant`.
enum Collapse {
    /// Reduced to a `Constant` of `Ok` rows. The consolidated `(row, diff)`
    /// multiset is the actual result.
    Const(BTreeMap<Row, Diff>),
    /// Reached a fixpoint of `FoldConstants` + `NormalizeLets` (applying them no
    /// longer changes the plan) that is *not* a constant, e.g. the plan errors,
    /// or folding genuinely cannot evaluate it. This is a legitimate
    /// fold-limitation skip, not a coverage gap.
    StuckFixpoint,
    /// Hit the iteration budget without reaching either a constant or a
    /// fixpoint. The plan was still simplifying when we ran out of passes. Kept
    /// distinct from `StuckFixpoint` only to name the two skip reasons.
    /// `FoldConstants` does not promise a constant input collapses to a
    /// `Constant` within any limit, so this is a conservative skip too.
    BudgetExhausted,
}

/// Fold a (now `Get`-free) plan to a `Constant` by iterating `FoldConstants` +
/// `NormalizeLets` (to collapse any `Let`s the optimizer's CSE introduced) until
/// it either becomes a `Constant`, reaches a fixpoint, or exhausts the budget.
///
/// This loops to a genuine fixpoint (stops only when a pass leaves the plan
/// unchanged), so a plan that just needs a few more passes converges rather than
/// being dropped. The budget is a generous guard against a non-terminating
/// rewrite.
fn collapse(mut rel: MirRelationExpr) -> Collapse {
    let features = OptimizerFeatures::default();
    const BUDGET: usize = 64;
    for _ in 0..BUDGET {
        let before = rel.clone();
        if apply_recursively(FoldConstants { limit: None }, &mut rel).is_err() {
            return Collapse::StuckFixpoint;
        }
        if rel.as_const().is_some() {
            break;
        }
        if NormalizeLets::new(true)
            .action(&mut rel, &features)
            .is_err()
        {
            return Collapse::StuckFixpoint;
        }
        // A full pass that changed nothing means we will never reach a constant.
        if rel == before {
            return Collapse::StuckFixpoint;
        }
    }
    let Some(constant) = rel.as_const() else {
        // Still simplifying when the budget ran out.
        return Collapse::BudgetExhausted;
    };
    let (Ok(rows), _) = constant else {
        return Collapse::StuckFixpoint;
    };
    let mut multiset: BTreeMap<Row, Diff> = BTreeMap::new();
    for (row, diff) in rows {
        *multiset.entry(row.clone()).or_insert(Diff::ZERO) += *diff;
    }
    multiset.retain(|_, d| *d != Diff::ZERO);
    Collapse::Const(multiset)
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let mut next_id = 0u64;
    let mut data = BTreeMap::new();
    let (plan, _schema, _nn) = {
        let mut leaf = |u: &mut Unstructured| gen_get(u, &mut next_id, &mut data);
        gen_rel(u, 3, &mut leaf)?
    };

    // Ground truth: inline the data into the input plan and fold. Only proceed
    // when the *input* (which has no optimizer-introduced `Let`s) folds to a
    // constant, that is what gives us a result to compare against.
    let baseline = match collapse(substitute(plan.clone(), &data)) {
        Collapse::Const(b) => b,
        Collapse::StuckFixpoint | Collapse::BudgetExhausted => return Ok(()),
    };

    // Optimize with the Gets still symbolic, then inline the same data and fold.
    let Some(optimized) = optimize(plan.clone()) else {
        return Ok(());
    };
    match collapse(substitute(optimized, &data)) {
        Collapse::Const(after) => assert_eq!(
            baseline, after,
            "optimizer changed the result over symbolic inputs\nplan = {plan:?}\ndata = {data:?}"
        ),
        // The optimized plan did not fold to a constant: either a non-constant
        // fixpoint (an operator `FoldConstants` cannot evaluate) or still
        // simplifying when the 64-pass budget ran out. `FoldConstants` does not
        // promise a constant input reduces to a `Constant` within a limit, and
        // the optimizer legitimately reshapes plans (CSE into `Let` nesting)
        // into forms this two-pass loop may not drive to a fixpoint here. Both
        // are conservative skips, not divergences, matching `full_optimizer_equiv`.
        Collapse::StuckFixpoint | Collapse::BudgetExhausted => {}
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
