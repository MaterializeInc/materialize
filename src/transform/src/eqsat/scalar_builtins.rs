// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Builtin appliers: rule right-hand sides computed by Rust (`mz_expr`
//! evaluation or type inference), not expressible as a declarative template.
//! Each is named from a `.rewrite` rule's `=> name(args)` RHS and called by the
//! generated `apply_NAME_base`. Their Lean theorems are permanent `sorry`s.

use mz_expr::{Eval, EvalError, MirScalarExpr, UnaryFunc};
use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, RowArena};

use crate::eqsat::egraph::{CNode, EGraph, Id};
use crate::eqsat::scalar::node::SNode;
use crate::eqsat::scalar_extract;

/// The scalar e-nodes of `id`'s canonical class, filtering out `CNode::Rel`.
///
/// Mirrors `egraph::view::BaseView::scalar_class_nodes`, but `apply` holds
/// `&mut EGraph`, not the read-only view, so builtins read the graph's own
/// `nodes` directly instead of going through the view.
fn scalar_class_nodes(g: &EGraph, id: Id) -> Vec<SNode> {
    g.nodes(id)
        .into_iter()
        .filter_map(|n| match n {
            CNode::Scalar(s) => Some(s),
            CNode::Rel(_) => None,
        })
        .collect()
}

/// The literal analysis of scalar class `id`, if any.
///
/// Reads `data().scalar.analysis` directly (the mutable-graph counterpart of
/// `view::BaseView::scalar_lit_bool_or_null`, which only the read-only view
/// exposes). An `Err(e)` literal is a valid result here: it means the class is
/// a literal *evaluation error*, not that the analysis is absent.
fn scalar_literal(g: &EGraph, id: Id) -> Option<(Result<Row, EvalError>, ReprColumnType)> {
    g.data().scalar.analysis.get(&g.find(id))?.literal.clone()
}

/// Constant-fold a scalar class: if any scalar call node in `class` has all
/// literal-analysis children (and is not `mz_panic` and not a leaf), evaluate it
/// and return the folded-literal id. `Err` when no representative folds, which
/// the saturation loop treats as "rule did not fire".
///
/// Class-level, unlike the old per-node `const_fold`: `apply` receives the class
/// id, not the matched node. Sound by congruence: a foldable representative's
/// value is the value of the whole class, and unioning that literal into the
/// class is exactly what the old rule did per node. Panic exclusion and the
/// eval itself mirror `scalar/rules.rs::const_fold` verbatim, including
/// reproducing an `Err` child literal as data by reconstructing it as a
/// `MirScalarExpr::Literal(Err(e), ty)` and re-evaluating, rather than dropping
/// or special-casing it.
pub fn const_eval(g: &mut EGraph, class: Id) -> Result<Id, String> {
    // Find a foldable representative. Collect the needed owned data before any
    // mutation so no borrow of `g` is held across `g.add`.
    let folded = {
        let mut chosen: Option<MirScalarExpr> = None;
        'nodes: for node in scalar_class_nodes(g, class) {
            // Leaves have nothing to fold.
            match node {
                SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(..) => {
                    continue;
                }
                // Never evaluate `mz_panic` at optimization time: folding it
                // would abort the optimizer instead of surfacing at runtime.
                // `reduce`/unary.rs makes the same exclusion.
                SNode::CallUnary {
                    func: UnaryFunc::Panic(_),
                    ..
                } => continue,
                _ => {}
            }
            // Every child class must carry a literal analysis.
            let children = node.children();
            let mut child_exprs: Vec<MirScalarExpr> = Vec::with_capacity(children.len());
            for &c in &children {
                let Some((row, col_type)) = scalar_literal(g, c) else {
                    continue 'nodes;
                };
                child_exprs.push(MirScalarExpr::Literal(row, col_type));
            }
            // Reassemble the call with literal children (no Column refs, so
            // empty datums/col_types are sound), matching old const_fold.
            let call = match &node {
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
                SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(..) => {
                    unreachable!("leaves filtered above")
                }
            };
            let temp = RowArena::new();
            chosen = Some(MirScalarExpr::literal(
                call.eval(&[], &temp),
                call.typ(&[]).scalar_type,
            ));
            break;
        }
        chosen
    };
    let Some(MirScalarExpr::Literal(row, col_type)) = folded else {
        return Err("const_eval: no foldable representative".to_string());
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}

/// The result scalar type of the call node `node` in the combined graph.
///
/// The combined-graph analog of `scalar/rules.rs::call_scalar_type`: raise
/// each child to its cheapest `MirScalarExpr`, reassemble the call, and type
/// it against the stored `col_types`. The raised children may carry `Column`s,
/// so `col_types` is required to type them.
fn call_scalar_type(g: &EGraph, node: &SNode) -> ReprScalarType {
    let col_types = g.data().scalar.col_types.clone();
    let raised: Vec<MirScalarExpr> = node
        .children()
        .iter()
        .map(|&c| scalar_extract::raise(g, c))
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
    assembled.typ(&col_types).scalar_type
}

/// `could_error` of scalar class `id`.
///
/// The mutable-graph reader a builtin needs: `apply` holds `&mut EGraph`, not
/// the read-only view, so the view's `scalar_could_error` is not available
/// here. A class with no analysis entry (not yet reachable from any typed
/// root) conservatively reads as `false`, matching an absent literal in
/// `scalar_literal`.
fn scalar_could_error(g: &EGraph, id: Id) -> bool {
    g.data()
        .scalar
        .analysis
        .get(&g.find(id))
        .map(|a| a.could_error)
        .unwrap_or(false)
}

/// Whether scalar class `id` is a literal `null`. Mirrors
/// `scalar/rules.rs::is_literal_null`.
fn is_literal_null(g: &EGraph, id: Id) -> bool {
    matches!(scalar_literal(g, id), Some((Ok(row), _)) if row.unpack_first() == Datum::Null)
}

/// The `EvalError` when class `id` is a literal error, else `None`. Mirrors
/// `scalar/rules.rs::literal_err`.
fn literal_err(g: &EGraph, id: Id) -> Option<EvalError> {
    match scalar_literal(g, id)? {
        (Err(e), _) => Some(e),
        (Ok(_), _) => None,
    }
}

/// `if(err_cond, then, els) -> err_cond` when the condition class is a
/// literal error. The Rust RHS of the `if_err_cond` declarative rule.
///
/// Mirrors `scalar/rules.rs::if_err_cond` exactly: no `could_error` gate, since
/// `If` evaluates `cond` first and short-circuits on its error before `then`
/// or `els` are ever touched, so the rewrite is sound regardless of what the
/// branches are. The result type is the union of the two branch types; if the
/// union fails (incompatible scalar types, which should not occur in
/// well-typed input), the rule conservatively does not fire rather than
/// unwrap-panic, matching `reduce_if`'s `Err(err)` arm.
pub fn if_err_cond(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::If { cond, then, els } = node else {
            return None;
        };
        let err = literal_err(g, cond)?;
        Some((err, then, els))
    });
    let Some((err, then, els)) = target else {
        return Err("if_err_cond: no If node with a literal-error condition".to_string());
    };
    let then_ty = scalar_extract::raise(g, then).typ(&g.data().scalar.col_types);
    let els_ty = scalar_extract::raise(g, els).typ(&g.data().scalar.col_types);
    let result_ty = then_ty
        .union(&els_ty)
        .map_err(|e| format!("if_err_cond: incompatible branch types: {e}"))?;
    Ok(g.add(CNode::Scalar(SNode::Literal(Err(err), result_ty))))
}

/// `f(null, b) -> null` and `f(a, null) -> null` for a binary `f` that
/// propagates nulls, GATED on the other operand being error-free. The Rust RHS
/// of the `null_prop_binary` declarative rule.
///
/// Mirrors `scalar/rules.rs::null_prop_binary` exactly, including its
/// deliberate deviation from `reduce`: reduce's binary null-prop is ungated on
/// the other operand, but eval returns the OTHER operand's error, not null,
/// when that operand errors (`eval(AddInt64(null, 1/0))` is `Err`, not
/// `Null`). Rewriting `f(null, x)` to null when `x` can error would turn an
/// error into null, unsound in general and, in the e-graph, worse: it would
/// union a null class with an error-producing class. The gate blocks that.
pub fn null_prop_binary(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::CallBinary { func, expr1, expr2 } = &node else {
            return None;
        };
        if !func.propagates_nulls() {
            return None;
        }
        let other = if is_literal_null(g, *expr1) {
            *expr2
        } else if is_literal_null(g, *expr2) {
            *expr1
        } else {
            return None;
        };
        if scalar_could_error(g, other) {
            return None;
        }
        Some(node.clone())
    });
    let Some(node) = target else {
        return Err("null_prop_binary: no eligible CallBinary node".to_string());
    };
    let ty = call_scalar_type(g, &node);
    let MirScalarExpr::Literal(row, col_type) = MirScalarExpr::literal_null(ty) else {
        unreachable!("MirScalarExpr::literal_null always builds a Literal variant")
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}

/// `f(err_lit, b) -> err_lit` and `f(a, err_lit) -> err_lit` for a binary `f`,
/// GATED on the other operand being error-free. The Rust RHS of the
/// `err_prop_binary` declarative rule.
///
/// Mirrors `scalar/rules.rs::err_prop_binary` exactly, including its
/// deliberate deviation from `reduce`: reduce's binary err-prop is ungated on
/// the other operand, but eval is left-to-right via `?`, so
/// `eval(f(x, err_lit))` returns `x`'s error when `x` errors first, not the
/// literal error. Rewriting without a gate would substitute one error class
/// for another. The gate fires on a literal-error operand only when the OTHER
/// operand cannot error, so the literal error is provably the call's result
/// regardless of evaluation order. No `propagates_nulls` gate, matching
/// reduce's binary err-prop (unconditional on the function).
pub fn err_prop_binary(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::CallBinary { expr1, expr2, .. } = &node else {
            return None;
        };
        if let Some(err) = literal_err(g, *expr1) {
            if !scalar_could_error(g, *expr2) {
                return Some((err, node.clone()));
            }
        }
        if let Some(err) = literal_err(g, *expr2) {
            if !scalar_could_error(g, *expr1) {
                return Some((err, node.clone()));
            }
        }
        None
    });
    let Some((err, node)) = target else {
        return Err("err_prop_binary: no eligible CallBinary node".to_string());
    };
    let ty = call_scalar_type(g, &node);
    let MirScalarExpr::Literal(row, col_type) = MirScalarExpr::literal(Err(err), ty) else {
        unreachable!("MirScalarExpr::literal always builds a Literal variant")
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}

#[cfg(test)]
mod tests {
    use mz_expr::{BinaryFunc, EvalError};
    use mz_repr::{Datum, ReprScalarType, Row};

    use super::*;

    fn int_ty() -> ReprColumnType {
        ReprColumnType {
            scalar_type: ReprScalarType::Int64,
            nullable: false,
        }
    }

    fn lit_int(g: &mut EGraph, v: i64) -> Id {
        g.add(CNode::Scalar(SNode::Literal(
            Ok(Row::pack_slice(&[Datum::Int64(v)])),
            int_ty(),
        )))
    }

    fn add64() -> BinaryFunc {
        BinaryFunc::AddInt64(mz_expr::func::AddInt64)
    }

    fn div64() -> BinaryFunc {
        BinaryFunc::DivInt64(mz_expr::func::DivInt64)
    }

    /// `1 + 2` is all-literal, so `const_eval` folds it to the literal `3`.
    #[mz_ore::test]
    fn const_eval_folds_all_literal_call() {
        let mut g = EGraph::new();
        let lit1 = lit_int(&mut g, 1);
        let lit2 = lit_int(&mut g, 2);
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: add64(),
            expr1: lit1,
            expr2: lit2,
        }));

        let folded = const_eval(&mut g, call).expect("all-literal call must fold");
        let (row, _ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(
            row.expect("1 + 2 does not error").unpack_first(),
            Datum::Int64(3)
        );
    }

    /// `1 / 0` is also all-literal, but evaluates to a runtime error. The fold
    /// must reproduce that error AS a literal (error-as-data), not drop it or
    /// refuse to fire: this is the exact behavior `const_fold` had, and the
    /// highest-risk part of the port.
    #[mz_ore::test]
    fn const_eval_folds_division_by_zero_to_error_literal() {
        let mut g = EGraph::new();
        let lit1 = lit_int(&mut g, 1);
        let lit2 = lit_int(&mut g, 0);
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: div64(),
            expr1: lit1,
            expr2: lit2,
        }));

        let folded =
            const_eval(&mut g, call).expect("all-literal call must fold, even to an error");
        let (row, _ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(row, Err(EvalError::DivisionByZero));
    }

    /// A call with a non-literal (`Column`) child has no foldable
    /// representative, so `const_eval` must return `Err` rather than fire.
    #[mz_ore::test]
    fn const_eval_errs_when_a_child_is_not_literal() {
        let mut g = EGraph::new();
        let lit1 = lit_int(&mut g, 1);
        let col = g.add(CNode::Scalar(SNode::Column(
            0,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: add64(),
            expr1: lit1,
            expr2: col,
        }));

        assert!(
            const_eval(&mut g, call).is_err(),
            "a call with a non-literal child must not fold"
        );
    }

    fn bool_ty() -> ReprColumnType {
        ReprColumnType {
            scalar_type: ReprScalarType::Bool,
            nullable: false,
        }
    }

    fn null_int_ty() -> ReprColumnType {
        ReprColumnType {
            scalar_type: ReprScalarType::Int64,
            nullable: true,
        }
    }

    fn lit_bool(g: &mut EGraph, v: bool) -> Id {
        g.add(CNode::Scalar(SNode::Literal(
            Ok(Row::pack_slice(&[if v {
                Datum::True
            } else {
                Datum::False
            }])),
            bool_ty(),
        )))
    }

    fn lit_null_int(g: &mut EGraph) -> Id {
        g.add(CNode::Scalar(SNode::Literal(
            Ok(Row::pack_slice(&[Datum::Null])),
            null_int_ty(),
        )))
    }

    fn err_lit(g: &mut EGraph, err: EvalError, ty: ReprColumnType) -> Id {
        g.add(CNode::Scalar(SNode::Literal(Err(err), ty)))
    }

    fn col(g: &mut EGraph, index: usize) -> Id {
        g.add(CNode::Scalar(SNode::Column(
            index,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )))
    }

    /// A production `BinaryFunc` with `propagates_nulls() == false`: it takes
    /// a raw `Datum` operand (any value including `Null`) rather than a typed
    /// native Rust value, so the default `propagates_nulls` derivation (from
    /// the function's `Input` type) comes out false. Verified against the
    /// codebase's `#[sqlfunc(..., propagates_nulls = false)]` attribute on
    /// `array_remove`. Semantics (removing an element from an array) are
    /// irrelevant here; only the `propagates_nulls` value matters.
    fn array_remove_func() -> BinaryFunc {
        BinaryFunc::ArrayRemove(mz_expr::func::ArrayRemove)
    }

    // --- if_err_cond ---

    /// `If(err_cond, then, els)` folds to the `err_cond` error, typed as the
    /// union of the branch types. `then`/`els` are columns so the fold
    /// exercises `col_types`, the reason `if_err_cond` needs the e-graph's
    /// stored column types at all.
    #[mz_ore::test]
    fn if_err_cond_folds_if_with_literal_error_cond() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty(), int_ty()];
        // Stands in for a constant-folded `1 / 0`: `if_err_cond` only reads
        // the condition class's literal analysis, not its syntactic origin.
        let cond = err_lit(&mut g, EvalError::DivisionByZero, int_ty());
        let then = col(&mut g, 0);
        let els = col(&mut g, 1);
        let if_node = g.add(CNode::Scalar(SNode::If { cond, then, els }));

        let folded = if_err_cond(&mut g, if_node).expect("literal-error cond must fire");
        let (row, ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(row, Err(EvalError::DivisionByZero));
        assert_eq!(ty.scalar_type, ReprScalarType::Int64);
    }

    /// A literal-`true` (non-error) condition does not match `if_err_cond`'s
    /// target shape.
    #[mz_ore::test]
    fn if_err_cond_does_not_fire_when_cond_is_literal_true() {
        let mut g = EGraph::new();
        let cond = lit_bool(&mut g, true);
        let then = lit_int(&mut g, 1);
        let els = lit_int(&mut g, 2);
        let if_node = g.add(CNode::Scalar(SNode::If { cond, then, els }));

        assert!(
            if_err_cond(&mut g, if_node).is_err(),
            "a non-error literal condition must not fire"
        );
    }

    /// A non-literal (`Column`) condition carries no literal analysis, so
    /// `literal_err` returns `None` and the rule does not fire.
    #[mz_ore::test]
    fn if_err_cond_does_not_fire_when_cond_is_a_column() {
        let mut g = EGraph::new();
        let cond = col(&mut g, 0);
        let then = lit_int(&mut g, 1);
        let els = lit_int(&mut g, 2);
        let if_node = g.add(CNode::Scalar(SNode::If { cond, then, els }));

        assert!(
            if_err_cond(&mut g, if_node).is_err(),
            "a non-literal condition must not fire"
        );
    }

    /// A class with no `If` node at all has no target shape.
    #[mz_ore::test]
    fn if_err_cond_errs_on_inapplicable_shape() {
        let mut g = EGraph::new();
        let lit = lit_int(&mut g, 1);
        assert!(
            if_err_cond(&mut g, lit).is_err(),
            "a class with no If node must not fire"
        );
    }

    // --- null_prop_binary ---

    /// `AddInt64(null, c0)`: `c0` is a bare column, which never `could_error`,
    /// so the gate permits null-prop and the call collapses to a typed null.
    #[mz_ore::test]
    fn null_prop_binary_folds_null_operand_with_safe_other() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        let null = lit_null_int(&mut g);
        let other = col(&mut g, 0);
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: add64(),
            expr1: null,
            expr2: other,
        }));

        let folded = null_prop_binary(&mut g, call).expect("safe other operand must fire");
        let (row, ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(
            row.expect("null-prop result must not itself be an error")
                .unpack_first(),
            Datum::Null
        );
        assert_eq!(ty.scalar_type, ReprScalarType::Int64);
    }

    /// `AddInt64(null, 1 / c0)`: the other operand can error (`DivInt64`
    /// always `could_error`), so the gate BLOCKS null-prop. This is the proof
    /// that the gate prevents the unsound err->null rewrite: eval of the
    /// input at `c0 == 0` is `Err`, never `Null`.
    #[mz_ore::test]
    fn null_prop_binary_blocked_when_other_can_error() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        let null = lit_null_int(&mut g);
        let c0 = col(&mut g, 0);
        let one = lit_int(&mut g, 1);
        let dividing = g.add(CNode::Scalar(SNode::CallBinary {
            func: div64(),
            expr1: one,
            expr2: c0,
        }));
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: add64(),
            expr1: null,
            expr2: dividing,
        }));

        assert!(
            null_prop_binary(&mut g, call).is_err(),
            "null-prop must not fire when the other operand can error"
        );
    }

    /// `ArrayRemove(null, x)`: `ArrayRemove.propagates_nulls() == false`, so
    /// the function-level gate blocks null-prop before the could_error check
    /// is even reached.
    #[mz_ore::test]
    fn null_prop_binary_blocked_for_non_null_propagating_func() {
        let mut g = EGraph::new();
        assert!(
            !array_remove_func().propagates_nulls(),
            "ArrayRemove must not propagate nulls for this test to be meaningful"
        );
        let null = lit_null_int(&mut g);
        let other = col(&mut g, 0);
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: array_remove_func(),
            expr1: null,
            expr2: other,
        }));

        assert!(
            null_prop_binary(&mut g, call).is_err(),
            "a non-null-propagating func must not fire"
        );
    }

    /// A class with no `CallBinary` node at all has no target shape.
    #[mz_ore::test]
    fn null_prop_binary_errs_on_inapplicable_shape() {
        let mut g = EGraph::new();
        let col_id = col(&mut g, 0);
        assert!(
            null_prop_binary(&mut g, col_id).is_err(),
            "a class with no CallBinary node must not fire"
        );
    }

    // --- err_prop_binary ---

    /// `AddInt64(1 / 0, c0)`: `c0` is a bare column and never `could_error`,
    /// so the gate permits err-prop. The result must reproduce the EXACT
    /// error, not a different one.
    #[mz_ore::test]
    fn err_prop_binary_folds_error_operand_with_safe_other() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        let err = err_lit(&mut g, EvalError::DivisionByZero, int_ty());
        let other = col(&mut g, 0);
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: add64(),
            expr1: err,
            expr2: other,
        }));

        let folded = err_prop_binary(&mut g, call).expect("safe other operand must fire");
        let (row, ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(row, Err(EvalError::DivisionByZero));
        assert_eq!(ty.scalar_type, ReprScalarType::Int64);
    }

    /// `AddInt64(1 / 0, 1 / c0)`: the other operand can also error, so the
    /// gate BLOCKS err-prop (substituting the literal error for whichever
    /// operand actually errors first at eval time would be unsound).
    #[mz_ore::test]
    fn err_prop_binary_blocked_when_other_can_error() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        let err = err_lit(&mut g, EvalError::DivisionByZero, int_ty());
        let c0 = col(&mut g, 0);
        let one = lit_int(&mut g, 1);
        let other_dividing = g.add(CNode::Scalar(SNode::CallBinary {
            func: div64(),
            expr1: one,
            expr2: c0,
        }));
        let call = g.add(CNode::Scalar(SNode::CallBinary {
            func: add64(),
            expr1: err,
            expr2: other_dividing,
        }));

        assert!(
            err_prop_binary(&mut g, call).is_err(),
            "err-prop must not fire when the other operand can also error"
        );
    }

    /// Nested: the error operand is itself a folded error literal (the
    /// product of `const_eval` on an inner `1 / 0`, not hand-built), the same
    /// shape `if_err_cond`/`const_eval` would leave behind mid-saturation.
    #[mz_ore::test]
    fn err_prop_binary_folds_nested_folded_error_operand() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        let one = lit_int(&mut g, 1);
        let zero = lit_int(&mut g, 0);
        let inner_div = g.add(CNode::Scalar(SNode::CallBinary {
            func: div64(),
            expr1: one,
            expr2: zero,
        }));
        let folded_inner = const_eval(&mut g, inner_div).expect("all-literal inner div must fold");
        let other = col(&mut g, 0);
        let outer = g.add(CNode::Scalar(SNode::CallBinary {
            func: add64(),
            expr1: folded_inner,
            expr2: other,
        }));

        let folded = err_prop_binary(&mut g, outer).expect("nested folded error must fire");
        let (row, _ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(row, Err(EvalError::DivisionByZero));
    }

    /// A class with no `CallBinary` node at all has no target shape.
    #[mz_ore::test]
    fn err_prop_binary_errs_on_inapplicable_shape() {
        let mut g = EGraph::new();
        let lit = lit_int(&mut g, 1);
        assert!(
            err_prop_binary(&mut g, lit).is_err(),
            "a class with no CallBinary node must not fire"
        );
    }
}
