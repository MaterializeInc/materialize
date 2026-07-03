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

/// `f(.., null, ..) -> null` for a variadic `f` that propagates nulls, GATED on
/// every non-null-literal operand being error-free. The Rust RHS of the
/// `null_prop_variadic` declarative rule.
///
/// Mirrors `scalar/rules.rs::null_prop_variadic` exactly, including its
/// deliberate deviation from `reduce`: reduce's variadic null-prop is ungated on
/// the other operands, but eval surfaces an operand's error over null
/// (`eval(makets(null, 1/0))` is `Err`, not `Null`). Rewriting `f(.., null, ..)`
/// to null when another operand can error would turn an error into null. The gate
/// blocks that. This rule never fires on `And`/`Or`, which do not propagate nulls.
pub fn null_prop_variadic(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::CallVariadic { func, exprs } = &node else {
            return None;
        };
        if !func.propagates_nulls() {
            return None;
        }
        if !exprs.iter().any(|&e| is_literal_null(g, e)) {
            return None;
        }
        // Every operand that is not itself a literal null must be error-free, else
        // that operand's error would surface instead of the propagated null.
        let other_can_error = exprs
            .iter()
            .any(|&e| !is_literal_null(g, e) && scalar_could_error(g, e));
        if other_can_error {
            return None;
        }
        Some(node.clone())
    });
    let Some(node) = target else {
        return Err("null_prop_variadic: no eligible CallVariadic node".to_string());
    };
    let ty = call_scalar_type(g, &node);
    let MirScalarExpr::Literal(row, col_type) = MirScalarExpr::literal_null(ty) else {
        unreachable!("MirScalarExpr::literal_null always builds a Literal variant")
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}

/// `f(.., err_lit, ..) -> err_lit` for a variadic `f` that propagates nulls,
/// GATED on every other operand being error-free. The Rust RHS of the
/// `err_prop_variadic` declarative rule.
///
/// Mirrors `scalar/rules.rs::err_prop_variadic` exactly. Takes the FIRST literal
/// error by iteration order. A second literal-error operand is excluded from the
/// "other" set (via `literal_err(..).is_none()`), so it does not block the fire.
/// That is sound: eval is left-to-right via `?`, so the first error is the one a
/// full evaluation surfaces, and `const_fold` agrees on the all-literal case. The
/// `propagates_nulls` gate mirrors reduce's variadic err-prop, so this never fires
/// on `And`/`Or`.
pub fn err_prop_variadic(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::CallVariadic { func, exprs } = &node else {
            return None;
        };
        if !func.propagates_nulls() {
            return None;
        }
        let err = exprs.iter().find_map(|&e| literal_err(g, e))?;
        // Every operand that is not itself a literal error must be error-free.
        let other_can_error = exprs
            .iter()
            .any(|&e| literal_err(g, e).is_none() && scalar_could_error(g, e));
        if other_can_error {
            return None;
        }
        Some((err, node.clone()))
    });
    let Some((err, node)) = target else {
        return Err("err_prop_variadic: no eligible CallVariadic node".to_string());
    };
    let ty = call_scalar_type(g, &node);
    let MirScalarExpr::Literal(row, col_type) = MirScalarExpr::literal(Err(err), ty) else {
        unreachable!("MirScalarExpr::literal always builds a Literal variant")
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}

/// `(a∧b)∨(a∧c) -> a∧(b∨c)` and the dual: undistribute a common factor out of an
/// AND/OR, residual-error gated. The Rust RHS of the `factor_and_or` builtin rule.
///
/// Mirrors `scalar/rules.rs::factor_and_or` exactly: full-intersection factoring
/// (the factor is the inner-set intersection common to EVERY branch), with the
/// non-empty-residual condition (an empty residual is the absorption case, left to
/// `absorb`) and the residual-error gate (every residual operand must be error-free;
/// the common factor MAY error and is deliberately exempt, the CLU-137 narrowing).
/// Non-destructive: builds the factored form for the caller to union in, so
/// extraction picks the cheaper form. `Err` only when no And/Or node in the class
/// factors at all.
///
/// EVERY eligible And/Or node in the class is tried, and every successful
/// factoring is unioned in. The standalone engine achieved this by invoking the
/// rule once per node. Trying only the first node (in hash order) would make
/// firing nondeterministic: a class can hold several And/Or nodes at once (a
/// `drop_unit` rewrite sits in the same class as its pre-drop form, and only the
/// dropped form factors), so a hash-order pick would fire or not depending on
/// iteration order, and the extracted plan would vary run to run. Unioning every
/// factoring leaves the same e-class regardless of node order, so extraction is
/// reproducible.
pub fn factor_and_or(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let candidates: Vec<(mz_expr::VariadicFunc, Vec<Id>)> = scalar_class_nodes(g, class)
        .into_iter()
        .filter_map(|node| {
            let SNode::CallVariadic { func, exprs } = node else {
                return None;
            };
            (is_and_or(&func) && exprs.len() >= 2).then_some((func, exprs))
        })
        .collect();

    let mut factored: Option<Id> = None;
    for (outer_func, outer_operands) in candidates {
        if let Some(id) = try_factor_one(g, outer_func, outer_operands) {
            match factored {
                None => factored = Some(id),
                Some(prev) => {
                    g.union(prev, id);
                }
            }
        }
    }
    factored.ok_or_else(|| "factor_and_or: no eligible factoring".to_string())
}

/// Undistribute one And/Or node `outer_func(outer_operands)`, returning the
/// factored form's class id. `None` if this node does not factor: no common
/// factor, an empty residual (the absorption case, left to `absorb`), or a
/// residual operand that can error. Mutates `g` only on success, since every
/// rejection precedes the first `add`.
fn try_factor_one(
    g: &mut EGraph,
    outer_func: mz_expr::VariadicFunc,
    outer_operands: Vec<Id>,
) -> Option<Id> {
    let inner_func = outer_func.switch_and_or();

    // Each outer operand's inner-operand set under `inner_func` (canonical, sorted,
    // unique), or a singleton `{find(operand)}` if it holds no such call.
    let branch_sets: Vec<Vec<Id>> = outer_operands
        .iter()
        .map(|&operand| {
            let canon = g.find(operand);
            for sibling in g.nodes(canon) {
                if let CNode::Scalar(SNode::CallVariadic { func, exprs }) = sibling {
                    if func == inner_func {
                        let mut ids: Vec<Id> = exprs.iter().map(|&e| g.find(e)).collect();
                        ids.sort();
                        ids.dedup();
                        return ids;
                    }
                }
            }
            vec![canon]
        })
        .collect();

    // The intersection common to every branch (built from branch 0, kept sorted/unique).
    let mut intersection = branch_sets[0].clone();
    for set in &branch_sets[1..] {
        intersection.retain(|id| set.contains(id));
    }
    if intersection.is_empty() {
        return None;
    }

    // Each branch's residual; an empty residual is the absorption case (deferred).
    let mut residuals: Vec<Vec<Id>> = Vec::with_capacity(branch_sets.len());
    for set in &branch_sets {
        let residual: Vec<Id> = set
            .iter()
            .copied()
            .filter(|id| !intersection.contains(id))
            .collect();
        if residual.is_empty() {
            return None;
        }
        residuals.push(residual);
    }

    // Residual-error gate: every residual operand must be provably error-free. The
    // common factor is intentionally exempt (never moved past a masking value).
    for residual in &residuals {
        if residual.iter().any(|&id| scalar_could_error(g, id)) {
            return None;
        }
    }

    // Build the factored form. Sort branch ids so the combination node is stable
    // across re-firing (hashcons keys on the operand vector).
    let mut branch_ids: Vec<Id> = residuals
        .into_iter()
        .map(|residual| {
            if residual.len() == 1 {
                residual[0]
            } else {
                g.add(CNode::Scalar(SNode::CallVariadic {
                    func: inner_func.clone(),
                    exprs: residual,
                }))
            }
        })
        .collect();
    branch_ids.sort();
    let residual_combination = g.add(CNode::Scalar(SNode::CallVariadic {
        func: outer_func,
        exprs: branch_ids,
    }));
    let mut factored_exprs = intersection;
    factored_exprs.push(residual_combination);
    Some(g.add(CNode::Scalar(SNode::CallVariadic {
        func: inner_func,
        exprs: factored_exprs,
    })))
}

/// An And or Or variadic (the connectives `factor_and_or` distributes over).
fn is_and_or(func: &mz_expr::VariadicFunc) -> bool {
    matches!(
        func,
        mz_expr::VariadicFunc::And(_) | mz_expr::VariadicFunc::Or(_)
    )
}

#[cfg(test)]
mod tests {
    use mz_expr::{BinaryFunc, EvalError, VariadicFunc};
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

    /// A `propagates_nulls` variadic over numeric inputs: `makets` takes five
    /// `i64` and one `f64`, all non-nullable, so `propagates_nulls()` is true.
    fn make_timestamp() -> VariadicFunc {
        VariadicFunc::MakeTimestamp(mz_expr::func::variadic::MakeTimestamp)
    }

    /// A production variadic with `propagates_nulls() == false`: `Coalesce`
    /// short-circuits on the first non-null operand rather than propagating a
    /// null operand, so the func-level gate must block null/err-prop on it.
    fn coalesce_func() -> VariadicFunc {
        VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce)
    }

    fn float_ty() -> ReprColumnType {
        ReprColumnType {
            scalar_type: ReprScalarType::Float64,
            nullable: false,
        }
    }

    fn lit_f64(g: &mut EGraph, v: f64) -> Id {
        g.add(CNode::Scalar(SNode::Literal(
            Ok(Row::pack_slice(&[Datum::Float64(
                ordered_float::OrderedFloat(v),
            )])),
            float_ty(),
        )))
    }

    fn variadic(g: &mut EGraph, func: VariadicFunc, exprs: Vec<Id>) -> Id {
        g.add(CNode::Scalar(SNode::CallVariadic { func, exprs }))
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

    // --- null_prop_variadic ---

    /// `makets(null, c0, 1, 0, 0, 0.0)`: the null year propagates, and every
    /// other operand (a bare column or a safe literal) never `could_error`, so
    /// the gate permits null-prop and the call collapses to a typed null.
    #[mz_ore::test]
    fn null_prop_variadic_folds_null_operand_with_safe_others() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        assert!(
            make_timestamp().propagates_nulls(),
            "makets must propagate nulls for this test to be meaningful"
        );
        let null = lit_null_int(&mut g);
        let c0 = col(&mut g, 0);
        let one = lit_int(&mut g, 1);
        let zero_a = lit_int(&mut g, 0);
        let zero_b = lit_int(&mut g, 0);
        let second = lit_f64(&mut g, 0.0);
        let call = variadic(
            &mut g,
            make_timestamp(),
            vec![null, c0, one, zero_a, zero_b, second],
        );

        let folded = null_prop_variadic(&mut g, call).expect("safe other operands must fire");
        let (row, _ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(
            row.expect("null-prop result must not itself be an error")
                .unpack_first(),
            Datum::Null
        );
    }

    /// `makets(null, 1 / c0, 1, 0, 0, 0.0)`: the `1 / c0` operand can error
    /// (`DivInt64` always `could_error`), so the gate BLOCKS null-prop. Eval of
    /// the input at `c0 == 0` is `Err`, never `Null`, so folding to null would be
    /// unsound.
    #[mz_ore::test]
    fn null_prop_variadic_blocked_when_other_can_error() {
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
        let month = lit_int(&mut g, 1);
        let hour = lit_int(&mut g, 0);
        let minute = lit_int(&mut g, 0);
        let second = lit_f64(&mut g, 0.0);
        let call = variadic(
            &mut g,
            make_timestamp(),
            vec![null, dividing, month, hour, minute, second],
        );

        assert!(
            null_prop_variadic(&mut g, call).is_err(),
            "null-prop must not fire when another operand can error"
        );
    }

    /// `Coalesce(null, c0)`: `Coalesce.propagates_nulls() == false`, so the
    /// func-level gate blocks null-prop before the could_error check is reached.
    #[mz_ore::test]
    fn null_prop_variadic_blocked_for_non_null_propagating_func() {
        let mut g = EGraph::new();
        assert!(
            !coalesce_func().propagates_nulls(),
            "Coalesce must not propagate nulls for this test to be meaningful"
        );
        let null = lit_null_int(&mut g);
        let c0 = col(&mut g, 0);
        let call = variadic(&mut g, coalesce_func(), vec![null, c0]);

        assert!(
            null_prop_variadic(&mut g, call).is_err(),
            "a non-null-propagating func must not fire"
        );
    }

    /// A class with no `CallVariadic` node at all has no target shape.
    #[mz_ore::test]
    fn null_prop_variadic_errs_on_inapplicable_shape() {
        let mut g = EGraph::new();
        let col_id = col(&mut g, 0);
        assert!(
            null_prop_variadic(&mut g, col_id).is_err(),
            "a class with no CallVariadic node must not fire"
        );
    }

    // --- err_prop_variadic ---

    /// `makets(1 / 0, c0, 1, 0, 0, 0.0)`: the literal-error year propagates, and
    /// every other operand is safe, so the gate permits err-prop. The result must
    /// reproduce the EXACT error.
    #[mz_ore::test]
    fn err_prop_variadic_folds_error_operand_with_safe_others() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        let err = err_lit(&mut g, EvalError::DivisionByZero, int_ty());
        let c0 = col(&mut g, 0);
        let one = lit_int(&mut g, 1);
        let zero_a = lit_int(&mut g, 0);
        let zero_b = lit_int(&mut g, 0);
        let second = lit_f64(&mut g, 0.0);
        let call = variadic(
            &mut g,
            make_timestamp(),
            vec![err, c0, one, zero_a, zero_b, second],
        );

        let folded = err_prop_variadic(&mut g, call).expect("safe other operands must fire");
        let (row, _ty) = scalar_literal(&g, folded).expect("result must be a literal class");
        assert_eq!(row, Err(EvalError::DivisionByZero));
    }

    /// `makets(1 / 0, 1 / c0, 1, 0, 0, 0.0)`: the `1 / c0` operand can also
    /// error, so the gate BLOCKS err-prop (substituting the literal error for
    /// whichever operand errors first at eval time would be unsound).
    #[mz_ore::test]
    fn err_prop_variadic_blocked_when_other_can_error() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![int_ty()];
        let err = err_lit(&mut g, EvalError::DivisionByZero, int_ty());
        let c0 = col(&mut g, 0);
        let one = lit_int(&mut g, 1);
        let dividing = g.add(CNode::Scalar(SNode::CallBinary {
            func: div64(),
            expr1: one,
            expr2: c0,
        }));
        let month = lit_int(&mut g, 1);
        let hour = lit_int(&mut g, 0);
        let minute = lit_int(&mut g, 0);
        let second = lit_f64(&mut g, 0.0);
        let call = variadic(
            &mut g,
            make_timestamp(),
            vec![err, dividing, month, hour, minute, second],
        );

        assert!(
            err_prop_variadic(&mut g, call).is_err(),
            "err-prop must not fire when another operand can also error"
        );
    }

    /// `Coalesce(1 / 0, c0)`: `Coalesce.propagates_nulls() == false`, so the
    /// func-level gate blocks err-prop even though a literal-error operand is
    /// present.
    #[mz_ore::test]
    fn err_prop_variadic_blocked_for_non_null_propagating_func() {
        let mut g = EGraph::new();
        assert!(
            !coalesce_func().propagates_nulls(),
            "Coalesce must not propagate nulls for this test to be meaningful"
        );
        let err = err_lit(&mut g, EvalError::DivisionByZero, int_ty());
        let c0 = col(&mut g, 0);
        let call = variadic(&mut g, coalesce_func(), vec![err, c0]);

        assert!(
            err_prop_variadic(&mut g, call).is_err(),
            "a non-null-propagating func must not fire"
        );
    }

    /// A class with no `CallVariadic` node at all has no target shape.
    #[mz_ore::test]
    fn err_prop_variadic_errs_on_inapplicable_shape() {
        let mut g = EGraph::new();
        let lit = lit_int(&mut g, 1);
        assert!(
            err_prop_variadic(&mut g, lit).is_err(),
            "a class with no CallVariadic node must not fire"
        );
    }

    // --- factor_and_or ---

    fn and_func() -> VariadicFunc {
        VariadicFunc::And(mz_expr::func::variadic::And)
    }

    fn or_func() -> VariadicFunc {
        VariadicFunc::Or(mz_expr::func::variadic::Or)
    }

    /// The `inner_func`-rooted operand ids of factored class `id`, asserting it
    /// holds exactly one such variadic node.
    fn variadic_operands(g: &EGraph, id: Id, want: &VariadicFunc) -> Vec<Id> {
        scalar_class_nodes(g, id)
            .into_iter()
            .find_map(|n| match n {
                SNode::CallVariadic { func, exprs } if &func == want => Some(exprs),
                _ => None,
            })
            .unwrap_or_else(|| panic!("class must hold a {want:?} node"))
    }

    /// `Or(And(c0,c1), And(c0,c2))` factors the common `c0` out to
    /// `And(c0, Or(c1,c2))`. The returned class is rooted at the inner
    /// connective (`And`) with the factor and the residual combination.
    #[mz_ore::test]
    fn factor_and_or_fires_on_or_of_ands() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![bool_ty(); 3];
        let c0 = col(&mut g, 0);
        let c1 = col(&mut g, 1);
        let c2 = col(&mut g, 2);
        let and01 = variadic(&mut g, and_func(), vec![c0, c1]);
        let and02 = variadic(&mut g, and_func(), vec![c0, c2]);
        let or = variadic(&mut g, or_func(), vec![and01, and02]);

        let factored = factor_and_or(&mut g, or).expect("common factor c0 must fire");

        let operands = variadic_operands(&g, factored, &and_func());
        assert_eq!(operands.len(), 2, "And(c0, Or(c1,c2)) has two operands");
        let c0c = g.find(c0);
        assert!(
            operands.contains(&c0c),
            "the common factor c0 is an operand"
        );
        let residual = operands
            .iter()
            .copied()
            .find(|&id| id != c0c)
            .expect("the residual combination is the other operand");
        let mut got = variadic_operands(&g, residual, &or_func());
        got.sort();
        let mut want = vec![g.find(c1), g.find(c2)];
        want.sort();
        assert_eq!(got, want, "residual is Or(c1,c2)");
    }

    /// The dual: `And(Or(c0,c1), Or(c0,c2))` factors to `Or(c0, And(c1,c2))`.
    #[mz_ore::test]
    fn factor_and_or_fires_on_and_of_ors() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![bool_ty(); 3];
        let c0 = col(&mut g, 0);
        let c1 = col(&mut g, 1);
        let c2 = col(&mut g, 2);
        let or01 = variadic(&mut g, or_func(), vec![c0, c1]);
        let or02 = variadic(&mut g, or_func(), vec![c0, c2]);
        let and = variadic(&mut g, and_func(), vec![or01, or02]);

        let factored = factor_and_or(&mut g, and).expect("common factor c0 must fire");

        let operands = variadic_operands(&g, factored, &or_func());
        assert_eq!(operands.len(), 2, "Or(c0, And(c1,c2)) has two operands");
        let c0c = g.find(c0);
        assert!(
            operands.contains(&c0c),
            "the common factor c0 is an operand"
        );
        let residual = operands
            .iter()
            .copied()
            .find(|&id| id != c0c)
            .expect("the residual combination is the other operand");
        let mut got = variadic_operands(&g, residual, &and_func());
        got.sort();
        let mut want = vec![g.find(c1), g.find(c2)];
        want.sort();
        assert_eq!(got, want, "residual is And(c1,c2)");
    }

    /// `Or(And(c0,c1), And(c2,c3))` has an empty inner-set intersection, so
    /// full-intersection factoring does not apply.
    #[mz_ore::test]
    fn factor_and_or_errs_on_no_common_factor() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![bool_ty(); 4];
        let c0 = col(&mut g, 0);
        let c1 = col(&mut g, 1);
        let c2 = col(&mut g, 2);
        let c3 = col(&mut g, 3);
        let and01 = variadic(&mut g, and_func(), vec![c0, c1]);
        let and23 = variadic(&mut g, and_func(), vec![c2, c3]);
        let or = variadic(&mut g, or_func(), vec![and01, and23]);

        assert!(
            factor_and_or(&mut g, or).is_err(),
            "no common factor must not fire"
        );
    }

    /// `Or(And(c0,c1), And(c0,c1,c2))`: the intersection `{c0,c1}` is the whole
    /// first branch, so its residual is empty. That is the absorption case,
    /// deferred to the absorption rule, not fired here.
    #[mz_ore::test]
    fn factor_and_or_errs_on_empty_residual_absorption() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![bool_ty(); 3];
        let c0 = col(&mut g, 0);
        let c1 = col(&mut g, 1);
        let c2 = col(&mut g, 2);
        let and01 = variadic(&mut g, and_func(), vec![c0, c1]);
        let and012 = variadic(&mut g, and_func(), vec![c0, c1, c2]);
        let or = variadic(&mut g, or_func(), vec![and01, and012]);

        assert!(
            factor_and_or(&mut g, or).is_err(),
            "an empty residual (absorption) must not fire"
        );
    }

    /// `Or(And(c0, 1/0::bool), And(c0, c2))`: the residual `1/0` of the first
    /// branch `could_error`, so the residual-error gate blocks factoring. The
    /// error literal is Bool-typed to match the connective's operand type.
    #[mz_ore::test]
    fn factor_and_or_errs_on_erroring_residual() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![bool_ty(); 3];
        let c0 = col(&mut g, 0);
        let c2 = col(&mut g, 2);
        let errb = err_lit(&mut g, EvalError::DivisionByZero, bool_ty());
        assert!(
            scalar_could_error(&g, errb),
            "the Bool error literal must could_error for this test to be meaningful"
        );
        let and_a = variadic(&mut g, and_func(), vec![c0, errb]);
        let and_b = variadic(&mut g, and_func(), vec![c0, c2]);
        let or = variadic(&mut g, or_func(), vec![and_a, and_b]);

        assert!(
            factor_and_or(&mut g, or).is_err(),
            "an erroring residual operand must block factoring"
        );
    }

    /// A bare column class holds no And/Or node, so there is no target shape.
    #[mz_ore::test]
    fn factor_and_or_errs_on_bare_column() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![bool_ty()];
        let c0 = col(&mut g, 0);
        assert!(
            factor_and_or(&mut g, c0).is_err(),
            "a class with no And/Or node must not fire"
        );
    }

    /// A single-branch `Or(c0)` has too few branches to find a common factor.
    #[mz_ore::test]
    fn factor_and_or_errs_on_single_branch() {
        let mut g = EGraph::new();
        g.data_mut().scalar.col_types = vec![bool_ty()];
        let c0 = col(&mut g, 0);
        let or = variadic(&mut g, or_func(), vec![c0]);
        assert!(
            factor_and_or(&mut g, or).is_err(),
            "a single-branch Or must not fire"
        );
    }
}
