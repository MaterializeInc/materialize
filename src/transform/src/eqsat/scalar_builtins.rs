// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Builtin appliers: rule right-hand sides computed by Rust (`mz_expr`
//! evaluation or type inference), not expressible as a declarative template.
//! Each is named from a `.rewrite` rule's `=> name(args)` RHS and called by the
//! generated `apply_NAME_base`. Their Lean theorems are permanent `sorry`s.

use mz_expr::{Eval, EvalError, MirScalarExpr, UnaryFunc};
use mz_repr::{ReprColumnType, Row, RowArena};

use crate::eqsat::egraph::{CNode, EGraph, Id};
use crate::eqsat::scalar::node::SNode;

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
}
