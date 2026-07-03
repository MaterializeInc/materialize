// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Per-e-class analyses for the scalar e-graph.
//!
//! Each e-class carries a [`ClassAnalysis`] value maintained incrementally as
//! nodes are added and classes merge. The analyses are read by Phase 1 rewrite
//! rules as side-condition guards.

use std::collections::HashMap;

use mz_expr::EvalError;
use mz_repr::{ReprColumnType, Row};

use crate::eqsat::core::Id;
use crate::eqsat::scalar::node::SNode;

/// Per-e-class analyses maintained by `scalar_saturate::recompute_analysis` as a
/// monotone least-fixpoint over the combined e-graph's scalar classes.
///
/// Both fields are conservative upper bounds: they may over-approximate, but
/// must never under-approximate. Over-approximation is safe because every
/// consumer (Phase 2 absorption gates) uses the values only to BLOCK rewrites,
/// never to enable them.
///
/// NOTE: "arity" was considered as a third field but is ill-defined at the
/// e-class level: a class can contain nodes of different shapes (e.g. after
/// congruence merges). Rules that need operand count read it from the concrete
/// `SNode` they are matching.
#[derive(Debug)]
pub struct ClassAnalysis {
    /// Conservative upper bound: could SOME evaluation of a value in this class
    /// raise an `EvalError`? `true` means "possibly yes"; `false` means "no,
    /// provably not". Over-approximation is sound because consumers only use
    /// this to BLOCK a rewrite.
    pub could_error: bool,
    /// `Some` iff this class is a literal constant. Carries the same payload as
    /// `SNode::Literal` so a rule can inspect the constant without searching
    /// the node set. `None` for non-literal classes (columns, calls, etc.).
    pub literal: Option<(Result<Row, EvalError>, ReprColumnType)>,
}

/// Compute a node's contribution to `ClassAnalysis`, reading children's
/// analyses from `store` via `find`.
///
/// This must be called bottom-up: all child classes must already have entries
/// in `store`. Panics if a required child entry is absent.
pub fn make(
    node: &SNode,
    store: &HashMap<Id, ClassAnalysis>,
    find: &dyn Fn(Id) -> Id,
) -> ClassAnalysis {
    match node {
        SNode::Column(..) => ClassAnalysis {
            could_error: false,
            literal: None,
        },
        SNode::Literal(row, col_type) => ClassAnalysis {
            could_error: row.is_err(),
            literal: Some((row.clone(), col_type.clone())),
        },
        SNode::CallUnmaterializable(_) => ClassAnalysis {
            could_error: true,
            literal: None,
        },
        SNode::CallUnary { func, expr } => {
            let child = child_analysis(store, find, *expr);
            ClassAnalysis {
                could_error: func.could_error() || child.could_error,
                literal: None,
            }
        }
        SNode::CallBinary { func, expr1, expr2 } => {
            let c1 = child_analysis(store, find, *expr1);
            let c2 = child_analysis(store, find, *expr2);
            ClassAnalysis {
                could_error: func.could_error() || c1.could_error || c2.could_error,
                literal: None,
            }
        }
        SNode::CallVariadic { func, exprs } => {
            let child_error = exprs
                .iter()
                .any(|&e| child_analysis(store, find, e).could_error);
            ClassAnalysis {
                could_error: func.could_error() || child_error,
                literal: None,
            }
        }
        SNode::If { cond, then, els } => {
            let c = child_analysis(store, find, *cond);
            let t = child_analysis(store, find, *then);
            let e = child_analysis(store, find, *els);
            ClassAnalysis {
                could_error: c.could_error || t.could_error || e.could_error,
                literal: None,
            }
        }
    }
}

/// Merge two class analyses when their classes are unioned.
///
/// `could_error` is OR (conservative: a class errors if any node can).
/// `literal` is `a.or(b)` (take whichever side carries a literal; they are
/// proven equal by construction, so any one is correct).
pub fn merge(a: ClassAnalysis, b: ClassAnalysis) -> ClassAnalysis {
    // The two classes are proven equal by construction, so if both carry a
    // literal it must be the same one. Assert this invariant in debug builds:
    // a mismatch means an upstream union joined two classes that are not
    // actually equal, which would make `a.literal.or(b.literal)` silently pick
    // a wrong constant. Release behavior is unchanged.
    debug_assert!(
        match (&a.literal, &b.literal) {
            (Some(la), Some(lb)) => la == lb,
            _ => true,
        },
        "merging classes with conflicting literals: {:?} vs {:?}",
        a.literal,
        b.literal
    );
    ClassAnalysis {
        could_error: a.could_error || b.could_error,
        literal: a.literal.or(b.literal),
    }
}

/// Look up the canonical analysis for `id` in `store`, following parent
/// pointers via `find`. Panics if the class is not present (a bottom-up
/// invariant violation).
fn child_analysis<'s>(
    store: &'s HashMap<Id, ClassAnalysis>,
    find: &dyn Fn(Id) -> Id,
    id: Id,
) -> &'s ClassAnalysis {
    let rep = find(id);
    store
        .get(&rep)
        .expect("child class must have an analysis entry before its parent is computed")
}
