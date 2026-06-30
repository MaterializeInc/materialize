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

use crate::eqsat::scalar::egraph::Id;
use crate::eqsat::scalar::node::SNode;

/// Per-e-class analyses maintained by [`crate::eqsat::scalar::egraph::ScalarEGraph`].
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

#[cfg(test)]
mod tests {
    use mz_expr::{BinaryFunc, EvalError, UnmaterializableFunc};
    use mz_repr::{Datum, ReprScalarType as RST};

    use crate::eqsat::scalar::egraph::ScalarEGraph;
    use crate::eqsat::scalar::lower::lower;
    use mz_expr::MirScalarExpr;

    fn ok_lit_int(v: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(v), RST::Int64)
    }

    fn err_lit() -> MirScalarExpr {
        MirScalarExpr::literal(Err(EvalError::DivisionByZero), RST::Int64)
    }

    fn col(i: usize) -> MirScalarExpr {
        MirScalarExpr::column(i)
    }

    fn add64() -> BinaryFunc {
        BinaryFunc::AddInt64(mz_expr::func::AddInt64)
    }

    // BitAndInt64 is infallible: `a & b` on i64 never errors.
    fn bitand64() -> BinaryFunc {
        BinaryFunc::BitAndInt64(mz_expr::func::BitAndInt64)
    }

    fn mz_now() -> MirScalarExpr {
        MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
    }

    // -----------------------------------------------------------------------
    // Test 1: could_error false for a column and an Ok literal.
    // -----------------------------------------------------------------------

    #[mz_ore::test]
    fn test_could_error_false_column() {
        let mut eg = ScalarEGraph::new();
        let id = lower(&mut eg, &col(0));
        assert!(!eg.analysis(id).could_error, "column must not could_error");
    }

    #[mz_ore::test]
    fn test_could_error_false_ok_literal() {
        let mut eg = ScalarEGraph::new();
        let id = lower(&mut eg, &ok_lit_int(42));
        assert!(
            !eg.analysis(id).could_error,
            "Ok literal must not could_error"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: could_error true for an Err literal.
    // -----------------------------------------------------------------------

    #[mz_ore::test]
    fn test_could_error_err_literal() {
        let mut eg = ScalarEGraph::new();
        let id = lower(&mut eg, &err_lit());
        assert!(eg.analysis(id).could_error, "Err literal must could_error");
    }

    // -----------------------------------------------------------------------
    // Test 3: could_error true for CallUnmaterializable(MzNow).
    // -----------------------------------------------------------------------

    #[mz_ore::test]
    fn test_could_error_unmaterializable() {
        let mut eg = ScalarEGraph::new();
        let id = lower(&mut eg, &mz_now());
        assert!(
            eg.analysis(id).could_error,
            "CallUnmaterializable must could_error"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: could_error propagates through CallBinary.
    // -----------------------------------------------------------------------

    #[mz_ore::test]
    fn test_could_error_binary_err_child() {
        // err_lit / col(0) with a non-erroring func: err propagates from child.
        let mut eg = ScalarEGraph::new();
        let expr = err_lit().call_binary(col(0), bitand64());
        let id = lower(&mut eg, &expr);
        assert!(
            eg.analysis(id).could_error,
            "binary with Err child must could_error even if func is safe"
        );
    }

    #[mz_ore::test]
    fn test_could_error_binary_safe() {
        // col(0) & col(1) with a non-erroring func over safe children: false.
        let mut eg = ScalarEGraph::new();
        let expr = col(0).call_binary(col(1), bitand64());
        let id = lower(&mut eg, &expr);
        assert!(
            !eg.analysis(id).could_error,
            "safe binary func over safe children must not could_error"
        );
    }

    #[mz_ore::test]
    fn test_could_error_binary_erroring_func() {
        // col(0) + col(1): AddInt64 can overflow, so could_error == true.
        let mut eg = ScalarEGraph::new();
        assert!(
            add64().could_error(),
            "AddInt64 must be erroring (overflow)"
        );
        let expr = col(0).call_binary(col(1), add64());
        let id = lower(&mut eg, &expr);
        assert!(
            eg.analysis(id).could_error,
            "binary with erroring func must could_error"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: literal field.
    // -----------------------------------------------------------------------

    #[mz_ore::test]
    fn test_literal_some_for_literal_class() {
        let mut eg = ScalarEGraph::new();
        let id = lower(&mut eg, &ok_lit_int(7));
        let a = eg.analysis(id);
        assert!(a.literal.is_some(), "literal class must have Some literal");
        let (row, col_type) = a.literal.as_ref().unwrap();
        assert_eq!(col_type.scalar_type, RST::Int64, "literal type must match");
        let row = row.as_ref().unwrap();
        let v: i64 = row.unpack_first().unwrap_int64();
        assert_eq!(v, 7, "literal value must match");
    }

    #[mz_ore::test]
    fn test_literal_none_for_column() {
        let mut eg = ScalarEGraph::new();
        let id = lower(&mut eg, &col(3));
        assert!(
            eg.analysis(id).literal.is_none(),
            "column must not be a literal"
        );
    }

    #[mz_ore::test]
    fn test_literal_none_for_call() {
        let mut eg = ScalarEGraph::new();
        let id = lower(&mut eg, &col(0).call_binary(col(1), add64()));
        assert!(
            eg.analysis(id).literal.is_none(),
            "call must not be a literal"
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: merge after union -- conservative OR survives.
    // -----------------------------------------------------------------------

    #[mz_ore::test]
    fn test_merge_could_error_survives_union() {
        let mut eg = ScalarEGraph::new();
        // safe: col(0) & col(1)
        let safe_expr = col(0).call_binary(col(1), bitand64());
        let safe_id = lower(&mut eg, &safe_expr);
        assert!(!eg.analysis(safe_id).could_error);

        // erroring: col(0) + col(1) (AddInt64 can overflow)
        let err_expr = col(0).call_binary(col(1), add64());
        let err_id = lower(&mut eg, &err_expr);
        assert!(eg.analysis(err_id).could_error);

        // Union the two classes: the merged class must could_error.
        eg.union(safe_id, err_id);
        eg.rebuild();

        let merged = eg.find(safe_id);
        assert!(
            eg.analysis(merged).could_error,
            "merged class must could_error (conservative OR)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 7: merge preserves a literal after union.
    // -----------------------------------------------------------------------

    #[mz_ore::test]
    fn test_merge_preserves_literal() {
        let mut eg = ScalarEGraph::new();

        // Literal class.
        let lit_id = lower(&mut eg, &ok_lit_int(99));
        assert!(eg.analysis(lit_id).literal.is_some());

        // A second class that we union with the literal.
        let col_id = lower(&mut eg, &col(0));

        eg.union(lit_id, col_id);
        eg.rebuild();

        let merged = eg.find(lit_id);
        assert!(
            eg.analysis(merged).literal.is_some(),
            "literal must survive merge with a non-literal class"
        );
    }
}
