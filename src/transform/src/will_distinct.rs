// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Propagates expression equivalence from leaves to root, and back down again.
//!
//! Expression equivalences are `MirScalarExpr` replacements by simpler expressions.
//! These equivalences derive from
//!   Filters:  predicates must evaluate to `Datum::True`.
//!   Maps:     new columns equal the expressions that define them.
//!   Joins:    equated columns must be equal.
//!   Others:   lots of other predicates we might learn (range constraints on aggregates; non-negativity)
//!
//! From leaf to root the equivalences are *enforced*, and communicate that the expression will not produce rows that do not satisfy the equivalence.
//! From root to leaf the equivalences are *advised*, and communicate that the expression may discard any outputs that do not satisfy the equivalence.
//!
//! Importantly, in descent the operator *may not* assume any equivalence filtering will be applied to its results.
//! It cannot therefore produce rows it would otherwise not, even rows that do not satisfy the equivalence.
//! Operators *may* introduce filtering in descent, and they must do so to take further advantage of the equivalences.
//!
//! The subtlety is due to the expressions themselves causing the equivalences, and allowing more rows may invalidate equivalences.
//! For example, we might learn that `Column(7)` equals `Literal(3)`, but must refrain from introducing that substitution in descent,
//! because it is possible that the equivalence derives from restrictions in the expression we are visiting. Were we certain that the
//! equivalence was independent of the expression (e.g. through a more nuanced expression traversal) we could imaging relaxing this.

use mz_expr::{MirRelationExpr, MirScalarExpr};

use crate::analysis::{DerivedView, NonNegative};

use crate::{TransformCtx, TransformError};

/// Pushes down the information that a collection will ultimately be subjected to a `Distinct` on specific columns.
///
/// This intends to recognize idiomatic stacks of `UNION` from SQL, which look like `Distinct` over `Union`, potentially
/// over other `Distinct` expressions.
#[derive(Debug, Default)]
pub struct WillDistinct;

impl crate::Transform for WillDistinct {
    #[mz_ore::instrument(
        target = "optimizer"
        level = "trace",
        fields(path.segment = "will_distinct")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Perform bottom-up equivalence class analysis.
        use crate::analysis::DerivedBuilder;
        let mut builder = DerivedBuilder::default();
        builder.require::<NonNegative>();
        let derived = builder.visit(relation);
        let derived = derived.as_view();

        self.apply(relation, derived);

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl WillDistinct {
    fn apply(&self, expr: &mut MirRelationExpr, derived: DerivedView) {
        // Record a list of expressions paired with optional "will distinct by" columns.
        let mut todo = vec![(expr, derived, None::<&[MirScalarExpr]>)];
        while let Some((expr, derived, distinct_by)) = todo.pop() {
            // If we find a `Distinct` expression in the shadow of another `Distinct` that will apply to its key columns,
            // we can remove this `Distinct` operator as the distinctness will be enforced by the other expression.
            if let (
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    ..
                },
                Some(columns),
            ) = (&*expr, distinct_by)
            {
                if aggregates.is_empty()
                    && columns
                        .iter()
                        .enumerate()
                        .all(|(c, e)| e == &MirScalarExpr::Column(c))
                    && columns.len() == group_key.len()
                {
                    let arity = input.arity();
                    *expr = input
                        .clone()
                        .map(group_key.clone())
                        .project((arity..arity + group_key.len()).collect::<Vec<_>>());
                }
            } else {
                match (expr, distinct_by) {
                    // A distinct expression whose keys are only column references can be communicated onward.
                    (
                        MirRelationExpr::Reduce {
                            input,
                            group_key,
                            aggregates,
                            ..
                        },
                        _,
                    ) => {
                        if aggregates.is_empty()
                            && group_key.iter().all(|e| {
                                if let MirScalarExpr::Column(_) = e {
                                    true
                                } else {
                                    false
                                }
                            })
                        {
                            todo.push((input, derived.last_child(), Some(&*group_key)));
                        } else {
                            todo.push((input, derived.last_child(), None))
                        }
                    }
                    // If all inputs to the union are non-negative, any distinct enforced above the expression can be
                    // communicated on to each input.
                    (MirRelationExpr::Union { base, inputs }, exprs) => {
                        if derived
                            .children_rev()
                            .all(|v| *v.value::<NonNegative>().unwrap())
                        {
                            let children_rev = inputs.iter_mut().rev().chain(Some(&mut **base));
                            todo.extend(
                                children_rev
                                    .zip(derived.children_rev())
                                    .map(|(x, y)| (x, y, exprs.clone())),
                            );
                        } else {
                            let children_rev = inputs.iter_mut().rev().chain(Some(&mut **base));
                            todo.extend(
                                children_rev
                                    .zip(derived.children_rev())
                                    .map(|(x, y)| (x, y, None)),
                            );
                        }
                    }
                    (x, _) => {
                        todo.extend(
                            x.children_mut()
                                .rev()
                                .zip(derived.children_rev())
                                .map(|(x, y)| (x, y, None)),
                        );
                    }
                }
            }
        }
    }
}
