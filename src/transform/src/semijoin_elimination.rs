// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Identifies semijoins that have no effect and can be removed.
//!
//! These semijoins are joins that involve a collection with a unique key,
//! where the join equivalences ensure that the semijoin restricts some
//! other join input, and the restriction can be expressed by a list of
//! predicates that can be migrated to the other input.

use mz_expr::{MirRelationExpr, MirScalarExpr};

use crate::analysis::provenance::Provenance;
use crate::analysis::{Arity, DerivedView, UniqueKeys};

use crate::{TransformCtx, TransformError};

/// Removes semijoins that can be seen to have no effect.
#[derive(Debug, Default)]
pub struct SemijoinElimination;

impl crate::Transform for SemijoinElimination {
    #[mz_ore::instrument(
        target = "optimizer"
        level = "trace",
        fields(path.segment = "equivalence_propagation")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Perform bottom-up equivalence class analysis.
        use crate::analysis::DerivedBuilder;
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(UniqueKeys);
        builder.require(Provenance);
        let derived = builder.visit(relation);
        let derived = derived.as_view();

        self.apply(relation, derived);

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl SemijoinElimination {
    /// Traverse `expr`, consulting each `Join` stage and looking for semijoins that can be removed.
    pub fn apply(&self, expr: &mut MirRelationExpr, derived: DerivedView) {
        let mut todo = vec![(expr, derived)];
        while let Some((expr, view)) = todo.pop() {
            let mut projection = None;

            if let MirRelationExpr::Join {
                inputs,
                equivalences,
                implementation,
                ..
            } = expr
            {
                // Each input has the potential to be removed from the join under some conditions.
                // The conditions are meant to capture "a semijoin that restricts another input,
                // but whose effect can be described by predicates".
                // 1.   It has a unique key.
                //      This will ensure that it is not increasing the multiplicity of records.
                // 2.   There is another remaining input.
                // 3.   There exists a common `id` such that each has provenance from that `id`.
                // 4.   Under that provenance, the join equates two identical columns from `id`.
                //      This ensures that the semijoin is a *restriction* of the other input.
                // 5.   Under that provenance, the input has `filters.is_some()` and the filters
                //      can be expressed using the expressions againist the other input.
                //      This allows us to migrate the filter to the other input.

                let mut arities = view
                    .children_rev()
                    .map(|d| *d.value::<Arity>().unwrap())
                    .collect::<Vec<_>>();
                arities.reverse();

                let mut unique_keys = view
                    .children_rev()
                    .map(|d| d.value::<UniqueKeys>().unwrap())
                    .collect::<Vec<_>>();
                unique_keys.reverse();

                let mut provenance = view
                    .children_rev()
                    .map(|d| d.value::<Provenance>().unwrap())
                    .collect::<Vec<_>>();
                provenance.reverse();

                let mut removed_input = None;
                for input in (0..inputs.len()).filter(|i| !unique_keys[*i].is_empty()) {
                    let prior_arity1: usize = arities[..input].iter().cloned().sum();
                    for prov1 in provenance[input]
                        .iter()
                        .filter(|p| p.columns.iter().all(|c| c.is_some()))
                    {
                        for other in (0..inputs.len()).filter(|o| *o != input) {
                            let prior_arity2: usize = arities[..other].iter().cloned().sum();
                            for prov2 in provenance[other].iter().filter(|p| p.id == prov1.id) {
                                // If we equate *all* columns of `input` with corresponding columns of `other`, we are good to go.
                                let mut equated = vec![None; arities[input]];
                                for c1 in 0..arities[input] {
                                    equated[c1] = (0..arities[other])
                                        .filter(|c2| {
                                            prov1.columns[c1] == prov2.columns[*c2]
                                                && equivalences.iter().any(|class| {
                                                    class.contains(&MirScalarExpr::Column(
                                                        c1 + prior_arity1,
                                                    )) && class.contains(&MirScalarExpr::Column(
                                                        *c2 + prior_arity2,
                                                    ))
                                                })
                                        })
                                        .next()
                                }
                                // If we have a full set of equivalences, and the input has filters, we can remove the input.
                                if equated.iter().all(|c| c.is_some())
                                    && prov1.filters.is_some()
                                    && removed_input.is_none()
                                {
                                    removed_input = Some((
                                        input,
                                        other,
                                        equated,
                                        prov1.filters.clone().unwrap(),
                                    ));
                                }

                                // TODO:
                                // If we equate all key columns of `input` with corresponding columns of `other`, ...
                                // then we may need `prov1.filters` to be `Some(Vec::new())` in order for the missing
                                // equivalences to come out of functional dependencies from `prov1.id`.
                            }
                        }
                    }
                }

                // If we have found an input to remove, that is the next step!
                if let Some((input, other, equated, mut filters)) = removed_input {
                    let prior_arity1: usize = arities[..input].iter().cloned().sum();
                    let prior_arity2: usize = arities[..other].iter().cloned().sum();

                    // We'll need to do a few things:
                    // 1. Migrate the filters into the join equivalences.
                    // 2. Remove the input from the join (and update equivalences).
                    // 3. Add a projection to mirror the columns of the removed input.
                    for predicate in filters.iter_mut() {
                        // Rewrite the predicate to refer to the other input.
                        predicate.visit_pre_mut(|e| {
                            if let MirScalarExpr::Column(c) = e {
                                *c = *c + prior_arity2;
                            }
                        });
                    }
                    filters.push(MirScalarExpr::literal_true());
                    equivalences.push(filters);

                    // Remove the input from the join.
                    // Drop the input, but also update all equivalences to refer to the new column indices.
                    for class in equivalences.iter_mut() {
                        for expr in class.iter_mut() {
                            expr.visit_pre_mut(|e| {
                                if let MirScalarExpr::Column(c) = e {
                                    // First, replace references to `input` with references to `other`.
                                    if *c >= prior_arity1 && *c < prior_arity1 + arities[input] {
                                        *c = prior_arity2 + equated[*c - prior_arity1].unwrap();
                                    }
                                    // Second, if we reference something after `input` we need to adjust.
                                    if *c >= prior_arity1 {
                                        *c -= arities[input];
                                    }
                                }
                            });
                        }
                    }

                    inputs.remove(input);
                    *implementation = mz_expr::JoinImplementation::Unimplemented;
                    projection = Some(
                        (0..prior_arity1)
                            .chain((0..arities[input]).map(|c| equated[c].unwrap() + prior_arity2))
                            .chain(prior_arity1 + arities[input]..arities.iter().sum())
                            .map(|c| {
                                if c >= prior_arity1 + arities[input] {
                                    c - arities[input]
                                } else {
                                    c
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }

            // If we removed an input then we should apply the necessary projection,
            // but not continue recursively because our input is a bit out of sync now.
            // We could think harder to continue recursively, but we can also wait for
            // the next time we hit this analysis, if we are hoping to trim more joins.
            if let Some(projection) = projection {
                *expr = expr.take_dangerous().project(projection);
            } else {
                todo.extend(expr.children_mut().rev().zip(view.children_rev()));
            }
        }
    }
}
