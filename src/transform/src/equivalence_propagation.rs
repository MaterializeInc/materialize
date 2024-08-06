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

use std::collections::BTreeMap;

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_repr::Datum;

use crate::analysis::equivalences::{EquivalenceClasses, Equivalences};
use crate::analysis::{Arity, DerivedView, RelationType};

use crate::{TransformCtx, TransformError};

/// Pulls up and pushes down predicate information represented as equivalences
#[derive(Debug, Default)]
pub struct EquivalencePropagation;

impl crate::Transform for EquivalencePropagation {
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
        builder.require(Equivalences);
        let derived = builder.visit(relation);
        let derived = derived.as_view();

        let mut get_equivalences = BTreeMap::default();
        self.apply(
            relation,
            derived,
            EquivalenceClasses::default(),
            &mut get_equivalences,
        );

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl EquivalencePropagation {
    /// Provides the opportunity to mutate `relation` in response to equivalences enforced by others.
    ///
    /// Provides the opportunity to mutate `relation` in response to equivalences enforced by their children,
    /// as presented in `derived`, and equivalences enforced of their output (by their ancestors), as presented
    /// in `outer_equivalences` and `get_equivalences`.
    ///
    /// The mutations should never invalidate an equivalence the operator has been reported as providing, as that
    /// information may have already been acted upon by others.
    ///
    /// The `expr_index` argument must equal `expr`s position in post-order, so that it can be used as a reference
    /// into `derived`. The argument can be used with the `SubtreeSize` analysis to determine the range of values
    /// associated with `expr`.
    ///
    /// After the call, `get_equivalences` will be populated with certainly equivalences that will be certainly
    /// enforced for all uses of each identifier. The information can be harvested and propagated to the definitions
    /// of those identifiers.
    pub fn apply(
        &self,
        expr: &mut MirRelationExpr,
        derived: DerivedView,
        mut outer_equivalences: EquivalenceClasses,
        get_equivalences: &mut BTreeMap<Id, EquivalenceClasses>,
    ) {
        // TODO: The top-down traversal can be coded as a worklist, with arguments tupled and enqueued.
        // This has the potential to do a lot more cloning (of `outer_equivalences`), and some care is needed
        // for `get_equivalences` which would be scoped to the whole method rather than tupled and enqueued.

        let expr_type = derived
            .value::<RelationType>()
            .expect("RelationType required");
        let expr_equivalences = derived
            .value::<Equivalences>()
            .expect("Equivalences required");

        // `None` analysis values indicate collections that can be pruned.
        let expr_equivalences = if let Some(e) = expr_equivalences {
            e
        } else {
            expr.take_safely();
            return;
        };

        // Optimize `outer_equivalences` in the context of `expr_type`.
        // If it ends up unsatisfiable, we can replace `expr` with an empty constant of the same relation type.
        for class in outer_equivalences.classes.iter_mut() {
            for expr in class.iter_mut() {
                expr_equivalences.reduce_expr(expr);
            }
        }

        outer_equivalences.minimize(expr_type);
        if outer_equivalences.unsatisfiable() {
            expr.take_safely();
            return;
        }

        match expr {
            MirRelationExpr::Constant { rows, typ: _ } => {
                if let Ok(rows) = rows {
                    let mut datum_vec = mz_repr::DatumVec::new();
                    // Delete any rows that violate the equivalences.
                    // Do not delete rows that produce errors, as they are semantically important.
                    rows.retain(|(row, _count)| {
                        let temp_storage = mz_repr::RowArena::new();
                        let datums = datum_vec.borrow_with(row);
                        outer_equivalences.classes.iter().all(|class| {
                            // Any subset of `Ok` results must equate, or we can drop the row.
                            let mut oks = class
                                .iter()
                                .filter_map(|e| e.eval(&datums[..], &temp_storage).ok());
                            if let Some(e1) = oks.next() {
                                oks.all(|e2| e1 == e2)
                            } else {
                                true
                            }
                        })
                    });
                }
            }
            MirRelationExpr::Get { id, .. } => {
                // Install and intersect with other equivalences from other `Get` sites.
                // These will be read out by the corresponding `Let` binding's `value`.
                if let Some(equivs) = get_equivalences.get_mut(id) {
                    *equivs = equivs.union(&outer_equivalences);
                } else {
                    get_equivalences.insert(*id, outer_equivalences);
                }
            }
            MirRelationExpr::Let { id, .. } => {
                let id = *id;
                // Traverse `body` first to assemble equivalences to push to `value`.
                // Descend without a key for `id`, treating the absence as the identity for union.
                // `Get` nodes with identifier `id` will populate the equivalence classes with the intersection of their guarantees.
                let mut children_rev = expr.children_mut().rev().zip(derived.children_rev());

                let body = children_rev.next().unwrap();
                let value = children_rev.next().unwrap();

                self.apply(body.0, body.1, outer_equivalences.clone(), get_equivalences);

                // We expect to find `id` in `get_equivalences`, as otherwise the binding is
                // not referenced and can be removed.
                if let Some(equivalences) = get_equivalences.get(&Id::Local(id)) {
                    self.apply(value.0, value.1, equivalences.clone(), get_equivalences);
                }
            }
            MirRelationExpr::LetRec { .. } => {
                let mut child_iter = expr.children_mut().rev().zip(derived.children_rev());
                // Continue in `body` with the outer equivalences.
                let (body, view) = child_iter.next().unwrap();
                self.apply(body, view, outer_equivalences, get_equivalences);
                // Continue recursively, but without the outer equivalences supplied to `body`.
                for (child, view) in child_iter {
                    self.apply(child, view, EquivalenceClasses::default(), get_equivalences);
                }
            }
            MirRelationExpr::Project { input, outputs } => {
                // Transform `outer_equivalences` to one relevant for `input`.
                outer_equivalences.permute(outputs);
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                );
            }
            MirRelationExpr::Map { input, scalars } => {
                // Optimize `scalars` with respect to input equivalences.
                let input_equivalences = derived
                    .last_child()
                    .value::<Equivalences>()
                    .expect("Equivalences required");

                if let Some(input_equivalences) = input_equivalences {
                    for expr in scalars.iter_mut() {
                        input_equivalences.reduce_expr(expr);
                    }
                    let input_arity = *derived
                        .last_child()
                        .value::<Arity>()
                        .expect("Arity required");
                    outer_equivalences.project(0..input_arity);
                    self.apply(
                        input,
                        derived.last_child(),
                        outer_equivalences,
                        get_equivalences,
                    );
                }
            }
            MirRelationExpr::FlatMap { input, exprs, .. } => {
                // Transform `exprs` by guarantees from `input` *and* from `outer`???
                let input_equivalences = derived
                    .last_child()
                    .value::<Equivalences>()
                    .expect("Equivalences required");

                if let Some(input_equivalences) = input_equivalences {
                    for expr in exprs.iter_mut() {
                        input_equivalences.reduce_expr(expr);
                    }
                    let input_arity = *derived
                        .last_child()
                        .value::<Arity>()
                        .expect("Arity required");
                    outer_equivalences.project(0..input_arity);
                    self.apply(
                        input,
                        derived.last_child(),
                        outer_equivalences,
                        get_equivalences,
                    );
                }
            }
            MirRelationExpr::Filter { input, predicates } => {
                // Transform `predicates` by guarantees from `input` *and* from `outer`???
                // If we reduce based on `input` guarantees, we won't be able to push those
                // constraints down into input, which may be fine but is worth considering.
                let input_equivalences = derived
                    .last_child()
                    .value::<Equivalences>()
                    .expect("Equivalences required");
                if let Some(input_equivalences) = input_equivalences {
                    let input_types = derived
                        .last_child()
                        .value::<RelationType>()
                        .expect("RelationType required");
                    for expr in predicates.iter_mut() {
                        input_equivalences.reduce_expr(expr);
                    }
                    // Incorporate `predicates` into `outer_equivalences`.
                    let mut class = predicates.clone();
                    class.push(MirScalarExpr::literal_ok(
                        Datum::True,
                        mz_repr::ScalarType::Bool,
                    ));
                    outer_equivalences.classes.push(class);
                    outer_equivalences.minimize(input_types);
                    self.apply(
                        input,
                        derived.last_child(),
                        outer_equivalences,
                        get_equivalences,
                    );
                }
            }

            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                // Certain equivalences are ensured by each of the inputs.
                // Other equivalences are imposed by parents of the expression.
                // We must not weaken the properties provided by the expression to its parents,
                // meaning we can optimize `equivalences` with respect to input guarantees,
                // but not with respect to `outer_equivalences`.

                // Each child can be presented with the integration of `join_equivalences`, `outer_equivalences`,
                // and each input equivalence *other than* their own, projected onto the input's columns.

                // Enumerate locations to find each child's analysis outputs.
                let mut children: Vec<_> = derived.children_rev().collect::<Vec<_>>();
                children.reverse();

                // Assemble the appended input types, for use in expression minimization.
                // Do not use `expr_types`, which may reflect nullability that does not hold for the inputs.
                let input_types = Some(
                    children
                        .iter()
                        .flat_map(|c| {
                            c.value::<RelationType>()
                                .expect("RelationType required")
                                .as_ref()
                                .unwrap()
                                .iter()
                                .cloned()
                        })
                        .collect::<Vec<_>>(),
                );

                // For each child, assemble its equivalences using join-relative column numbers.
                // Don't do anything with the children yet, as we'll want to revisit each with
                // this information at hand.
                let mut columns = 0;
                let mut input_equivalences = Vec::with_capacity(children.len());
                for child in children.iter() {
                    let child_arity = child.value::<Arity>().expect("Arity required");
                    let equivalences = child
                        .value::<Equivalences>()
                        .expect("Equivalences required")
                        .clone();

                    if let Some(mut equivalences) = equivalences {
                        let permutation = (columns..(columns + child_arity)).collect::<Vec<_>>();
                        equivalences.permute(&permutation);
                        equivalences.minimize(&input_types);
                        input_equivalences.push(equivalences);
                    }
                    columns += child_arity;
                }

                // Form the equivalences we will use to replace `equivalences`.
                let mut join_equivalences = EquivalenceClasses::default();
                join_equivalences
                    .classes
                    .extend(equivalences.iter().cloned());
                // // Optionally, introduce `outer_equivalences` into `equivalences`.
                // // This is not required, but it could be very helpful. To be seen.
                // join_equivalences
                //     .classes
                //     .extend(outer_equivalences.classes.clone());

                // Reduce join equivalences by the input equivalences.
                for input_equivs in input_equivalences.iter() {
                    for class in join_equivalences.classes.iter_mut() {
                        for expr in class.iter_mut() {
                            // Semijoin elimination currently fails if you do more advanced simplification than
                            // literal substitution.
                            let old = expr.clone();
                            input_equivs.reduce_expr(expr);
                            expr.reduce(input_types.as_ref().unwrap());
                            if !expr.is_literal() {
                                expr.clone_from(&old);
                            }
                        }
                    }
                }
                // Minimize relative to appended input types.
                join_equivalences.minimize(&input_types);

                // Revisit each child, determining the information to present to it, and recurring.
                let mut columns = 0;
                for ((index, child), expr) in
                    children.into_iter().enumerate().zip(inputs.iter_mut())
                {
                    let child_arity = child.value::<Arity>().expect("Arity required");

                    let mut push_equivalences = join_equivalences.clone();
                    push_equivalences
                        .classes
                        .extend(outer_equivalences.classes.clone());
                    for (other, input_equivs) in input_equivalences.iter().enumerate() {
                        if index != other {
                            push_equivalences
                                .classes
                                .extend(input_equivs.classes.clone());
                        }
                    }
                    push_equivalences.project(columns..(columns + child_arity));
                    self.apply(expr, child, push_equivalences, get_equivalences);

                    columns += child_arity;
                }

                equivalences.clone_from(&join_equivalences.classes);
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                // TODO: MIN, MAX, ANY, ALL aggregates pass through all certain properties of their columns.
                // This may involve projection and permutation, to reposition the information appropriately.
                // TODO: Non-null constraints likely push down into the support of the aggregate expressions.

                // Apply any equivalences about the input to key and aggregate expressions.
                let input_equivalences = derived
                    .last_child()
                    .value::<Equivalences>()
                    .expect("Equivalences required");
                if let Some(input_equivalences) = input_equivalences {
                    let input_type = derived
                        .last_child()
                        .value::<RelationType>()
                        .expect("RelationType required");
                    for key in group_key.iter_mut() {
                        // Semijoin elimination currently fails if you do more advanced simplification than
                        // literal substitution.
                        let old_key = key.clone();
                        input_equivalences.reduce_expr(key);
                        key.reduce(input_type.as_ref().unwrap());
                        if !key.is_literal() {
                            key.clone_from(&old_key);
                        }
                    }
                    for aggr in aggregates.iter_mut() {
                        input_equivalences.reduce_expr(&mut aggr.expr);
                    }
                }

                // To transform `outer_equivalences` to one about `input`, we will "pretend" to pre-pend all of
                // the input columns, introduce equivalences about the evaluation of `group_key` on them
                // and the key columns themselves, and then project onto these "input" columns.
                let input_arity = *derived
                    .last_child()
                    .value::<Arity>()
                    .expect("Arity required");
                let output_arity = *derived.value::<Arity>().expect("Arity required");

                // Permute `outer_equivalences` to reference columns `input_arity` later.
                let permutation = (input_arity..(input_arity + output_arity)).collect::<Vec<_>>();
                outer_equivalences.permute(&permutation[..]);
                for (index, group) in group_key.iter().enumerate() {
                    outer_equivalences.classes.push(vec![
                        MirScalarExpr::Column(input_arity + index),
                        group.clone(),
                    ]);
                }
                outer_equivalences.project(0..input_arity);
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                );
            }
            MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                ..
            } => {
                // Apply input equivalences to `limit`, `group_key`, and `order_key`.
                // `limit` is optimized, but it is important that it track reflect the 
                // optimizations applied to `group_key` as it ust reference those columns.
                // The two keys can have column references optimized, and any that are
                // equivalent to literals can be removed.
                let input_equivalences = derived
                    .last_child()
                    .value::<Equivalences>()
                    .expect("Equivalences required");
                if let Some(input_equivalences) = input_equivalences {
                    if let Some(expr) = limit {
                        input_equivalences.reduce_expr(expr);
                    }
                    group_key.retain(|col| {
                        let mut col = MirScalarExpr::Column(*col);
                        input_equivalences.reduce_expr(&mut col);
                        !col.is_literal()
                    });
                    for column in group_key.iter_mut() {
                        input_equivalences.reduce_column(column);
                    }
                    order_key.retain(|ent| {
                        let mut col = MirScalarExpr::Column(ent.column);
                        input_equivalences.reduce_expr(&mut col);
                        !col.is_literal()
                    });
                    for entry in order_key.iter_mut() {
                        input_equivalences.reduce_column(&mut entry.column);
                    }
                }

                // Discard equivalences among non-key columns, as it is not correct that `input` may drop rows
                // that violate constraints among non-key columns without affecting the result.
                outer_equivalences.project(0..group_key.len());
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                );
            }
            MirRelationExpr::Negate { input } => {
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                );
            }
            MirRelationExpr::Threshold { input } => {
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                );
            }
            MirRelationExpr::Union { .. } => {
                for (child, derived) in expr.children_mut().rev().zip(derived.children_rev()) {
                    self.apply(child, derived, outer_equivalences.clone(), get_equivalences);
                }
            }
            MirRelationExpr::ArrangeBy { input, .. } => {
                // TODO: Option to alter arrangement keys, though .. terrifying.
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                );
            }
        }
    }
}
