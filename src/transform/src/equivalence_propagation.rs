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

use itertools::Itertools;
use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_repr::Datum;
use tracing::debug;

use crate::analysis::equivalences::{
    EqClassesImpl, EquivalenceClasses, EquivalenceClassesWithholdingErrors, Equivalences,
    ExpressionReducer,
};
use crate::analysis::{Arity, DerivedView, SqlRelationType};

use crate::{TransformCtx, TransformError};

/// Pulls up and pushes down predicate information represented as equivalences
#[derive(Debug, Default)]
pub struct EquivalencePropagation;

impl crate::Transform for EquivalencePropagation {
    fn name(&self) -> &'static str {
        "EquivalencePropagation"
    }

    #[mz_ore::instrument(
        target = "optimizer"
        level = "trace",
        fields(path.segment = "equivalence_propagation")
    )]
    fn actually_perform_transform(
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

        let prior = relation.clone();

        let mut get_equivalences = BTreeMap::default();
        self.apply(
            relation,
            derived,
            EquivalenceClasses::default(),
            &mut get_equivalences,
            ctx,
        );

        // Trace the plan as the result of `equivalence_propagation` before potentially applying
        // `ColumnKnowledge`. (If `ColumnKnowledge` runs, it will trace its own result.)
        mz_repr::explain::trace_plan(&*relation);

        if prior == *relation {
            let ck = crate::ColumnKnowledge::default();
            ck.transform(relation, ctx)?;
            if prior != *relation {
                // This used to be tracing::error, but it became too common with
                // dequadratic_eqprop_map.
                tracing::warn!(
                    ?ctx.global_id,
                    "ColumnKnowledge performed work after EquivalencePropagation",
                );
            }
        }

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
        ctx: &mut TransformCtx,
    ) {
        // TODO: The top-down traversal can be coded as a worklist, with arguments tupled and enqueued.
        // This has the potential to do a lot more cloning (of `outer_equivalences`), and some care is needed
        // for `get_equivalences` which would be scoped to the whole method rather than tupled and enqueued.

        let expr_type = derived
            .value::<SqlRelationType>()
            .expect("SqlRelationType required");
        assert!(expr_type.is_some());
        let expr_equivalences = derived
            .value::<Equivalences>()
            .expect("Equivalences required");

        // `None` analysis values indicate collections that can be pruned.
        let expr_equivalences = if let Some(e) = expr_equivalences {
            e
        } else {
            expr.take_safely_with_col_types(expr_type.clone().unwrap());
            return;
        };

        // Optimize `outer_equivalences` in the context of `expr_type`.
        // If it ends up unsatisfiable, we can replace `expr` with an empty constant of the same relation type.
        let reducer = expr_equivalences.reducer();
        for class in outer_equivalences.classes.iter_mut() {
            for expr in class.iter_mut() {
                reducer.reduce_expr(expr);
            }
        }

        outer_equivalences.minimize(expr_type.as_ref().map(|x| &x[..]));
        if outer_equivalences.unsatisfiable() {
            expr.take_safely_with_col_types(expr_type.clone().unwrap());
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
                let mut children_rev = expr.children_mut().rev().zip_eq(derived.children_rev());

                let body = children_rev.next().unwrap();
                let value = children_rev.next().unwrap();

                self.apply(
                    body.0,
                    body.1,
                    outer_equivalences.clone(),
                    get_equivalences,
                    ctx,
                );

                // We expect to find `id` in `get_equivalences`, as otherwise the binding is
                // not referenced and can be removed.
                if let Some(equivalences) = get_equivalences.get(&Id::Local(id)) {
                    self.apply(
                        value.0,
                        value.1,
                        equivalences.clone(),
                        get_equivalences,
                        ctx,
                    );
                }
            }
            MirRelationExpr::LetRec { .. } => {
                let mut child_iter = expr.children_mut().rev().zip_eq(derived.children_rev());
                // Continue in `body` with the outer equivalences.
                let (body, view) = child_iter.next().unwrap();
                self.apply(body, view, outer_equivalences, get_equivalences, ctx);
                // Continue recursively, but without the outer equivalences supplied to `body`.
                for (child, view) in child_iter {
                    self.apply(
                        child,
                        view,
                        EquivalenceClasses::default(),
                        get_equivalences,
                        ctx,
                    );
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
                    ctx,
                );
            }
            MirRelationExpr::Map { input, scalars } => {
                // Optimize `scalars` with respect to input equivalences.
                let input_equivalences = derived
                    .last_child()
                    .value::<Equivalences>()
                    .expect("Equivalences required");

                if let Some(input_equivalences_orig) = input_equivalences {
                    // We clone `input_equivalences` only if we want to modify it, which is when
                    // `enable_dequadratic_eqprop_map` is off. Otherwise, we work with the original
                    // `input_equivalences`.
                    let mut input_equivalences_cloned = None;
                    if !ctx.features.enable_dequadratic_eqprop_map {
                        // We mutate them for variadic Maps if the feature flag is not set.
                        input_equivalences_cloned = Some(input_equivalences_orig.clone());
                    }
                    // Get all output types, to reveal a prefix to each scaler expr.
                    let input_types = derived
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required")
                        .as_ref()
                        .unwrap();
                    let input_arity = input_types.len() - scalars.len();
                    for (index, expr) in scalars.iter_mut().enumerate() {
                        let reducer = if !ctx.features.enable_dequadratic_eqprop_map {
                            input_equivalences_cloned
                                .as_ref()
                                .expect("always filled if feature flag is not set")
                                .reducer()
                        } else {
                            input_equivalences_orig.reducer()
                        };
                        let changed = reducer.reduce_expr(expr);
                        if changed || !ctx.features.enable_less_reduce_in_eqprop {
                            expr.reduce(&input_types[..(input_arity + index)]);
                        }
                        if !ctx.features.enable_dequadratic_eqprop_map {
                            // Unfortunately, we had to stop doing the following, because it
                            // was making the `Map` handling quadratic.
                            // TODO: Get back to this when we have e-graphs.
                            // https://github.com/MaterializeInc/database-issues/issues/9157
                            //
                            // Introduce the fact relating the mapped expression and corresponding
                            // column. This allows subsequent expressions to be optimized with this
                            // information.
                            input_equivalences_cloned
                                .as_mut()
                                .expect("always filled if feature flag is not set")
                                .classes
                                .push(vec![
                                    expr.clone(),
                                    MirScalarExpr::column(input_arity + index),
                                ]);
                            input_equivalences_cloned
                                .as_mut()
                                .expect("always filled if feature flag is not set")
                                .minimize(Some(input_types));
                        }
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
                        ctx,
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
                    let input_types = derived
                        .last_child()
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required");
                    let reducer = input_equivalences.reducer();
                    for expr in exprs.iter_mut() {
                        let changed = reducer.reduce_expr(expr);
                        if changed || !ctx.features.enable_less_reduce_in_eqprop {
                            expr.reduce(input_types.as_ref().unwrap());
                        }
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
                        ctx,
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
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required");
                    let reducer = input_equivalences.reducer();
                    for expr in predicates.iter_mut() {
                        let changed = reducer.reduce_expr(expr);
                        if changed || !ctx.features.enable_less_reduce_in_eqprop {
                            expr.reduce(input_types.as_ref().unwrap());
                        }
                    }
                    // Incorporate `predicates` into `outer_equivalences`.
                    let mut class = predicates.clone();
                    class.push(MirScalarExpr::literal_ok(
                        Datum::True,
                        mz_repr::SqlScalarType::Bool,
                    ));
                    outer_equivalences.classes.push(class);
                    outer_equivalences.minimize(input_types.as_ref().map(|x| &x[..]));
                    self.apply(
                        input,
                        derived.last_child(),
                        outer_equivalences,
                        get_equivalences,
                        ctx,
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
                let mut input_types = Some(
                    children
                        .iter()
                        .flat_map(|c| {
                            c.value::<SqlRelationType>()
                                .expect("SqlRelationType required")
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
                        equivalences.minimize(input_types.as_ref().map(|x| &x[..]));
                        input_equivalences.push(equivalences);
                    }
                    columns += child_arity;
                }

                // Form the equivalences we will use to replace `equivalences`.
                let mut join_equivalences: EqClassesImpl =
                    if ctx.features.enable_eq_classes_withholding_errors {
                        EqClassesImpl::EquivalenceClassesWithholdingErrors(
                            EquivalenceClassesWithholdingErrors::default(),
                        )
                    } else {
                        EqClassesImpl::EquivalenceClasses(EquivalenceClasses::default())
                    };
                join_equivalences.extend_equivalences(equivalences.clone());

                // // Optionally, introduce `outer_equivalences` into `equivalences`.
                // // This is not required, but it could be very helpful. To be seen.
                // join_equivalences
                //     .classes
                //     .extend(outer_equivalences.classes.clone());

                // Reduce join equivalences by the input equivalences.
                for input_equivs in input_equivalences.iter() {
                    let reducer = input_equivs.reducer();
                    for class in join_equivalences
                        .equivalence_classes_mut()
                        .classes
                        .iter_mut()
                    {
                        for expr in class.iter_mut() {
                            // Semijoin elimination currently fails if you do more advanced simplification than
                            // literal substitution.
                            let old = expr.clone();
                            let changed = reducer.reduce_expr(expr);
                            let acceptable_sub = literal_domination(&old, expr);
                            if changed || !ctx.features.enable_less_reduce_in_eqprop {
                                expr.reduce(input_types.as_ref().unwrap());
                            }
                            if !acceptable_sub && !literal_domination(&old, expr)
                                || expr.contains_err()
                            {
                                expr.clone_from(&old);
                            }
                        }
                    }
                }
                // Remove nullability information, as it has already been incorporated from input equivalences,
                // and if it was reduced out relative to input equivalences we don't want to re-introduce it.
                if let Some(input_types) = input_types.as_mut() {
                    for col in input_types.iter_mut() {
                        col.nullable = true;
                    }
                }
                join_equivalences
                    .equivalence_classes_mut()
                    .minimize(input_types.as_ref().map(|x| &x[..]));

                // Revisit each child, determining the information to present to it, and recurring.
                let mut columns = 0;
                for ((index, child), expr) in
                    children.into_iter().enumerate().zip_eq(inputs.iter_mut())
                {
                    let child_arity = child.value::<Arity>().expect("Arity required");

                    let mut push_equivalences = join_equivalences.clone();
                    push_equivalences.extend_equivalences(outer_equivalences.classes.clone());

                    for (other, input_equivs) in input_equivalences.iter().enumerate() {
                        if index != other {
                            push_equivalences.extend_equivalences(input_equivs.classes.clone());
                        }
                    }
                    push_equivalences.project(columns..(columns + child_arity));
                    self.apply(
                        expr,
                        child,
                        push_equivalences.equivalence_classes().clone(),
                        get_equivalences,
                        ctx,
                    );

                    columns += child_arity;
                }

                let extracted_equivalences =
                    join_equivalences.extract_equivalences(input_types.as_ref().map(|x| &x[..]));

                debug!(
                    ?inputs,
                    ?extracted_equivalences,
                    "Join equivalences extracted"
                );
                equivalences.clone_from(&extracted_equivalences);
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
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required");
                    let reducer = input_equivalences.reducer();
                    for key in group_key.iter_mut() {
                        // Semijoin elimination currently fails if you do more advanced simplification than
                        // literal substitution.
                        let old_key = key.clone();
                        let changed = reducer.reduce_expr(key);
                        let acceptable_sub = literal_domination(&old_key, key);
                        if changed || !ctx.features.enable_less_reduce_in_eqprop {
                            key.reduce(input_type.as_ref().unwrap());
                        }
                        if !acceptable_sub && !literal_domination(&old_key, key) {
                            key.clone_from(&old_key);
                        }
                    }
                    for aggr in aggregates.iter_mut() {
                        let changed = reducer.reduce_expr(&mut aggr.expr);
                        if changed || !ctx.features.enable_less_reduce_in_eqprop {
                            aggr.expr.reduce(input_type.as_ref().unwrap());
                        }
                        // A count expression over a non-null expression can discard the expression.
                        if aggr.func == mz_expr::AggregateFunc::Count && !aggr.distinct {
                            let mut probe = aggr.expr.clone().call_is_null();
                            reducer.reduce_expr(&mut probe);
                            if probe.is_literal_false() {
                                aggr.expr = MirScalarExpr::literal_true();
                            }
                        }
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
                        MirScalarExpr::column(input_arity + index),
                        group.clone(),
                    ]);
                }
                outer_equivalences.project(0..input_arity);
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                    ctx,
                );
            }
            MirRelationExpr::TopK {
                input,
                group_key,
                limit,
                ..
            } => {
                // We must be careful when updating `limit` to not install column references
                // outside of `group_key`. We'll do this for now with `literal_domination`,
                // which will ensure we only perform substitutions by a literal.
                let input_equivalences = derived
                    .last_child()
                    .value::<Equivalences>()
                    .expect("Equivalences required");
                if let Some(input_equivalences) = input_equivalences {
                    let input_types = derived
                        .last_child()
                        .value::<SqlRelationType>()
                        .expect("SqlRelationType required");
                    let reducer = input_equivalences.reducer();
                    if let Some(expr) = limit {
                        let old_expr = expr.clone();
                        let changed = reducer.reduce_expr(expr);
                        let acceptable_sub = literal_domination(&old_expr, expr);
                        if changed || !ctx.features.enable_less_reduce_in_eqprop {
                            expr.reduce(input_types.as_ref().unwrap());
                        }
                        if !acceptable_sub && !literal_domination(&old_expr, expr) {
                            expr.clone_from(&old_expr);
                        }
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
                    ctx,
                );
            }
            MirRelationExpr::Negate { input } => {
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                    ctx,
                );
            }
            MirRelationExpr::Threshold { input } => {
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                    ctx,
                );
            }
            MirRelationExpr::Union { .. } => {
                for (child, derived) in expr.children_mut().rev().zip_eq(derived.children_rev()) {
                    self.apply(
                        child,
                        derived,
                        outer_equivalences.clone(),
                        get_equivalences,
                        ctx,
                    );
                }
            }
            MirRelationExpr::ArrangeBy { input, .. } => {
                // TODO: Option to alter arrangement keys, though .. terrifying.
                self.apply(
                    input,
                    derived.last_child(),
                    outer_equivalences,
                    get_equivalences,
                    ctx,
                );
            }
        }
    }
}

/// Logic encapsulating our willingness to accept an expression simplification.
///
/// For reasons of robustness, we cannot yet perform all recommended simplifications.
/// Certain transforms expect idiomatic expressions, often around precise use of column
/// identifiers, rather than equivalent identifiers.
///
/// The substitutions we are confident with are those that introduce literals for columns,
/// or which replace column nullability checks with literals.
fn literal_domination(old: &MirScalarExpr, new: &MirScalarExpr) -> bool {
    let mut todo = vec![(old, new)];
    while let Some((old, new)) = todo.pop() {
        match (old, new) {
            (_, MirScalarExpr::Literal(_, _)) => {
                // Substituting a literal is always acceptable; we don't need to consult
                // the result of the old expression to determine this.
            }
            (
                MirScalarExpr::CallUnary { func: f0, expr: e0 },
                MirScalarExpr::CallUnary { func: f1, expr: e1 },
            ) => {
                if f0 != f1 {
                    return false;
                } else {
                    todo.push((&**e0, &**e1));
                }
            }
            (
                MirScalarExpr::CallBinary {
                    func: f0,
                    expr1: e01,
                    expr2: e02,
                },
                MirScalarExpr::CallBinary {
                    func: f1,
                    expr1: e11,
                    expr2: e12,
                },
            ) => {
                if f0 != f1 {
                    return false;
                } else {
                    todo.push((&**e01, &**e11));
                    todo.push((&**e02, &**e12));
                }
            }
            (
                MirScalarExpr::CallVariadic {
                    func: f0,
                    exprs: e0s,
                },
                MirScalarExpr::CallVariadic {
                    func: f1,
                    exprs: e1s,
                },
            ) => {
                use itertools::Itertools;
                if f0 != f1 || e0s.len() != e1s.len() {
                    return false;
                } else {
                    todo.extend(e0s.iter().zip_eq(e1s));
                }
            }
            (
                MirScalarExpr::If {
                    cond: c0,
                    then: t0,
                    els: e0,
                },
                MirScalarExpr::If {
                    cond: c1,
                    then: t1,
                    els: e1,
                },
            ) => {
                todo.push((&**c0, &**c1));
                todo.push((&**t0, &**t1));
                todo.push((&**e0, &**e1))
            }
            _ => {
                if old != new {
                    return false;
                }
            }
        }
    }
    true
}
