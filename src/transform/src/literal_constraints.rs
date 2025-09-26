// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! See if there are predicates of the form `<expr> = literal` that can be sped up using an index.
//! More specifically, look for an MFP on top of a Get, where the MFP has an appropriate filter, and
//! the Get has a matching index. Convert these to `IndexedFilter` joins, which is a semi-join with
//! a constant collection.
//!
//! E.g.: Logically, we go from something like
//! `SELECT f1, f2, f3 FROM t WHERE t.f1 = lit1 AND t.f2 = lit2`
//! to
//! `SELECT f1, f2, f3 FROM t, (SELECT * FROM (VALUES (lit1, lit2))) as filter_list
//!  WHERE t.f1 = filter_list.column1 AND t.f2 = filter_list.column2`

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::JoinImplementation::IndexedFilter;
use mz_expr::canonicalize::canonicalize_predicates;
use mz_expr::visit::{Visit, VisitChildren};
use mz_expr::{BinaryFunc, Id, MapFilterProject, MirRelationExpr, MirScalarExpr, VariadicFunc};
use mz_ore::collections::CollectionExt;
use mz_ore::iter::IteratorExt;
use mz_ore::stack::RecursionLimitError;
use mz_ore::vec::swap_remove_multiple;
use mz_repr::{Diff, GlobalId, Row, SqlRelationType};

use crate::TransformCtx;
use crate::canonicalize_mfp::CanonicalizeMfp;
use crate::notice::IndexTooWideForLiteralConstraints;

/// Convert literal constraints into `IndexedFilter` joins.
#[derive(Debug)]
pub struct LiteralConstraints;

impl crate::Transform for LiteralConstraints {
    fn name(&self) -> &'static str {
        "LiteralConstraints"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "literal_constraints")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let result = self.action(relation, ctx);
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl LiteralConstraints {
    fn action(
        &self,
        relation: &mut MirRelationExpr,
        transform_ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let mut mfp = MapFilterProject::extract_non_errors_from_expr_mut(relation);
        relation.try_visit_mut_children(|e| self.action(e, transform_ctx))?;

        if let MirRelationExpr::Get {
            id: Id::Global(id),
            ref typ,
            ..
        } = *relation
        {
            let orig_mfp = mfp.clone();

            // Preparation for the literal constraints detection.
            Self::inline_literal_constraints(&mut mfp);
            Self::list_of_predicates_to_and_of_predicates(&mut mfp);
            Self::distribute_and_over_or(&mut mfp)?;
            Self::unary_and(&mut mfp);

            /// The above preparation might make the MFP more complicated, so we'll later want to
            /// either undo the preparation transformations or get back to `orig_mfp`.
            fn undo_preparation(
                mfp: &mut MapFilterProject,
                orig_mfp: &MapFilterProject,
                relation: &MirRelationExpr,
                relation_type: SqlRelationType,
            ) {
                // undo list_of_predicates_to_and_of_predicates, distribute_and_over_or, unary_and
                // (It undoes the latter 2 through `MirScalarExp::reduce`.)
                LiteralConstraints::canonicalize_predicates(mfp, relation, relation_type);
                // undo inline_literal_constraints
                mfp.optimize();
                // We can usually undo, but sometimes not (see comment on `distribute_and_over_or`),
                // so in those cases we might have a more complicated MFP than the original MFP
                // (despite the removal of the literal constraints and/or contradicting OR args).
                // So let's use the simpler one.
                if LiteralConstraints::predicates_size(orig_mfp)
                    < LiteralConstraints::predicates_size(mfp)
                {
                    *mfp = orig_mfp.clone();
                }
            }

            let removed_contradicting_or_args = Self::remove_impossible_or_args(&mut mfp)?;

            // todo: We might want to also call `canonicalize_equivalences`,
            // see near the end of literal_constraints.slt.

            let inp_typ = typ.clone();

            let key_val = Self::detect_literal_constraints(&mfp, id, transform_ctx);

            match key_val {
                None => {
                    // We didn't find a usable index, so no chance to remove literal constraints.
                    // But, we might have removed contradicting OR args.
                    if removed_contradicting_or_args {
                        undo_preparation(&mut mfp, &orig_mfp, relation, inp_typ);
                    } else {
                        // We didn't remove anything, so let's go with the original MFP.
                        mfp = orig_mfp;
                    }
                }
                Some((idx_id, key, possible_vals)) => {
                    // We found a usable index. We'll try to remove the corresponding literal
                    // constraints.
                    if Self::remove_literal_constraints(&mut mfp, &key)
                        || removed_contradicting_or_args
                    {
                        // We were able to remove the literal constraints or contradicting OR args,
                        // so we would like to use this new MFP, so we try undoing the preparation.
                        undo_preparation(&mut mfp, &orig_mfp, relation, inp_typ.clone());
                    } else {
                        // We were not able to remove the literal constraint, so `mfp` is
                        // equivalent to `orig_mfp`, but `orig_mfp` is often simpler (or the same).
                        mfp = orig_mfp;
                    }

                    // We transform the Get into a semi-join with a constant collection.

                    let inp_id = id.clone();
                    let filter_list = MirRelationExpr::Constant {
                        rows: Ok(possible_vals
                            .iter()
                            .map(|val| (val.clone(), Diff::ONE))
                            .collect()),
                        typ: mz_repr::SqlRelationType {
                            column_types: key
                                .iter()
                                .map(|e| {
                                    e.typ(&inp_typ.column_types)
                                        // We make sure to not include a null in `expr_eq_literal`.
                                        .nullable(false)
                                })
                                .collect(),
                            // (Note that the key inference for `MirRelationExpr::Constant` inspects
                            // the constant values to detect keys not listed within the node, but it
                            // can only detect a single-column key this way. A multi-column key is
                            // common here, so we explicitly add it.)
                            keys: vec![(0..key.len()).collect()],
                        },
                    }
                    .arrange_by(&[(0..key.len()).map(MirScalarExpr::column).collect_vec()]);

                    if possible_vals.is_empty() {
                        // Even better than what we were hoping for: Found contradicting
                        // literal constraints, so the whole relation is empty.
                        relation.take_safely(Some(inp_typ));
                    } else {
                        // The common case: We need to build the join which is the main point of
                        // this transform.
                        *relation = MirRelationExpr::Join {
                            // It's important to keep the `filter_list` in the second position.
                            // Both the lowering and EXPLAIN depends on this.
                            inputs: vec![
                                relation.clone().arrange_by(std::slice::from_ref(&key)),
                                filter_list,
                            ],
                            equivalences: key
                                .iter()
                                .enumerate()
                                .map(|(i, e)| {
                                    vec![(*e).clone(), MirScalarExpr::column(i + inp_typ.arity())]
                                })
                                .collect(),
                            implementation: IndexedFilter(
                                inp_id,
                                idx_id,
                                key.clone(),
                                possible_vals,
                            ),
                        };

                        // Rebuild the MFP to add the projection that removes the columns coming from
                        // the filter_list side of the join.
                        let (map, filter, project) = mfp.as_map_filter_project();
                        mfp = MapFilterProject::new(inp_typ.arity() + key.len())
                            .project(0..inp_typ.arity()) // make the join semi
                            .map(map)
                            .filter(filter)
                            .project(project);
                        mfp.optimize()
                    }
                }
            }
        }

        CanonicalizeMfp::rebuild_mfp(mfp, relation);

        Ok(())
    }

    /// Detects literal constraints in an MFP on top of a Get of `id`, and a matching index that can
    /// be used to speed up the Filter of the MFP.
    ///
    /// For example, if there is an index on `(f1, f2)`, and the Filter is
    /// `(f1 = 3 AND f2 = 5) OR (f1 = 7 AND f2 = 9)`, it returns `Some([f1, f2], [[3,5], [7,9]])`.
    ///
    /// We can use an index if each argument of the OR includes a literal constraint on each of the
    /// key fields of the index. Extra predicates inside the OR arguments are ok.
    ///
    /// Returns (idx_id, idx_key, values to lookup in the index).
    fn detect_literal_constraints(
        mfp: &MapFilterProject,
        get_id: GlobalId,
        transform_ctx: &mut TransformCtx,
    ) -> Option<(GlobalId, Vec<MirScalarExpr>, Vec<Row>)> {
        // Checks whether an index with the specified key can be used to speed up the given filter.
        // See comment of `IndexMatch`.
        fn match_index(key: &[MirScalarExpr], or_args: &Vec<MirScalarExpr>) -> IndexMatch {
            if key.is_empty() {
                // Nothing to do with an index that has an empty key.
                return IndexMatch::UnusableNoSubset;
            }
            if !key.iter().all_unique() {
                // This is a weird index. Why does it have duplicate key expressions?
                return IndexMatch::UnusableNoSubset;
            }
            let mut literal_values = Vec::new();
            let mut inv_cast_any = false;
            // This starts with all key fields of the index.
            // At the end, it will contain a subset S of index key fields such that if the index had
            // only S as its key, then the index would be usable.
            let mut usable_key_fields = key.iter().collect::<BTreeSet<_>>();
            let mut usable = true;
            for or_arg in or_args {
                let mut row = Row::default();
                let mut packer = row.packer();
                for key_field in key {
                    let and_args = or_arg.and_or_args(VariadicFunc::And);
                    // Let's find a constraint for this key field
                    if let Some((literal, inv_cast)) = and_args
                        .iter()
                        .find_map(|and_arg| and_arg.expr_eq_literal(key_field))
                    {
                        // (Note that the above find_map can find only 0 or 1 result, because
                        // of `remove_impossible_or_args`.)
                        packer.push(literal.unpack_first());
                        inv_cast_any |= inv_cast;
                    } else {
                        // There is an `or_arg` where we didn't find a constraint for a key field,
                        // so the index is unusable. Throw out the field from the usable fields.
                        usable = false;
                        usable_key_fields.remove(key_field);
                        if usable_key_fields.is_empty() {
                            return IndexMatch::UnusableNoSubset;
                        }
                    }
                }
                literal_values.push(row);
            }
            if usable {
                // We should deduplicate, because a constraint can be duplicated by
                // `distribute_and_over_or`. For example: `IN ('l1', 'l2') AND (a > 0 OR a < 5)`:
                // the 2 args of the OR will cause the IN constraints to be duplicated. This doesn't
                // alter the meaning of the expression when evaluated as a filter, but if we extract
                // those literals 2 times into `literal_values` then the Peek code will look up
                // those keys from the index 2 times, leading to duplicate results.
                literal_values.sort();
                literal_values.dedup();
                IndexMatch::Usable(literal_values, inv_cast_any)
            } else {
                if usable_key_fields.is_empty() {
                    IndexMatch::UnusableNoSubset
                } else {
                    IndexMatch::UnusableTooWide(
                        usable_key_fields.into_iter().cloned().collect_vec(),
                    )
                }
            }
        }

        let or_args = Self::get_or_args(mfp);

        let index_matches = transform_ctx
            .indexes
            .indexes_on(get_id)
            .map(|(index_id, key)| (index_id, key.to_owned(), match_index(key, &or_args)))
            .collect_vec();

        let result = index_matches
            .iter()
            .cloned()
            .filter_map(|(idx_id, key, index_match)| match index_match {
                IndexMatch::Usable(vals, inv_cast) => Some((idx_id, key, vals, inv_cast)),
                _ => None,
            })
            // Maximize:
            //  1. number of predicates that are sped using a single index.
            //  2. whether we are using a simpler index by having removed a cast from the key expr.
            .max_by_key(|(_idx_id, key, _vals, inv_cast)| (key.len(), *inv_cast))
            .map(|(idx_id, key, vals, _inv_cast)| (idx_id, key, vals));

        if result.is_none() && !or_args.is_empty() {
            // Let's see if we can give a hint to the user.
            index_matches
                .into_iter()
                .for_each(|(index_id, index_key, index_match)| {
                    match index_match {
                        IndexMatch::UnusableTooWide(usable_subset) => {
                            // see comment of `UnusableTooWide`
                            assert!(!usable_subset.is_empty());
                            // Determine literal values that we would get if the index was on
                            // `usable_subset`.
                            let literal_values = match match_index(&usable_subset, &or_args) {
                                IndexMatch::Usable(literal_vals, _) => literal_vals,
                                _ => unreachable!(), // `usable_subset` would make the index usable.
                            };

                            // Let's come up with a recommendation for what columns to index:
                            // Intersect literal constraints across all OR args. (Which might
                            // include columns that are NOT in this index, and therefore not in
                            // `usable_subset`.)
                            let recommended_key = or_args
                                .iter()
                                .map(|or_arg| {
                                    let and_args = or_arg.and_or_args(VariadicFunc::And);
                                    and_args
                                        .iter()
                                        .filter_map(|and_arg| and_arg.any_expr_eq_literal())
                                        .collect::<BTreeSet<_>>()
                                })
                                .reduce(|fields1, fields2| {
                                    fields1.intersection(&fields2).cloned().collect()
                                })
                                // The unwrap is safe because above we checked `!or_args.is_empty()`
                                .unwrap()
                                .into_iter()
                                .collect_vec();

                            transform_ctx.df_meta.push_optimizer_notice_dedup(
                                IndexTooWideForLiteralConstraints {
                                    index_id,
                                    index_key,
                                    usable_subset,
                                    literal_values,
                                    index_on_id: get_id,
                                    recommended_key,
                                },
                            )
                        }
                        _ => (),
                    }
                });
        }

        result
    }

    /// Removes the expressions that [LiteralConstraints::detect_literal_constraints] found, if
    /// possible. Returns whether it removed anything.
    /// For example, if the key of the detected literal constraint is just `f1`, and we have the
    /// expression
    /// `(f1 = 3 AND f2 = 5) OR (f1 = 7 AND f2 = 5)`, then this modifies it to `f2 = 5`.
    /// However, if OR branches differ in their non-key parts, then we cannot remove the literal
    /// constraint. For example,
    /// `(f1 = 3 AND f2 = 5) OR (f1 = 7 AND f2 = 555)`, then we cannot remove the `f1` parts,
    /// because then the filter wouldn't know whether to check `f2 = 5` or `f2 = 555`.
    fn remove_literal_constraints(mfp: &mut MapFilterProject, key: &Vec<MirScalarExpr>) -> bool {
        let or_args = Self::get_or_args(mfp);
        if or_args.len() == 0 {
            return false;
        }

        // In simple situations it would be enough to check here that if we remove the detected
        // literal constraints from each OR arg, then the residual OR args are all equal.
        // However, this wouldn't be able to perform the removal when the expression that should
        // remain in the end has an OR. This is because conversion to DNF makes duplicates of
        // every literal constraint, with different residuals. To also handle this case, we collect
        // the possible residuals for every literal constraint row, and check that all sets are
        // equal. Example: The user wrote
        // `WHERE ((a=1 AND b=1) OR (a=2 AND b=2)) AND (c OR (d AND e))`.
        // The DNF of this is
        // `(a=1 AND b=1 AND c) OR (a=1 AND b=1 AND d AND e) OR (a=2 AND b=2 AND c) OR (a=2 AND b=2 AND d AND e)`.
        // Then `constraints_to_residual_sets` will be:
        // [
        //   [`a=1`, `b=1`]  ->  {[`c`], [`d`, `e`]},
        //   [`a=2`, `b=2`]  ->  {[`c`], [`d`, `e`]}
        // ]
        // After removing the literal constraints we have
        // `c OR (d AND e)`
        let mut constraints_to_residual_sets = BTreeMap::new();
        or_args.iter().for_each(|or_arg| {
            let and_args = or_arg.and_or_args(VariadicFunc::And);
            let (mut constraints, mut residual): (Vec<_>, Vec<_>) =
                and_args.iter().cloned().partition(|and_arg| {
                    key.iter()
                        .any(|key_field| matches!(and_arg.expr_eq_literal(key_field), Some(..)))
                });
            // In every or_arg there has to be some literal constraints, otherwise
            // `detect_literal_constraints` would have returned None.
            assert!(constraints.len() >= 1);
            // `remove_impossible_or_args` made sure that inside each or_arg, each
            // expression can be literal constrained only once. So if we find one of the
            // key fields being literal constrained, then it's definitely that literal
            // constraint that detect_literal_constraints based one of its return values on.
            //
            // This is important, because without `remove_impossible_or_args`, we might
            // have the situation here that or_arg would be something like
            // `a = 5 AND a = 8`, of which `detect_literal_constraints` found only the `a = 5`,
            // but here we would remove both the `a = 5` and the `a = 8`.
            constraints.sort();
            residual.sort();
            let entry = constraints_to_residual_sets
                .entry(constraints)
                .or_insert_with(BTreeSet::new);
            entry.insert(residual);
        });
        let residual_sets = constraints_to_residual_sets
            .into_iter()
            .map(|(_constraints, residual_set)| residual_set)
            .collect::<Vec<_>>();
        if residual_sets.iter().all_equal() {
            // We can remove the literal constraint
            assert!(residual_sets.len() >= 1); // We already checked `or_args.len() == 0` above
            let residual_set = residual_sets.into_iter().into_first();
            let new_pred = MirScalarExpr::CallVariadic {
                func: VariadicFunc::Or,
                exprs: residual_set
                    .into_iter()
                    .map(|residual| MirScalarExpr::CallVariadic {
                        func: VariadicFunc::And,
                        exprs: residual,
                    })
                    .collect::<Vec<_>>(),
            };
            let (map, _predicates, project) = mfp.as_map_filter_project();
            *mfp = MapFilterProject::new(mfp.input_arity)
                .map(map)
                .filter(std::iter::once(new_pred))
                .project(project);

            true
        } else {
            false
        }
    }

    /// 1. Removes such OR args in which there are contradicting literal constraints.
    /// 2. Also, if an OR arg doesn't have any contradiction, this fn just deduplicates
    /// the AND arg list of that OR arg. (Might additionally sort all AND arg lists.)
    ///
    /// Returns whether it performed any removal or deduplication.
    ///
    /// Example for 1:
    /// `<arg1> OR (a = 5 AND a = 5 AND a = 8) OR <arg3>`
    /// -->
    /// `<arg1> OR <arg3> `
    ///
    /// Example for 2:
    /// `<arg1> OR (a = 5 AND a = 5 AND b = 8) OR <arg3>`
    /// -->
    /// `<arg1> OR (a = 5 AND b = 8) OR <arg3>`
    fn remove_impossible_or_args(mfp: &mut MapFilterProject) -> Result<bool, RecursionLimitError> {
        let mut or_args = Self::get_or_args(mfp);
        if or_args.len() == 0 {
            return Ok(false);
        }
        let mut to_remove = Vec::new();
        let mut changed = false;
        or_args.iter_mut().enumerate().for_each(|(i, or_arg)| {
            if let MirScalarExpr::CallVariadic {
                func: VariadicFunc::And,
                exprs: and_args,
            } = or_arg
            {
                if and_args
                    .iter()
                    .any(|e| e.impossible_literal_equality_because_types())
                {
                    changed = true;
                    to_remove.push(i);
                } else {
                    and_args.sort_by_key(|e: &MirScalarExpr| e.invert_casts_on_expr_eq_literal());
                    let and_args_before_dedup = and_args.clone();
                    and_args
                        .dedup_by_key(|e: &mut MirScalarExpr| e.invert_casts_on_expr_eq_literal());
                    if *and_args != and_args_before_dedup {
                        changed = true;
                    }
                    // Deduplicated, so we cannot have something like `a = 5 AND a = 5`.
                    // This means that if we now have `<expr1> = <literal1> AND <expr1> = <literal2>`,
                    // then `literal1` is definitely not the same as `literal2`. This means that this
                    // whole or_arg is a contradiction, because it's something like `a = 5 AND a = 8`.
                    let mut literal_constrained_exprs = and_args
                        .iter()
                        .filter_map(|and_arg| and_arg.any_expr_eq_literal());
                    if !literal_constrained_exprs.all_unique() {
                        changed = true;
                        to_remove.push(i);
                    }
                }
            } else {
                // `unary_and` made sure that each OR arg is an AND
                unreachable!("OR arg was not an AND in remove_impossible_or_args");
            }
        });
        // We remove the marked OR args.
        // (If the OR has 0 or 1 args remaining, then `reduce_and_canonicalize_and_or` will later
        // further simplify.)
        swap_remove_multiple(&mut or_args, to_remove);
        // Rebuild the MFP if needed
        if changed {
            let new_predicates = vec![MirScalarExpr::CallVariadic {
                func: VariadicFunc::Or,
                exprs: or_args,
            }];
            let (map, _predicates, project) = mfp.as_map_filter_project();
            *mfp = MapFilterProject::new(mfp.input_arity)
                .map(map)
                .filter(new_predicates)
                .project(project);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns the arguments of the predicate's top-level OR as a Vec.
    /// If there is no top-level OR, then interpret the predicate as a 1-arg OR, i.e., return a
    /// 1-element Vec.
    ///
    /// Assumes that [LiteralConstraints::list_of_predicates_to_and_of_predicates] has already run.
    fn get_or_args(mfp: &MapFilterProject) -> Vec<MirScalarExpr> {
        assert_eq!(mfp.predicates.len(), 1); // list_of_predicates_to_and_of_predicates ensured this
        let (_, pred) = mfp.predicates.get(0).unwrap();
        pred.and_or_args(VariadicFunc::Or)
    }

    /// Makes the job of [LiteralConstraints::detect_literal_constraints] easier by undoing some CSE to
    /// reconstruct literal constraints.
    fn inline_literal_constraints(mfp: &mut MapFilterProject) {
        let mut should_inline = vec![false; mfp.input_arity + mfp.expressions.len()];
        // Mark those expressions for inlining that contain a subexpression of the form
        // `<xxx> = <lit>` or `<lit> = <xxx>`.
        for (i, e) in mfp.expressions.iter().enumerate() {
            e.visit_pre(|s| {
                if s.any_expr_eq_literal().is_some() {
                    should_inline[i + mfp.input_arity] = true;
                }
            });
        }
        // Whenever
        // `<Column(i)> = <lit>` or `<lit> = <Column(i)>`
        // appears in a predicate, mark the ith expression to be inlined.
        for (_before, p) in mfp.predicates.iter() {
            p.visit_pre(|e| {
                if let MirScalarExpr::CallBinary {
                    func: BinaryFunc::Eq(_),
                    expr1,
                    expr2,
                } = e
                {
                    if matches!(**expr1, MirScalarExpr::Literal(..)) {
                        if let MirScalarExpr::Column(col, _) = **expr2 {
                            if col >= mfp.input_arity {
                                should_inline[col] = true;
                            }
                        }
                    }
                    if matches!(**expr2, MirScalarExpr::Literal(..)) {
                        if let MirScalarExpr::Column(col, _) = **expr1 {
                            if col >= mfp.input_arity {
                                should_inline[col] = true;
                            }
                        }
                    }
                }
            });
        }
        // Perform the marked inlinings.
        mfp.perform_inlining(should_inline);
    }

    /// MFPs have a Vec of predicates `[p1, p2, ...]`, which logically represents `p1 AND p2 AND ...`.
    /// This function performs this conversion. Note that it might create a variadic AND with
    /// 0 or 1 args, so the resulting predicate Vec always has exactly 1 element.
    fn list_of_predicates_to_and_of_predicates(mfp: &mut MapFilterProject) {
        // Rebuild the MFP. (Unfortunately, we cannot modify the predicates in place, because MFP
        // predicates also have a "before" field, which we need to update. (`filter` will recompute
        // these.)
        let (map, _predicates, project) = mfp.as_map_filter_project();
        let new_predicates = vec![MirScalarExpr::CallVariadic {
            func: VariadicFunc::And,
            exprs: mfp.predicates.iter().map(|(_, p)| p.clone()).collect(),
        }];
        *mfp = MapFilterProject::new(mfp.input_arity)
            .map(map)
            .filter(new_predicates)
            .project(project);
    }

    /// Call [mz_expr::canonicalize::canonicalize_predicates] on each of the predicates in the MFP.
    fn canonicalize_predicates(
        mfp: &mut MapFilterProject,
        relation: &MirRelationExpr,
        relation_type: SqlRelationType,
    ) {
        let (map, mut predicates, project) = mfp.as_map_filter_project();
        let typ_after_map = relation
            .clone()
            .map(map.clone())
            .typ_with_input_types(&[relation_type]);
        canonicalize_predicates(&mut predicates, &typ_after_map.column_types);
        // Rebuild the MFP with the new predicates.
        *mfp = MapFilterProject::new(mfp.input_arity)
            .map(map)
            .filter(predicates)
            .project(project);
    }

    /// Distribute AND over OR + do flatten_and_or until fixed point.
    /// This effectively converts to disjunctive normal form (DNF) (i.e., an OR of ANDs), because
    /// [MirScalarExpr::reduce] did Demorgans and double-negation-elimination. So after
    /// [MirScalarExpr::reduce], we get here a tree of AND/OR nodes. A distribution step lifts an OR
    /// up the tree by 1 level, and a [MirScalarExpr::flatten_associative] merges two ORs that are at
    /// adjacent levels, so eventually we'll end up with just one OR that is at the top of the tree,
    /// with ANDs below it.
    /// For example:
    /// (a || b) && (c || d)
    ///   ->
    /// ((a || b) && c) || ((a || b) && d)
    ///   ->
    /// (a && c) || (b && c) || (a && d) || (b && d)
    /// (This is a variadic OR with 4 arguments.)
    ///
    /// Example:
    /// User wrote `WHERE (a,b) IN ((1,2), (1,4), (8,5))`,
    /// from which [MirScalarExpr::undistribute_and_or] made this before us:
    /// (#0 = 1 AND (#1 = 2 OR #1 = 4)) OR (#0 = 8 AND #1 = 5)
    /// And now we distribute the first AND over the first OR in 2 steps: First to
    /// ((#0 = 1 AND #1 = 2) OR (#0 = 1 AND #1 = 4)) OR (#0 = 8 AND #1 = 5)
    /// then [MirScalarExpr::flatten_associative]:
    /// (#0 = 1 AND #1 = 2) OR (#0 = 1 AND #1 = 4) OR (#0 = 8 AND #1 = 5)
    ///
    /// Note that [MirScalarExpr::undistribute_and_or] is not exactly an inverse to this because
    /// 1) it can undistribute both AND over OR and OR over AND.
    /// 2) it cannot always undo the distribution, because an expression might have multiple
    /// overlapping undistribution opportunities, see comment there.
    fn distribute_and_over_or(mfp: &mut MapFilterProject) -> Result<(), RecursionLimitError> {
        mfp.predicates.iter_mut().try_for_each(|(_, p)| {
            let mut old_p = MirScalarExpr::column(0);
            while old_p != *p {
                let size = p.size();
                // We might make the expression exponentially larger, so we should have some limit.
                // Below 1000 (e.g., a single IN list of ~300 elements, or 3 IN lists of 4-5
                // elements each), we are <10 ms for a single IN list, and even less for multiple IN
                // lists.
                if size > 1000 {
                    break;
                }
                old_p = p.clone();
                p.visit_mut_post(&mut |e: &mut MirScalarExpr| {
                    if let MirScalarExpr::CallVariadic {
                        func: VariadicFunc::And,
                        exprs: and_args,
                    } = e
                    {
                        if let Some((i, _)) = and_args.iter().enumerate().find(|(_i, a)| {
                            matches!(
                                a,
                                MirScalarExpr::CallVariadic {
                                    func: VariadicFunc::Or,
                                    ..
                                }
                            )
                        }) {
                            // We found an AND whose ith argument is an OR. We'll distribute the other
                            // args of the AND over this OR.
                            let mut or = and_args.swap_remove(i);
                            let to_distribute = MirScalarExpr::CallVariadic {
                                func: VariadicFunc::And,
                                exprs: (*and_args).clone(),
                            };
                            if let MirScalarExpr::CallVariadic {
                                func: VariadicFunc::Or,
                                exprs: ref mut or_args,
                            } = or
                            {
                                or_args.iter_mut().for_each(|a| {
                                    *a = a.clone().and(to_distribute.clone());
                                });
                            } else {
                                unreachable!(); // because the `find` found a match already
                            }
                            *e = or; // The modified OR will be the new top-level expr.
                        }
                    }
                })?;
                p.visit_mut_post(&mut |e: &mut MirScalarExpr| {
                    e.flatten_associative();
                })?;
            }
            Ok(())
        })
    }

    /// For each of the arguments of the top-level OR (if no top-level OR, then interpret the whole
    /// expression as a 1-arg OR, see [LiteralConstraints::get_or_args]), check if it's an AND, and
    /// if not, then wrap it in a 1-arg AND.
    fn unary_and(mfp: &mut MapFilterProject) {
        let mut or_args = Self::get_or_args(mfp);
        let mut changed = false;
        or_args.iter_mut().for_each(|or_arg| {
            if !matches!(
                or_arg,
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::And,
                    ..
                }
            ) {
                *or_arg = MirScalarExpr::CallVariadic {
                    func: VariadicFunc::And,
                    exprs: vec![or_arg.clone()],
                };
                changed = true;
            }
        });
        if changed {
            let new_predicates = vec![MirScalarExpr::CallVariadic {
                func: VariadicFunc::Or,
                exprs: or_args,
            }];
            let (map, _predicates, project) = mfp.as_map_filter_project();
            *mfp = MapFilterProject::new(mfp.input_arity)
                .map(map)
                .filter(new_predicates)
                .project(project);
        }
    }

    fn predicates_size(mfp: &MapFilterProject) -> usize {
        let mut sum = 0;
        for (_, p) in mfp.predicates.iter() {
            sum = sum + p.size();
        }
        sum
    }
}

/// Whether an index is usable to speed up a Filter with literal constraints.
#[derive(Clone)]
enum IndexMatch {
    /// The index is usable, that is, each OR argument constrains each key field.
    ///
    /// The `Vec<Row>` has the constraining literal values, where each Row corresponds to one OR
    /// argument, and each value in the Row corresponds to one key field.
    ///
    /// The `bool` indicates whether we needed to inverse cast equalities to match them up with key
    /// fields. The inverse cast enables index usage when an implicit cast is wrapping a key field.
    /// E.g., if `a` is smallint, and the user writes `a = 5`, then HIR inserts an implicit cast:
    /// `smallint_to_integer(a) = 5`, which we invert to `a = 5`, where the `5` is a smallint
    /// literal. For more details on the inversion, see `invert_casts_on_expr_eq_literal_inner`.
    Usable(Vec<Row>, bool),
    /// The index is unusable. However, there is a subset of key fields such that if the index would
    /// be only on this subset, then it would be usable.
    /// Note: this Vec is never empty. (If it were empty, then we'd get `UnusableNoSubset` instead.)
    UnusableTooWide(Vec<MirScalarExpr>),
    /// The index is unusable. Moreover, none of its key fields could be used as an alternate index
    /// to speed up this filter.
    UnusableNoSubset,
}
