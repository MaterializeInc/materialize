// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Canonicalizes MFPs, performs CSEs, and speeds up certain filters.
//!
//! This transform takes a sequence of Maps, Filters, and Projects and
//! canonicalizes it to a sequence like this:
//! | Index
//! | Predicates sped up by the index
//! | Map
//! | Filter
//! | Project
//!
//! As part of canonicalizing, this transform looks at the Map-Filter-Project
//! subsequence and identifies common `ScalarExpr` expressions across and within
//! expressions that are arguments to the Map-Filter-Project. It reforms the
//! `Map-Filter-Project` subsequence to build each distinct expression at most
//! once and to re-use expressions instead of re-evaluating them.
//!
//! The re-use policy at the moment is severe and re-uses everything.
//! It may be worth considering relations of this if it results in more
//! busywork and less efficiency, but the wins can be substantial when
//! expressions re-use complex subexpressions.
//!
//! Right now, predicates sped up by an index are still present in the "Filter"
//! that comes after the Map. Issue #13774 has been filed to track removing
//! said predicates from "Filter".

use crate::{IndexOracle, TransformArgs};
use mz_expr::canonicalize::canonicalize_predicates;
use mz_expr::visit::{Visit, VisitChildren};
use mz_expr::JoinImplementation::IndexedFilter;
use mz_expr::{BinaryFunc, Id, MapFilterProject, MirRelationExpr, MirScalarExpr, VariadicFunc};
use mz_ore::stack::RecursionLimitError;
use mz_ore::vec::swap_remove_multiple;
use mz_repr::{GlobalId, Row};

/// Canonicalizes MFPs and performs common sub-expression elimination.
#[derive(Debug)]
pub struct CanonicalizeMfp;

impl crate::Transform for CanonicalizeMfp {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation, args.indexes)
    }
}

impl CanonicalizeMfp {
    fn action(
        &self,
        relation: &mut MirRelationExpr,
        indexes: &dyn IndexOracle,
    ) -> Result<(), crate::TransformError> {
        let mut mfp = mz_expr::MapFilterProject::extract_non_errors_from_expr_mut(relation);
        relation.try_visit_mut_children(|e| self.action(e, indexes))?;
        // perform CSE
        mfp.optimize();

        // See if there are predicates of the form <expr>=literal that can be
        // sped up using an index.
        if let MirRelationExpr::Get {
            id: Id::Global(id), ..
        } = *relation
        {
            let orig_mfp = mfp.clone();

            // Preparation for the literal constraints detection.
            Self::inline_literal_constraints(&mut mfp)?;
            Self::list_of_predicates_to_and_of_predicates(&mut mfp);
            Self::distribute_and_over_or(&mut mfp)?;
            Self::unary_and(&mut mfp);

            /// The above preparation might make the MFP more complicated, so we'll later want to
            /// either undo the preparation transformations or get back to `orig_mfp`.
            fn undo_preparation(
                mfp: &mut MapFilterProject,
                orig_mfp: &MapFilterProject,
                relation: &mut MirRelationExpr,
            ) -> Result<(), RecursionLimitError> {
                // undo list_of_predicates_to_and_of_predicates, distribute_and_over_or, unary_and
                // (It undoes the latter 2 through `MirScalarExp::reduce`.)
                CanonicalizeMfp::canonicalize_predicates(mfp, &relation);
                // undo inline_literal_constraints
                mfp.optimize();
                // We can usually undo, but sometimes not (see comment on `distribute_and_over_or`),
                // so in those cases we might have a more complicated MFP than the original MFP
                // (despite the removal of the literal constraints and/or contradicting OR args).
                // So let's use the simpler one.
                if CanonicalizeMfp::predicates_size(orig_mfp)?
                    < CanonicalizeMfp::predicates_size(mfp)?
                {
                    *mfp = orig_mfp.clone();
                }
                Ok(())
            }

            let removed_contradicting_or_args = Self::remove_impossible_or_args(&mut mfp)?;

            // todo: We might want to also call `canonicalize_equivalences`,
            // see near the end of literal_constraints.slt.

            let key_val = Self::detect_literal_constraints(&mfp, id, indexes);

            match key_val {
                None => {
                    // We didn't find a usable index, so no chance to remove literal constraints.
                    // But, we might have removed contradicting OR args.
                    if removed_contradicting_or_args {
                        undo_preparation(&mut mfp, &orig_mfp, relation)?;
                    } else {
                        // We didn't remove anything, so let's go with the original MFP.
                        mfp = orig_mfp;
                    }
                }
                Some((key, possible_vals)) => {
                    // We found a usable index. We'll try to remove the corresponding literal
                    // constraints.
                    if Self::remove_literal_constraints(&mut mfp, &key)
                        || removed_contradicting_or_args
                    {
                        // We were able to remove the literal constraints or contradicting OR args,
                        // so we would like to use this new MFP, so we try undoing the preparation.
                        undo_preparation(&mut mfp, &orig_mfp, relation)?;
                    } else {
                        // We were not able to remove the literal constraint, so `mfp` is
                        // equivalent to `orig_mfp`, but `orig_mfp` is often simpler (or the same).
                        mfp = orig_mfp;
                    }

                    // We transform the Get into a semi-join with a constant collection.
                    // E.g.: we go from something like
                    // `SELECT f1, f2, f3 FROM t WHERE t.f1 = lit1 AND t.f2 = lit2
                    // to
                    // `SELECT f1, f2, f3 FROM t, (SELECT * FROM (VALUES (lit1, lit2))) as filter_list
                    //  WHERE t.f1 = filter_list.column1 AND t.f2 = filter_list.column2`

                    let inp_id = id.clone();
                    let inp_typ = relation.typ();
                    let filter_list = MirRelationExpr::Constant {
                        rows: Ok(possible_vals.iter().map(|val| (val.clone(), 1)).collect()),
                        typ: mz_repr::RelationType {
                            column_types: key
                                .iter()
                                .map(|e| e.typ(&inp_typ.column_types))
                                .collect(),
                            keys: vec![],
                        },
                    };

                    *relation = MirRelationExpr::Join {
                        inputs: vec![relation.clone().arrange_by(&[key.clone()]), filter_list],
                        equivalences: key
                            .iter()
                            .enumerate()
                            .map(|(i, e)| {
                                vec![(*e).clone(), MirScalarExpr::column(i + inp_typ.arity())]
                            })
                            .collect(),
                        implementation: IndexedFilter(inp_id, key.clone(), possible_vals),
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
        // Canonicalize the MapFilterProject to Map-Filter-Project, in that order.
        if !mfp.is_identity() {
            let (map, filter, project) = mfp.as_map_filter_project();
            let total_arity = mfp.input_arity + map.len();
            if !map.is_empty() {
                *relation = relation.take_dangerous().map(map);
            }
            if !filter.is_empty() {
                *relation = relation.take_dangerous().filter(filter);
                crate::fusion::filter::Filter.action(relation);
            }
            if project.len() != total_arity || !project.iter().enumerate().all(|(i, o)| i == *o) {
                *relation = relation.take_dangerous().project(project);
            }
        }
        Ok(())
    }

    /// Detects literal constraints in an MFP on top of a get of `id`.
    /// For example, for `(f1 = 3 AND f2 = 5) OR (f1 = 7 AND f2 = 9)`,
    /// it returns `Some([f1, f2], [[3,5], [7,9]])`
    /// We can use an index if each argument of the OR includes a literal constraint on each of the
    /// key fields of the index. Extra predicates inside the OR arguments are ok.
    fn detect_literal_constraints(
        mfp: &MapFilterProject,
        id: GlobalId,
        indexes: &dyn IndexOracle,
    ) -> Option<(Vec<MirScalarExpr>, Vec<Row>)> {
        fn each_or_arg_constrains_each_key_field(
            key: &[MirScalarExpr],
            or_args: Vec<MirScalarExpr>,
        ) -> Option<Vec<Row>> {
            let mut literal_values = Vec::new();
            for or_arg in or_args {
                let mut row = Row::default();
                let mut packer = row.packer();
                for key_field in key {
                    let and_args = or_arg.and_or_args(VariadicFunc::And);
                    if let Some(literal) = and_args
                        .iter()
                        .find_map(|and_arg| and_arg.expr_eq_literal(key_field))
                    {
                        // (Note that the above find_map can find only 0 or 1 result, because
                        // of `remove_impossible_or_args`.)
                        packer.push(literal);
                    } else {
                        return None;
                    }
                }
                literal_values.push(row);
            }
            // We should deduplicate, because a constraint can be duplicated by
            // `distribute_and_over_or`. For example: `IN ('l1', 'l2') AND (a > 0 OR a < 5)`. Here,
            // the 2 args of the OR will cause the IN constraints to be duplicated. This doesn't
            // alter the meaning of the expression when evaluated as a filter, but if we extract
            // those literals 2 times into `literal_values` then the Peek code will look up those
            // keys from the index 2 times, leading to duplicate results.
            literal_values.sort();
            literal_values.dedup();
            Some(literal_values)
        }

        indexes
            .indexes_on(id)
            .filter_map(|key| {
                let possible_vals = if key.is_empty() {
                    None
                } else {
                    let or_args = Self::get_or_args(mfp);
                    each_or_arg_constrains_each_key_field(key, or_args)
                };
                possible_vals.map(|vals| (key.to_owned(), vals))
            })
            // Maximize number of predicates that are sped using a single index.
            .max_by_key(|(key, _val)| key.len())
    }

    /// Removes the expressions that [CanonicalizeMfp::detect_literal_constraints] found, if
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
        let or_args_residual = or_args
            .iter()
            .map(|or_arg| {
                let mut and_args = or_arg.and_or_args(VariadicFunc::And);
                // Note that `remove_impossible_or_args` made sure that inside each or_arg, each
                // expression can have only one literal constraint. So if we find one of the
                // key fields being literal constrained, then it's definitely that literal
                // constraint that detect_literal_constraints based it's return values on.
                //
                // This is important, because without `remove_impossible_or_args`, we might
                // have the situation here that or_arg would be something like
                // `a = 5 AND a = 8`, of which `detect_literal_constraints` found only the `a = 5`,
                // but here we would remove both the `a = 5` and the `a = 8`.
                and_args.retain(|and_arg| {
                    !key.iter()
                        .any(|key_field| matches!(and_arg.expr_eq_literal(key_field), Some(..)))
                });
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::And,
                    exprs: and_args,
                }
            })
            .collect::<Vec<_>>();
        assert!(or_args_residual.len() >= 1); // We already checked `or_args.len() == 0` above
        let or_args_residual_first = or_args_residual.get(0).unwrap();
        if or_args_residual
            .iter()
            .all(|or_arg| or_arg == or_args_residual_first)
        {
            // We can remove the literal constraint
            let new_pred = or_args_residual_first.clone();
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
    /// 2. Also, if an OR arg doesn't have any contradiction, this fn just deduplicates and sorts
    /// the AND arg list of that OR arg.
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
                let and_args_orig = and_args.clone();
                and_args.sort();
                and_args.dedup();
                if *and_args != and_args_orig {
                    changed = true;
                }
                // Deduplicated, so we cannot have something like `a = 5 AND a = 5`.
                // This means that if we now have `<expr1> = <literal1> AND <expr1> = <literal2>`,
                // then `literal1` is definitely not the same as `literal2`. This means that this
                // whole or_arg is a contradiction, because it's something like `a = 5 AND a = 8`.
                let literal_constrained_exprs = and_args
                    .iter()
                    .filter_map(|and_arg| and_arg.any_expr_eq_literal());
                if Self::has_duplicates(literal_constrained_exprs) {
                    changed = true;
                    to_remove.push(i);
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

    /// Determines whether the given iterable has any duplicates.
    fn has_duplicates<T>(iter: T) -> bool
    where
        T: IntoIterator,
        T::Item: std::hash::Hash + Eq,
    {
        let mut seen = std::collections::HashSet::new();
        // Insert each element. If any are not newly inserted then it's a duplicate.
        iter.into_iter().any(move |x| !seen.insert(x))
    }

    /// Returns the arguments of the predicate's top-level OR as a Vec.
    /// If there is no top-level OR, then interpret the predicate as a 1-arg OR, i.e., return a
    /// 1-element Vec.
    ///
    /// Assumes that [CanonicalizeMfp::list_of_predicates_to_and_of_predicates] has already run.
    fn get_or_args(mfp: &MapFilterProject) -> Vec<MirScalarExpr> {
        assert_eq!(mfp.predicates.len(), 1); // list_of_predicates_to_and_of_predicates ensured this
        let (_, pred) = mfp.predicates.get(0).unwrap();
        pred.and_or_args(VariadicFunc::Or)
    }

    /// Makes the job of [CanonicalizeMfp::detect_literal_constraints] easier by undoing some CSE to
    /// reconstruct literal constraints.
    fn inline_literal_constraints(mfp: &mut MapFilterProject) -> Result<(), RecursionLimitError> {
        let mut should_inline = vec![false; mfp.input_arity + mfp.expressions.len()];
        // Mark those expressions for inlining that are of the form
        // `<xxx> = <lit>` or `<lit> = <xxx>`.
        for (i, e) in mfp.expressions.iter().enumerate() {
            if let MirScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1,
                expr2,
            } = e
            {
                if matches!(**expr1, MirScalarExpr::Literal(..))
                    || matches!(**expr2, MirScalarExpr::Literal(..))
                {
                    should_inline[i + mfp.input_arity] = true;
                }
            }
        }
        // Whenever
        // `<Column(i)> = <lit>` or `<lit> = <Column(i)>`
        // appears in a predicate, mark the ith expression to be inlined.
        for (_before, p) in mfp.predicates.iter() {
            p.visit_post(&mut |e| {
                if let MirScalarExpr::CallBinary {
                    func: BinaryFunc::Eq,
                    expr1,
                    expr2,
                } = e
                {
                    if matches!(**expr1, MirScalarExpr::Literal(..)) {
                        if let MirScalarExpr::Column(col) = **expr2 {
                            if col >= mfp.input_arity {
                                should_inline[col] = true;
                            }
                        }
                    }
                    if matches!(**expr2, MirScalarExpr::Literal(..)) {
                        if let MirScalarExpr::Column(col) = **expr1 {
                            if col >= mfp.input_arity {
                                should_inline[col] = true;
                            }
                        }
                    }
                }
            })?;
        }
        // Perform the marked inlinings.
        mfp.perform_inlining(should_inline);
        Ok(())
    }

    /// MFPs have a Vec of predicates [p1, p2, ...], which logically represents `p1 AND p2 AND ...`.
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
    fn canonicalize_predicates(mfp: &mut MapFilterProject, relation: &MirRelationExpr) {
        let (map, mut predicates, project) = mfp.as_map_filter_project();
        let typ_after_map = relation.clone().map(map.clone()).typ();
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
    /// up the tree by 1 level, and a [MirScalarExpr::flatten_and_or] merges two ORs that are at
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
    /// then [MirScalarExpr::flatten_and_or]:
    /// (#0 = 1 AND #1 = 2) OR (#0 = 1 AND #1 = 4) OR (#0 = 8 AND #1 = 5)
    ///
    /// Note that [MirScalarExpr::undistribute_and_or] is not exactly an inverse to this because
    /// 1) it can undistribute both AND over OR and OR over AND.
    /// 2) it cannot always undo the distribution, because an expression might have multiple
    /// overlapping undistribution opportunities, see comment there.
    fn distribute_and_over_or(mfp: &mut MapFilterProject) -> Result<(), RecursionLimitError> {
        mfp.predicates.iter_mut().try_for_each(|(_, p)| {
            let mut old_p = MirScalarExpr::column(0);
            let orig_size = p.size()?;
            // We might make the expression exponentially larger, so we should have some limit.
            while old_p != *p && p.size()? < orig_size * 10 {
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
                    e.flatten_and_or();
                })?;
            }
            Ok(())
        })
    }

    /// For each of the arguments of the top-level OR (if no top-level OR, then interpret the whole
    /// expression as a 1-arg OR, see [CanonicalizeMfp::get_or_args]), check if it's an AND, and
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

    fn predicates_size(mfp: &MapFilterProject) -> Result<usize, RecursionLimitError> {
        let mut sum = 0;
        for (_, p) in mfp.predicates.iter() {
            sum = sum + p.size()?;
        }
        Ok(sum)
    }
}
