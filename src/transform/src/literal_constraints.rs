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
//! The detection is index-directed: for each candidate index we ask what the predicate says
//! about that index's key expressions, and read the answer off in a single pass. See
//! [`key_bounds`].
//!
//! E.g.: Logically, we go from something like
//! `SELECT f1, f2, f3 FROM t WHERE t.f1 = lit1 AND t.f2 = lit2`
//! to
//! `SELECT f1, f2, f3 FROM t, (SELECT * FROM (VALUES (lit1, lit2))) as filter_list
//!  WHERE t.f1 = filter_list.column1 AND t.f2 = filter_list.column2`

mod key_bounds;

use itertools::Itertools;
use key_bounds::{KeyBounds, literal_constrained_exprs, prune_unsatisfiable};
use mz_expr::JoinImplementation::IndexedFilter;
use mz_expr::visit::VisitChildren;
use mz_expr::{BinaryFunc, Id, MapFilterProject, MirRelationExpr, MirScalarExpr};
use mz_repr::{Diff, GlobalId, ReprRelationType, Row};

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
            let inp_typ = typ.clone();

            // Detection reads a copy with CSE undone, so that a literal equality hidden
            // behind a mapped column can still be matched against an index key. Nothing is
            // rewritten unless we end up using an index, so there is no preparation to
            // undo and no need to compare the result against the original MFP.
            let mut probe_mfp = mfp.clone();
            Self::inline_literal_constraints(&mut probe_mfp);

            // Every expression the predicate pins to literal values anywhere. Treating
            // these as a key asks what the predicate says about all of them at once, which
            // answers two questions at no extra cost: whether the predicate contradicts
            // itself, and what key a user whose index was too wide should have indexed.
            let constrained =
                literal_constrained_exprs(probe_mfp.predicates.iter().map(|(_, p)| p));

            // Disjuncts that contradict themselves are dead weight in the filter, and
            // pruning them needs no index. Done before detection so that detection sees the
            // simplified predicate.
            let (pruned, empty) = Self::prune_unsatisfiable(&mut probe_mfp, &constrained);

            if empty {
                // Some expression is pinned to two different values, so nothing can pass
                // the filter. This needs no index.
                relation.take_safely(Some(inp_typ));
            } else if let Some((idx_id, key, possible_vals)) =
                Self::detect_literal_constraints(&probe_mfp, id, &constrained, transform_ctx)
            {
                // The lookup enforces every predicate that is exactly a constraint on the
                // key, so those can come out of the filter.
                if Self::remove_literal_constraints(&mut probe_mfp, &key) || pruned {
                    // Redo the CSE that inlining undid.
                    probe_mfp.optimize();
                    mfp = probe_mfp;
                }

                let inp_id = id.clone();
                let filter_list = MirRelationExpr::Constant {
                    rows: Ok(possible_vals
                        .iter()
                        .map(|val| (val.clone(), Diff::ONE))
                        .collect()),
                    typ: ReprRelationType {
                        column_types: key
                            .iter()
                            .map(|e| e.typ(&inp_typ.column_types).scalar_type.nullable(false))
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
                        // Both the lowering and EXPLAIN depend on this.
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
                        implementation: IndexedFilter(inp_id, idx_id, key.clone(), possible_vals),
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
            } else if pruned {
                probe_mfp.optimize();
                mfp = probe_mfp;
            }
        }

        CanonicalizeMfp::rebuild_mfp(mfp, relation);

        Ok(())
    }

    /// Detects literal constraints in an MFP on top of a Get of `id`, and a matching index that
    /// can be used to speed up the Filter of the MFP.
    ///
    /// For example, if there is an index on `(f1, f2)`, and the Filter is
    /// `(f1 = 3 AND f2 = 5) OR (f1 = 7 AND f2 = 9)`, it returns `Some([f1, f2], [[3,5], [7,9]])`.
    ///
    /// The question is asked once per candidate index, about that index's key expressions.
    /// Predicate structure that says nothing about those expressions costs a single visit
    /// and contributes nothing, which is what keeps the work linear in the predicate size
    /// no matter how the disjunctions in it are arranged.
    ///
    /// Returns (idx_id, idx_key, values to lookup in the index).
    fn detect_literal_constraints(
        mfp: &MapFilterProject,
        get_id: GlobalId,
        constrained: &[MirScalarExpr],
        transform_ctx: &mut TransformCtx,
    ) -> Option<(GlobalId, Vec<MirScalarExpr>, Vec<Row>)> {
        // Checks whether an index with the specified key can be used to speed up the given
        // filter. See comment of `IndexMatch`.
        fn match_index(key: &[MirScalarExpr], mfp: &MapFilterProject) -> IndexMatch {
            if key.is_empty() {
                // Nothing to do with an index that has an empty key.
                return IndexMatch::UnusableNoSubset;
            }
            if !key.iter().all_unique() {
                // This is a weird index. Why does it have duplicate key expressions?
                return IndexMatch::UnusableNoSubset;
            }
            let bounds = LiteralConstraints::key_bounds(mfp, key);
            if bounds.bounds_every_field() {
                match bounds.lookup_values() {
                    Some(vals) => IndexMatch::Usable(vals, bounds.inv_cast),
                    // Too many values to be worth looking up one at a time. There is no
                    // narrower index that would help, so there is nothing to advise.
                    None => IndexMatch::UnusableNoSubset,
                }
            } else {
                let subset = bounds
                    .bounded_fields()
                    .into_iter()
                    .map(|i| key[i].clone())
                    .collect_vec();
                if subset.is_empty() {
                    IndexMatch::UnusableNoSubset
                } else {
                    IndexMatch::UnusableTooWide(subset)
                }
            }
        }

        let index_matches = transform_ctx
            .indexes
            .indexes_on(get_id)
            .map(|(index_id, key)| (index_id, key.to_owned(), match_index(key, mfp)))
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

        if result.is_none() {
            // Let's see if we can give a hint to the user.
            //
            // The recommendation is index-blind: gather every expression the predicate
            // pins to literal values anywhere, then keep those it pins in all cases. An
            // index on exactly those would have been usable.
            let recommended_key = LiteralConstraints::key_bounds(mfp, constrained)
                .bounded_fields()
                .into_iter()
                .map(|i| constrained[i].clone())
                .collect_vec();
            if recommended_key.is_empty() {
                return result;
            }
            index_matches
                .into_iter()
                .for_each(|(index_id, index_key, index_match)| {
                    match index_match {
                        IndexMatch::UnusableTooWide(usable_subset) => {
                            // see comment of `UnusableTooWide`
                            assert!(!usable_subset.is_empty());
                            // Determine literal values that we would get if the index was on
                            // `usable_subset`.
                            let bounds = LiteralConstraints::key_bounds(mfp, &usable_subset);
                            let Some(literal_values) = bounds.lookup_values() else {
                                return;
                            };

                            transform_ctx.df_meta.push_optimizer_notice_dedup(
                                IndexTooWideForLiteralConstraints {
                                    index_id,
                                    index_key,
                                    usable_subset,
                                    literal_values,
                                    index_on_id: get_id,
                                    recommended_key: recommended_key.clone(),
                                },
                            )
                        }
                        _ => (),
                    }
                });
        }

        result
    }

    /// Prunes unsatisfiable disjuncts from every predicate. See [`prune_unsatisfiable`].
    ///
    /// Returns whether anything was pruned, and whether the whole relation is now empty.
    fn prune_unsatisfiable(mfp: &mut MapFilterProject, key: &[MirScalarExpr]) -> (bool, bool) {
        if key.is_empty() {
            return (false, false);
        }
        let (map, mut predicates, project) = mfp.as_map_filter_project();
        let (changed, empty) = prune_unsatisfiable(predicates.iter_mut(), key);
        if changed {
            *mfp = MapFilterProject::new(mfp.input_arity)
                .map(map)
                .filter(predicates)
                .project(project);
        }
        (changed, empty)
    }

    /// What the MFP's predicates jointly imply about `key`.
    ///
    /// The predicate list is an implicit conjunction, so the per-predicate bounds are
    /// combined the same way an `AND` node's arguments would be.
    fn key_bounds(mfp: &MapFilterProject, key: &[MirScalarExpr]) -> KeyBounds {
        KeyBounds::conjunction(mfp.predicates.iter().map(|(_, p)| p), key)
    }

    /// Removes the predicates that are exactly constraints on `key`, since the lookup that
    /// [LiteralConstraints::detect_literal_constraints] found now enforces them. Returns
    /// whether it removed anything.
    ///
    /// A predicate is removable when it is equivalent to a bound on the key fields, so that
    /// dropping it loses nothing. `(f1 = 3 AND f2 = 5) OR (f1 = 7 AND f2 = 5)` with a key
    /// of just `f1` is not removable, because the residual `f2 = 5` is entangled with the
    /// `f1` constraint. `f1 IN (3, 7) AND f2 = 5` is: the first predicate goes, the second
    /// stays.
    ///
    /// NOTE: This is sound only because the lookup values are the intersection of what
    /// *every* predicate implies, including the ones we keep. So the retained predicates
    /// can only narrow the key further, never widen it past what a removed predicate
    /// allowed.
    fn remove_literal_constraints(mfp: &mut MapFilterProject, key: &[MirScalarExpr]) -> bool {
        let (map, predicates, project) = mfp.as_map_filter_project();
        let kept = predicates
            .into_iter()
            .filter(|p| !KeyBounds::extract(p, key).exact())
            .collect_vec();
        if kept.len() == mfp.predicates.len() {
            return false;
        }
        *mfp = MapFilterProject::new(mfp.input_arity)
            .map(map)
            .filter(kept)
            .project(project);
        true
    }

    /// Makes the job of [LiteralConstraints::detect_literal_constraints] easier by undoing some
    /// CSE to reconstruct literal constraints.
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
}

/// Whether an index is usable to speed up a Filter with literal constraints.
#[derive(Clone)]
enum IndexMatch {
    /// The index is usable, that is, the predicate bounds every key field to literal values.
    ///
    /// The `Vec<Row>` has the key values to look up, one Row per value of the whole key.
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
