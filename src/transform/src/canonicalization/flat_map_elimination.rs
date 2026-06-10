// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! For a `FlatMap` where the table function's arguments are all constants, turns it into `Map` if
//! only 1 row is produced by the table function, or turns it into an empty constant collection if 0
//! rows are produced by the table function.
//!
//! It does an additional optimization on the `Wrap` table function: when `Wrap`'s width is larger
//! than its number of arguments, it removes the `FlatMap Wrap ...`, because such `Wrap`s would have
//! no effect.

use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::{Id, MapFilterProject, MirRelationExpr, MirScalarExpr, TableFunc};
use mz_repr::{Diff, RowArena};

use crate::TransformCtx;

/// Attempts to eliminate FlatMaps that are sure to have 0 or 1 results on each input row.
#[derive(Debug)]
pub struct FlatMapElimination;

impl crate::Transform for FlatMapElimination {
    fn name(&self) -> &'static str {
        "FlatMapElimination"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "flat_map_elimination")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_post(&mut Self::action);
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl FlatMapElimination {
    /// Apply `FlatMapElimination` to the root of the given `MirRelationExpr`.
    pub fn action(relation: &mut MirRelationExpr) {
        // Treat Wrap specially: we can sometimes optimize it out even when it has non-literal
        // arguments.
        //
        // (No need to look for WithOrdinality here, as that never occurs with Wrap: users can't
        // call Wrap directly; we only create calls to Wrap ourselves, and we don't use
        // WithOrdinality on it.)
        if let MirRelationExpr::FlatMap { func, exprs, input } = relation {
            if let TableFunc::Wrap { width, .. } = func {
                if *width >= exprs.len() {
                    *relation = input.take_dangerous().map(std::mem::take(exprs));
                }
            }
        }
        // Strength-reduce a `FlatMap eval_relation(..)` whose housed relation is a
        // row-preserving, single-valued `Map`/`Project` over the placeholder input
        // row back to a plain `Map`/`Project` on the input. Such an `EvalRelation`
        // emits exactly one output row per input row, so it needs neither a table
        // function nor (downstream) an arrangement + join input.
        if let MirRelationExpr::FlatMap {
            func: TableFunc::EvalRelation { .. },
            ..
        } = relation
        {
            Self::eliminate_eval_relation(relation);
        }
        // For all other table functions (and Wraps that are not covered by the above), check
        // whether all arguments are literals (with no errors), in which case we'll evaluate the
        // table function and check how many output rows it has, and maybe turn the FlatMap into
        // something simpler.
        if let MirRelationExpr::FlatMap { func, exprs, input } = relation {
            if let Some(args) = exprs
                .iter()
                .map(|e| e.as_literal_non_error())
                .collect::<Option<Vec<_>>>()
            {
                let temp_storage = RowArena::new();
                let (first, second) = match func.eval(&args, &temp_storage) {
                    Ok(mut r) => (r.next(), r.next()),
                    // don't play with errors
                    Err(_) => return,
                };
                match (first, second) {
                    // The table function evaluated to an empty collection.
                    (None, _) => {
                        relation.take_safely(None);
                    }
                    // The table function evaluated to a collection with exactly 1 row.
                    (Some((first_row, Diff::ONE)), None) => {
                        let types = func.output_type().column_types;
                        let map_exprs = first_row
                            .into_iter()
                            .zip_eq(types)
                            .map(|(d, typ)| MirScalarExpr::literal_ok(d, typ.scalar_type))
                            .collect();
                        *relation = input.take_dangerous().map(map_exprs);
                    }
                    // The table function evaluated to a collection with more than 1 row; nothing to do.
                    _ => {}
                }
            }
        }
    }

    /// If `relation` is a `FlatMap eval_relation(..)` whose housed relation is a
    /// row-preserving, single-valued `Map`/`Project` over the placeholder input
    /// row, rewrite it in place to a plain `Map`/`Project` on `input`.
    ///
    /// `eval_relation` evaluates the housed relation per input row, binding the
    /// `FlatMap` argument `exprs` to the placeholder `Get(Id::Local(input_id))`,
    /// and the `FlatMap` emits `input_row ++ housed_output` for each input row
    /// (see `mz_expr::relation::eval`). If the housed relation is exactly a
    /// `MapFilterProject` (no `Filter`, since predicates can drop rows) rooted at
    /// `Get(input_id)`, then it produces exactly one row per input row, computed
    /// as a deterministic function of the placeholder columns. We can therefore
    /// inline it: substitute each placeholder column reference with the
    /// corresponding `exprs` entry and apply the resulting `Map`/`Project`
    /// directly to `input`.
    fn eliminate_eval_relation(relation: &mut MirRelationExpr) {
        let MirRelationExpr::FlatMap { func, input, exprs } = relation else {
            return;
        };
        let TableFunc::EvalRelation {
            relation: housed,
            input_id,
            ..
        } = func
        else {
            return;
        };

        // Extract the longest `Map`/`Filter`/`Project` chain at the root of the
        // housed relation. We require the residual leaf to be exactly the
        // placeholder `Get(Id::Local(input_id))`; anything else (a `Constant`, a
        // `Reduce`, a `TopK`, ...) could change cardinality and is left alone.
        let (mfp, leaf) = MapFilterProject::extract_from_expression(housed);
        match leaf {
            MirRelationExpr::Get {
                id: Id::Local(id), ..
            } if id == input_id => {}
            _ => return,
        }
        // A `Filter` can drop rows, breaking the one-row-out-per-row-in property.
        if !mfp.predicates.is_empty() {
            return;
        }
        // The placeholder's arity must match the number of `FlatMap` arguments:
        // each placeholder column `c` is bound to `exprs[c]`.
        let key_arity = exprs.len();
        if mfp.input_arity != key_arity {
            return;
        }

        let input_arity = input.arity();
        let MapFilterProject {
            expressions,
            projection,
            ..
        } = mfp;

        // Translate a column reference in the housed relation's column space into
        // the rewritten relation's column space, which is `input`'s columns
        // (`0..input_arity`) followed by the housed `expressions`
        // (`input_arity..`):
        //
        // * a placeholder column `c < key_arity` is the value bound to the
        //   placeholder, i.e. `exprs[c]`;
        // * a housed-expression column `key_arity + i` lives at `input_arity + i`.
        let translate = |c: usize| -> MirScalarExpr {
            if c < key_arity {
                exprs[c].clone()
            } else {
                MirScalarExpr::column(input_arity + (c - key_arity))
            }
        };
        // Substitute placeholder/expression column references inside a housed
        // scalar expression so it reads from the rewritten column space.
        let substitute = |mut e: MirScalarExpr| -> MirScalarExpr {
            #[allow(deprecated)]
            e.visit_mut_post_nolimit(&mut |node| {
                if let MirScalarExpr::Column(c, _name) = node {
                    *node = translate(*c);
                }
            });
            e
        };

        // The housed `expressions` become `Map` scalars appended to `input`.
        let map_scalars: Vec<MirScalarExpr> = expressions.into_iter().map(substitute).collect();
        // The `eval_relation` output columns are the housed `projection`; append
        // them after `input`'s columns so the result mirrors the `FlatMap`'s
        // `input_row ++ housed_output` row shape.
        let output_scalars: Vec<MirScalarExpr> = projection.into_iter().map(&translate).collect();

        let output_count = output_scalars.len();
        let mut new = input.take_dangerous().map(map_scalars);
        let mapped_arity = new.arity();
        // Append the output columns, then project away the intermediate housed
        // expressions, keeping `input`'s columns followed by the output columns.
        new = new.map(output_scalars).project(
            (0..input_arity)
                .chain(mapped_arity..(mapped_arity + output_count))
                .collect::<Vec<_>>(),
        );
        *relation = new;
    }
}
