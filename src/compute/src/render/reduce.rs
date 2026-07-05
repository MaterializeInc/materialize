// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reduction dataflow construction.
//!
//! Consult [ReducePlan] documentation for details.

use std::collections::BTreeMap;

use columnation::{Columnation, CopyRegion};
use dec::OrderedDecimal;
use differential_dataflow::Diff as _;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::cursor::{
    BatchCursor, BatchDiff, BatchValOwn, Cursor, Navigable,
};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Builder, Trace};
use differential_dataflow::{Data, VecCollection};
use itertools::Itertools;
use mz_compute_types::dyncfgs::{ENABLE_COMPUTE_TEMPORAL_BUCKETING, TEMPORAL_BUCKETING_SUMMARY};
use mz_compute_types::plan::ArrangementStrategy;
use mz_compute_types::plan::reduce::{
    AccumulablePlan, BasicPlan, BucketedPlan, HierarchicalPlan, KeyValPlan, MonotonicPlan,
    ReducePlan, ReductionType, SingleBasicPlan, reduction_type,
};
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_expr::{AggregateExpr, AggregateFunc, EvalError, SafeMfpPlan};
use mz_ore::cast::CastLossy;
use mz_repr::adt::numeric::{self, Numeric, NumericAgg};
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{Datum, DatumVec, Diff, Row, RowArena, SharedRow};
use mz_timely_util::columnation::ColumnationChunker;
use mz_timely_util::operator::CollectionExt;
use num_traits::Float;
use serde::{Deserialize, Serialize};
use timely::Container;
use timely::container::{CapacityContainerBuilder, PushInto};
use tracing::warn;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::extensions::reduce::{ClearContainer, MzReduce};
use crate::render::context::{CollectionBundle, Context};
use crate::render::errors::DataflowErrorSer;
use crate::render::errors::MaybeValidatingRow;
use crate::render::reduce::monoids::{ReductionMonoid, get_monoid};
use crate::render::{ArrangementFlavor, Pairer, RenderTimestamp};
use crate::typedefs::{
    ErrBatcher, ErrBuilder, KeyBatcher, RowErrBuilder, RowErrSpine, RowRowAgent, RowRowArrangement,
    RowRowSpine, RowSpine, RowValSpine,
};
use mz_row_spine::{
    DatumSeq, RowBatcher, RowBuilder, RowRowBatcher, RowRowBuilder, RowValBatcher, RowValBuilder,
};

impl<'scope, T: RenderTimestamp> Context<'scope, T> {
    /// Renders a `MirRelationExpr::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    pub fn render_reduce(
        &self,
        input_key: Option<Vec<LirScalarExpr>>,
        input: CollectionBundle<'scope, T>,
        key_val_plan: KeyValPlan,
        reduce_plan: ReducePlan,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
        temporal_bucketing_strategy: ArrangementStrategy,
    ) -> CollectionBundle<'scope, T>
    where
        T: crate::render::MaybeBucketByTime,
    {
        input.scope().region_named("Reduce", |inner| {
            let KeyValPlan {
                mut key_plan,
                mut val_plan,
            } = key_val_plan;
            let key_arity = key_plan.projection.len();
            let mut datums = DatumVec::new();

            // Determine the columns we'll need from the row.
            let mut demand = Vec::new();
            demand.extend(key_plan.demand());
            demand.extend(val_plan.demand());
            demand.sort();
            demand.dedup();

            // remap column references to the subset we use.
            let mut demand_map = BTreeMap::new();
            for column in demand.iter() {
                demand_map.insert(*column, demand_map.len());
            }
            let demand_map_len = demand_map.len();
            key_plan.permute_fn(|c| demand_map[&c], demand_map_len);
            val_plan.permute_fn(|c| demand_map[&c], demand_map_len);
            let max_demand = demand.iter().max().map(|x| *x + 1).unwrap_or(0);
            let skips = mz_compute_types::plan::reduce::convert_indexes_to_skips(demand);

            let (key_val_input, err) = input
                .enter_region(inner)
                .flat_map::<_, ConsolidatingContainerBuilder<Vec<((Row, Row), T, Diff)>>, _>(
                    input_key.map(|k| (k, None)),
                    max_demand,
                    move |row_datums, time, diff, ok_session, err_session| {
                        let mut row_builder = SharedRow::get();
                        let temp_storage = RowArena::new();

                        let mut row_iter = row_datums.drain(..);
                        let mut datums_local = datums.borrow();
                        // Unpack only the demanded columns.
                        for skip in skips.iter() {
                            datums_local.push(row_iter.nth(*skip).unwrap());
                        }

                        // Evaluate the key expressions.
                        let key = key_plan.evaluate_into(
                            &mut datums_local,
                            &temp_storage,
                            &mut row_builder,
                        );
                        let key = match key {
                            Err(e) => {
                                err_session.give((e.into(), time, diff));
                                return 1;
                            }
                            Ok(Some(key)) => key.clone(),
                            Ok(None) => panic!("Row expected as no predicate was used"),
                        };

                        // Evaluate the value expressions.
                        // The prior evaluation may have left additional columns we should delete.
                        datums_local.truncate(skips.len());
                        let val = val_plan.evaluate_into(
                            &mut datums_local,
                            &temp_storage,
                            &mut row_builder,
                        );
                        let val = match val {
                            Err(e) => {
                                err_session.give((e.into(), time, diff));
                                return 1;
                            }
                            Ok(Some(val)) => val.clone(),
                            Ok(None) => panic!("Row expected as no predicate was used"),
                        };

                        ok_session.give(((key, val), time, diff));
                        1
                    },
                );

            // Bucket the keyed `(key, val)` stream when lowering chose `TemporalBucketing`.
            // `Reduce` builds its own arrangement via `KeyValPlan`, bypassing
            // `ensure_collections`, so the strategy is plumbed through `PlanNode::Reduce`
            // rather than inferred at the arrangement site. No-op for `Direct`.
            let key_val_collection = key_val_input.as_collection();
            let key_val_collection = if matches!(
                temporal_bucketing_strategy,
                ArrangementStrategy::TemporalBucketing
            ) && ENABLE_COMPUTE_TEMPORAL_BUCKETING.get(&self.config_set)
            {
                let summary: mz_repr::Timestamp = TEMPORAL_BUCKETING_SUMMARY
                    .get(&self.config_set)
                    .try_into()
                    .expect("must fit");
                T::maybe_apply_temporal_bucketing(
                    key_val_collection.inner,
                    self.as_of_frontier.clone(),
                    summary,
                )
            } else {
                key_val_collection
            };

            // Render the reduce plan
            self.render_reduce_plan(reduce_plan, key_val_collection, err, key_arity, mfp_after)
                .leave_region(self.scope)
        })
    }

    /// Render a dataflow based on the provided plan.
    ///
    /// The output will be an arrangements that looks the same as if
    /// we just had a single reduce operator computing everything together, and
    /// this arrangement can also be re-used.
    fn render_reduce_plan<'s>(
        &self,
        plan: ReducePlan,
        collection: VecCollection<'s, T, (Row, Row), Diff>,
        err_input: VecCollection<'s, T, DataflowErrorSer, Diff>,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
    ) -> CollectionBundle<'s, T> {
        let mut errors = Default::default();
        let arrangement =
            self.render_reduce_plan_inner(plan, collection, &mut errors, key_arity, mfp_after);
        let errs: KeyCollection<_, _, _> = err_input.concatenate(errors).into();
        CollectionBundle::from_columns(
            0..key_arity,
            ArrangementFlavor::Local(
                arrangement,
                errs.mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
                    "Arrange bundle err",
                ),
            ),
        )
    }

    fn render_reduce_plan_inner<'s>(
        &self,
        plan: ReducePlan,
        collection: VecCollection<'s, T, (Row, Row), Diff>,
        errors: &mut Vec<VecCollection<'s, T, DataflowErrorSer, Diff>>,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
    ) -> Arranged<'s, RowRowAgent<T, Diff>> {
        // TODO(vmarcos): Arrangement specialization here could eventually be extended to keys,
        // not only values (database-issues#6658).
        let arrangement = match plan {
            // If we have no aggregations or just a single type of reduction, we
            // can go ahead and render them directly.
            ReducePlan::Distinct => {
                let (arranged_output, errs) = self.build_distinct(collection, mfp_after);
                errors.push(errs);
                arranged_output
            }
            ReducePlan::Accumulable(expr) => {
                let (arranged_output, errs) =
                    self.build_accumulable(collection, expr, key_arity, mfp_after);
                errors.push(errs);
                arranged_output
            }
            ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(expr)) => {
                let (output, errs) = self.build_monotonic(collection, expr, mfp_after);
                errors.push(errs);
                output
            }
            ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(expr)) => {
                let (output, errs) = self.build_bucketed(collection, expr, key_arity, mfp_after);
                errors.push(errs);
                output
            }
            ReducePlan::Basic(BasicPlan::Single(SingleBasicPlan {
                expr,
                fused_unnest_list,
            })) => {
                // Note that we skip validating for negative diffs when we have a fused unnest list,
                // because this is already a CPU-intensive situation due to the non-incrementalness
                // of window functions.
                let validating = !fused_unnest_list;
                let (output, errs) = self.build_basic_aggregate(
                    collection,
                    0,
                    &expr,
                    validating,
                    key_arity,
                    mfp_after,
                    fused_unnest_list,
                );
                if validating {
                    errors.push(errs.expect("validation should have occurred as it was requested"));
                }
                output
            }
            ReducePlan::Basic(BasicPlan::Multiple(aggrs)) => {
                let (output, errs) =
                    self.build_basic_aggregates(collection, aggrs, key_arity, mfp_after);
                errors.push(errs);
                output
            }
        };
        arrangement
    }

    /// Build the dataflow to compute the set of distinct keys.
    fn build_distinct<'s>(
        &self,
        collection: VecCollection<'s, T, (Row, Row), Diff>,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
    ) -> (
        Arranged<'s, TraceAgent<RowRowSpine<T, Diff>>>,
        VecCollection<'s, T, DataflowErrorSer, Diff>,
    ) {
        let error_logger = self.error_logger();

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());

        let arranged = collection
            .mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >(
                "Arranged DistinctBy",
            );
        let output = arranged
            .clone()
            .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                "DistinctBy",
                move |key, _input, output| {
                    let temp_storage = RowArena::new();
                    let mut datums_local = datums1.borrow();
                    key.extend_datums(&temp_storage, &mut datums_local, None);

                    // Note that the key contains all the columns in a `Distinct` and that `mfp_after` is
                    // required to preserve the key. Therefore, if `mfp_after` maps, then it must project
                    // back to the key. As a consequence, we can treat `mfp_after` as a filter here.
                    if mfp_after1
                        .as_ref()
                        .map(|mfp| mfp.evaluate_inner(&mut datums_local, &temp_storage))
                        .unwrap_or(Ok(true))
                        == Ok(true)
                    {
                        // We're pushing a unit value here because the key is implicitly added by the
                        // arrangement, and the permutation logic takes care of using the key part of the
                        // output.
                        output.push((Row::default(), Diff::ONE));
                    }
                },
            );
        let errors = arranged.mz_reduce_abelian::<_, RowErrBuilder<_, _>, RowErrSpine<_, _>>(
            "DistinctByErrorCheck",
            move |key, input: &[(_, Diff)], output: &mut Vec<(DataflowErrorSer, _)>| {
                for (_, count) in input.iter() {
                    if count.is_positive() {
                        continue;
                    }
                    let message = "Non-positive multiplicity in DistinctBy";
                    error_logger.log(message, &format!("row={key:?}, count={count}"));
                    output.push((EvalError::Internal(message.into()).into(), Diff::ONE));
                    return;
                }
                // If `mfp_after` can error, then evaluate it here.
                let Some(mfp) = &mfp_after2 else { return };
                let temp_storage = RowArena::new();
                let mut datums_local = datums2.borrow();
                key.extend_datums(&temp_storage, &mut datums_local, None);

                if let Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage) {
                    output.push((e.into(), Diff::ONE));
                }
            },
        );
        (output, errors.as_collection(|_k, v| v.clone()))
    }

    /// Build the dataflow to compute and arrange multiple non-accumulable,
    /// non-hierarchical aggregations on `input`.
    ///
    /// This function assumes that we are explicitly rendering multiple basic aggregations.
    /// For each aggregate, we render a different reduce operator, and then fuse
    /// results together into a final arrangement that presents all the results
    /// in the order specified by `aggrs`.
    fn build_basic_aggregates<'s>(
        &self,
        input: VecCollection<'s, T, (Row, Row), Diff>,
        aggrs: Vec<AggregateExpr>,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
    ) -> (
        RowRowArrangement<'s, T>,
        VecCollection<'s, T, DataflowErrorSer, Diff>,
    ) {
        // We are only using this function to render multiple basic aggregates and
        // stitch them together. If that's not true we should complain.
        if aggrs.len() <= 1 {
            self.error_logger().soft_panic_or_log(
                "Too few aggregations when building basic aggregates",
                &format!("len={}", aggrs.len()),
            )
        }
        let mut err_output = None;
        let mut to_collect = Vec::new();
        for (index, aggr) in aggrs.into_iter().enumerate() {
            let (result, errs) = self.build_basic_aggregate(
                input.clone(),
                index,
                &aggr,
                err_output.is_none(),
                key_arity,
                None,
                false,
            );
            if errs.is_some() {
                err_output = errs
            }
            to_collect
                .push(result.as_collection(move |key, val| (key.to_row(), (index, val.to_row()))));
        }

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());

        let arranged = differential_dataflow::collection::concatenate(input.scope(), to_collect)
            .mz_arrange::<
                ColumnationChunker<_>,
                RowValBatcher<_, _, _>,
                RowValBuilder<_, _, _>,
                RowValSpine<_, _, _>,
            >(
            "Arranged ReduceFuseBasic input",
        );

        let output = arranged
            .clone()
            .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>("ReduceFuseBasic", {
                move |key, input, output| {
                    let temp_storage = RowArena::new();
                    let mut datums_local = datums1.borrow();
                    key.extend_datums(&temp_storage, &mut datums_local, None);
                    let key_len = datums_local.len();

                    for ((_, row), _) in input.iter() {
                        datums_local.push(row.unpack_first());
                    }

                    if let Some(row) =
                        evaluate_mfp_after(&mfp_after1, &mut datums_local, &temp_storage, key_len)
                    {
                        output.push((row, Diff::ONE));
                    }
                }
            });
        // If `mfp_after` can error, then we need to render a paired reduction
        // to scan for these potential errors. Note that we cannot directly use
        // `mz_timely_util::reduce::ReduceExt::reduce_pair` here because we only
        // conditionally render the second component of the reduction pair.
        let validation_errs = err_output.expect("expected to validate in at least one aggregate");
        if let Some(mfp) = mfp_after2 {
            let mfp_errs = arranged
                .mz_reduce_abelian::<_, RowErrBuilder<_, _>, RowErrSpine<_, _>>(
                    "ReduceFuseBasic Error Check",
                    move |key, input, output| {
                        // Since negative accumulations are checked in at least one component
                        // aggregate, we only need to look for MFP errors here.
                        let temp_storage = RowArena::new();
                        let mut datums_local = datums2.borrow();
                        key.extend_datums(&temp_storage, &mut datums_local, None);

                        for ((_, row), _) in input.iter() {
                            datums_local.push(row.unpack_first());
                        }

                        if let Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage) {
                            output.push((e.into(), Diff::ONE));
                        }
                    },
                )
                .as_collection(|_, v| v.clone());
            (output, validation_errs.concat(mfp_errs))
        } else {
            (output, validation_errs)
        }
    }

    /// Build the dataflow to compute a single basic aggregation.
    ///
    /// This method also applies distinctness if required.
    fn build_basic_aggregate<'s>(
        &self,
        input: VecCollection<'s, T, (Row, Row), Diff>,
        index: usize,
        aggr: &AggregateExpr,
        validating: bool,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
        fused_unnest_list: bool,
    ) -> (
        RowRowArrangement<'s, T>,
        Option<VecCollection<'s, T, DataflowErrorSer, Diff>>,
    ) {
        let AggregateExpr {
            func,
            expr: _,
            distinct,
        } = aggr.clone();

        // Extract the value we were asked to aggregate over.
        let mut partial = input.map(move |(key, row)| {
            let mut row_builder = SharedRow::get();
            let value = row.iter().nth(index).unwrap();
            row_builder.packer().push(value);
            (key, row_builder.clone())
        });

        let mut err_output = None;

        // If `distinct` is set, we restrict ourselves to the distinct `(key, val)`.
        if distinct {
            // We map `(Row, Row)` to `Row` to take advantage of `Row*Spine` types.
            let pairer = Pairer::new(key_arity);
            let keyed = partial.map(move |(key, val)| pairer.merge(&key, &val));
            if validating {
                let (oks, errs) = self
                    .build_reduce_inaccumulable_distinct::<
                        RowValBuilder<Result<(), String>, _, _>,
                        RowValSpine<Result<(), String>, _, _>,
                    >(keyed, None)
                    .as_collection(|k, v| {
                        (
                            k.to_row(),
                            v.as_ref()
                                .map(|&()| ())
                                .map_err(|m| m.as_str().into()),
                        )
                    })
                    .map_fallible::<
                        CapacityContainerBuilder<_>,
                        CapacityContainerBuilder<_>,
                        _,
                        _,
                        _,
                    >(
                        "Demux Errors",
                        move |(key_val, result)| match result {
                            Ok(()) => Ok(pairer.split(&key_val)),
                            Err(m) => {
                                Err(EvalError::Internal(m).into())
                            }
                        },
                    );
                err_output = Some(errs);
                partial = oks;
            } else {
                partial = self
                    .build_reduce_inaccumulable_distinct::<RowBuilder<_, _>, RowSpine<_, _>>(
                        keyed,
                        Some(" [val: empty]"),
                    )
                    .as_collection(move |key_val_iter, _| pairer.split(key_val_iter));
            }
        }

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mut datums_key_1 = DatumVec::new();
        let mut datums_key_2 = DatumVec::new();
        // Scratch buffers for decoding each input value's (single) datum into the
        // arena, so the aggregates iterate arena-resident datums rather than the
        // packed value bytes — a prerequisite for compressed value representations.
        let mut vals1 = DatumVec::new();
        let mut vals2 = DatumVec::new();
        let mut vals_key_1 = DatumVec::new();
        let mut vals_key_2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let func2 = func.clone();

        let name = if !fused_unnest_list {
            "ReduceInaccumulable"
        } else {
            "FusedReduceUnnestList"
        };
        let arranged = partial
            .mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >(&format!(
                "Arranged {name}"
            ));
        let oks = if !fused_unnest_list {
            arranged
                .clone()
                .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>(name, {
                    move |key, source, target| {
                        let temp_storage = RowArena::new();
                        // Decode each input value's single datum into the arena, reusing one
                        // scratch buffer; the datum is `Copy` and is copied out before the
                        // buffer is overwritten on the next row. We pass the multiplicity
                        // through (unlike in hierarchical aggregation) because we don't know
                        // that the aggregation method is not sensitive to the number of
                        // records. The aggregate decides how to consume it.
                        let mut val_scratch = vals1.borrow();
                        let iter = source.iter().map(|(v, w)| {
                            val_scratch.clear();
                            v.extend_datums(&temp_storage, &mut val_scratch, Some(1));
                            (val_scratch[0], *w)
                        });

                        let mut datums_local = datums1.borrow();
                        key.extend_datums(&temp_storage, &mut datums_local, None);
                        let key_len = datums_local.len();
                        datums_local.push(
                        // Note that this is not necessarily a window aggregation, in which case
                        // `eval_with_fast_window_agg` delegates to the normal `eval`.
                        func.eval_with_fast_window_agg::<_, window_agg_helpers::OneByOneAggrImpls>(
                            iter,
                            &temp_storage,
                        ),
                    );

                        if let Some(row) = evaluate_mfp_after(
                            &mfp_after1,
                            &mut datums_local,
                            &temp_storage,
                            key_len,
                        ) {
                            target.push((row, Diff::ONE));
                        }
                    }
                })
        } else {
            arranged
                .clone()
                .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>(name, {
                    move |key, source, target| {
                        // This part is the same as in the `!fused_unnest_list` if branch above.
                        let temp_storage = RowArena::new();
                        let mut val_scratch = vals_key_1.borrow();
                        let iter = source.iter().map(|(v, w)| {
                            val_scratch.clear();
                            v.extend_datums(&temp_storage, &mut val_scratch, Some(1));
                            (val_scratch[0], *w)
                        });

                        // This is the part that is specific to the `fused_unnest_list` branch.
                        let mut datums_local = datums_key_1.borrow();
                        key.extend_datums(&temp_storage, &mut datums_local, None);
                        let key_len = datums_local.len();
                        for datum in func
                            .eval_with_unnest_list::<_, window_agg_helpers::OneByOneAggrImpls>(
                                iter,
                                &temp_storage,
                            )
                        {
                            datums_local.truncate(key_len);
                            datums_local.push(datum);
                            if let Some(row) = evaluate_mfp_after(
                                &mfp_after1,
                                &mut datums_local,
                                &temp_storage,
                                key_len,
                            ) {
                                target.push((row, Diff::ONE));
                            }
                        }
                    }
                })
        };

        // Note that we would prefer to use `mz_timely_util::reduce::ReduceExt::reduce_pair` here, but
        // we then wouldn't be able to do this error check conditionally.  See its documentation for the
        // rationale around using a second reduction here.
        let must_validate = validating && err_output.is_none();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());
        if must_validate || mfp_after2.is_some() {
            let error_logger = self.error_logger();

            let errs = if !fused_unnest_list {
                arranged
                    .mz_reduce_abelian::<_, RowErrBuilder<_, _>, RowErrSpine<_, _>>(
                        &format!("{name} Error Check"),
                        move |key, source, target| {
                            // Negative counts would be surprising, but until we are 100% certain we won't
                            // see them, we should report when we do. We may want to bake even more info
                            // in here in the future.
                            if must_validate {
                                for (value, count) in source.iter() {
                                    if count.is_positive() {
                                        continue;
                                    }
                                    let value = value.to_row();
                                    let message =
                                        "Non-positive accumulation in ReduceInaccumulable";
                                    error_logger
                                        .log(message, &format!("value={value:?}, count={count}"));
                                    let err = EvalError::Internal(message.into());
                                    target.push((err.into(), Diff::ONE));
                                    return;
                                }
                            }

                            // We know that `mfp_after` can error if it exists, so try to evaluate it here.
                            let Some(mfp) = &mfp_after2 else { return };
                            let temp_storage = RowArena::new();
                            let mut val_scratch = vals2.borrow();
                            let iter = source.iter().map(|(v, w)| {
                                val_scratch.clear();
                                v.extend_datums(&temp_storage, &mut val_scratch, Some(1));
                                (val_scratch[0], *w)
                            });

                            let mut datums_local = datums2.borrow();
                            key.extend_datums(&temp_storage, &mut datums_local, None);
                            datums_local.push(
                                func2.eval_with_fast_window_agg::<
                                    _,
                                    window_agg_helpers::OneByOneAggrImpls,
                                >(
                                    iter, &temp_storage
                                ),
                            );
                            if let Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage) {
                                target.push((e.into(), Diff::ONE));
                            }
                        },
                    )
                    .as_collection(|_, v| v.clone())
            } else {
                // `render_reduce_plan_inner` doesn't request validation when `fused_unnest_list`.
                assert!(!must_validate);
                // We couldn't have got into this if branch due to `must_validate`, so it must be
                // because of the `mfp_after2.is_some()`.
                let Some(mfp) = mfp_after2 else {
                    unreachable!()
                };
                arranged
                    .mz_reduce_abelian::<_, RowErrBuilder<_, _>, RowErrSpine<_, _>>(
                        &format!("{name} Error Check"),
                        move |key, source, target| {
                            let temp_storage = RowArena::new();
                            let mut val_scratch = vals_key_2.borrow();
                            let iter = source.iter().map(|(v, w)| {
                                val_scratch.clear();
                                v.extend_datums(&temp_storage, &mut val_scratch, Some(1));
                                (val_scratch[0], *w)
                            });

                            let mut datums_local = datums_key_2.borrow();
                            key.extend_datums(&temp_storage, &mut datums_local, None);
                            let key_len = datums_local.len();
                            for datum in func2
                                .eval_with_unnest_list::<_, window_agg_helpers::OneByOneAggrImpls>(
                                    iter,
                                    &temp_storage,
                                )
                            {
                                datums_local.truncate(key_len);
                                datums_local.push(datum);
                                // We know that `mfp` can error (because of the `could_error` call
                                // above), so try to evaluate it here.
                                if let Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage)
                                {
                                    target.push((e.into(), Diff::ONE));
                                }
                            }
                        },
                    )
                    .as_collection(|_, v| v.clone())
            };

            if let Some(e) = err_output {
                err_output = Some(e.concat(errs));
            } else {
                err_output = Some(errs);
            }
        }
        (oks, err_output)
    }

    fn build_reduce_inaccumulable_distinct<'s, Bu, Tr>(
        &self,
        input: VecCollection<'s, T, Row, Diff>,
        name_tag: Option<&str>,
    ) -> Arranged<'s, TraceAgent<Tr>>
    where
        Tr: Trace<Batch: Navigable, Time = T> + 'static,
        for<'a> BatchCursor<Tr>: Cursor<
                Key<'a> = DatumSeq<'a>,
                KeyContainer: BatchContainer<Owned = Row>,
                Time = T,
                Diff = Diff,
                ValOwn: Data + MaybeValidatingRow<(), String>,
            >,
        Bu: Builder<
                Time = T,
                Input: Container
                           + ClearContainer
                           + PushInto<((Row, BatchValOwn<Tr>), Tr::Time, BatchDiff<Tr>)>,
                Output = Tr::Batch,
            > + 'static,
        Arranged<'s, TraceAgent<Tr>>: ArrangementSize,
    {
        let error_logger = self.error_logger();

        let output_name = format!(
            "ReduceInaccumulable Distinct{}",
            name_tag.unwrap_or_default()
        );

        let input: KeyCollection<_, _, _> = input.into();
        input
            .mz_arrange::<
                ColumnationChunker<_>,
                RowBatcher<_, _>,
                RowBuilder<_, _>,
                RowSpine<_, _>,
            >(
                "Arranged ReduceInaccumulable Distinct [val: empty]",
            )
            .mz_reduce_abelian::<_, Bu, Tr>(&output_name, move |_, source, t| {
                if let Some(err) = BatchValOwn::<Tr>::into_error() {
                    for (value, count) in source.iter() {
                        if count.is_positive() {
                            continue;
                        }

                        let message = "Non-positive accumulation in ReduceInaccumulable DISTINCT";
                        error_logger.log(message, &format!("value={value:?}, count={count}"));
                        t.push((err(message.to_string()), Diff::ONE));
                        return;
                    }
                }
                t.push((BatchValOwn::<Tr>::ok(()), Diff::ONE))
            })
    }

    /// Build the dataflow to compute and arrange multiple hierarchical aggregations
    /// on non-monotonic inputs.
    ///
    /// This function renders a single reduction tree that computes aggregations with
    /// a priority queue implemented with a series of reduce operators that partition
    /// the input into buckets, and compute the aggregation over very small buckets
    /// and feed the results up to larger buckets.
    ///
    /// Note that this implementation currently ignores the distinct bit because we
    /// currently only perform min / max hierarchically and the reduction tree
    /// efficiently suppresses non-distinct updates.
    ///
    /// `buckets` indicates the number of buckets in this stage. We do some non-obvious
    /// trickery here to limit the memory usage per layer by internally
    /// holding only the elements that were rejected by this stage. However, the
    /// output collection maintains the `((key, bucket), (passing value)` for this
    /// stage.
    fn build_bucketed<'s>(
        &self,
        input: VecCollection<'s, T, (Row, Row), Diff>,
        BucketedPlan {
            aggr_funcs,
            buckets,
        }: BucketedPlan,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
    ) -> (
        RowRowArrangement<'s, T>,
        VecCollection<'s, T, DataflowErrorSer, Diff>,
    ) {
        let mut err_output: Option<VecCollection<'s, T, _, _>> = None;
        let outer_scope = input.scope();
        let arranged_output = outer_scope
            .clone()
            .region_named("ReduceHierarchical", |inner| {
                let input = input.enter(inner);

                // The first mod to apply to the hash.
                let first_mod = buckets.get(0).copied().unwrap_or(1);
                let aggregations = aggr_funcs.len();

                // Gather the relevant keys with their hashes along with values ordered by aggregation_index.
                let mut stage = input.map(move |(key, row)| {
                    let mut row_builder = SharedRow::get();
                    let mut row_packer = row_builder.packer();
                    row_packer.extend(row.iter().take(aggregations));
                    let values = row_builder.clone();

                    // Apply the initial mod here.
                    let hash = values.hashed() % first_mod;
                    let hash_key =
                        row_builder.pack_using(std::iter::once(Datum::from(hash)).chain(&key));
                    (hash_key, values)
                });

                // Repeatedly apply hierarchical reduction with a progressively coarser key.
                for (index, b) in buckets.into_iter().enumerate() {
                    // Apply subsequent bucket mods for all but the first round.
                    let input = if index == 0 {
                        stage
                    } else {
                        stage.map(move |(hash_key, values)| {
                            let mut hash_key_iter = hash_key.iter();
                            let hash = hash_key_iter.next().unwrap().unwrap_uint64() % b;
                            // TODO: Convert the `chain(hash_key_iter...)` into a memcpy.
                            let hash_key = SharedRow::pack(
                                std::iter::once(Datum::from(hash))
                                    .chain(hash_key_iter.take(key_arity)),
                            );
                            (hash_key, values)
                        })
                    };

                    // We only want the first stage to perform validation of whether invalid accumulations
                    // were observed in the input. Subsequently, we will either produce an error in the error
                    // stream or produce correct data in the output stream.
                    let validating = err_output.is_none();

                    let (oks, errs) = self.build_bucketed_stage(&aggr_funcs, input, validating);
                    if let Some(errs) = errs {
                        err_output = Some(errs.leave_region(outer_scope));
                    }
                    stage = oks
                }

                // Discard the hash from the key and return to the format of the input data.
                let partial = stage.map(move |(hash_key, values)| {
                    let mut hash_key_iter = hash_key.iter();
                    let _hash = hash_key_iter.next();
                    (SharedRow::pack(hash_key_iter.take(key_arity)), values)
                });

                // Allocations for the two closures.
                let mut datums1 = DatumVec::new();
                let mut datums2 = DatumVec::new();
                // Scratch buffers for decoding the input values (one column per aggregate)
                // into the arena, so the aggregates iterate arena-resident datums rather
                // than the packed value bytes.
                let mut vals1 = DatumVec::new();
                let mut vals2 = DatumVec::new();
                let mfp_after1 = mfp_after.clone();
                let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());
                let aggr_funcs2 = aggr_funcs.clone();

                // Build a series of stages for the reduction
                // Arrange the final result into (key, Row)
                let error_logger = self.error_logger();
                // NOTE(vmarcos): The input operator name below is used in the tuning advice built-in
                // view mz_introspection.mz_expected_group_size_advice.
                let arranged = partial
                    .mz_arrange::<
                        ColumnationChunker<_>,
                        RowRowBatcher<_, _>,
                        RowRowBuilder<_, _>,
                        RowRowSpine<_, _>,
                    >(
                        "Arrange ReduceMinsMaxes",
                    );
                // Note that we would prefer to use `mz_timely_util::reduce::ReduceExt::reduce_pair` here,
                // but we then wouldn't be able to do this error check conditionally.  See its documentation
                // for the rationale around using a second reduction here.
                let must_validate = err_output.is_none();
                if must_validate || mfp_after2.is_some() {
                    let errs = arranged
                        .clone()
                        .mz_reduce_abelian::<_, RowErrBuilder<_, _>, RowErrSpine<_, _>>(
                            "ReduceMinsMaxes Error Check",
                            move |key, source, target| {
                                // Negative counts would be surprising, but until we are 100% certain we wont
                                // see them, we should report when we do. We may want to bake even more info
                                // in here in the future.
                                if must_validate {
                                    for (val, count) in source.iter() {
                                        if count.is_positive() {
                                            continue;
                                        }
                                        let val = val.to_row();
                                        let message =
                                            "Non-positive accumulation in ReduceMinsMaxes";
                                        error_logger
                                            .log(message, &format!("val={val:?}, count={count}"));
                                        target.push((
                                            EvalError::Internal(message.into()).into(),
                                            Diff::ONE,
                                        ));
                                        return;
                                    }
                                }

                                // We know that `mfp_after` can error if it exists, so try to evaluate it here.
                                let Some(mfp) = &mfp_after2 else { return };
                                let temp_storage = RowArena::new();
                                let mut datums_local = datums2.borrow();
                                key.extend_datums(&temp_storage, &mut datums_local, None);

                                // Decode every value row's datums into the arena, one column
                                // per aggregate, then iterate them column-major below. Min/max
                                // hierarchical aggregates are multiplicity-insensitive, so each
                                // row contributes once (`Diff::ONE`) regardless of `_cnt`.
                                let arity = aggr_funcs2.len();
                                let mut decoded = vals2.borrow();
                                for (values, _cnt) in source.iter() {
                                    values.extend_datums(&temp_storage, &mut decoded, None);
                                }
                                assert_eq!(decoded.len(), source.len() * arity);
                                for (col, func) in aggr_funcs2.iter().enumerate() {
                                    let column_iter = (0..source.len())
                                        .map(|r| (decoded[r * arity + col], Diff::ONE));
                                    datums_local.push(func.eval(column_iter, &temp_storage));
                                }
                                if let Result::Err(e) =
                                    mfp.evaluate_inner(&mut datums_local, &temp_storage)
                                {
                                    target.push((e.into(), Diff::ONE));
                                }
                            },
                        )
                        .as_collection(|_, v| v.clone())
                        .leave_region(outer_scope);
                    if let Some(e) = err_output.take() {
                        err_output = Some(e.concat(errs));
                    } else {
                        err_output = Some(errs);
                    }
                }
                arranged
                    .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                        "ReduceMinsMaxes",
                        move |key, source, target| {
                            let temp_storage = RowArena::new();
                            let mut datums_local = datums1.borrow();
                            key.extend_datums(&temp_storage, &mut datums_local, None);
                            let key_len = datums_local.len();

                            // Decode every value row's datums into the arena, one column
                            // per aggregate, then iterate them column-major below. Min/max
                            // hierarchical aggregates are multiplicity-insensitive, so each
                            // row contributes once (`Diff::ONE`) regardless of `_cnt`.
                            let arity = aggr_funcs.len();
                            let mut decoded = vals1.borrow();
                            for (values, _cnt) in source.iter() {
                                values.extend_datums(&temp_storage, &mut decoded, None);
                            }
                            assert_eq!(decoded.len(), source.len() * arity);
                            for (col, func) in aggr_funcs.iter().enumerate() {
                                let column_iter = (0..source.len())
                                    .map(|r| (decoded[r * arity + col], Diff::ONE));
                                datums_local.push(func.eval(column_iter, &temp_storage));
                            }

                            if let Some(row) = evaluate_mfp_after(
                                &mfp_after1,
                                &mut datums_local,
                                &temp_storage,
                                key_len,
                            ) {
                                target.push((row, Diff::ONE));
                            }
                        },
                    )
                    .leave_region(outer_scope)
            });
        (
            arranged_output,
            err_output.expect("expected to validate in one level of the hierarchy"),
        )
    }

    /// Build a bucketed stage fragment that wraps [`Self::build_bucketed_negated_output`], and
    /// adds validation if `validating` is true. It returns the consolidated inputs concatenated
    /// with the negation of what's produced by the reduction.
    /// `validating` indicates whether we want this stage to perform error detection
    /// for invalid accumulations. Once a stage is clean of such errors, subsequent
    /// stages can skip validation.
    fn build_bucketed_stage<'s>(
        &self,
        aggr_funcs: &Vec<AggregateFunc>,
        input: VecCollection<'s, T, (Row, Row), Diff>,
        validating: bool,
    ) -> (
        VecCollection<'s, T, (Row, Row), Diff>,
        Option<VecCollection<'s, T, DataflowErrorSer, Diff>>,
    ) {
        let (input, negated_output, errs) = if validating {
            let (input, reduced) = self
                .build_bucketed_negated_output::<
                    RowValBuilder<_, _, _>,
                    RowValSpine<Result<Row, Row>, _, _>,
                >(
                    input.clone(),
                    aggr_funcs.clone(),
                );
            let (oks, errs) = reduced
                .as_collection(|k, v| (k.to_row(), v.clone()))
                .map_fallible::<CapacityContainerBuilder<_>, CapacityContainerBuilder<_>, _, _, _>(
                "Checked Invalid Accumulations",
                |(hash_key, result)| match result {
                    Err(hash_key) => {
                        let mut hash_key_iter = hash_key.iter();
                        let _hash = hash_key_iter.next();
                        let key = SharedRow::pack(hash_key_iter);
                        let message = format!(
                            "Invalid data in source, saw non-positive accumulation \
                                         for key {key:?} in hierarchical mins-maxes aggregate"
                        );
                        Err(EvalError::Internal(message.into()).into())
                    }
                    Ok(values) => Ok((hash_key, values)),
                },
            );
            (input, oks, Some(errs))
        } else {
            let (input, reduced) = self
                .build_bucketed_negated_output::<RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                    input,
                    aggr_funcs.clone(),
                );
            // TODO: Here is a good moment where we could apply the next `mod` calculation. Note
            // that we need to apply the mod on both input and oks.
            let oks = reduced.as_collection(|k, v| (k.to_row(), v.to_row()));
            (input, oks, None)
        };

        let input = input.as_collection(|k, v| (k.to_row(), v.to_row()));
        let oks = negated_output.concat(input);
        (oks, errs)
    }

    /// Build a dataflow fragment for one stage of a reduction tree for multiple hierarchical
    /// aggregates to arrange and reduce the inputs. Returns the arranged input and the reduction,
    /// with all diffs in the reduction's output negated.
    fn build_bucketed_negated_output<'s, Bu, Tr>(
        &self,
        input: VecCollection<'s, T, (Row, Row), Diff>,
        aggrs: Vec<AggregateFunc>,
    ) -> (
        Arranged<'s, TraceAgent<RowRowSpine<T, Diff>>>,
        Arranged<'s, TraceAgent<Tr>>,
    )
    where
        Tr: Trace<Batch: Navigable, Time = T> + 'static,
        for<'a> BatchCursor<Tr>: Cursor<
                Key<'a> = DatumSeq<'a>,
                KeyContainer: BatchContainer<Owned = Row>,
                ValOwn: Data + MaybeValidatingRow<Row, Row>,
                Time = T,
                Diff = Diff,
            >,
        Bu: Builder<
                Time = T,
                Input: Container
                           + ClearContainer
                           + PushInto<((Row, BatchValOwn<Tr>), Tr::Time, BatchDiff<Tr>)>,
                Output = Tr::Batch,
            > + 'static,
        Arranged<'s, TraceAgent<Tr>>: ArrangementSize,
    {
        let error_logger = self.error_logger();
        // NOTE(vmarcos): The input operator name below is used in the tuning advice built-in
        // view mz_introspection.mz_expected_group_size_advice.
        let arranged_input = input
            .mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >(
                "Arranged MinsMaxesHierarchical input",
            );

        // Scratch buffer for decoding the input values (one column per aggregate) into the
        // arena, so the aggregates iterate arena-resident datums rather than the packed bytes.
        let mut value_datums = DatumVec::new();
        let reduced = arranged_input.clone().mz_reduce_abelian::<_, Bu, Tr>(
            "Reduced Fallibly MinsMaxesHierarchical",
            move |key, source, target| {
                if let Some(err) = BatchValOwn::<Tr>::into_error() {
                    // Should negative accumulations reach us, we should loudly complain.
                    for (value, count) in source.iter() {
                        if count.is_positive() {
                            continue;
                        }
                        error_logger.log(
                            "Non-positive accumulation in MinsMaxesHierarchical",
                            &format!("key={key:?}, value={value:?}, count={count}"),
                        );
                        // After complaining, output an error here so that we can eventually
                        // report it in an error stream.
                        target.push((
                            err(<<BatchCursor<Tr> as Cursor>::KeyContainer as BatchContainer>::into_owned(key)),
                            Diff::ONE,
                        ));
                        return;
                    }
                }

                // Decode every value row's datums into the arena, one column per aggregate,
                // then iterate them column-major below.
                let temp_storage = RowArena::new();
                let arity = aggrs.len();
                let mut decoded = value_datums.borrow();
                for (values, _cnt) in source.iter() {
                    values.extend_datums(&temp_storage, &mut decoded, None);
                }
                assert_eq!(decoded.len(), source.len() * arity);

                let mut row_builder = SharedRow::get();
                let mut row_packer = row_builder.packer();
                for (col, func) in aggrs.iter().enumerate() {
                    // Min/max hierarchical aggregates are multiplicity-insensitive, so each
                    // row contributes once (`Diff::ONE`) regardless of `_cnt`.
                    let column_iter =
                        (0..source.len()).map(|r| (decoded[r * arity + col], Diff::ONE));
                    row_packer.push(func.eval(column_iter, &temp_storage));
                }
                // We only want to arrange the parts of the input that are not part of the output.
                // More specifically, we want to arrange it so that `input.concat(&output.negate())`
                // gives us the intended value of this aggregate function. Also we assume that regardless
                // of the multiplicity of the final result in the input, we only want to have one copy
                // in the output.
                target.reserve(source.len().saturating_add(1));
                target.push((BatchValOwn::<Tr>::ok(row_builder.clone()), Diff::MINUS_ONE));
                target.extend(source.iter().map(|(values, cnt)| {
                    let mut cnt = *cnt;
                    cnt.negate();
                    (BatchValOwn::<Tr>::ok(values.to_row()), cnt)
                }));
            },
        );
        (arranged_input, reduced)
    }

    /// Build the dataflow to compute and arrange multiple hierarchical aggregations
    /// on monotonic inputs.
    fn build_monotonic<'s>(
        &self,
        collection: VecCollection<'s, T, (Row, Row), Diff>,
        MonotonicPlan {
            aggr_funcs,
            must_consolidate,
        }: MonotonicPlan,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
    ) -> (
        RowRowArrangement<'s, T>,
        VecCollection<'s, T, DataflowErrorSer, Diff>,
    ) {
        let aggregations = aggr_funcs.len();
        // Gather the relevant values into a vec of rows ordered by aggregation_index
        let collection = collection
            .map(move |(key, row)| {
                let mut row_builder = SharedRow::get();
                let mut values = Vec::with_capacity(aggregations);
                values.extend(
                    row.iter()
                        .take(aggregations)
                        .map(|v| row_builder.pack_using(std::iter::once(v))),
                );

                (key, values)
            })
            .consolidate_named_if::<KeyBatcher<_, _, _>>(
                must_consolidate,
                "Consolidated ReduceMonotonic input",
            );

        // It should be now possible to ensure that we have a monotonic collection.
        let error_logger = self.error_logger();
        let (partial, validation_errs) = collection.ensure_monotonic(move |data, diff| {
            error_logger.log(
                "Non-monotonic input to ReduceMonotonic",
                &format!("data={data:?}, diff={diff}"),
            );
            let m = "tried to build a monotonic reduction on non-monotonic input".into();
            (EvalError::Internal(m).into(), Diff::ONE)
        });
        // We can place our rows directly into the diff field, and
        // only keep the relevant one corresponding to evaluating our
        // aggregate, instead of having to do a hierarchical reduction.
        let partial = partial.explode_one(move |(key, values)| {
            let mut output = Vec::new();
            for (row, func) in values.into_iter().zip_eq(aggr_funcs.iter()) {
                output.push(monoids::get_monoid(row, func).expect(
                    "hierarchical aggregations are expected to have monoid implementations",
                ));
            }
            (key, output)
        });

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());

        let partial: KeyCollection<_, _, _> = partial.into();
        let arranged = partial
            .mz_arrange::<
                ColumnationChunker<_>,
                RowBatcher<_, _>,
                RowBuilder<_, _>,
                RowSpine<_, Vec<ReductionMonoid>>,
            >(
                "ArrangeMonotonic [val: empty]",
            );
        let output = arranged
            .clone()
            .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>("ReduceMonotonic", {
                move |key, input, output| {
                    let temp_storage = RowArena::new();
                    let mut datums_local = datums1.borrow();
                    key.extend_datums(&temp_storage, &mut datums_local, None);
                    let key_len = datums_local.len();
                    let accum = &input[0].1;
                    for monoid in accum.iter() {
                        datums_local.extend(monoid.finalize().iter());
                    }

                    if let Some(row) =
                        evaluate_mfp_after(&mfp_after1, &mut datums_local, &temp_storage, key_len)
                    {
                        output.push((row, Diff::ONE));
                    }
                }
            });

        // If `mfp_after` can error, then we need to render a paired reduction
        // to scan for these potential errors. Note that we cannot directly use
        // `mz_timely_util::reduce::ReduceExt::reduce_pair` here because we only
        // conditionally render the second component of the reduction pair.
        if let Some(mfp) = mfp_after2 {
            let mfp_errs = arranged
                .mz_reduce_abelian::<_, RowErrBuilder<_, _>, RowErrSpine<_, _>>(
                    "ReduceMonotonic Error Check",
                    move |key, input, output| {
                        let temp_storage = RowArena::new();
                        let mut datums_local = datums2.borrow();
                        key.extend_datums(&temp_storage, &mut datums_local, None);
                        let accum = &input[0].1;
                        for monoid in accum.iter() {
                            datums_local.extend(monoid.finalize().iter());
                        }
                        if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage)
                        {
                            output.push((e.into(), Diff::ONE));
                        }
                    },
                )
                .as_collection(|_k, v| v.clone());
            (output, validation_errs.concat(mfp_errs))
        } else {
            (output, validation_errs)
        }
    }

    /// Build the dataflow to compute and arrange multiple accumulable aggregations.
    ///
    /// The incoming values are moved to the update's "difference" field, at which point
    /// they can be accumulated in place. The `count` operator promotes the accumulated
    /// values to data, at which point a final map applies operator-specific logic to
    /// yield the final aggregate.
    fn build_accumulable<'s>(
        &self,
        collection: VecCollection<'s, T, (Row, Row), Diff>,
        AccumulablePlan {
            full_aggrs,
            simple_aggrs,
            distinct_aggrs,
        }: AccumulablePlan,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan<LirScalarExpr>>,
    ) -> (
        RowRowArrangement<'s, T>,
        VecCollection<'s, T, DataflowErrorSer, Diff>,
    ) {
        let collection_scope = collection.scope();

        // we must have called this function with something to reduce
        if full_aggrs.len() == 0 || simple_aggrs.len() + distinct_aggrs.len() != full_aggrs.len() {
            self.error_logger().soft_panic_or_log(
                "Incorrect numbers of aggregates in accummulable reduction rendering",
                &format!(
                    "full_aggrs={}, simple_aggrs={}, distinct_aggrs={}",
                    full_aggrs.len(),
                    simple_aggrs.len(),
                    distinct_aggrs.len(),
                ),
            );
        }

        // Some of the aggregations may have the `distinct` bit set, which means that they'll
        // need to be extracted from `collection` and be subjected to `distinct` with `key`.
        // Other aggregations can be directly moved in to the `diff` field.
        //
        // In each case, the resulting collection should have `data` shaped as `(key, ())`
        // and a `diff` that is a vector with length `3 * aggrs.len()`. The three values are
        // generally the count, and then two aggregation-specific values. The size could be
        // reduced if we want to specialize for the aggregations.

        // Instantiate a default vector for diffs with the correct types at each
        // position.
        let zero_diffs: (Vec<_>, Diff) = (
            full_aggrs
                .iter()
                .map(|f| accumulable_zero(&f.func))
                .collect(),
            Diff::ZERO,
        );

        let mut to_aggregate = Vec::new();
        if simple_aggrs.len() > 0 {
            // First, collect all non-distinct aggregations in one pass.
            let collection = collection.clone();
            let easy_cases = collection.explode_one({
                let zero_diffs = zero_diffs.clone();
                move |(key, row)| {
                    let mut diffs = zero_diffs.clone();
                    // Try to unpack only the datums we need. Unfortunately, since we
                    // can't random access into a Row, we have to iterate through one by one.
                    // TODO: Even though we don't have random access, we could still avoid unpacking
                    // everything that we don't care about, and it might be worth it to extend the
                    // Row API to do that.
                    let mut row_iter = row.iter().enumerate();
                    for (datum_index, aggr) in simple_aggrs.iter() {
                        let mut datum = row_iter.next().unwrap();
                        while datum_index != &datum.0 {
                            datum = row_iter.next().unwrap();
                        }
                        let datum = datum.1;
                        diffs.0[*datum_index] = datum_to_accumulator(&aggr.func, datum);
                        diffs.1 = Diff::ONE;
                    }
                    ((key, ()), diffs)
                }
            });
            to_aggregate.push(easy_cases);
        }

        // Next, collect all aggregations that require distinctness.
        for (datum_index, aggr) in distinct_aggrs.into_iter() {
            let pairer = Pairer::new(key_arity);
            let collection = collection
                .clone()
                .map(move |(key, row)| {
                    let value = row.iter().nth(datum_index).unwrap();
                    (pairer.merge(&key, std::iter::once(value)), ())
                })
                .mz_arrange::<
                    ColumnationChunker<_>,
                    RowBatcher<_, _>,
                    RowBuilder<_, _>,
                    RowSpine<_, _>,
                >(
                    "Arranged Accumulable Distinct [val: empty]",
                )
                .mz_reduce_abelian::<_, RowBuilder<_, _>, RowSpine<_, _>>(
                    "Reduced Accumulable Distinct [val: empty]",
                    move |_k, _s, t| t.push(((), Diff::ONE)),
                )
                .as_collection(move |key_val_iter, _| pairer.split(key_val_iter))
                .explode_one({
                    let zero_diffs = zero_diffs.clone();
                    move |(key, row)| {
                        let datum = row.iter().next().unwrap();
                        let mut diffs = zero_diffs.clone();
                        diffs.0[datum_index] = datum_to_accumulator(&aggr.func, datum);
                        diffs.1 = Diff::ONE;
                        ((key, ()), diffs)
                    }
                });
            to_aggregate.push(collection);
        }

        // now concatenate, if necessary, multiple aggregations
        let collection = if to_aggregate.len() == 1 {
            to_aggregate.remove(0)
        } else {
            differential_dataflow::collection::concatenate(collection_scope, to_aggregate)
        };

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());
        let full_aggrs2 = full_aggrs.clone();

        let error_logger = self.error_logger();
        let err_full_aggrs = full_aggrs.clone();
        let arranged = collection
            .mz_arrange::<
                ColumnationChunker<_>,
                RowBatcher<_, _>,
                RowBuilder<_, _>,
                RowSpine<_, (Vec<Accum>, Diff)>,
            >(
                "ArrangeAccumulable [val: empty]",
            );
        let arranged_output = arranged
            .clone()
            .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>("ReduceAccumulable", {
                move |key, input, output| {
                    let (ref accums, total) = input[0].1;

                    let temp_storage = RowArena::new();
                    let mut datums_local = datums1.borrow();
                    key.extend_datums(&temp_storage, &mut datums_local, None);
                    let key_len = datums_local.len();
                    for (aggr, accum) in full_aggrs.iter().zip_eq(accums) {
                        datums_local.push(finalize_accum(&aggr.func, accum, total));
                    }

                    if let Some(row) =
                        evaluate_mfp_after(&mfp_after1, &mut datums_local, &temp_storage, key_len)
                    {
                        output.push((row, Diff::ONE));
                    }
                }
            });
        let arranged_errs = arranged
            .mz_reduce_abelian::<_, RowErrBuilder<_, _>, RowErrSpine<_, _>>(
                "AccumulableErrorCheck",
                move |key, input, output| {
                    let (ref accums, total) = input[0].1;
                    for (aggr, accum) in err_full_aggrs.iter().zip_eq(accums) {
                        // We first test here if inputs without net-positive records are present,
                        // producing an error to the logs and to the query output if that is the case.
                        if total == Diff::ZERO && !accum.is_zero() {
                            error_logger.log(
                                "Net-zero records with non-zero accumulation in ReduceAccumulable",
                                &format!("aggr={aggr:?}, accum={accum:?}"),
                            );
                            let key = key.to_row();
                            let message = format!(
                                "Invalid data in source, saw net-zero records for key {key} \
                                 with non-zero accumulation in accumulable aggregate"
                            );
                            output.push((EvalError::Internal(message.into()).into(), Diff::ONE));
                        }
                        match (&aggr.func, &accum) {
                            (AggregateFunc::SumUInt16, Accum::SimpleNumber { accum, .. })
                            | (AggregateFunc::SumUInt32, Accum::SimpleNumber { accum, .. })
                            | (AggregateFunc::SumUInt64, Accum::SimpleNumber { accum, .. }) => {
                                if accum.is_negative() {
                                    error_logger.log(
                                    "Invalid negative unsigned aggregation in ReduceAccumulable",
                                    &format!("aggr={aggr:?}, accum={accum:?}"),
                                );
                                    let key = key.to_row();
                                    let message = format!(
                                        "Invalid data in source, saw negative accumulation with \
                                         unsigned type for key {key}"
                                    );
                                    let err = EvalError::Internal(message.into());
                                    output.push((err.into(), Diff::ONE));
                                }
                            }
                            _ => (), // no more errors to check for at this point!
                        }
                    }

                    // If `mfp_after` can error, then evaluate it here.
                    let Some(mfp) = &mfp_after2 else { return };
                    let temp_storage = RowArena::new();
                    let mut datums_local = datums2.borrow();
                    key.extend_datums(&temp_storage, &mut datums_local, None);
                    for (aggr, accum) in full_aggrs2.iter().zip_eq(accums) {
                        datums_local.push(finalize_accum(&aggr.func, accum, total));
                    }

                    if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage) {
                        output.push((e.into(), Diff::ONE));
                    }
                },
            );
        (
            arranged_output,
            arranged_errs.as_collection(|_key, error| error.clone()),
        )
    }
}

/// Evaluates the fused MFP, if one exists, on a reconstructed `DatumVecBorrow`
/// containing key and aggregate values, then returns a result `Row` or `None`
/// if the MFP filters the result out.
fn evaluate_mfp_after<'a, 'b>(
    mfp_after: &'a Option<SafeMfpPlan<LirScalarExpr>>,
    datums_local: &'b mut mz_repr::DatumVecBorrow<'a>,
    temp_storage: &'a RowArena,
    key_len: usize,
) -> Option<Row> {
    let mut row_builder = SharedRow::get();
    // Apply MFP if it exists and pack a Row of
    // aggregate values from `datums_local`.
    if let Some(mfp) = mfp_after {
        // It must ignore errors here, but they are scanned
        // for elsewhere if the MFP can error.
        if let Ok(Some(iter)) = mfp.evaluate_iter(datums_local, temp_storage) {
            // The `mfp_after` must preserve the key columns,
            // so we can skip them to form aggregation results.
            Some(row_builder.pack_using(iter.skip(key_len)))
        } else {
            None
        }
    } else {
        Some(row_builder.pack_using(&datums_local[key_len..]))
    }
}

fn accumulable_zero(aggr_func: &AggregateFunc) -> Accum {
    match aggr_func {
        AggregateFunc::Any | AggregateFunc::All => Accum::Bool {
            trues: Diff::ZERO,
            falses: Diff::ZERO,
        },
        AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => Accum::Float {
            accum: AccumCount::ZERO,
            pos_infs: Diff::ZERO,
            neg_infs: Diff::ZERO,
            nans: Diff::ZERO,
            non_nulls: Diff::ZERO,
        },
        AggregateFunc::SumNumeric => Accum::Numeric {
            accum: OrderedDecimal(NumericAgg::zero()),
            pos_infs: Diff::ZERO,
            neg_infs: Diff::ZERO,
            nans: Diff::ZERO,
            non_nulls: Diff::ZERO,
        },
        _ => Accum::SimpleNumber {
            accum: AccumCount::ZERO,
            non_nulls: Diff::ZERO,
        },
    }
}

/// The number of fractional bits of binary precision retained by the
/// fixed-point representation used to accumulate float sums. The fixed-point
/// scale is `FLOAT_SCALE == 2^FLOAT_SCALE_EXP`.
const FLOAT_SCALE_EXP: u32 = 24;

/// The fixed-point scale applied to float sums, i.e. `2^FLOAT_SCALE_EXP`.
#[allow(clippy::as_conversions)] // Integer-to-float cast, exact and const-evaluable.
const FLOAT_SCALE: f64 = (1_u64 << FLOAT_SCALE_EXP) as f64;

/// Maps a finite `f64` onto the fixed-point `i128` domain used to accumulate
/// float sums, i.e. computes `trunc(n * FLOAT_SCALE)` reduced modulo `2^128`.
///
/// Conceptually this multiplies `n` by `FLOAT_SCALE` and truncates towards zero,
/// but it does so using *wrapping* (modulo `2^128`) rather than *saturating*
/// semantics, and it never forms the intermediate product `n * FLOAT_SCALE` as
/// an `f64` (which could itself overflow to infinity for very large `n`).
///
/// Wrapping is what makes this conversion a group homomorphism into the additive
/// group of `i128` (mod `2^128`), matching the wrapping arithmetic used when
/// accumulators are combined and retracted. As a result, a set of large finite
/// values whose *sum* is representable produces the correct result even when the
/// individual values fall outside the representable fixed-point range. Saturating
/// instead breaks this: e.g. `1.1e31` and `-1.1e31` both overflow the domain and
/// would saturate to `i128::MAX` and `i128::MIN`, which sum to `-1` rather than
/// `0` (see database-issues#11265).
fn float_to_fixed_point(n: f64) -> i128 {
    debug_assert!(n.is_finite());

    // Decompose `n` into integer parts such that `n == sign * mantissa *
    // 2^exponent`. Folding in the `* 2^FLOAT_SCALE_EXP` scaling then amounts to
    // shifting `mantissa` left by `exponent + FLOAT_SCALE_EXP` bits.
    let (mantissa, exponent, sign) = Float::integer_decode(n);
    let significand = u128::from(mantissa);
    let exp = i64::from(exponent) + i64::from(FLOAT_SCALE_EXP);

    let magnitude: u128 = if exp >= 0 {
        // Left shifts of 128 or more bits leave nothing within the 128-bit
        // window; smaller shifts keep only the low 128 bits (i.e. mod `2^128`).
        match u32::try_from(exp) {
            Ok(shift) if shift < 128 => significand << shift,
            _ => 0,
        }
    } else {
        // Right shift truncates the fractional part towards zero. Subnormals
        // (and zero) shift entirely out of the window and become zero.
        match u32::try_from(-exp) {
            Ok(shift) if shift < 128 => significand >> shift,
            _ => 0,
        }
    };

    // Reinterpret the magnitude as a signed `i128` (wrapping into the signed
    // domain) and apply the sign of `n`.
    let magnitude = magnitude.cast_signed();
    if sign < 0 {
        magnitude.wrapping_neg()
    } else {
        magnitude
    }
}

fn datum_to_accumulator(aggregate_func: &AggregateFunc, datum: Datum) -> Accum {
    match aggregate_func {
        AggregateFunc::Count => Accum::SimpleNumber {
            accum: AccumCount::ZERO, // unused for AggregateFunc::Count
            non_nulls: if datum.is_null() {
                Diff::ZERO
            } else {
                Diff::ONE
            },
        },
        AggregateFunc::Any | AggregateFunc::All => match datum {
            Datum::True => Accum::Bool {
                trues: Diff::ONE,
                falses: Diff::ZERO,
            },
            Datum::Null => Accum::Bool {
                trues: Diff::ZERO,
                falses: Diff::ZERO,
            },
            Datum::False => Accum::Bool {
                trues: Diff::ZERO,
                falses: Diff::ONE,
            },
            x => panic!("Invalid argument to AggregateFunc::Any: {x:?}"),
        },
        AggregateFunc::Dummy => match datum {
            Datum::Dummy => Accum::SimpleNumber {
                accum: AccumCount::ZERO,
                non_nulls: Diff::ZERO,
            },
            x => panic!("Invalid argument to AggregateFunc::Dummy: {x:?}"),
        },
        AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => {
            let n = match datum {
                Datum::Float32(n) => f64::from(*n),
                Datum::Float64(n) => *n,
                Datum::Null => 0f64,
                x => panic!("Invalid argument to AggregateFunc::{aggregate_func:?}: {x:?}"),
            };

            let nans = Diff::from(n.is_nan());
            let pos_infs = Diff::from(n == f64::INFINITY);
            let neg_infs = Diff::from(n == f64::NEG_INFINITY);
            let non_nulls = Diff::from(datum != Datum::Null);

            // Map the floating point value onto a fixed precision domain
            // All special values should map to zero, since they are tracked separately
            let accum = if nans.is_positive() || pos_infs.is_positive() || neg_infs.is_positive() {
                AccumCount::ZERO
            } else {
                // Wrap (rather than saturate) on overflow, so that the mapping is
                // a group homomorphism and large finite values whose sum is in
                // range still produce correct results (database-issues#11265).
                float_to_fixed_point(n).into()
            };

            Accum::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            }
        }
        AggregateFunc::SumNumeric => match datum {
            Datum::Numeric(n) => {
                let (accum, pos_infs, neg_infs, nans) = if n.0.is_infinite() {
                    if n.0.is_negative() {
                        (NumericAgg::zero(), Diff::ZERO, Diff::ONE, Diff::ZERO)
                    } else {
                        (NumericAgg::zero(), Diff::ONE, Diff::ZERO, Diff::ZERO)
                    }
                } else if n.0.is_nan() {
                    (NumericAgg::zero(), Diff::ZERO, Diff::ZERO, Diff::ONE)
                } else {
                    // Take a narrow decimal (datum) into a wide decimal
                    // (aggregator).
                    let mut cx_agg = numeric::cx_agg();
                    (cx_agg.to_width(n.0), Diff::ZERO, Diff::ZERO, Diff::ZERO)
                };

                Accum::Numeric {
                    accum: OrderedDecimal(accum),
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls: Diff::ONE,
                }
            }
            Datum::Null => Accum::Numeric {
                accum: OrderedDecimal(NumericAgg::zero()),
                pos_infs: Diff::ZERO,
                neg_infs: Diff::ZERO,
                nans: Diff::ZERO,
                non_nulls: Diff::ZERO,
            },
            x => panic!("Invalid argument to AggregateFunc::SumNumeric: {x:?}"),
        },
        _ => {
            // Other accumulations need to disentangle the accumulable
            // value from its NULL-ness, which is not quite as easily
            // accumulated.
            match datum {
                Datum::Int16(i) => Accum::SimpleNumber {
                    accum: i.into(),
                    non_nulls: Diff::ONE,
                },
                Datum::Int32(i) => Accum::SimpleNumber {
                    accum: i.into(),
                    non_nulls: Diff::ONE,
                },
                Datum::Int64(i) => Accum::SimpleNumber {
                    accum: i.into(),
                    non_nulls: Diff::ONE,
                },
                Datum::UInt16(u) => Accum::SimpleNumber {
                    accum: u.into(),
                    non_nulls: Diff::ONE,
                },
                Datum::UInt32(u) => Accum::SimpleNumber {
                    accum: u.into(),
                    non_nulls: Diff::ONE,
                },
                Datum::UInt64(u) => Accum::SimpleNumber {
                    accum: u.into(),
                    non_nulls: Diff::ONE,
                },
                Datum::MzTimestamp(t) => Accum::SimpleNumber {
                    accum: u64::from(t).into(),
                    non_nulls: Diff::ONE,
                },
                Datum::Null => Accum::SimpleNumber {
                    accum: AccumCount::ZERO,
                    non_nulls: Diff::ZERO,
                },
                x => panic!("Accumulating non-integer data: {x:?}"),
            }
        }
    }
}

fn finalize_accum<'a>(aggr_func: &'a AggregateFunc, accum: &'a Accum, total: Diff) -> Datum<'a> {
    // The finished value depends on the aggregation function in a variety of ways.
    // For all aggregates but count, if only null values were
    // accumulated, then the output is null.
    if total.is_positive() && accum.is_zero() && *aggr_func != AggregateFunc::Count {
        Datum::Null
    } else {
        match (&aggr_func, &accum) {
            (AggregateFunc::Count, Accum::SimpleNumber { non_nulls, .. }) => {
                Datum::Int64(non_nulls.into_inner())
            }
            (AggregateFunc::All, Accum::Bool { falses, trues }) => {
                // If any false, else if all true, else must be no false and some nulls.
                if falses.is_positive() {
                    Datum::False
                } else if *trues == total {
                    Datum::True
                } else {
                    Datum::Null
                }
            }
            (AggregateFunc::Any, Accum::Bool { falses, trues }) => {
                // If any true, else if all false, else must be no true and some nulls.
                if trues.is_positive() {
                    Datum::True
                } else if *falses == total {
                    Datum::False
                } else {
                    Datum::Null
                }
            }
            (AggregateFunc::Dummy, _) => Datum::Dummy,
            // If any non-nulls, just report the aggregate.
            (AggregateFunc::SumInt16, Accum::SimpleNumber { accum, .. })
            | (AggregateFunc::SumInt32, Accum::SimpleNumber { accum, .. }) => {
                // This conversion is safe, as long as we have less than 2^32
                // summands.
                // TODO(benesch): are we guaranteed to have less than 2^32 summands?
                // If so, rewrite to avoid `as`.
                #[allow(clippy::as_conversions)]
                Datum::Int64(accum.into_inner() as i64)
            }
            (AggregateFunc::SumInt64, Accum::SimpleNumber { accum, .. }) => Datum::from(*accum),
            (AggregateFunc::SumUInt16, Accum::SimpleNumber { accum, .. })
            | (AggregateFunc::SumUInt32, Accum::SimpleNumber { accum, .. }) => {
                if !accum.is_negative() {
                    // Our semantics of overflow are not clearly articulated wrt.
                    // unsigned vs. signed types (database-issues#5172). We adopt an
                    // unsigned wrapping behavior to match what we do above for
                    // signed types.
                    // TODO(vmarcos): remove potentially dangerous usage of `as`.
                    #[allow(clippy::as_conversions)]
                    Datum::UInt64(accum.into_inner() as u64)
                } else {
                    // Note that we return a value here, but an error in the other
                    // operator of the reduce_pair. Therefore, we expect that this
                    // value will never be exposed as an output.
                    Datum::Null
                }
            }
            (AggregateFunc::SumUInt64, Accum::SimpleNumber { accum, .. }) => {
                if !accum.is_negative() {
                    Datum::from(*accum)
                } else {
                    // Note that we return a value here, but an error in the other
                    // operator of the reduce_pair. Therefore, we expect that this
                    // value will never be exposed as an output.
                    Datum::Null
                }
            }
            (
                AggregateFunc::SumFloat32,
                Accum::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls: _,
                },
            ) => {
                if nans.is_positive() || (pos_infs.is_positive() && neg_infs.is_positive()) {
                    // NaNs are NaNs and cases where we've seen a
                    // mixture of positive and negative infinities.
                    Datum::from(f32::NAN)
                } else if pos_infs.is_positive() {
                    Datum::from(f32::INFINITY)
                } else if neg_infs.is_positive() {
                    Datum::from(f32::NEG_INFINITY)
                } else {
                    let sum = f64::cast_lossy(accum.into_inner()) / FLOAT_SCALE;
                    Datum::from(f32::cast_lossy(sum))
                }
            }
            (
                AggregateFunc::SumFloat64,
                Accum::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls: _,
                },
            ) => {
                if nans.is_positive() || (pos_infs.is_positive() && neg_infs.is_positive()) {
                    // NaNs are NaNs and cases where we've seen a
                    // mixture of positive and negative infinities.
                    Datum::from(f64::NAN)
                } else if pos_infs.is_positive() {
                    Datum::from(f64::INFINITY)
                } else if neg_infs.is_positive() {
                    Datum::from(f64::NEG_INFINITY)
                } else {
                    Datum::from(f64::cast_lossy(accum.into_inner()) / FLOAT_SCALE)
                }
            }
            (
                AggregateFunc::SumNumeric,
                Accum::Numeric {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls: _,
                },
            ) => {
                let mut cx_datum = numeric::cx_datum();
                let d = cx_datum.to_width(accum.0);
                // Take a wide decimal (aggregator) into a
                // narrow decimal (datum). If this operation
                // overflows the datum, this new value will be
                // +/- infinity. However, the aggregator tracks
                // the amount of overflow, making it invertible.
                let inf_d = d.is_infinite();
                let neg_d = d.is_negative();
                let pos_inf = pos_infs.is_positive() || (inf_d && !neg_d);
                let neg_inf = neg_infs.is_positive() || (inf_d && neg_d);
                if nans.is_positive() || (pos_inf && neg_inf) {
                    // NaNs are NaNs and cases where we've seen a
                    // mixture of positive and negative infinities.
                    Datum::from(Numeric::nan())
                } else if pos_inf {
                    Datum::from(Numeric::infinity())
                } else if neg_inf {
                    let mut cx = numeric::cx_datum();
                    let mut d = Numeric::infinity();
                    cx.neg(&mut d);
                    Datum::from(d)
                } else {
                    Datum::from(d)
                }
            }
            _ => panic!(
                "Unexpected accumulation (aggr={:?}, accum={accum:?})",
                aggr_func
            ),
        }
    }
}

/// The type for accumulator counting. Set to [`Overflowing<u128>`](mz_ore::Overflowing).
type AccumCount = mz_ore::Overflowing<i128>;

/// Accumulates values for the various types of accumulable aggregations.
///
/// We assume that there are not more than 2^32 elements for the aggregation.
/// Thus we can perform a summation over i32 in an i64 accumulator
/// and not worry about exceeding its bounds.
///
/// The float accumulator performs accumulation in fixed point arithmetic. The fixed
/// point representation has less precision than a double. It is entirely possible
/// that the values of the accumulator overflow, thus we have to use wrapping arithmetic
/// to preserve group guarantees.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
enum Accum {
    /// Accumulates boolean values.
    Bool {
        /// The number of `true` values observed.
        trues: Diff,
        /// The number of `false` values observed.
        falses: Diff,
    },
    /// Accumulates simple numeric values.
    SimpleNumber {
        /// The accumulation of all non-NULL values observed.
        accum: AccumCount,
        /// The number of non-NULL values observed.
        non_nulls: Diff,
    },
    /// Accumulates float values.
    Float {
        /// Accumulates non-special float values, mapped to a fixed precision i128 domain to
        /// preserve associativity and commutativity
        accum: AccumCount,
        /// Counts +inf
        pos_infs: Diff,
        /// Counts -inf
        neg_infs: Diff,
        /// Counts NaNs
        nans: Diff,
        /// Counts non-NULL values
        non_nulls: Diff,
    },
    /// Accumulates arbitrary precision decimals.
    Numeric {
        /// Accumulates non-special values
        accum: OrderedDecimal<NumericAgg>,
        /// Counts +inf
        pos_infs: Diff,
        /// Counts -inf
        neg_infs: Diff,
        /// Counts NaNs
        nans: Diff,
        /// Counts non-NULL values
        non_nulls: Diff,
    },
}

impl IsZero for Accum {
    fn is_zero(&self) -> bool {
        match self {
            Accum::Bool { trues, falses } => trues.is_zero() && falses.is_zero(),
            Accum::SimpleNumber { accum, non_nulls } => accum.is_zero() && non_nulls.is_zero(),
            Accum::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => {
                accum.is_zero()
                    && pos_infs.is_zero()
                    && neg_infs.is_zero()
                    && nans.is_zero()
                    && non_nulls.is_zero()
            }
            Accum::Numeric {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => {
                accum.0.is_zero()
                    && pos_infs.is_zero()
                    && neg_infs.is_zero()
                    && nans.is_zero()
                    && non_nulls.is_zero()
            }
        }
    }
}

impl Semigroup for Accum {
    fn plus_equals(&mut self, other: &Accum) {
        match (&mut *self, other) {
            (
                Accum::Bool { trues, falses },
                Accum::Bool {
                    trues: other_trues,
                    falses: other_falses,
                },
            ) => {
                *trues += other_trues;
                *falses += other_falses;
            }
            (
                Accum::SimpleNumber { accum, non_nulls },
                Accum::SimpleNumber {
                    accum: other_accum,
                    non_nulls: other_non_nulls,
                },
            ) => {
                *accum += other_accum;
                *non_nulls += other_non_nulls;
            }
            (
                Accum::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls,
                },
                Accum::Float {
                    accum: other_accum,
                    pos_infs: other_pos_infs,
                    neg_infs: other_neg_infs,
                    nans: other_nans,
                    non_nulls: other_non_nulls,
                },
            ) => {
                *accum = accum.checked_add(*other_accum).unwrap_or_else(|| {
                    warn!("Float accumulator overflow. Incorrect results possible");
                    accum.wrapping_add(*other_accum)
                });
                *pos_infs += other_pos_infs;
                *neg_infs += other_neg_infs;
                *nans += other_nans;
                *non_nulls += other_non_nulls;
            }
            (
                Accum::Numeric {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls,
                },
                Accum::Numeric {
                    accum: other_accum,
                    pos_infs: other_pos_infs,
                    neg_infs: other_neg_infs,
                    nans: other_nans,
                    non_nulls: other_non_nulls,
                },
            ) => {
                let mut cx_agg = numeric::cx_agg();
                cx_agg.add(&mut accum.0, &other_accum.0);
                // `rounded` signals we have exceeded the aggregator's max
                // precision, which means we've lost commutativity and
                // associativity; nothing to be done here, so panic. For more
                // context, see the DEC_Rounded definition at
                // http://speleotrove.com/decimal/dncont.html
                assert!(!cx_agg.status().rounded(), "Accum::Numeric overflow");
                // Reduce to reclaim unused decimal precision. Note that this
                // reduction must happen somewhere to make the following
                // invertible:
                // ```
                // CREATE TABLE a (a numeric);
                // CREATE MATERIALIZED VIEW t as SELECT sum(a) FROM a;
                // INSERT INTO a VALUES ('9e39'), ('9e-39');
                // ```
                // This will now return infinity. However, we can retract the
                // value that blew up its precision:
                // ```
                // INSERT INTO a VALUES ('-9e-39');
                // ```
                // This leaves `t`'s aggregator with a value of 9e39. However,
                // without doing a reduction, `libdecnum` will store the value
                // as 9e39+0e-39, which still exceeds the narrower context's
                // precision. By doing the reduction, we can "reclaim" the 39
                // digits of precision.
                cx_agg.reduce(&mut accum.0);
                *pos_infs += other_pos_infs;
                *neg_infs += other_neg_infs;
                *nans += other_nans;
                *non_nulls += other_non_nulls;
            }
            (l, r) => unreachable!(
                "Accumulator::plus_equals called with non-matching variants: {l:?} vs {r:?}"
            ),
        }
    }
}

impl Multiply<Diff> for Accum {
    type Output = Accum;

    fn multiply(self, factor: &Diff) -> Accum {
        let factor = *factor;
        match self {
            Accum::Bool { trues, falses } => Accum::Bool {
                trues: trues * factor,
                falses: falses * factor,
            },
            Accum::SimpleNumber { accum, non_nulls } => Accum::SimpleNumber {
                accum: accum * AccumCount::from(factor),
                non_nulls: non_nulls * factor,
            },
            Accum::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => Accum::Float {
                accum: accum
                    .checked_mul(AccumCount::from(factor))
                    .unwrap_or_else(|| {
                        warn!("Float accumulator overflow. Incorrect results possible");
                        accum.wrapping_mul(AccumCount::from(factor))
                    }),
                pos_infs: pos_infs * factor,
                neg_infs: neg_infs * factor,
                nans: nans * factor,
                non_nulls: non_nulls * factor,
            },
            Accum::Numeric {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => {
                let mut cx = numeric::cx_agg();
                let mut f = NumericAgg::from(factor.into_inner());
                // Unlike `plus_equals`, not necessary to reduce after this operation because `f` will
                // always be an integer, i.e. we are never increasing the
                // values' scale.
                cx.mul(&mut f, &accum.0);
                // `rounded` signals we have exceeded the aggregator's max
                // precision, which means we've lost commutativity and
                // associativity; nothing to be done here, so panic. For more
                // context, see the DEC_Rounded definition at
                // http://speleotrove.com/decimal/dncont.html
                assert!(!cx.status().rounded(), "Accum::Numeric multiply overflow");
                Accum::Numeric {
                    accum: OrderedDecimal(f),
                    pos_infs: pos_infs * factor,
                    neg_infs: neg_infs * factor,
                    nans: nans * factor,
                    non_nulls: non_nulls * factor,
                }
            }
        }
    }
}

impl Columnation for Accum {
    type InnerRegion = CopyRegion<Self>;
}

/// Monoids for in-place compaction of monotonic streams.
mod monoids {

    // We can improve the performance of some aggregations through the use of algebra.
    // In particular, we can move some of the aggregations in to the `diff` field of
    // updates, by changing `diff` from integers to a different algebraic structure.
    //
    // The one we use is called a "semigroup", and it means that the structure has a
    // symmetric addition operator. The trait we use also allows the semigroup elements
    // to present as "zero", meaning they always act as the identity under +. Here,
    // `Datum::Null` acts as the identity under +, _but_ we don't want to make this
    // known to DD by the `is_zero` method, see comment there. So, from the point of view
    // of DD, this Semigroup should _not_ have a zero.
    //
    // WARNING: `Datum::Null` should continue to act as the identity of our + (even if we
    // add a new enum variant here), because other code (e.g., `HierarchicalOneByOneAggr`)
    // assumes this.

    use columnation::{Columnation, Region};
    use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
    use mz_expr::AggregateFunc;
    use mz_ore::soft_panic_or_log;
    use mz_repr::{Datum, Diff, Row};
    use serde::{Deserialize, Serialize};

    /// A monoid containing a single-datum row.
    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Serialize, Deserialize, Hash)]
    pub enum ReductionMonoid {
        Min(Row),
        Max(Row),
    }

    impl ReductionMonoid {
        pub fn finalize(&self) -> &Row {
            use ReductionMonoid::*;
            match self {
                Min(row) | Max(row) => row,
            }
        }
    }

    impl Clone for ReductionMonoid {
        fn clone(&self) -> Self {
            use ReductionMonoid::*;
            match self {
                Min(row) => Min(row.clone()),
                Max(row) => Max(row.clone()),
            }
        }

        fn clone_from(&mut self, source: &Self) {
            use ReductionMonoid::*;

            let mut row = std::mem::take(match self {
                Min(row) | Max(row) => row,
            });

            let source_row = match source {
                Min(row) | Max(row) => row,
            };

            row.clone_from(source_row);

            match source {
                Min(_) => *self = Min(row),
                Max(_) => *self = Max(row),
            }
        }
    }

    impl Multiply<Diff> for ReductionMonoid {
        type Output = Self;

        fn multiply(self, factor: &Diff) -> Self {
            // Multiplication in ReductionMonoid is idempotent, and
            // its users must ascertain its monotonicity beforehand
            // (typically with ensure_monotonic) since it has no zero
            // value for us to use here.
            assert!(factor.is_positive());
            self
        }
    }

    impl Semigroup for ReductionMonoid {
        fn plus_equals(&mut self, rhs: &Self) {
            match (self, rhs) {
                (ReductionMonoid::Min(lhs), ReductionMonoid::Min(rhs)) => {
                    let swap = {
                        let lhs_val = lhs.unpack_first();
                        let rhs_val = rhs.unpack_first();
                        // Datum::Null is the identity, not a small element.
                        match (lhs_val, rhs_val) {
                            (_, Datum::Null) => false,
                            (Datum::Null, _) => true,
                            (lhs, rhs) => rhs < lhs,
                        }
                    };
                    if swap {
                        lhs.clone_from(rhs);
                    }
                }
                (ReductionMonoid::Max(lhs), ReductionMonoid::Max(rhs)) => {
                    let swap = {
                        let lhs_val = lhs.unpack_first();
                        let rhs_val = rhs.unpack_first();
                        // Datum::Null is the identity, not a large element.
                        match (lhs_val, rhs_val) {
                            (_, Datum::Null) => false,
                            (Datum::Null, _) => true,
                            (lhs, rhs) => rhs > lhs,
                        }
                    };
                    if swap {
                        lhs.clone_from(rhs);
                    }
                }
                (lhs, rhs) => {
                    soft_panic_or_log!(
                        "Mismatched monoid variants in reduction! lhs: {lhs:?} rhs: {rhs:?}"
                    );
                }
            }
        }
    }

    impl IsZero for ReductionMonoid {
        fn is_zero(&self) -> bool {
            // It totally looks like we could return true here for `Datum::Null`, but don't do this!
            // DD uses true results of this method to make stuff disappear. This makes sense when
            // diffs mean really just diffs, but for `ReductionMonoid` diffs hold reduction results.
            // We don't want funny stuff, like disappearing, happening to reduction results even
            // when they are null. (This would confuse, e.g., `ReduceCollation` for null inputs.)
            false
        }
    }

    impl Columnation for ReductionMonoid {
        type InnerRegion = ReductionMonoidRegion;
    }

    /// Region for [`ReductionMonoid`]. This region is special in that it stores both enum variants
    /// in the same backing region. Alternatively, it could store it in two regions, but we select
    /// the former for simplicity reasons.
    #[derive(Default)]
    pub struct ReductionMonoidRegion {
        inner: <Row as Columnation>::InnerRegion,
    }

    impl Region for ReductionMonoidRegion {
        type Item = ReductionMonoid;

        unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
            use ReductionMonoid::*;
            match item {
                Min(row) => Min(unsafe { self.inner.copy(row) }),
                Max(row) => Max(unsafe { self.inner.copy(row) }),
            }
        }

        fn clear(&mut self) {
            self.inner.clear();
        }

        fn reserve_items<'a, I>(&mut self, items: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self::Item> + Clone,
        {
            self.inner
                .reserve_items(items.map(ReductionMonoid::finalize));
        }

        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.inner.reserve_regions(regions.map(|r| &r.inner));
        }

        fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            self.inner.heap_size(callback);
        }
    }

    /// Get the correct monoid implementation for a given aggregation function. Note that
    /// all hierarchical aggregation functions need to supply a monoid implementation.
    pub fn get_monoid(row: Row, func: &AggregateFunc) -> Option<ReductionMonoid> {
        match func {
            AggregateFunc::MaxNumeric
            | AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxMzTimestamp
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MaxInterval
            | AggregateFunc::MaxTime => Some(ReductionMonoid::Max(row)),
            AggregateFunc::MinNumeric
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinMzTimestamp
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::MinInterval
            | AggregateFunc::MinTime => Some(ReductionMonoid::Min(row)),
            AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumNumeric
            | AggregateFunc::Count
            | AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::Dummy
            | AggregateFunc::JsonbAgg { .. }
            | AggregateFunc::JsonbObjectAgg { .. }
            | AggregateFunc::MapAgg { .. }
            | AggregateFunc::ArrayConcat { .. }
            | AggregateFunc::ListConcat { .. }
            | AggregateFunc::StringAgg { .. }
            | AggregateFunc::RowNumber { .. }
            | AggregateFunc::Rank { .. }
            | AggregateFunc::DenseRank { .. }
            | AggregateFunc::LagLead { .. }
            | AggregateFunc::FirstValue { .. }
            | AggregateFunc::LastValue { .. }
            | AggregateFunc::WindowAggregate { .. }
            | AggregateFunc::FusedValueWindowFunc { .. }
            | AggregateFunc::FusedWindowAggregate { .. } => None,
        }
    }
}

mod window_agg_helpers {
    use crate::render::reduce::*;

    /// TODO: It would be better for performance to do the branching that is in the methods of this
    /// enum at the place where we are calling `eval_fast_window_agg`. Then we wouldn't need an enum
    /// here, and would parameterize `eval_fast_window_agg` with one of the implementations
    /// directly.
    pub enum OneByOneAggrImpls {
        Accumulable(AccumulableOneByOneAggr),
        Hierarchical(HierarchicalOneByOneAggr),
        Basic(mz_expr::NaiveOneByOneAggr),
    }

    impl mz_expr::OneByOneAggr for OneByOneAggrImpls {
        fn new(agg: &AggregateFunc, reverse: bool) -> Self {
            match reduction_type(agg) {
                ReductionType::Basic => {
                    OneByOneAggrImpls::Basic(mz_expr::NaiveOneByOneAggr::new(agg, reverse))
                }
                ReductionType::Accumulable => {
                    OneByOneAggrImpls::Accumulable(AccumulableOneByOneAggr::new(agg))
                }
                ReductionType::Hierarchical => {
                    OneByOneAggrImpls::Hierarchical(HierarchicalOneByOneAggr::new(agg))
                }
            }
        }

        fn give(&mut self, d: &Datum) {
            match self {
                OneByOneAggrImpls::Basic(i) => i.give(d),
                OneByOneAggrImpls::Accumulable(i) => i.give(d),
                OneByOneAggrImpls::Hierarchical(i) => i.give(d),
            }
        }

        fn get_current_aggregate<'a>(&self, temp_storage: &'a RowArena) -> Datum<'a> {
            // Note that the `reverse` parameter is currently forwarded only for Basic aggregations.
            match self {
                OneByOneAggrImpls::Basic(i) => i.get_current_aggregate(temp_storage),
                OneByOneAggrImpls::Accumulable(i) => i.get_current_aggregate(temp_storage),
                OneByOneAggrImpls::Hierarchical(i) => i.get_current_aggregate(temp_storage),
            }
        }
    }

    pub struct AccumulableOneByOneAggr {
        aggr_func: AggregateFunc,
        accum: Accum,
        total: Diff,
    }

    impl AccumulableOneByOneAggr {
        fn new(aggr_func: &AggregateFunc) -> Self {
            AccumulableOneByOneAggr {
                aggr_func: aggr_func.clone(),
                accum: accumulable_zero(aggr_func),
                total: Diff::ZERO,
            }
        }

        fn give(&mut self, d: &Datum) {
            self.accum
                .plus_equals(&datum_to_accumulator(&self.aggr_func, d.clone()));
            self.total += Diff::ONE;
        }

        fn get_current_aggregate<'a>(&self, temp_storage: &'a RowArena) -> Datum<'a> {
            temp_storage.make_datum(|packer| {
                packer.push(finalize_accum(&self.aggr_func, &self.accum, self.total));
            })
        }
    }

    pub struct HierarchicalOneByOneAggr {
        aggr_func: AggregateFunc,
        // Warning: We are assuming that `Datum::Null` acts as the identity for `ReductionMonoid`'s
        // `plus_equals`. (But _not_ relying here on `ReductionMonoid::is_zero`.)
        monoid: ReductionMonoid,
    }

    impl HierarchicalOneByOneAggr {
        fn new(aggr_func: &AggregateFunc) -> Self {
            let mut row_buf = Row::default();
            row_buf.packer().push(Datum::Null);
            HierarchicalOneByOneAggr {
                aggr_func: aggr_func.clone(),
                monoid: get_monoid(row_buf, aggr_func)
                    .expect("aggr_func should be a hierarchical aggregation function"),
            }
        }

        fn give(&mut self, d: &Datum) {
            let mut row_buf = Row::default();
            row_buf.packer().push(d);
            let m = get_monoid(row_buf, &self.aggr_func)
                .expect("aggr_func should be a hierarchical aggregation function");
            self.monoid.plus_equals(&m);
        }

        fn get_current_aggregate<'a>(&self, temp_storage: &'a RowArena) -> Datum<'a> {
            temp_storage.make_datum(|packer| packer.extend(self.monoid.finalize().iter()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The saturating conversion that `float_to_fixed_point` replaces. Used to
    /// assert that the new wrapping conversion agrees on the in-range values
    /// where the old conversion was already correct.
    #[allow(clippy::as_conversions)]
    fn saturating_convert(n: f64) -> i128 {
        (n * FLOAT_SCALE) as i128
    }

    #[mz_ore::test]
    fn float_to_fixed_point_matches_saturating_in_range() {
        // For values whose scaled magnitude comfortably fits in an `i128`, the
        // wrapping conversion must produce exactly the same result the previous
        // saturating cast did.
        let cases = [
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.1,
            -0.1,
            0.5,
            -0.5,
            3.25,
            -3.25,
            123456.789,
            -123456.789,
            1e10,
            -1e10,
            1e20,
            -1e20,
            5e30, // large, but scaled magnitude still fits comfortably in i128
            -5e30,
        ];
        for n in cases {
            assert_eq!(
                float_to_fixed_point(n),
                saturating_convert(n),
                "mismatch for n = {n}"
            );
        }
    }

    #[mz_ore::test]
    fn float_to_fixed_point_truncates_toward_zero() {
        // 1.75 * 2^24 = 29360128, exactly representable.
        assert_eq!(float_to_fixed_point(1.75), 29_360_128);
        assert_eq!(float_to_fixed_point(-1.75), -29_360_128);

        // Fractional results truncate toward zero, matching the previous cast.
        let frac = 0.123_456_7_f64;
        assert_eq!(float_to_fixed_point(frac), saturating_convert(frac));
        assert_eq!(float_to_fixed_point(-frac), saturating_convert(-frac));
        assert_eq!(float_to_fixed_point(-frac), -float_to_fixed_point(frac));
    }

    #[mz_ore::test]
    fn float_to_fixed_point_subnormals_round_to_zero() {
        assert_eq!(float_to_fixed_point(0.0), 0);
        assert_eq!(float_to_fixed_point(-0.0), 0);
        assert_eq!(float_to_fixed_point(f64::MIN_POSITIVE / 2.0), 0);
        assert_eq!(float_to_fixed_point(5e-324), 0); // smallest subnormal
    }

    #[mz_ore::test]
    fn float_to_fixed_point_cancels_large_finite_values() {
        // Regression test for database-issues#11265: large finite values that
        // individually overflow the fixed-point domain must still sum to the
        // correct result when their mathematical sum is representable. The
        // previous saturating conversion produced `i128::MAX + i128::MIN == -1`.
        for &n in &[1.1e31_f64, 1e32, 5e33, 1e284] {
            assert_eq!(
                float_to_fixed_point(n).wrapping_add(float_to_fixed_point(-n)),
                0,
                "n = {n} did not cancel with -n"
            );
        }
    }

    #[mz_ore::test]
    fn float_to_fixed_point_sum_via_accumulator() {
        // Exercise the full accumulate-then-finalize path for the reported case.
        let func = AggregateFunc::SumFloat64;
        let mut acc = accumulable_zero(&func);
        acc.plus_equals(&datum_to_accumulator(&func, Datum::from(1.1e31_f64)));
        acc.plus_equals(&datum_to_accumulator(&func, Datum::from(-1.1e31_f64)));
        let datum = finalize_accum(&func, &acc, Diff::from(2_i64));
        assert_eq!(datum, Datum::from(0.0_f64));
    }
}
