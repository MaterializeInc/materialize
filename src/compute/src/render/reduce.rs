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
use std::sync::LazyLock;

use dec::OrderedDecimal;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{Batch, Batcher, Builder, Trace, TraceReader};
use differential_dataflow::{Collection, Diff as _};
use mz_compute_types::plan::reduce::{
    reduction_type, AccumulablePlan, BasicPlan, BucketedPlan, HierarchicalPlan, KeyValPlan,
    MonotonicPlan, ReducePlan, ReductionType,
};
use mz_expr::{
    AggregateExpr, AggregateFunc, EvalError, MapFilterProject, MirScalarExpr, SafeMfpPlan,
};
use mz_repr::adt::numeric::{self, Numeric, NumericAgg};
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{Datum, DatumList, DatumVec, Diff, Row, RowArena, SharedRow};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use serde::{Deserialize, Serialize};
use timely::container::columnation::{Columnation, CopyRegion, TimelyStack};
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::Container;
use tracing::warn;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::extensions::reduce::{MzReduce, ReduceExt};
use crate::render::context::{CollectionBundle, Context, MzArrangement};
use crate::render::errors::MaybeValidatingRow;
use crate::render::reduce::monoids::{get_monoid, ReductionMonoid};
use crate::render::{ArrangementFlavor, Pairer};
use crate::row_spine::DatumSeq;
use crate::typedefs::{
    KeyBatcher, RowErrSpine, RowRowArrangement, RowRowSpine, RowSpine, RowValSpine,
};

impl<G, T> Context<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
{
    /// Renders a `MirRelationExpr::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    pub fn render_reduce(
        &mut self,
        input: CollectionBundle<G, T>,
        key_val_plan: KeyValPlan,
        reduce_plan: ReducePlan,
        input_key: Option<Vec<MirScalarExpr>>,
        mfp_after: Option<MapFilterProject>,
    ) -> CollectionBundle<G, T> {
        // Convert `mfp_after` to an actionable plan.
        let mfp_after = mfp_after.map(|m| {
            m.into_plan()
                .expect("MFP planning must succeed")
                .into_nontemporal()
                .expect("Fused Reduce MFPs do not have temporal predicates")
        });

        input.scope().region_named("Reduce", |inner| {
            let KeyValPlan {
                mut key_plan,
                mut val_plan,
            } = key_val_plan;
            let key_arity = key_plan.projection.len();
            let mut datums = DatumVec::new();
            let (key_val_input, err_input): (
                timely::dataflow::Stream<_, (Result<(Row, Row), DataflowError>, _, _)>,
                _,
            ) = input
                .enter_region(inner)
                .flat_map(input_key.map(|k| (k, None)), || {
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
                    key_plan.permute(demand_map.clone(), demand_map_len);
                    val_plan.permute(demand_map, demand_map_len);
                    let skips = mz_compute_types::plan::reduce::convert_indexes_to_skips(demand);
                    move |row_datums, time, diff| {
                        let binding = SharedRow::get();
                        let mut row_builder = binding.borrow_mut();
                        let temp_storage = RowArena::new();

                        let mut row_iter = row_datums.drain(..);
                        let mut datums_local = datums.borrow();
                        // Unpack only the demanded columns.
                        for skip in skips.iter() {
                            datums_local.push(row_iter.nth(*skip).unwrap());
                        }

                        // Evaluate the key expressions.
                        let key = match key_plan.evaluate_into(
                            &mut datums_local,
                            &temp_storage,
                            &mut row_builder,
                        ) {
                            Err(e) => {
                                return Some((
                                    Err(DataflowError::from(e)),
                                    time.clone(),
                                    diff.clone(),
                                ))
                            }
                            Ok(key) => key.expect("Row expected as no predicate was used"),
                        };
                        // Evaluate the value expressions.
                        // The prior evaluation may have left additional columns we should delete.
                        datums_local.truncate(skips.len());
                        let val = match val_plan.evaluate_iter(&mut datums_local, &temp_storage) {
                            Err(e) => {
                                return Some((
                                    Err(DataflowError::from(e)),
                                    time.clone(),
                                    diff.clone(),
                                ))
                            }
                            Ok(val) => val.expect("Row expected as no predicate was used"),
                        };
                        row_builder.packer().extend(val);
                        let row = row_builder.clone();
                        Some((Ok((key, row)), time.clone(), diff.clone()))
                    }
                });

            // Demux out the potential errors from key and value selector evaluation.
            let (ok, mut err) = key_val_input
                .as_collection()
                .flat_map_fallible::<ConsolidatingContainerBuilder<_>, ConsolidatingContainerBuilder<_>, _, _, _, _>("OkErrDemux", Some);

            err = err.concat(&err_input);

            // Render the reduce plan
            self.render_reduce_plan(reduce_plan, ok, err, key_arity, mfp_after)
                .leave_region()
        })
    }

    /// Render a dataflow based on the provided plan.
    ///
    /// The output will be an arrangements that looks the same as if
    /// we just had a single reduce operator computing everything together, and
    /// this arrangement can also be re-used.
    fn render_reduce_plan<S>(
        &self,
        plan: ReducePlan,
        collection: Collection<S, (Row, Row), Diff>,
        err_input: Collection<S, DataflowError, Diff>,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan>,
    ) -> CollectionBundle<S, T>
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let mut errors = Default::default();
        let arrangement =
            self.render_reduce_plan_inner(plan, collection, &mut errors, key_arity, mfp_after);
        let errs: KeyCollection<_, _, _> = err_input.concatenate(errors).into();
        CollectionBundle::from_columns(
            0..key_arity,
            ArrangementFlavor::Local(arrangement, errs.mz_arrange("Arrange bundle err")),
        )
    }

    fn render_reduce_plan_inner<S>(
        &self,
        plan: ReducePlan,
        collection: Collection<S, (Row, Row), Diff>,
        errors: &mut Vec<Collection<S, DataflowError, Diff>>,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan>,
    ) -> MzArrangement<S>
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        // TODO(vmarcos): Arrangement specialization here could eventually be extended to keys,
        // not only values (#22103).
        let arrangement = match plan {
            // If we have no aggregations or just a single type of reduction, we
            // can go ahead and render them directly.
            ReducePlan::Distinct => {
                let (arranged_output, errs) = self.dispatch_build_distinct(collection, mfp_after);
                errors.push(errs);
                arranged_output
            }
            ReducePlan::Accumulable(expr) => {
                let (arranged_output, errs) =
                    self.build_accumulable(collection, expr, key_arity, mfp_after);
                errors.push(errs);
                MzArrangement::RowRow(arranged_output)
            }
            ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(expr)) => {
                let (output, errs) = self.build_monotonic(collection, expr, mfp_after);
                errors.push(errs);
                MzArrangement::RowRow(output)
            }
            ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(expr)) => {
                let (output, errs) = self.build_bucketed(collection, expr, key_arity, mfp_after);
                errors.push(errs);
                MzArrangement::RowRow(output)
            }
            ReducePlan::Basic(BasicPlan::Single(index, aggr)) => {
                let (output, errs) = self
                    .build_basic_aggregate(collection, index, &aggr, true, key_arity, mfp_after);
                errors.push(errs.expect("validation should have occurred as it was requested"));
                MzArrangement::RowRow(output)
            }
            ReducePlan::Basic(BasicPlan::Multiple(aggrs)) => {
                let (output, errs) =
                    self.build_basic_aggregates(collection, aggrs, key_arity, mfp_after);
                errors.push(errs);
                MzArrangement::RowRow(output)
            }
            // Otherwise, we need to render something different for each type of
            // reduction, and then stitch them together.
            ReducePlan::Collation(expr) => {
                // First, we need to render our constituent aggregations.
                let mut to_collate = vec![];

                for plan in [
                    expr.hierarchical.map(ReducePlan::Hierarchical),
                    expr.accumulable.map(ReducePlan::Accumulable),
                    expr.basic.map(ReducePlan::Basic),
                ]
                .into_iter()
                .flat_map(std::convert::identity)
                {
                    let r#type = ReductionType::try_from(&plan)
                        .expect("only representable reduction types were used above");

                    let arrangement = match self.render_reduce_plan_inner(
                        plan,
                        collection.clone(),
                        errors,
                        key_arity,
                        None,
                    ) {
                        MzArrangement::RowRow(arranged) => arranged,
                    };
                    to_collate.push((r#type, arrangement));
                }

                // Now we need to collate them together.
                let (oks, errs) = self.build_collation(
                    to_collate,
                    expr.aggregate_types,
                    &mut collection.scope(),
                    mfp_after,
                );
                errors.push(errs);
                MzArrangement::RowRow(oks)
            }
        };
        arrangement
    }

    /// Build the dataflow to combine arrangements containing results of different
    /// aggregation types into a single arrangement.
    ///
    /// This computes the same thing as a join on the group key followed by shuffling
    /// the values into the correct order. This implementation assumes that all input
    /// arrangements present values in a way that respects the desired output order,
    /// so we can do a linear merge to form the output.
    fn build_collation<S>(
        &self,
        arrangements: Vec<(ReductionType, RowRowArrangement<S>)>,
        aggregate_types: Vec<ReductionType>,
        scope: &mut S,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (RowRowArrangement<S>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let error_logger = self.error_logger();

        // We must have more than one arrangement to collate.
        if arrangements.len() <= 1 {
            error_logger.soft_panic_or_log(
                "Incorrect number of arrangements in reduce collation",
                &format!("len={}", arrangements.len()),
            );
        }

        let mut to_concat = vec![];

        // First, lets collect all results into a single collection.
        for (reduction_type, arrangement) in arrangements.into_iter() {
            let collection = arrangement.as_collection(move |key, val| {
                (key.into_owned(), (reduction_type, val.into_owned()))
            });
            to_concat.push(collection);
        }

        // For each key above, we need to have exactly as many rows as there are distinct
        // reduction types required by `aggregate_types`. We thus prepare here a properly
        // deduplicated version of `aggregate_types` for validation during processing below.
        let mut distinct_aggregate_types = aggregate_types.clone();
        distinct_aggregate_types.sort_unstable();
        distinct_aggregate_types.dedup();
        let n_distinct_aggregate_types = distinct_aggregate_types.len();

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());

        let aggregate_types_err = aggregate_types.clone();
        let (oks, errs) = differential_dataflow::collection::concatenate(scope, to_concat)
            .mz_arrange::<RowValSpine<_, _, _>>("Arrange ReduceCollation")
            .reduce_pair::<_, _, _, RowRowSpine<_, _>, _, _, RowErrSpine<_, _>>(
                "ReduceCollation",
                "ReduceCollation Errors",
                {
                    move |key, input, output| {
                        // The inputs are pairs of a reduction type, and a row consisting of densely
                        // packed fused aggregate values.
                        //
                        // We need to reconstitute the final value by:
                        // 1. Extracting out the fused rows by type
                        // 2. For each aggregate, figure out what type it is, and grab the relevant
                        //    value from the corresponding fused row.
                        // 3. Stitch all the values together into one row.
                        let mut accumulable = DatumList::empty().iter();
                        let mut hierarchical = DatumList::empty().iter();
                        let mut basic = DatumList::empty().iter();

                        // Note that hierarchical and basic reductions guard against negative
                        // multiplicities, and if we only had accumulable aggregations, we would not
                        // have produced a collation plan, so we do not repeat the check here.
                        if input.len() != n_distinct_aggregate_types {
                            return;
                        }

                        for (item, _) in input.iter() {
                            let reduction_type = &item.0;
                            let row = &item.1;
                            match reduction_type {
                                ReductionType::Accumulable => accumulable = row.iter(),
                                ReductionType::Hierarchical => hierarchical = row.iter(),
                                ReductionType::Basic => basic = row.iter(),
                            }
                        }

                        let temp_storage = RowArena::new();
                        let datum_iter = key.to_datum_iter();
                        let mut datums_local = datums1.borrow();
                        datums_local.extend(datum_iter);
                        let key_len = datums_local.len();

                        // Merge results into the order they were asked for.
                        for typ in aggregate_types.iter() {
                            let datum = match typ {
                                ReductionType::Accumulable => accumulable.next(),
                                ReductionType::Hierarchical => hierarchical.next(),
                                ReductionType::Basic => basic.next(),
                            };
                            let Some(datum) = datum else { return };
                            datums_local.push(datum);
                        }

                        // If we did not have enough values to stitch together, then we do not
                        // generate an output row. Not outputting here corresponds to the semantics
                        // of an equi-join on the key, similarly to the proposal in PR #17013.
                        //
                        // Note that we also do not want to have anything left over to stich. If we
                        // do, then we also have an error, reported elsewhere, and would violate
                        // join semantics.
                        if (accumulable.next(), hierarchical.next(), basic.next())
                            == (None, None, None)
                        {
                            if let Some(row) = evaluate_mfp_after(
                                &mfp_after1,
                                &mut datums_local,
                                &temp_storage,
                                key_len,
                            ) {
                                output.push((row, 1));
                            }
                        }
                    }
                },
                move |key, input, output| {
                    if input.len() != n_distinct_aggregate_types {
                        // We expected to stitch together exactly as many aggregate types as requested
                        // by the collation. If we cannot, we log an error and produce no output for
                        // this key.
                        let message = "Mismatched aggregates for key in ReduceCollation";
                        error_logger.log(
                            message,
                            &format!(
                                "key={key:?}, n_aggregates_requested={requested}, \
                                 n_distinct_aggregate_types={n_distinct_aggregate_types}",
                                requested = input.len(),
                            ),
                        );
                        output.push((EvalError::Internal(message.to_string()).into(), 1));
                        return;
                    }

                    let mut accumulable = DatumList::empty().iter();
                    let mut hierarchical = DatumList::empty().iter();
                    let mut basic = DatumList::empty().iter();
                    for (item, _) in input.iter() {
                        let reduction_type = &item.0;
                        let row = &item.1;
                        match reduction_type {
                            ReductionType::Accumulable => accumulable = row.iter(),
                            ReductionType::Hierarchical => hierarchical = row.iter(),
                            ReductionType::Basic => basic = row.iter(),
                        }
                    }

                    let temp_storage = RowArena::new();
                    let datum_iter = key.to_datum_iter();
                    let mut datums_local = datums2.borrow();
                    datums_local.extend(datum_iter);

                    for typ in aggregate_types_err.iter() {
                        let datum = match typ {
                            ReductionType::Accumulable => accumulable.next(),
                            ReductionType::Hierarchical => hierarchical.next(),
                            ReductionType::Basic => basic.next(),
                        };
                        if let Some(datum) = datum {
                            datums_local.push(datum);
                        } else {
                            // We cannot properly reconstruct a row if aggregates are missing.
                            // This situation is not expected, so we log an error if it occurs.
                            let message = "Missing value for key in ReduceCollation";
                            error_logger.log(message, &format!("typ={typ:?}, key={key:?}"));
                            output.push((EvalError::Internal(message.to_string()).into(), 1));
                            return;
                        }
                    }

                    // Note that we also do not want to have anything left over to stich.
                    // If we do, then we also have an error and would violate join semantics.
                    if (accumulable.next(), hierarchical.next(), basic.next()) != (None, None, None)
                    {
                        let message = "Rows too large for key in ReduceCollation";
                        error_logger.log(message, &format!("key={key:?}"));
                        output.push((EvalError::Internal(message.to_string()).into(), 1));
                    }

                    // Finally, if `mfp_after` can produce errors, then we should also report
                    // these here.
                    let Some(mfp) = &mfp_after2 else { return };
                    if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage) {
                        output.push((e.into(), 1));
                    }
                },
            );
        (oks, errs.as_collection(|_, v| v.clone()))
    }

    fn dispatch_build_distinct<S>(
        &self,
        collection: Collection<S, (Row, Row), Diff>,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (MzArrangement<S>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let (arrangement, errs) = self
            .build_distinct::<RowRowSpine<_, _>, RowErrSpine<_, _>, _>(collection, "", mfp_after);
        (MzArrangement::RowRow(arrangement), errs)
    }

    /// Build the dataflow to compute the set of distinct keys.
    fn build_distinct<T1, T2, S>(
        &self,
        collection: Collection<S, (Row, Row), Diff>,
        tag: &str,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (
        Arranged<S, TraceAgent<T1>>,
        Collection<S, DataflowError, Diff>,
    )
    where
        S: Scope<Timestamp = G::Timestamp>,
        for<'a> T1: Trace<Val<'a> = DatumSeq<'a>, Time = G::Timestamp, Diff = Diff> + 'static,
        for<'a> T1::Key<'a>: IntoOwned<'a, Owned = Row>,
        T1::Batch: Batch,
        T1::Batcher: Batcher<Input = Vec<((Row, Row), G::Timestamp, Diff)>>,
        <T1::Batcher as Batcher>::Output: Container + PushInto<((Row, Row), T1::Time, T1::Diff)>,
        T1::Builder: Builder,
        for<'a> T1::Key<'a>: std::fmt::Debug + ToDatumIter,
        Arranged<S, TraceAgent<T1>>: ArrangementSize,
        T2: for<'a> Trace<
                Key<'a> = T1::Key<'a>,
                Val<'a> = &'a DataflowError,
                Time = G::Timestamp,
                Diff = Diff,
            > + 'static,
        T2::Batch: Batch,
        T2::Batcher: Batcher<Input = Vec<((Row, DataflowError), G::Timestamp, Diff)>>,
        <T2::Batcher as Batcher>::Output:
            Container + PushInto<((Row, DataflowError), T2::Time, T2::Diff)>,
        Arranged<S, TraceAgent<T2>>: ArrangementSize,
    {
        let error_logger = self.error_logger();

        let (input_name, output_name) = (
            format!("Arranged DistinctBy{}", tag),
            format!("DistinctBy{}", tag),
        );

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());

        let (output, errors) = collection
            .mz_arrange::<T1>(&input_name)
            .reduce_pair::<_, Row, Row, T1, _, _, T2>(
                &output_name,
                "DistinctByErrorCheck",
                move |key, _input, output| {
                    let temp_storage = RowArena::new();
                    let mut datums_local = datums1.borrow();
                    datums_local.extend(key.to_datum_iter());

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
                        output.push((Row::default(), 1));
                    }
                },
                move |key, input: &[(_, Diff)], output: &mut Vec<(DataflowError, _)>| {
                    for (_, count) in input.iter() {
                        if count.is_positive() {
                            continue;
                        }
                        let message = "Non-positive multiplicity in DistinctBy";
                        error_logger.log(message, &format!("row={key:?}, count={count}"));
                        output.push((EvalError::Internal(message.to_string()).into(), 1));
                        return;
                    }
                    // If `mfp_after` can error, then evaluate it here.
                    let Some(mfp) = &mfp_after2 else { return };
                    let temp_storage = RowArena::new();
                    let datum_iter = key.to_datum_iter();
                    let mut datums_local = datums2.borrow();
                    datums_local.extend(datum_iter);

                    if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage) {
                        output.push((e.into(), 1));
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
    fn build_basic_aggregates<S>(
        &self,
        input: Collection<S, (Row, Row), Diff>,
        aggrs: Vec<(usize, AggregateExpr)>,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (RowRowArrangement<S>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
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
        for (index, aggr) in aggrs {
            let (result, errs) = self.build_basic_aggregate(
                input.clone(),
                index,
                &aggr,
                err_output.is_none(),
                key_arity,
                None,
            );
            if errs.is_some() {
                err_output = errs
            }
            to_collect.push(
                result.as_collection(move |key, val| (key.into_owned(), (index, val.into_owned()))),
            );
        }

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());

        let arranged =
            differential_dataflow::collection::concatenate(&mut input.scope(), to_collect)
                .mz_arrange::<RowValSpine<_, _, _>>("Arranged ReduceFuseBasic input");

        let output = arranged.mz_reduce_abelian::<_, _, _, RowRowSpine<_, _>>("ReduceFuseBasic", {
            move |key, input, output| {
                let temp_storage = RowArena::new();
                let datum_iter = key.to_datum_iter();
                let mut datums_local = datums1.borrow();
                datums_local.extend(datum_iter);
                let key_len = datums_local.len();

                for ((_, row), _) in input.iter() {
                    datums_local.push(row.unpack_first());
                }

                if let Some(row) =
                    evaluate_mfp_after(&mfp_after1, &mut datums_local, &temp_storage, key_len)
                {
                    output.push((row, 1));
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
                .mz_reduce_abelian::<_, _, _, RowErrSpine<_, _>>(
                    "ReduceFuseBasic Error Check",
                    move |key, input, output| {
                        // Since negative accumulations are checked in at least one component
                        // aggregate, we only need to look for MFP errors here.
                        let temp_storage = RowArena::new();
                        let datum_iter = key.to_datum_iter();
                        let mut datums_local = datums2.borrow();
                        datums_local.extend(datum_iter);

                        for ((_, row), _) in input.iter() {
                            datums_local.push(row.unpack_first());
                        }

                        if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage)
                        {
                            output.push((e.into(), 1));
                        }
                    },
                )
                .as_collection(|_, v| v.into_owned());
            (output, validation_errs.concat(&mfp_errs))
        } else {
            (output, validation_errs)
        }
    }

    /// Build the dataflow to compute a single basic aggregation.
    ///
    /// This method also applies distinctness if required.
    fn build_basic_aggregate<S>(
        &self,
        input: Collection<S, (Row, Row), Diff>,
        index: usize,
        aggr: &AggregateExpr,
        validating: bool,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (
        RowRowArrangement<S>,
        Option<Collection<S, DataflowError, Diff>>,
    )
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let AggregateExpr {
            func,
            expr: _,
            distinct,
        } = aggr.clone();

        // Extract the value we were asked to aggregate over.
        let mut partial = input.map(move |(key, row)| {
            let binding = SharedRow::get();
            let mut row_builder = binding.borrow_mut();
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
                    .build_reduce_inaccumulable_distinct::<_, _, RowValSpine<Result<(), String>, _, _>>(keyed, None)
                    .as_collection(|k, v| (k.into_owned(), v.into_owned()))
                    .map_fallible::<CapacityContainerBuilder<_>, CapacityContainerBuilder<_>, _, _, _>("Demux Errors", move |(key_val, result)| match result {
                        Ok(()) => Ok(pairer.split(&key_val)),
                        Err(m) => Err(EvalError::Internal(m).into()),
                    });
                err_output = Some(errs);
                partial = oks;
            } else {
                partial = self
                    .build_reduce_inaccumulable_distinct::<_, _, RowSpine<_, _>>(
                        keyed,
                        Some(" [val: empty]"),
                    )
                    .as_collection(move |key_val_iter, _| pairer.split(key_val_iter));
            }
        }

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());
        let func2 = func.clone();

        let arranged = partial.mz_arrange::<RowRowSpine<_, _>>("Arranged ReduceInaccumulable");
        let oks =
            arranged.mz_reduce_abelian::<_, _, _, RowRowSpine<_, _>>("ReduceInaccumulable", {
                move |key, source, target| {
                    // We respect the multiplicity here (unlike in hierarchical aggregation)
                    // because we don't know that the aggregation method is not sensitive
                    // to the number of records.
                    let iter = source.iter().flat_map(|(v, w)| {
                        // Note that in the non-positive case, this is wrong, but harmless because
                        // our other reduction will produce an error.
                        let count = usize::try_from(*w).unwrap_or(0);
                        std::iter::repeat(v.to_datum_iter().next().unwrap()).take(count)
                    });

                    let temp_storage = RowArena::new();
                    let datum_iter = key.to_datum_iter();
                    let mut datums_local = datums1.borrow();
                    datums_local.extend(datum_iter);
                    let key_len = datums_local.len();
                    datums_local.push(
                        // Note that this is not necessarily a window aggregation, in which case
                        // `eval_with_fast_window_agg` delegates to the normal `eval`.
                        func.eval_with_fast_window_agg::<_, window_agg_helpers::OneByOneAggrImpls>(
                            iter,
                            &temp_storage,
                        ),
                    );

                    if let Some(row) =
                        evaluate_mfp_after(&mfp_after1, &mut datums_local, &temp_storage, key_len)
                    {
                        target.push((row, 1));
                    }
                }
            });

        // Note that we would prefer to use `mz_timely_util::reduce::ReduceExt::reduce_pair` here, but
        // we then wouldn't be able to do this error check conditionally.  See its documentation for the
        // rationale around using a second reduction here.
        let must_validate = validating && err_output.is_none();
        if must_validate || mfp_after2.is_some() {
            let error_logger = self.error_logger();

            let errs = arranged
                .mz_reduce_abelian::<_, _, _, RowErrSpine<_, _>>(
                    "ReduceInaccumulable Error Check",
                    move |key, source, target| {
                        // Negative counts would be surprising, but until we are 100% certain we won't
                        // see them, we should report when we do. We may want to bake even more info
                        // in here in the future.
                        if must_validate {
                            for (value, count) in source.iter() {
                                if count.is_positive() {
                                    continue;
                                }
                                let value = value.into_owned();
                                let message = "Non-positive accumulation in ReduceInaccumulable";
                                error_logger
                                    .log(message, &format!("value={value:?}, count={count}"));
                                target.push((EvalError::Internal(message.to_string()).into(), 1));
                                return;
                            }
                        }

                        // We know that `mfp_after` can error if it exists, so try to evaluate it here.
                        let Some(mfp) = &mfp_after2 else { return };
                        let iter = source.iter().flat_map(|(mut v, w)| {
                            let count = usize::try_from(*w).unwrap_or(0);
                            // This would ideally use `to_datum_iter` but we cannot as it needs to
                            // borrow `v` and only presents datums with that lifetime, not any longer.
                            std::iter::repeat(v.next().unwrap()).take(count)
                        });

                        let temp_storage = RowArena::new();
                        let datum_iter = key.to_datum_iter();
                        let mut datums_local = datums2.borrow();
                        datums_local.extend(datum_iter);
                        datums_local.push(
                            func2.eval_with_fast_window_agg::<_, window_agg_helpers::OneByOneAggrImpls>(
                                iter,
                                &temp_storage,
                            ),
                        );
                        if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage)
                        {
                            target.push((e.into(), 1));
                        }
                    },
                )
                .as_collection(|_, v| v.into_owned());
            if let Some(e) = err_output {
                err_output = Some(e.concat(&errs));
            } else {
                err_output = Some(errs);
            }
        }
        (oks, err_output)
    }

    fn build_reduce_inaccumulable_distinct<S, V, Tr>(
        &self,
        input: Collection<S, Row, Diff>,
        name_tag: Option<&str>,
    ) -> Arranged<S, TraceAgent<Tr>>
    where
        S: Scope<Timestamp = G::Timestamp>,
        V: MaybeValidatingRow<(), String>,
        Tr: Trace
            + for<'a> TraceReader<Key<'a> = DatumSeq<'a>, Time = G::Timestamp, Diff = Diff>
            + 'static,
        Tr::Batch: Batch,
        Tr::Builder: Builder<Input = TimelyStack<((Row, V), G::Timestamp, Diff)>>,
        for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = Row>,
        for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = V>,
        Arranged<S, TraceAgent<Tr>>: ArrangementSize,
    {
        let error_logger = self.error_logger();

        let output_name = format!(
            "ReduceInaccumulable Distinct{}",
            name_tag.unwrap_or_default()
        );

        let input: KeyCollection<_, _, _> = input.into();
        input
            .mz_arrange::<RowSpine<_, _>>("Arranged ReduceInaccumulable Distinct [val: empty]")
            .mz_reduce_abelian::<_, _, _, Tr>(&output_name, move |_, source, t| {
                if let Some(err) = V::into_error() {
                    for (value, count) in source.iter() {
                        if count.is_positive() {
                            continue;
                        }

                        let message = "Non-positive accumulation in ReduceInaccumulable DISTINCT";
                        error_logger.log(message, &format!("value={value:?}, count={count}"));
                        t.push((err(message.to_string()), 1));
                        return;
                    }
                }
                t.push((V::ok(()), 1))
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
    fn build_bucketed<S>(
        &self,
        input: Collection<S, (Row, Row), Diff>,
        BucketedPlan {
            aggr_funcs,
            skips,
            buckets,
        }: BucketedPlan,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (RowRowArrangement<S>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let mut err_output: Option<Collection<S, _, _>> = None;
        let arranged_output = input.scope().region_named("ReduceHierarchical", |inner| {
            let input = input.enter(inner);

            // The first mod to apply to the hash.
            let first_mod = buckets.get(0).copied().unwrap_or(1);

            // Gather the relevant keys with their hashes along with values ordered by aggregation_index.
            let mut stage = input.map(move |(key, row)| {
                let binding = SharedRow::get();
                let mut row_builder = binding.borrow_mut();
                let mut row_packer = row_builder.packer();
                let mut row_iter = row.iter();
                for skip in skips.iter() {
                    row_packer.push(row_iter.nth(*skip).unwrap());
                }
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
                            std::iter::once(Datum::from(hash)).chain(hash_key_iter.take(key_arity)),
                        );
                        (hash_key, values)
                    })
                };

                // We only want the first stage to perform validation of whether invalid accumulations
                // were observed in the input. Subsequently, we will either produce an error in the error
                // stream or produce correct data in the output stream.
                let validating = err_output.is_none();

                let (oks, errs) = self.build_bucketed_stage(&aggr_funcs, &input, validating);
                if let Some(errs) = errs {
                    err_output = Some(errs.leave_region());
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
            let mfp_after1 = mfp_after.clone();
            let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());
            let aggr_funcs2 = aggr_funcs.clone();

            // Build a series of stages for the reduction
            // Arrange the final result into (key, Row)
            let error_logger = self.error_logger();
            // NOTE(vmarcos): The input operator name below is used in the tuning advice built-in
            // view mz_introspection.mz_expected_group_size_advice.
            let arranged = partial.mz_arrange::<RowRowSpine<_, _>>("Arrange ReduceMinsMaxes");
            // Note that we would prefer to use `mz_timely_util::reduce::ReduceExt::reduce_pair` here,
            // but we then wouldn't be able to do this error check conditionally.  See its documentation
            // for the rationale around using a second reduction here.
            let must_validate = err_output.is_none();
            if must_validate || mfp_after2.is_some() {
                let errs = arranged
                    .mz_reduce_abelian::<_, _, _, RowErrSpine<_, _>>(
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
                                    let val = val.into_owned();
                                    let message = "Non-positive accumulation in ReduceMinsMaxes";
                                    error_logger
                                        .log(message, &format!("val={val:?}, count={count}"));
                                    target
                                        .push((EvalError::Internal(message.to_string()).into(), 1));
                                    return;
                                }
                            }

                            // We know that `mfp_after` can error if it exists, so try to evaluate it here.
                            let Some(mfp) = &mfp_after2 else { return };
                            let temp_storage = RowArena::new();
                            let datum_iter = key.to_datum_iter();
                            let mut datums_local = datums2.borrow();
                            datums_local.extend(datum_iter);

                            let mut source_iters = source
                                .iter()
                                .map(|(values, _cnt)| *values)
                                .collect::<Vec<_>>();
                            for func in aggr_funcs2.iter() {
                                let column_iter = (0..source_iters.len())
                                    .map(|i| source_iters[i].next().unwrap());
                                datums_local.push(func.eval(column_iter, &temp_storage));
                            }
                            if let Result::Err(e) =
                                mfp.evaluate_inner(&mut datums_local, &temp_storage)
                            {
                                target.push((e.into(), 1));
                            }
                        },
                    )
                    .as_collection(|_, v| v.into_owned())
                    .leave_region();
                if let Some(e) = &err_output {
                    err_output = Some(e.concat(&errs));
                } else {
                    err_output = Some(errs);
                }
            }
            arranged
                .mz_reduce_abelian::<_, _, _, RowRowSpine<_, _>>(
                    "ReduceMinsMaxes",
                    move |key, source, target| {
                        let temp_storage = RowArena::new();
                        let datum_iter = key.to_datum_iter();
                        let mut datums_local = datums1.borrow();
                        datums_local.extend(datum_iter);
                        let key_len = datums_local.len();

                        let mut source_iters = source
                            .iter()
                            .map(|(values, _cnt)| *values)
                            .collect::<Vec<_>>();
                        for func in aggr_funcs.iter() {
                            let column_iter =
                                (0..source_iters.len()).map(|i| source_iters[i].next().unwrap());
                            datums_local.push(func.eval(column_iter, &temp_storage));
                        }

                        if let Some(row) = evaluate_mfp_after(
                            &mfp_after1,
                            &mut datums_local,
                            &temp_storage,
                            key_len,
                        ) {
                            target.push((row, 1));
                        }
                    },
                )
                .leave_region()
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
    fn build_bucketed_stage<S>(
        &self,
        aggr_funcs: &Vec<AggregateFunc>,
        input: &Collection<S, (Row, Row), Diff>,
        validating: bool,
    ) -> (
        Collection<S, (Row, Row), Diff>,
        Option<Collection<S, DataflowError, Diff>>,
    )
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let (input, negated_output, errs) = if validating {
            let (input, reduced) = self
                .build_bucketed_negated_output::<_, _, RowValSpine<Result<Row, Row>, _, _>>(
                    input,
                    aggr_funcs.clone(),
                );
            let (oks, errs) = reduced
                .as_collection(|k, v| (k.into_owned(), v.clone()))
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
                        Err(EvalError::Internal(message).into())
                    }
                    Ok(values) => Ok((hash_key, values)),
                },
            );
            (input, oks, Some(errs))
        } else {
            let (input, reduced) = self.build_bucketed_negated_output::<_, _, RowRowSpine<_, _>>(
                input,
                aggr_funcs.clone(),
            );
            // TODO: Here is a good moment where we could apply the next `mod` calculation. Note
            // that we need to apply the mod on both input and oks.
            let oks = reduced.as_collection(|k, v| (k.into_owned(), v.into_owned()));
            (input, oks, None)
        };

        let input = input.as_collection(|k, v| (k.into_owned(), v.into_owned()));
        let oks = negated_output.concat(&input);
        (oks, errs)
    }

    /// Build a dataflow fragment for one stage of a reduction tree for multiple hierarchical
    /// aggregates to arrange and reduce the inputs. Returns the arranged input and the reduction,
    /// with all diffs in the reduction's output negated.
    fn build_bucketed_negated_output<S, V, Tr>(
        &self,
        input: &Collection<S, (Row, Row), Diff>,
        aggrs: Vec<AggregateFunc>,
    ) -> (
        Arranged<S, TraceAgent<RowRowSpine<G::Timestamp, Diff>>>,
        Arranged<S, TraceAgent<Tr>>,
    )
    where
        S: Scope<Timestamp = G::Timestamp>,
        V: MaybeValidatingRow<Row, Row>,
        Tr: Trace
            + for<'a> TraceReader<Key<'a> = DatumSeq<'a>, Time = G::Timestamp, Diff = Diff>
            + 'static,
        Tr::Batch: Batch,
        Tr::Builder: Builder<Input = TimelyStack<((Row, V), G::Timestamp, Diff)>>,
        for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = V>,
        Arranged<S, TraceAgent<Tr>>: ArrangementSize,
    {
        let error_logger = self.error_logger();
        // NOTE(vmarcos): The input operator name below is used in the tuning advice built-in
        // view mz_introspection.mz_expected_group_size_advice.
        let arranged_input =
            input.mz_arrange::<RowRowSpine<_, _>>("Arranged MinsMaxesHierarchical input");

        let reduced = arranged_input.mz_reduce_abelian::<_, _, _, Tr>(
            "Reduced Fallibly MinsMaxesHierarchical",
            move |key, source, target| {
                if let Some(err) = V::into_error() {
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
                        target.push((err(key.into_owned()), -1));
                        return;
                    }
                }

                let binding = SharedRow::get();
                let mut row_builder = binding.borrow_mut();
                let mut row_packer = row_builder.packer();

                let mut source_iters = source
                    .iter()
                    .map(|(values, _cnt)| *values)
                    .collect::<Vec<_>>();
                for func in aggrs.iter() {
                    let column_iter =
                        (0..source_iters.len()).map(|i| source_iters[i].next().unwrap());
                    row_packer.push(func.eval(column_iter, &RowArena::new()));
                }
                // We only want to arrange the parts of the input that are not part of the output.
                // More specifically, we want to arrange it so that `input.concat(&output.negate())`
                // gives us the intended value of this aggregate function. Also we assume that regardless
                // of the multiplicity of the final result in the input, we only want to have one copy
                // in the output.
                target.reserve(source.len().saturating_add(1));
                target.push((V::ok(row_builder.clone()), -1));
                target.extend(source.iter().map(|(values, cnt)| {
                    let mut cnt = *cnt;
                    cnt.negate();
                    (V::ok((*values).into_owned()), cnt)
                }));
            },
        );
        (arranged_input, reduced)
    }

    /// Build the dataflow to compute and arrange multiple hierarchical aggregations
    /// on monotonic inputs.
    fn build_monotonic<S>(
        &self,
        collection: Collection<S, (Row, Row), Diff>,
        MonotonicPlan {
            aggr_funcs,
            skips,
            must_consolidate,
        }: MonotonicPlan,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (RowRowArrangement<S>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        // Gather the relevant values into a vec of rows ordered by aggregation_index
        let collection = collection
            .map(move |(key, row)| {
                let binding = SharedRow::get();
                let mut row_builder = binding.borrow_mut();
                let mut values = Vec::with_capacity(skips.len());
                let mut row_iter = row.iter();
                for skip in skips.iter() {
                    values.push(
                        row_builder.pack_using(std::iter::once(row_iter.nth(*skip).unwrap())),
                    );
                }

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
            let m = "tried to build a monotonic reduction on non-monotonic input".to_string();
            (EvalError::Internal(m).into(), 1)
        });
        // We can place our rows directly into the diff field, and
        // only keep the relevant one corresponding to evaluating our
        // aggregate, instead of having to do a hierarchical reduction.
        let partial = partial.explode_one(move |(key, values)| {
            let mut output = Vec::new();
            for (row, func) in values.into_iter().zip(aggr_funcs.iter()) {
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
            .mz_arrange::<RowSpine<_, Vec<ReductionMonoid>>>("ArrangeMonotonic [val: empty]");
        let output = arranged.mz_reduce_abelian::<_, _, _, RowRowSpine<_, _>>("ReduceMonotonic", {
            move |key, input, output| {
                let temp_storage = RowArena::new();
                let datum_iter = key.to_datum_iter();
                let mut datums_local = datums1.borrow();
                datums_local.extend(datum_iter);
                let key_len = datums_local.len();
                let accum = &input[0].1;
                for monoid in accum.iter() {
                    datums_local.extend(monoid.finalize().iter());
                }

                if let Some(row) =
                    evaluate_mfp_after(&mfp_after1, &mut datums_local, &temp_storage, key_len)
                {
                    output.push((row, 1));
                }
            }
        });

        // If `mfp_after` can error, then we need to render a paired reduction
        // to scan for these potential errors. Note that we cannot directly use
        // `mz_timely_util::reduce::ReduceExt::reduce_pair` here because we only
        // conditionally render the second component of the reduction pair.
        if let Some(mfp) = mfp_after2 {
            let mfp_errs = arranged
                .mz_reduce_abelian::<_, _, _, RowErrSpine<_, _>>(
                    "ReduceMonotonic Error Check",
                    move |key, input, output| {
                        let temp_storage = RowArena::new();
                        let datum_iter = key.to_datum_iter();
                        let mut datums_local = datums2.borrow();
                        datums_local.extend(datum_iter);
                        let accum = &input[0].1;
                        for monoid in accum.iter() {
                            datums_local.extend(monoid.finalize().iter());
                        }
                        if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage)
                        {
                            output.push((e.into(), 1));
                        }
                    },
                )
                .as_collection(|_k, v| v.clone());
            (output, validation_errs.concat(&mfp_errs))
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
    fn build_accumulable<S>(
        &self,
        collection: Collection<S, (Row, Row), Diff>,
        AccumulablePlan {
            full_aggrs,
            simple_aggrs,
            distinct_aggrs,
        }: AccumulablePlan,
        key_arity: usize,
        mfp_after: Option<SafeMfpPlan>,
    ) -> (RowRowArrangement<S>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
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
            0,
        );

        let mut to_aggregate = Vec::new();
        if simple_aggrs.len() > 0 {
            // First, collect all non-distinct aggregations in one pass.
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
                    for (accumulable_index, datum_index, aggr) in simple_aggrs.iter() {
                        let mut datum = row_iter.next().unwrap();
                        while datum_index != &datum.0 {
                            datum = row_iter.next().unwrap();
                        }
                        let datum = datum.1;
                        diffs.0[*accumulable_index] = datum_to_accumulator(&aggr.func, datum);
                        diffs.1 = 1;
                    }
                    ((key, ()), diffs)
                }
            });
            to_aggregate.push(easy_cases);
        }

        // Next, collect all aggregations that require distinctness.
        for (accumulable_index, datum_index, aggr) in distinct_aggrs.into_iter() {
            let pairer = Pairer::new(key_arity);
            let collection = collection
                .map(move |(key, row)| {
                    let value = row.iter().nth(datum_index).unwrap();
                    (pairer.merge(&key, std::iter::once(value)), ())
                })
                .mz_arrange::<RowSpine<_, _>>("Arranged Accumulable Distinct [val: empty]")
                .mz_reduce_abelian::<_, _, _, RowSpine<_, _>>(
                    "Reduced Accumulable Distinct [val: empty]",
                    move |_k, _s, t| t.push(((), 1)),
                )
                .as_collection(move |key_val_iter, _| pairer.split(key_val_iter))
                .explode_one({
                    let zero_diffs = zero_diffs.clone();
                    move |(key, row)| {
                        let datum = row.iter().next().unwrap();
                        let mut diffs = zero_diffs.clone();
                        diffs.0[accumulable_index] = datum_to_accumulator(&aggr.func, datum);
                        diffs.1 = 1;
                        ((key, ()), diffs)
                    }
                });
            to_aggregate.push(collection);
        }

        // now concatenate, if necessary, multiple aggregations
        let collection = if to_aggregate.len() == 1 {
            to_aggregate.remove(0)
        } else {
            differential_dataflow::collection::concatenate(&mut collection.scope(), to_aggregate)
        };

        // Allocations for the two closures.
        let mut datums1 = DatumVec::new();
        let mut datums2 = DatumVec::new();
        let mfp_after1 = mfp_after.clone();
        let mfp_after2 = mfp_after.filter(|mfp| mfp.could_error());
        let full_aggrs2 = full_aggrs.clone();

        let error_logger = self.error_logger();
        let err_full_aggrs = full_aggrs.clone();
        let (arranged_output, arranged_errs) = collection
            .mz_arrange::<RowSpine<_, (Vec<Accum>, Diff)>>("ArrangeAccumulable [val: empty]")
            .reduce_pair::<_, _, _, RowRowSpine<_, _>, _, _, RowErrSpine<_, _>>(
                "ReduceAccumulable",
                "AccumulableErrorCheck",
                {
                    move |key, input, output| {
                        let (ref accums, total) = input[0].1;

                        let temp_storage = RowArena::new();
                        let datum_iter = key.to_datum_iter();
                        let mut datums_local = datums1.borrow();
                        datums_local.extend(datum_iter);
                        let key_len = datums_local.len();
                        for (aggr, accum) in full_aggrs.iter().zip(accums) {
                            datums_local.push(finalize_accum(&aggr.func, accum, total));
                        }

                        if let Some(row) = evaluate_mfp_after(
                            &mfp_after1,
                            &mut datums_local,
                            &temp_storage,
                            key_len,
                        ) {
                            output.push((row, 1));
                        }
                    }
                },
                move |key, input, output| {
                    let (ref accums, total) = input[0].1;
                    for (aggr, accum) in err_full_aggrs.iter().zip(accums) {
                        // We first test here if inputs without net-positive records are present,
                        // producing an error to the logs and to the query output if that is the case.
                        if total == 0 && !accum.is_zero() {
                            error_logger.log(
                                "Net-zero records with non-zero accumulation in ReduceAccumulable",
                                &format!("aggr={aggr:?}, accum={accum:?}"),
                            );
                            let key = key.into_owned();
                            let message = format!(
                                "Invalid data in source, saw net-zero records for key {key} \
                                 with non-zero accumulation in accumulable aggregate"
                            );
                            output.push((EvalError::Internal(message).into(), 1));
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
                                    let key = key.into_owned();
                                    let message = format!(
                                        "Invalid data in source, saw negative accumulation with \
                                         unsigned type for key {key}"
                                    );
                                    output.push((EvalError::Internal(message).into(), 1));
                                }
                            }
                            _ => (), // no more errors to check for at this point!
                        }
                    }

                    // If `mfp_after` can error, then evaluate it here.
                    let Some(mfp) = &mfp_after2 else { return };
                    let temp_storage = RowArena::new();
                    let datum_iter = key.to_datum_iter();
                    let mut datums_local = datums2.borrow();
                    datums_local.extend(datum_iter);
                    for (aggr, accum) in full_aggrs2.iter().zip(accums) {
                        datums_local.push(finalize_accum(&aggr.func, accum, total));
                    }

                    if let Result::Err(e) = mfp.evaluate_inner(&mut datums_local, &temp_storage) {
                        output.push((e.into(), 1));
                    }
                },
            );
        (
            arranged_output,
            arranged_errs.as_collection(|_key, error| error.into_owned()),
        )
    }
}

/// Evaluates the fused MFP, if one exists, on a reconstructed `DatumVecBorrow`
/// containing key and aggregate values, then returns a result `Row` or `None`
/// if the MFP filters the result out.
fn evaluate_mfp_after<'a, 'b>(
    mfp_after: &'a Option<SafeMfpPlan>,
    datums_local: &'b mut mz_repr::DatumVecBorrow<'a>,
    temp_storage: &'a RowArena,
    key_len: usize,
) -> Option<Row> {
    let binding = SharedRow::get();
    let mut row_builder = binding.borrow_mut();
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
            trues: 0,
            falses: 0,
        },
        AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => Accum::Float {
            accum: 0,
            pos_infs: 0,
            neg_infs: 0,
            nans: 0,
            non_nulls: 0,
        },
        AggregateFunc::SumNumeric => Accum::Numeric {
            accum: OrderedDecimal(NumericAgg::zero()),
            pos_infs: 0,
            neg_infs: 0,
            nans: 0,
            non_nulls: 0,
        },
        _ => Accum::SimpleNumber {
            accum: 0,
            non_nulls: 0,
        },
    }
}

static FLOAT_SCALE: LazyLock<f64> = LazyLock::new(|| f64::from(1 << 24));

fn datum_to_accumulator(aggregate_func: &AggregateFunc, datum: Datum) -> Accum {
    match aggregate_func {
        AggregateFunc::Count => Accum::SimpleNumber {
            accum: 0, // unused for AggregateFunc::Count
            non_nulls: if datum.is_null() { 0 } else { 1 },
        },
        AggregateFunc::Any | AggregateFunc::All => match datum {
            Datum::True => Accum::Bool {
                trues: 1,
                falses: 0,
            },
            Datum::Null => Accum::Bool {
                trues: 0,
                falses: 0,
            },
            Datum::False => Accum::Bool {
                trues: 0,
                falses: 1,
            },
            x => panic!("Invalid argument to AggregateFunc::Any: {x:?}"),
        },
        AggregateFunc::Dummy => match datum {
            Datum::Dummy => Accum::SimpleNumber {
                accum: 0,
                non_nulls: 0,
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
            let accum = if nans > 0 || pos_infs > 0 || neg_infs > 0 {
                0
            } else {
                // This operation will truncate to i128::MAX if out of range.
                // TODO(benesch): rewrite to avoid `as`.
                #[allow(clippy::as_conversions)]
                {
                    (n * *FLOAT_SCALE) as i128
                }
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
                        (NumericAgg::zero(), 0, 1, 0)
                    } else {
                        (NumericAgg::zero(), 1, 0, 0)
                    }
                } else if n.0.is_nan() {
                    (NumericAgg::zero(), 0, 0, 1)
                } else {
                    // Take a narrow decimal (datum) into a wide decimal
                    // (aggregator).
                    let mut cx_agg = numeric::cx_agg();
                    (cx_agg.to_width(n.0), 0, 0, 0)
                };

                Accum::Numeric {
                    accum: OrderedDecimal(accum),
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls: 1,
                }
            }
            Datum::Null => Accum::Numeric {
                accum: OrderedDecimal(NumericAgg::zero()),
                pos_infs: 0,
                neg_infs: 0,
                nans: 0,
                non_nulls: 0,
            },
            x => panic!("Invalid argument to AggregateFunc::SumNumeric: {x:?}"),
        },
        _ => {
            // Other accumulations need to disentangle the accumulable
            // value from its NULL-ness, which is not quite as easily
            // accumulated.
            match datum {
                Datum::Int16(i) => Accum::SimpleNumber {
                    accum: i128::from(i),
                    non_nulls: 1,
                },
                Datum::Int32(i) => Accum::SimpleNumber {
                    accum: i128::from(i),
                    non_nulls: 1,
                },
                Datum::Int64(i) => Accum::SimpleNumber {
                    accum: i128::from(i),
                    non_nulls: 1,
                },
                Datum::UInt16(u) => Accum::SimpleNumber {
                    accum: i128::from(u),
                    non_nulls: 1,
                },
                Datum::UInt32(u) => Accum::SimpleNumber {
                    accum: i128::from(u),
                    non_nulls: 1,
                },
                Datum::UInt64(u) => Accum::SimpleNumber {
                    accum: i128::from(u),
                    non_nulls: 1,
                },
                Datum::MzTimestamp(t) => Accum::SimpleNumber {
                    accum: i128::from(u64::from(t)),
                    non_nulls: 1,
                },
                Datum::Null => Accum::SimpleNumber {
                    accum: 0,
                    non_nulls: 0,
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
    if total > 0 && accum.is_zero() && *aggr_func != AggregateFunc::Count {
        Datum::Null
    } else {
        match (&aggr_func, &accum) {
            (AggregateFunc::Count, Accum::SimpleNumber { non_nulls, .. }) => {
                Datum::Int64(*non_nulls)
            }
            (AggregateFunc::All, Accum::Bool { falses, trues }) => {
                // If any false, else if all true, else must be no false and some nulls.
                if *falses > 0 {
                    Datum::False
                } else if *trues == total {
                    Datum::True
                } else {
                    Datum::Null
                }
            }
            (AggregateFunc::Any, Accum::Bool { falses, trues }) => {
                // If any true, else if all false, else must be no true and some nulls.
                if *trues > 0 {
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
                Datum::Int64(*accum as i64)
            }
            (AggregateFunc::SumInt64, Accum::SimpleNumber { accum, .. }) => Datum::from(*accum),
            (AggregateFunc::SumUInt16, Accum::SimpleNumber { accum, .. })
            | (AggregateFunc::SumUInt32, Accum::SimpleNumber { accum, .. }) => {
                if !accum.is_negative() {
                    // Our semantics of overflow are not clearly articulated wrt.
                    // unsigned vs. signed types (#17758). We adopt an unsigned
                    // wrapping behavior to match what we do above for signed types.
                    // TODO(vmarcos): remove potentially dangerous usage of `as`.
                    #[allow(clippy::as_conversions)]
                    Datum::UInt64(*accum as u64)
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
                if *nans > 0 || (*pos_infs > 0 && *neg_infs > 0) {
                    // NaNs are NaNs and cases where we've seen a
                    // mixture of positive and negative infinities.
                    Datum::from(f32::NAN)
                } else if *pos_infs > 0 {
                    Datum::from(f32::INFINITY)
                } else if *neg_infs > 0 {
                    Datum::from(f32::NEG_INFINITY)
                } else {
                    // TODO(benesch): remove potentially dangerous usage of `as`.
                    #[allow(clippy::as_conversions)]
                    {
                        Datum::from(((*accum as f64) / *FLOAT_SCALE) as f32)
                    }
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
                if *nans > 0 || (*pos_infs > 0 && *neg_infs > 0) {
                    // NaNs are NaNs and cases where we've seen a
                    // mixture of positive and negative infinities.
                    Datum::from(f64::NAN)
                } else if *pos_infs > 0 {
                    Datum::from(f64::INFINITY)
                } else if *neg_infs > 0 {
                    Datum::from(f64::NEG_INFINITY)
                } else {
                    // TODO(benesch): remove potentially dangerous usage of `as`.
                    #[allow(clippy::as_conversions)]
                    {
                        Datum::from((*accum as f64) / *FLOAT_SCALE)
                    }
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
                let pos_inf = *pos_infs > 0 || (inf_d && !neg_d);
                let neg_inf = *neg_infs > 0 || (inf_d && neg_d);
                if *nans > 0 || (pos_inf && neg_inf) {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
        accum: i128,
        /// The number of non-NULL values observed.
        non_nulls: Diff,
    },
    /// Accumulates float values.
    Float {
        /// Accumulates non-special float values, mapped to a fixed precision i128 domain to
        /// preserve associativity and commutativity
        accum: i128,
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
                accum: accum * i128::from(factor),
                non_nulls: non_nulls * factor,
            },
            Accum::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => Accum::Float {
                accum: accum.checked_mul(i128::from(factor)).unwrap_or_else(|| {
                    warn!("Float accumulator overflow. Incorrect results possible");
                    accum.wrapping_mul(i128::from(factor))
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
                let mut f = NumericAgg::from(factor);
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

    use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
    use mz_expr::AggregateFunc;
    use mz_ore::soft_panic_or_log;
    use mz_repr::{Datum, Diff, Row};
    use serde::{Deserialize, Serialize};
    use timely::container::columnation::{Columnation, Region};

    /// A monoid containing a single-datum row.
    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
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
                Min(row) => Min(self.inner.copy(row)),
                Max(row) => Max(self.inner.copy(row)),
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
            | AggregateFunc::FusedValueWindowFunc { .. } => None,
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
                total: 0,
            }
        }

        fn give(&mut self, d: &Datum) {
            self.accum
                .plus_equals(&datum_to_accumulator(&self.aggr_func, d.clone()));
            self.total += 1;
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
