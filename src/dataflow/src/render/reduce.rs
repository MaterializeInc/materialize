// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reduction execution planning and dataflow construction.
//!
//! Materialize needs to be able to maintain reductions incrementally (roughly, using
//! time proportional to the number of changes in the input) and ideally, with a
//! memory footprint proportional to the number of reductions being computed. To achieve that,
//! reduction functions are divied into three categories, each with their own specialized plan
//! and dataflow.
//!
//! 1. Accumulable:
//!    Accumulable reductions can be computed inline in a Differential update's `difference`
//!    field because they basically boil down to tracking counts of things. `sum()` is an
//!    example of an accumulable reduction, and when some element `x` is removed from the set
//!    of elements being summed, we can introduce `-x` to incrementally maintain the sum. More
//!    formally, accumulable reductions correspond to instances of Abelian groups.
//! 2. Hierarchical:
//!    Hierarchical reductions don't have a meaningful negation like accumulable reductions do, but
//!    they are still associative, which lets us compute the reduction over subsets of the
//!    input, and then compute the reduction again on those results. For example:
//!    `min[2, 5, 1, 10]` is the same as `min[ min[2, 5], min[1, 10]]`. When we compute hierarchical
//!    reductions this way, we can maintain the computation in sublinear time with respect to
//!    the overall input. `min` and `max` are two examples of hierarchical reductions. More formally,
//!    hierarchical reductions correspond to instances of semigroups, in that they are associative,
//!    but in order to benefit from being computed hierarchically, they need to have some reduction
//!    in data size as well. A function like "concat-everything-to-a-string" wouldn't benefit from
//!    hierarchical evaluation.
//!
//!    When the input is append-only, or monotonic, reductions that would otherwise have to be computed
//!    hierarchically can instead be computed in-place, because we only need to keep the value that's
//!    better than the "best" (minimal or maximal for min and max) seen so far.
//! 3. Basic:
//!    Basic reductions are a bit like the Hufflepuffs of this trifecta. They are neither accumulable nor
//!    hierarchical (most likely they are associative but don't involve any data reduction) and so for these
//!    we can't do much more than just defer to Differential's reduce operator and eat a large maintenance cost.
//!
//! When render these reductions we want to limit the number of arrangements we produce. Therefore, if we only
//! have multiple instances of a single reduction type in our reduce expression, we'll specialize and only
//! render a dataflow to compute those functions. If instead we have instances of multiple reduction types
//! in the same expression, we'll need to divide them up by type, render them separately, and then take those
//! results and collate them back in the requested order.
//!
//! We build `ReducePlan`s to manage the complexity of planning the generated dataflow for a given reduce
//! expression. The intent here is that each creating a `ReducePlan` should capture all of the decision making
//! about what kind of dataflow do we need to render and what each operator needs to do, and then actually
//! rendering the plan can be a relatively simple application of (as much as possible) straight line code.
//! Furthermore, the goal is to encode invariants (such as "Hierarchical reductions are either all monotonic or
//! all bucketed") in the type system so that they can be checked by the compiler.

use std::collections::BTreeMap;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::DiffVector;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::{Consolidate, Reduce, Threshold};
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::DataflowError;
use expr::{AggregateExpr, AggregateFunc, MirRelationExpr};
use repr::{Datum, DatumList, Row, RowArena, RowPacker};

use super::context::Context;
use crate::render::context::Arrangement;
use crate::render::datum_vec::DatumVec;

// Top level object for the Reduce lower-level IR
#[derive(Clone, Debug)]
enum ReducePlan {
    // No aggregations, just distinct
    Distinct,
    // Aggregations that can be computed in place
    Accumulable(AccumulablePlan),
    // Aggregations that can be computed hierarchically
    Hierarchical(HierarchicalPlan),
    // Aggregations that cannot be computed in place
    // or hierarchically.
    Basic(BasicPlan),
    // A mix of multiple types of aggregates that need
    // to be combined together.
    Collation(CollationPlan),
}

// LIR node for accumulable reductions
#[derive(Clone, Debug)]
struct AccumulablePlan {
    // All of the accumulable aggregations, ordered according to
    // the output of the accumulable dataflow.
    full_aggrs: Vec<AggregateExpr>,
    // All of the non-distinct accumulable aggregates.
    // Each element represents:
    // (index of the aggregation among accumulable aggregations,
    //   index of the datum among inputs, aggregation expr)
    // These will all be rendered together in one dataflow fragment.
    simple_aggrs: Vec<(usize, usize, AggregateExpr)>,
    // Same as above but for all of the `DISTINCT` accumulable aggregations.
    distinct_aggrs: Vec<(usize, usize, AggregateExpr)>,
    // Total number of accumulable aggregations.
    num_accumulable: usize,
}

// LIR node for hierarchical reductions
#[derive(Clone, Debug)]
enum HierarchicalPlan {
    // LIR node for hierarchical, monotonic reductions
    Monotonic(MonotonicPlan),
    // LIR node for hierarchical reductions that are not
    // monotonic and thus have to be rendered via a reduction
    // tree that partitions reductions into fine and then coarser
    // buckets.
    Bucketed(BucketedPlan),
}

// LIR node for reductions that are hierarchical but presented with a monotonic
// input stream.
#[derive(Clone, Debug)]
struct MonotonicPlan {
    // Aggregations being computed monotonically.
    aggr_funcs: Vec<AggregateFunc>,
    // Set of "skips" or calls to `nth()` an iterator needs to do over
    // the input to extract the relevant datums.
    skips: Vec<usize>,
}

// LIR node for hierarchical reductions that are not
// monotonic. This gets rendered with a series of reduce
// operators that serve as a min / max heap
#[derive(Clone, Debug)]
struct BucketedPlan {
    // Aggregations being computed hierarchically
    aggr_funcs: Vec<AggregateFunc>,
    // Set of "skips" or calls to `nth()` an iterator needs to do over
    // the input to extract the relevant datums.
    skips: Vec<usize>,
    // Sequence of bitshifts for each key sub-division expression.
    // To perform a large hierarchical reduction with stable runtimes
    // under updates we'll subdivide the group key into buckets, compute
    // the reduction in each of those buckets and then combine into a
    // coarser bucket and redo the reduction in another layer.
    // These shifts denote the log_base_2 of the number of
    // buckets in each layer.
    shifts: Vec<usize>,
}

// LIR node for basic reductions - those that are neither
// computed in place nor computed hierarchically.
#[derive(Clone, Debug)]
enum BasicPlan {
    // LIR node for reductions with only a single basic reduction
    Single(usize, AggregateExpr),
    // LIR node for reductions with multiple basic reductions
    // These need to then be collated together in an additional
    // reduction.
    Multiple(Vec<(usize, AggregateExpr)>),
}

// LIR for reduce collation. We use this when the underlying
// list of aggregates contains more than one type of reduction
// to stitch answers together in the order requested by the user
// TODO: could we express this as a delta join
#[derive(Clone, Debug, Default)]
struct CollationPlan {
    accumulable: Option<AccumulablePlan>,
    hierarchical: Option<HierarchicalPlan>,
    basic: Option<BasicPlan>,
    aggregate_types: Vec<ReductionType>,
}

// This enum indicates what class of reduction each aggregate function is.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
enum ReductionType {
    // Accumulable functions can be subtracted from (are invertible), and associative.
    // We can compute these results by moving some data to the diff field under arbitrary
    // changes to inputs. Examples include sum or count.
    Accumulable = 1,
    // Hierarchical functions are associative, which means we can split up the work of
    // computing them across subsets. Note that hierarchical reductions should also
    // reduce the data in some way, as otherwise rendering them hierarchically is not
    // worth it. Examples include min or max.
    Hierarchical = 2,
    // Basic, for lack of a better word, are functions that are neither accumulable
    // nor hierarchical. Examples include jsonb_agg.
    Basic = 3,
}

impl<G, T> Context<G, MirRelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders a `MirRelationExpr::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    pub fn render_reduce(&mut self, relation_expr: &MirRelationExpr) {
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } = relation_expr
        {
            // The reduce operator may have multiple aggregation functions, some of
            // which should only be applied to distinct values for each key. We need
            // to build a non-trivial dataflow fragment to robustly implement these
            // aggregations, including:
            //
            // 1. Different reductions for each aggregation, to avoid maintaining
            //    state proportional to the cross-product of values.
            //
            // 2. Distinct operators before each reduction which requires distinct
            //    inputs, to avoid recomputation when the distinct set is stable.
            //
            // 3. Hierachical aggregation for operators like min and max that we
            //    cannot perform in the diff field.
            //
            // Our plan is to perform these actions, and the re-integrate the results
            // in a final reduce whose output arrangement looks just as if we had
            // applied a single reduction (which should be good for any consumers
            // of the operator and its arrangement).

            // Our first step is to extract `(key, vals)` from `input`.
            // We do this carefully, attempting to avoid unneccesary allocations
            // that would result from cloning rows in input arrangements.

            let input_arity = input.arity();

            // TODO(mcsherry): These two MFPs could be unified into one, which would
            // allow optimization across their computation, e.g. if both parsed input
            // strings to typed data, but it involves a bit of dancing around when we
            // pull the data out of their output (i.e. as an iterator, rather than use
            // the built-in evaluation direction to a `Row`).

            // Form an operator for evaluating key expressions.
            let mut key_mfp = expr::MapFilterProject::new(input_arity)
                .map(group_key.iter().cloned())
                .project(input_arity..(input_arity + group_key.len()));

            // Form an operator for evaluating value expressions.
            let mut val_mfp = expr::MapFilterProject::new(input_arity)
                .map(aggregates.iter().map(|a| a.expr.clone()))
                .project(input_arity..(input_arity + aggregates.len()));

            // Determine the columns we'll need from the row.
            let mut demand = Vec::new();
            demand.extend(key_mfp.demand());
            demand.extend(val_mfp.demand());
            demand.sort();
            demand.dedup();
            // remap column references to the subset we use.
            let mut demand_map = std::collections::HashMap::new();
            for column in demand.iter() {
                demand_map.insert(*column, demand_map.len());
            }
            key_mfp.permute(&demand_map, demand_map.len());
            key_mfp.optimize();
            val_mfp.permute(&demand_map, demand_map.len());
            val_mfp.optimize();

            let skips = convert_indexes_to_skips(demand);

            let mut row_packer = RowPacker::new();
            let mut datums = DatumVec::new();
            let (key_val_input, mut err_input): (
                Collection<_, Result<(Row, Row), DataflowError>, _>,
                _,
            ) = self
                .flat_map_ref(
                    input,
                    |_expr| None,
                    move |row| {
                        let temp_storage = RowArena::new();

                        // Unpack only the demanded columns.
                        let mut datums_local = datums.borrow();
                        let mut row_iter = row.iter();
                        for skip in skips.iter() {
                            datums_local.push((&mut row_iter).nth(*skip).unwrap());
                        }

                        // Evaluate the key expressions.
                        row_packer.clear();
                        let key = match key_mfp.evaluate(
                            &mut datums_local,
                            &temp_storage,
                            &mut row_packer,
                        ) {
                            Err(e) => return Some(Err(DataflowError::from(e))),
                            Ok(key) => key.expect("Row expected as no predicate was used"),
                        };
                        // Evaluate the value expressions.
                        // The prior evaluation may have left additional columns we should delete.
                        datums_local.truncate(skips.len());
                        let val = match val_mfp.evaluate_iter(&mut datums_local, &temp_storage) {
                            Err(e) => return Some(Err(DataflowError::from(e))),
                            Ok(val) => val.expect("Row expected as no predicate was used"),
                        };
                        row_packer.extend(val);
                        drop(datums_local);

                        // Mint the final row, ideally re-using resources.
                        // TODO(mcsherry): This can perhaps be extracted for
                        // re-use if it seems to be a common pattern.
                        use timely::communication::message::RefOrMut;
                        let row = match row {
                            RefOrMut::Ref(_) => row_packer.finish_and_reuse(),
                            RefOrMut::Mut(row) => {
                                row_packer.finish_into(row);
                                std::mem::take(row)
                            }
                        };
                        return Some(Ok((key, row)));
                    },
                )
                .unwrap();

            // Demux out the potential errors from key and value selector evaluation.
            use timely::dataflow::operators::ok_err::OkErr;
            let (ok, err) = key_val_input.inner.ok_err(|(x, t, d)| match x {
                Ok(x) => Ok((x, t, d)),
                Err(x) => Err((x, t, d)),
            });

            let ok_input = ok.as_collection();
            err_input = err.as_collection().concat(&err_input);

            // At this point, we need plan out the reduction based on the aggregation
            // functions used.
            //   1. If there are no aggregation functions, the operation is a "distinct"
            //      and we can / should just apply that differential operator.
            //   2. We can decompose the remaining aggregation functions into one of three
            //      types: accumulable, hierarchical, or basic.
            //   3. If we only have one type of reduction in our dataflow we can render its
            //      fragment individually.
            //   4. Otherwise, we have to merge them together.

            // Distinct is a special case, as there are no aggregates to aggregate.
            // In this case, we use a special implementation that does not rely on
            // collating aggregates.

            // First, let's plan out what we are going to do with this reduce
            let lir = plan_reduce(aggregates.clone(), *monotonic, *expected_group_size);
            let arrangement = build_reduce(ok_input, lir);
            let index = (0..group_key.len()).collect::<Vec<_>>();
            self.set_local_columns(
                relation_expr,
                &index[..],
                (arrangement, err_input.arrange()),
            );
        }
    }
}

// Generate a lower-level IR node for the supplied aggregations that
// describes what the resulting dataflow will look like.
fn plan_reduce(
    aggregates: Vec<AggregateExpr>,
    monotonic: bool,
    expected_group_size: Option<usize>,
) -> ReducePlan {
    let lower = move |typ: ReductionType, aggregates_list: Vec<(usize, AggregateExpr)>| {
        assert!(
            aggregates_list.len() > 0,
            "error: tried to render a reduce dataflow with no aggregates"
        );
        match typ {
            ReductionType::Accumulable => {
                let mut simple_aggrs = vec![];
                let mut distinct_aggrs = vec![];
                let num_accumulable = aggregates_list.len();
                let full_aggrs: Vec<_> = aggregates_list
                    .iter()
                    .cloned()
                    .map(|(_, aggr)| aggr)
                    .collect();
                for (accumulable_index, (datum_index, aggr)) in
                    aggregates_list.into_iter().enumerate()
                {
                    if aggr.distinct {
                        distinct_aggrs.push((accumulable_index, datum_index, aggr));
                    } else {
                        simple_aggrs.push((accumulable_index, datum_index, aggr));
                    };
                }
                ReducePlan::Accumulable(AccumulablePlan {
                    full_aggrs,
                    simple_aggrs,
                    distinct_aggrs,
                    num_accumulable,
                })
            }
            ReductionType::Hierarchical => {
                let aggr_funcs: Vec<_> = aggregates_list
                    .iter()
                    .cloned()
                    .map(|(_, aggr)| aggr.func)
                    .collect();
                let indexes: Vec<_> = aggregates_list
                    .into_iter()
                    .map(|(index, _)| index)
                    .collect();

                let skips = convert_indexes_to_skips(indexes);
                if monotonic {
                    let monotonic = MonotonicPlan { aggr_funcs, skips };
                    ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(monotonic))
                } else {
                    let mut shifts = vec![];
                    let mut current = 4usize;

                    // Plan for 4B records in the expected case
                    let limit = expected_group_size.unwrap_or(4_000_000_000);

                    while (1 << current) < limit {
                        shifts.push(current);
                        current += 4;
                    }

                    shifts.reverse();

                    let bucketed = BucketedPlan {
                        aggr_funcs,
                        skips,
                        shifts,
                    };

                    ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(bucketed))
                }
            }
            ReductionType::Basic => {
                if aggregates_list.len() == 1 {
                    ReducePlan::Basic(BasicPlan::Single(
                        aggregates_list[0].0,
                        aggregates_list[0].1.clone(),
                    ))
                } else {
                    ReducePlan::Basic(BasicPlan::Multiple(aggregates_list))
                }
            }
        }
    };

    // If we don't have any aggregations we are just computing a distinct.
    if aggregates.is_empty() {
        return ReducePlan::Distinct;
    }

    // Otherwise, we need to group aggregations according to their
    // reduction type (accumulable, hierarchical, or basic)
    let mut reduction_types = BTreeMap::new();
    // We need to make sure that each list of aggregates by type forms
    // a subsequence of the overall sequence of aggregates.
    for index in 0..aggregates.len() {
        let typ = reduction_type(&aggregates[index].func);
        let aggregates_list = reduction_types.entry(typ).or_insert_with(Vec::new);
        aggregates_list.push((index, aggregates[index].clone()));
    }

    // Convert each grouped list of reductions into a LIR node.
    let lir: Vec<_> = reduction_types
        .into_iter()
        .map(|(typ, aggregates_list)| lower(typ, aggregates_list))
        .collect();

    // If we only have a single type of aggregation present we can
    // render that directly
    if lir.len() == 1 {
        return lir[0].clone();
    }

    // Otherwise, we have to stitch reductions together.
    assert!(lir.len() <= 3);
    let mut collation: CollationPlan = Default::default();
    let aggregate_types = aggregates
        .iter()
        .map(|a| reduction_type(&a.func))
        .collect::<Vec<_>>();

    collation.aggregate_types = aggregate_types;

    for expr in lir.into_iter() {
        match expr {
            ReducePlan::Accumulable(e) => {
                assert!(collation.accumulable.is_none());
                collation.accumulable = Some(e);
            }
            ReducePlan::Hierarchical(e) => {
                assert!(collation.hierarchical.is_none());
                collation.hierarchical = Some(e);
            }
            ReducePlan::Basic(e) => {
                assert!(collation.basic.is_none());
                collation.basic = Some(e);
            }
            _ => panic!("Inner reduce LIR expr was unsupported type!"),
        }
    }

    ReducePlan::Collation(collation)
}

// Render a dataflow based on the provided LIR node.
fn build_reduce<G>(collection: Collection<G, (Row, Row)>, lir: ReducePlan) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let build_hierarchical = |collection: Collection<G, (Row, Row)>,
                              expr: HierarchicalPlan,
                              top_level: bool| match expr {
        HierarchicalPlan::Monotonic(expr) => build_monotonic(collection, expr, top_level),
        HierarchicalPlan::Bucketed(expr) => build_bucketed(collection, expr, top_level),
    };

    let build_basic =
        |collection: Collection<G, (Row, Row)>, expr: BasicPlan, top_level: bool| match expr {
            BasicPlan::Single(index, aggr) => {
                build_basic_aggregate(collection, index, &aggr, top_level)
            }
            BasicPlan::Multiple(aggrs) => build_basic_aggregates(collection, aggrs, top_level),
        };

    match lir {
        ReducePlan::Distinct => build_distinct(collection),
        ReducePlan::Accumulable(expr) => build_accumulable(collection, expr, true),
        ReducePlan::Hierarchical(expr) => build_hierarchical(collection, expr, true),
        ReducePlan::Basic(expr) => build_basic(collection, expr, true),
        ReducePlan::Collation(expr) => {
            // First, we need to render our constituent aggregations.
            let mut to_collate = vec![];

            if let Some(accumulable) = expr.accumulable {
                to_collate.push((
                    ReductionType::Accumulable,
                    build_accumulable(collection.clone(), accumulable, false),
                ));
            }
            if let Some(hierarchical) = expr.hierarchical {
                to_collate.push((
                    ReductionType::Hierarchical,
                    build_hierarchical(collection.clone(), hierarchical, false),
                ));
            }
            if let Some(basic) = expr.basic {
                to_collate.push((
                    ReductionType::Basic,
                    build_basic(collection.clone(), basic, false),
                ));
            }
            // Now we need to collate them together.
            build_collation(to_collate, expr.aggregate_types, &mut collection.scope())
        }
    }
}

// Collate multiple arrangements together into a single arrangement. This
// is basically the same thing as a join on the group key followed by
// shuffling the values into the correct order.
// This implementation assumes that all input arrangements
// present values in a way thats respects the desired output order,
// so we can do a linear merge to form the output.
fn build_collation<G>(
    arrangements: Vec<(ReductionType, Arrangement<G, Row>)>,
    aggregate_types: Vec<ReductionType>,
    scope: &mut G,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let mut to_concat = vec![];

    for (reduction_type, arrangement) in arrangements.into_iter() {
        let collection =
            arrangement.as_collection(move |key, val| (key.clone(), (reduction_type, val.clone())));
        to_concat.push(collection);
    }

    use differential_dataflow::collection::concatenate;
    concatenate(scope, to_concat)
        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceCollation", {
            let mut row_packer = RowPacker::new();
            move |key, input, output| {
                // The inputs are pairs of a reduction type, and a row consisting of densely packed fused
                // aggregate values.
                // We need to reconstitute the final value by:
                // 1. Extracting out the fused rows by type
                // 2. For each aggregate, figure out what type it is, and grab the relevant value
                //    from the corresponding fused row.
                // 3. Stitch all the values together into one row.

                let mut accumulable = DatumList::empty().iter();
                let mut hierarchical = DatumList::empty().iter();
                let mut basic = DatumList::empty().iter();

                // We expect not to have any negative multiplicities, but are not 100% sure it will
                // never happen so for now just log an error if it does.
                for (val, cnt) in input.iter() {
                    if cnt < &0 {
                        log::error!("[customer-data] Negative accumulation in ReduceCollation: {:?} with count {:?}", val, cnt);
                    }
                }

                for ((reduction_type, row), _) in input.iter() {
                    match reduction_type {
                        ReductionType::Accumulable => {
                            accumulable = row.iter();
                        }
                        ReductionType::Hierarchical => {
                            hierarchical = row.iter();
                        }
                        ReductionType::Basic => {
                            basic = row.iter();
                        }
                    }
                }

                // First, fill our output row with key information.
                row_packer.extend(key.iter());
                // Next merge results into the order they were asked for.
                for typ in aggregate_types.iter() {
                    match typ {
                        ReductionType::Accumulable => {
                            row_packer.push(accumulable.next().unwrap())
                        }
                        ReductionType::Hierarchical => {
                            row_packer.push(hierarchical.next().unwrap())
                        }
                        ReductionType::Basic => {
                            row_packer.push(basic.next().unwrap())
                        }
                    }
                }
                output.push((row_packer.finish_and_reuse(), 1));
            }
        })
}

fn build_distinct<G>(collection: Collection<G, (Row, Row)>) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    collection.reduce_abelian::<_, OrdValSpine<_, _, _, _>>("DistinctBy", {
        |key, _input, output| {
            output.push((key.clone(), 1));
        }
    })
}

fn build_basic_aggregates<G>(
    collection: Collection<G, (Row, Row)>,
    aggrs: Vec<(usize, AggregateExpr)>,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // We are only using this function to render multiple basic aggregates and
    // stitch them together
    assert!(aggrs.len() > 1);
    let mut to_collect = Vec::new();
    for (index, aggr) in aggrs {
        let result = build_basic_aggregate(collection.clone(), index, &aggr, false);
        to_collect.push(result.as_collection(move |key, val| (key.clone(), (index, val.clone()))));
    }
    differential_dataflow::collection::concatenate(&mut collection.scope(), to_collect)
        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceFuseBasic", {
            let mut row_packer = RowPacker::new();
            move |key, input, output| {
                // First, fill our output row with key information.
                if prepend_key {
                    row_packer.extend(key.iter());
                }

                for ((_, row), _) in input.iter() {
                    let datum = row.unpack_first();
                    row_packer.push(datum);
                }
                output.push((row_packer.finish_and_reuse(), 1));
            }
        })
}

/// Reduce and arrange `input` by `group_key` and `aggr`.
///
/// Computes a single, non-accumulable, non-hierarchical aggregate.
/// This method also applies distinctness if required.
fn build_basic_aggregate<G>(
    ok_input: Collection<G, (Row, Row)>,
    index: usize,
    aggr: &AggregateExpr,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let AggregateExpr {
        func,
        expr: _,
        distinct,
    } = aggr.clone();

    let mut partial = if !prepend_key {
        let mut packer = RowPacker::new();
        ok_input.map(move |(key, row)| {
            let value = row.iter().nth(index).unwrap();
            packer.push(value);
            (key, packer.finish_and_reuse())
        })
    } else {
        ok_input
    };

    // If `distinct` is set, we restrict ourselves to the distinct `(key, val)`.
    if distinct {
        partial = partial.distinct();
    }

    partial.reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceInaccumulable", {
        let mut row_packer = RowPacker::new();
        move |key, source, target| {
            // Negative counts would be surprising, but until we are 100% certain we wont
            // see them, we should report when we do. We may want to bake even more info
            // in here in the future.
            if source.iter().any(|(_val, cnt)| cnt < &0) {
                // XXX: This reports user data, which we perhaps should not do!
                for (val, cnt) in source.iter() {
                    if cnt < &0 {
                        log::error!("[customer-data] Negative accumulation in ReduceInaccumulable: {:?} with count {:?}", val, cnt);
                    }
                }
            } else {
                // We respect the multiplicity here (unlike in hierarchical aggregation)
                // because we don't know that the aggregation method is not sensitive
                // to the number of records.
                let iter = source.iter().flat_map(|(v, w)| {
                    std::iter::repeat(v.iter().next().unwrap()).take(*w as usize)
                });
                if prepend_key {
                    row_packer.extend(key.iter());
                }
                row_packer.push(func.eval(iter, &RowArena::new()));
                target.push((row_packer.finish_and_reuse(), 1));
            }
        }
    })
}

// Render a single reduction tree that computes aggregations
// hierarchically. If the input is monotonic, we further specialize and
// render them as a fused series of monoids similar to the accumulable reductions.
// Note that we ignore the distinct bit, because currently all hierarchical
// aggregates are min / max which efficiently suppress updates for non-distinct
// items. If we add more hierarchical aggregates we will have to revise this
// implementation.
fn build_bucketed<G>(
    collection: Collection<G, (Row, Row)>,
    BucketedPlan {
        aggr_funcs,
        skips,
        shifts,
    }: BucketedPlan,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Gather the relevant values into a vec of rows ordered by aggregation_index
    let mut packer = RowPacker::new();
    let collection = collection.map(move |(key, row)| {
        let mut values = Vec::with_capacity(skips.len());
        let mut row_iter = row.iter();
        for skip in skips.iter() {
            packer.push((&mut row_iter).nth(*skip).unwrap());
            values.push(packer.finish_and_reuse());
        }

        (key, values)
    });

    // Repeatedly apply hierarchical reduction with a progressively coarser key.
    let mut stage = collection.map(move |(key, values)| ((key, values.hashed()), values));
    for log_modulus in shifts.iter() {
        stage = build_bucketed_stage(stage, aggr_funcs.clone(), 1u64 << log_modulus);
    }

    // Discard the hash from the key and return to the format of the input data.
    let partial = stage.map(|((key, _hash), values)| (key, values));

    // Build a series of stages for the reduction
    // Arrange the final result into (key, Row)
    partial.reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceMinsMaxes", {
        let mut row_packer = RowPacker::new();
        move |key, source, target| {
            // Negative counts would be surprising, but until we are 100% certain we wont
            // see them, we should report when we do. We may want to bake even more info
            // in here in the future.
            if source.iter().any(|(_val, cnt)| cnt < &0) {
                // XXX: This reports user data, which we perhaps should not do!
                for (val, cnt) in source.iter() {
                    if cnt < &0 {
                        log::error!("[customer-data] Negative accumulation in ReduceMinsMaxes: {:?} with count {:?}", val, cnt);
                    }
                }
            } else {
                // Pack the value with the key as the result.
                if prepend_key {
                    row_packer.extend(key.iter());
                }
                for (aggr_index, func) in aggr_funcs.iter().enumerate() {
                    let iter = source.iter().map(|(values, _cnt)| values[aggr_index].iter().next().unwrap());
                    row_packer.push(func.eval(iter, &RowArena::new()));
                }
                target.push((row_packer.finish_and_reuse(), 1));
            }
        }
    })
}

// Renders one stage of a fused reduction tree for a set of hierarchical aggregations.
fn build_bucketed_stage<G>(
    collection: Collection<G, ((Row, u64), Vec<Row>)>,
    aggrs: Vec<AggregateFunc>,
    modulus: u64,
) -> Collection<G, ((Row, u64), Vec<Row>)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let input = collection.map(move |((key, hash), values)| ((key, hash % modulus), values));

    let negated_output = input
        .reduce_named("MinsMaxesHierarchical", {
            let mut row_packer = repr::RowPacker::new();
            move |key, source, target| {
                // Should negative accumulations reach us, we should loudly complain.
                if source.iter().any(|(_val, cnt)| cnt <= &0) {
                    for (val, cnt) in source.iter() {
                        if cnt <= &0 {
                            // XXX: This reports user data, which we perhaps should not do!
                            log::error!("[customer-data] Non-positive accumulation in MinsMaxesHierarchical: key: {:?}\tvalue: {:?}\tcount: {:?}", key, val, cnt);
                        }
                    }
                } else {
                    let mut output = Vec::with_capacity(aggrs.len());
                    for (aggr_index, func) in aggrs.iter().enumerate() {
                        let iter = source.iter().map(|(values, _cnt)| values[aggr_index].iter().next().unwrap());
                        output.push(row_packer.pack(Some(func.eval(iter, &RowArena::new()))));
                    }
                    // We only want to arrange the parts of the input that are not part of the output.
                    // More specifically, we want to arrange it so that `input.concat(&output.negate())`
                    // gives us the intended value of this aggregate function. Also we assume that regardless
                    // of the multiplicity of the final result in the input, we only want to have one copy
                    // in the output.

                    target.push((output, -1));
                    target.extend(source.iter().map(|(values, cnt)| ((*values).clone(), *cnt)));
                }
            }
        });

    negated_output.negate().concat(&input).consolidate()
}

fn build_monotonic<G>(
    collection: Collection<G, (Row, Row)>,
    MonotonicPlan { aggr_funcs, skips }: MonotonicPlan,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Gather the relevant values into a vec of rows ordered by aggregation_index
    let mut packer = RowPacker::new();
    let collection = collection.map(move |(key, row)| {
        let mut values = Vec::with_capacity(skips.len());
        let mut row_iter = row.iter();
        for skip in skips.iter() {
            packer.push((&mut row_iter).nth(*skip).unwrap());
            values.push(packer.finish_and_reuse());
        }

        (key, values)
    });

    // We can place our rows directly into the diff field, and only keep the
    // relevant one corresponding to evaluating our aggregate, instead of having
    // to do a hierarchical reduction.
    use timely::dataflow::operators::Map;

    // We arrange the inputs ourself to force it into a leaner structure because we know we
    // won't care about values.
    let partial = collection
        .consolidate()
        .inner
        .map(move |((key, values), time, diff)| {
            assert!(diff > 0);
            let mut output = Vec::new();
            for (row, func) in values.into_iter().zip(aggr_funcs.iter()) {
                output.push(monoids::get_monoid(row, func).expect(
                    "hierarchical aggregations are expected to have monoid implementations",
                ));
            }

            (key, time, DiffVector::new(output))
        })
        .as_collection();
    partial
        .arrange_by_self()
        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceMonotonic", {
            let mut row_packer = RowPacker::new();
            move |key, input, output| {
                let accum = &input[0].1;
                // Pack the value with the key as the result.
                if prepend_key {
                    row_packer.extend(key.iter());
                }

                for monoid in accum.iter() {
                    match monoid {
                        monoids::ReductionMonoid::Min(row) => row_packer.extend(row.iter()),
                        monoids::ReductionMonoid::Max(row) => row_packer.extend(row.iter()),
                    }
                }
                output.push((row_packer.finish_and_reuse(), 1));
            }
        })
}

/// Builds the dataflow for reductions that can be performed in-place.
///
/// The incoming values are moved to the update's "difference" field, at which point
/// they can be accumulated in place. The `count` operator promotes the accumulated
/// values to data, at which point a final map applies operator-specific logic to
/// yield the final aggregate.
///
/// If `prepend_key` is specified, the key is prepended to the arranged values, making
/// the arrangement suitable for publication itself.
fn build_accumulable<G>(
    collection: Collection<G, (Row, Row)>,
    AccumulablePlan {
        full_aggrs,
        simple_aggrs,
        distinct_aggrs,
        num_accumulable,
    }: AccumulablePlan,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Some of the aggregations may have the `distinct` bit set, which means that they'll
    // need to be extracted from `collection` and be subjected to `distinct` with `key`.
    // Other aggregations can be directly moved in to the `diff` field.
    //
    // In each case, the resulting collection should have `data` shaped as `(key, ())`
    // and a `diff` that is a vector with length `3 * aggrs.len()`. The three values are
    // generally the count, and then two aggregation-specific values. The size could be
    // reduced if we want to specialize for the aggregations.

    use timely::dataflow::operators::map::Map;

    let float_scale = f64::from(1 << 24);

    // Two aggregation-specific values for each aggregation.
    let datum_aggr_values = move |datum: Datum, aggr: &AggregateFunc| {
        match aggr {
            AggregateFunc::Count => {
                // Count needs to distinguish nulls from zero.
                (1, if datum.is_null() { 0 } else { 1 })
            }
            AggregateFunc::Any => match datum {
                Datum::True => (1, 0),
                Datum::Null => (0, 0),
                Datum::False => (0, 1),
                x => panic!("Invalid argument to AggregateFunc::Any: {:?}", x),
            },
            AggregateFunc::All => match datum {
                Datum::True => (1, 0),
                Datum::Null => (0, 0),
                Datum::False => (0, 1),
                x => panic!("Invalid argument to AggregateFunc::All: {:?}", x),
            },
            AggregateFunc::Dummy => match datum {
                Datum::Dummy => (0, 0),
                x => panic!("Invalid argument to AggregateFunc::Dummy: {:?}", x),
            },
            _ => {
                // Other accumulations need to disentangle the accumulable
                // value from its NULL-ness, which is not quite as easily
                // accumulated.
                match datum {
                    Datum::Int32(i) => (i128::from(i), 1),
                    Datum::Int64(i) => (i128::from(i), 1),
                    Datum::Float32(f) => ((f64::from(*f) * float_scale) as i128, 1),
                    Datum::Float64(f) => ((*f * float_scale) as i128, 1),
                    Datum::Decimal(d) => (d.as_i128(), 1),
                    Datum::Null => (0, 0),
                    x => panic!("Accumulating non-integer data: {:?}", x),
                }
            }
        }
    };

    let mut to_aggregate = Vec::new();
    let diffs_len = num_accumulable * 3;
    // First, collect all non-distinct aggregations in one pass.
    let easy_cases = collection
        .inner
        .map(|(d, t, r)| (d, t, r as i128))
        .as_collection()
        .explode({
            move |(key, row)| {
                let mut diffs = vec![0i128; diffs_len];
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
                    let (agg1, agg2) = datum_aggr_values(datum, &aggr.func);
                    diffs[3 * accumulable_index] = 1i128;
                    diffs[3 * accumulable_index + 1] = agg1;
                    diffs[3 * accumulable_index + 2] = agg2;
                }
                Some((key, DiffVector::new(diffs)))
            }
        });
    to_aggregate.push(easy_cases);

    // Next, collect all aggregations that require distinctness.
    for (accumulable_index, datum_index, aggr) in distinct_aggrs.into_iter() {
        let mut packer = RowPacker::new();
        let collection = collection
            .map(move |(key, row)| {
                let value = row.iter().nth(datum_index).unwrap();
                packer.push(value);
                (key, packer.finish_and_reuse())
            })
            .distinct()
            .inner
            .map(|(d, t, r)| (d, t, r as i128))
            .as_collection()
            .explode({
                move |(key, row)| {
                    let datum = row.iter().next().unwrap();
                    let mut diffs = vec![0i128; diffs_len];
                    let (agg1, agg2) = datum_aggr_values(datum, &aggr.func);
                    diffs[3 * accumulable_index] = 1i128;
                    diffs[3 * accumulable_index + 1] = agg1;
                    diffs[3 * accumulable_index + 2] = agg2;
                    Some((key, DiffVector::new(diffs)))
                }
            });
        to_aggregate.push(collection);
    }
    let collection =
        differential_dataflow::collection::concatenate(&mut collection.scope(), to_aggregate);

    collection
        .arrange_by_self()
        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
            "ReduceAccumulable", {
            let mut row_packer = RowPacker::new();
            move |key, input, output| {
                let accum = &input[0].1;
                // Pack the value with the key as the result.
                if prepend_key {
                    row_packer.extend(key.iter());
                }

                for (index, aggr) in full_aggrs.iter().enumerate() {
                    // For most aggregations, the first aggregate is the "data" and the second is the number
                    // of non-null elements (so that we can determine if we should produce 0 or a Null).
                    // For Any and All, the two aggregates are the numbers of true and false records, resp.
                    let tot = accum[3 * index];
                    let agg1 = accum[3 * index + 1];
                    let agg2 = accum[3 * index + 2];

                    if tot == 0 && (agg1 != 0 || agg2 != 0) {
                        // This should perhaps be un-recoverable, as we risk panicking in the ReduceCollation
                        // operator, when this key is presented but matching aggregates are not found. We will
                        // suppress the output for inputs without net-positive records, which *should* avoid
                        // that panic.
                        log::error!("[customer-data] ReduceAccumulable observed net-zero records with non-zero accumulation: {:?}: {:?}, {:?}", aggr, agg1, agg2);
                    }

                    // The finished value depends on the aggregation function in a variety of ways.
                    let value = match (&aggr.func, agg2) {
                        (AggregateFunc::Count, _) => Datum::Int64(agg2 as i64),
                        (AggregateFunc::All, _) => {
                            // If any false, else if all true, else must be no false and some nulls.
                            if agg2 > 0 {
                                Datum::False
                            } else if tot == agg1 {
                                Datum::True
                            } else {
                                Datum::Null
                            }
                        }
                        (AggregateFunc::Any, _) => {
                            // If any true, else if all false, else must be no true and some nulls.
                            if agg1 > 0 {
                                Datum::True
                            } else if tot == agg2 {
                                Datum::False
                            } else {
                                Datum::Null
                            }
                        }
                        (AggregateFunc::Dummy, _) => Datum::Dummy,
                        // Below this point, anything with only nulls should be null.
                        (_, 0) => Datum::Null,
                        // If any non-nulls, just report the aggregate.
                        (AggregateFunc::SumInt32, _) => Datum::Int64(agg1 as i64),
                        (AggregateFunc::SumInt64, _) => Datum::from(agg1),
                        (AggregateFunc::SumFloat32, _) => {
                            Datum::Float32((((agg1 as f64) / float_scale) as f32).into())
                        }
                        (AggregateFunc::SumFloat64, _) => {
                            Datum::Float64(((agg1 as f64) / float_scale).into())
                        }
                        (AggregateFunc::SumDecimal, _) => Datum::from(agg1),
                        x => panic!("Unexpected accumulable aggregation: {:?}", x),
                    };

                    row_packer.push(value);
                }
                output.push((row_packer.finish_and_reuse(), 1));
            }},
        )
}

// Transforms a vector containing indexes of needed columns
// into a vector containing the "skips" an iterator over a Row would
// need to perform to see those values
// E.g. [3, 6, 10, 15] turns into [3, 3, 4, 5]
fn convert_indexes_to_skips(mut demand: Vec<usize>) -> Vec<usize> {
    // transform `demand` into "skips"
    for index in (1..demand.len()).rev() {
        demand[index] -= demand[index - 1];
        demand[index] -= 1;
    }

    demand
}

/// Determines whether a function can be accumulated in an update's "difference" field,
/// and whether it can be subjected to recursive (hierarchical) aggregation.
///
/// Accumulable aggregations will be packed into differential dataflow's "difference" field,
/// which can be accumulated in-place using the addition operation on the type. Aggregations
/// that indicate they are accumulable will still need to provide an action that takes their
/// data and introduces it as a difference, and the post-processing when the accumulated value
/// is presented as data.
///
/// Hierarchical aggregations will be subjected to repeated aggregation on initially small but
/// increasingly large subsets of each key. This has the intended property that no invocation
/// is on a significantly large set of values (and so, no incremental update needs to reform
/// significant input data). Hierarchical aggregates can be rendered more efficiently if the
/// input stream is append-only as then we only need to retain the "currently winning" value.
/// Every hierarchical aggregate needs to supply a corresponding ReductionMonoid implementation.
fn reduction_type(func: &AggregateFunc) -> ReductionType {
    match func {
        AggregateFunc::SumInt32
        | AggregateFunc::SumInt64
        | AggregateFunc::SumFloat32
        | AggregateFunc::SumFloat64
        | AggregateFunc::SumDecimal
        | AggregateFunc::Count
        | AggregateFunc::Any
        | AggregateFunc::All
        | AggregateFunc::Dummy => ReductionType::Accumulable,
        AggregateFunc::MaxInt32
        | AggregateFunc::MaxInt64
        | AggregateFunc::MaxFloat32
        | AggregateFunc::MaxFloat64
        | AggregateFunc::MaxDecimal
        | AggregateFunc::MaxBool
        | AggregateFunc::MaxString
        | AggregateFunc::MaxDate
        | AggregateFunc::MaxTimestamp
        | AggregateFunc::MaxTimestampTz
        | AggregateFunc::MinInt32
        | AggregateFunc::MinInt64
        | AggregateFunc::MinFloat32
        | AggregateFunc::MinFloat64
        | AggregateFunc::MinDecimal
        | AggregateFunc::MinBool
        | AggregateFunc::MinString
        | AggregateFunc::MinDate
        | AggregateFunc::MinTimestamp
        | AggregateFunc::MinTimestampTz => ReductionType::Hierarchical,
        AggregateFunc::JsonbAgg => ReductionType::Basic,
    }
}

/// Monoids for in-place compaction of monotonic streams.
pub mod monoids {

    // We can improve the performance of some aggregations through the use of algebra.
    // In particular, we can move some of the aggregations in to the `diff` field of
    // updates, by changing `diff` from integers to a different algebraic structure.
    //
    // The one we use is called a "semigroup", and it means that the structure has a
    // symmetric addition operator. The trait we use also allows the semigroup elements
    // to present as "zero", meaning they always act as the identity under +, but we
    // will not have such elements in this case (they would correspond to positive and
    // negative infinity, which we do not represent).

    use std::ops::AddAssign;

    use differential_dataflow::difference::Semigroup;
    use serde::{Deserialize, Serialize};

    use expr::AggregateFunc;
    use repr::{Datum, Row};

    /// A monoid containing a single-datum row.
    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
    pub enum ReductionMonoid {
        Min(Row),
        Max(Row),
    }

    impl<'a> AddAssign<&'a Self> for ReductionMonoid {
        fn add_assign(&mut self, rhs: &'a Self) {
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
                        lhs.clone_from(&rhs);
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
                        lhs.clone_from(&rhs);
                    }
                }
                (lhs, rhs) => log::error!(
                    "Mismatched monoid variants in reduction! lhs: {:?} rhs: {:?}",
                    lhs,
                    rhs
                ),
            }
        }
    }

    impl Semigroup for ReductionMonoid {
        fn is_zero(&self) -> bool {
            false
        }
    }

    /// Get the correct monoid implementation for a given aggregation function. Note that
    // all hierarchical aggregation functions need to supply a monoid implementation.
    pub fn get_monoid(row: Row, func: &AggregateFunc) -> Option<ReductionMonoid> {
        match func {
            AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxDecimal
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz => Some(ReductionMonoid::Max(row)),
            AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinDecimal
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz => Some(ReductionMonoid::Min(row)),
            AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumDecimal
            | AggregateFunc::Count
            | AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::Dummy
            | AggregateFunc::JsonbAgg => None,
        }
    }
}
