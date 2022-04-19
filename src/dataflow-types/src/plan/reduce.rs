// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reduction execution planning and dataflow construction.

//! We build `ReducePlan`s to manage the complexity of planning the generated dataflow for a
//! given reduce expression. The intent here is that each creating a `ReducePlan` should capture
//! all of the decision making about what kind of dataflow do we need to render and what each
//! operator needs to do, and then actually rendering the plan can be a relatively simple application
//! of (as much as possible) straight line code.
//!
//! Materialize needs to be able to maintain reductions incrementally (roughly, using
//! time proportional to the number of changes in the input) and ideally, with a
//! memory footprint proportional to the number of reductions being computed. We have to employ
//! several tricks to achieve that, and these tricks constitute most of the complexity involved
//! with planning and rendering reduce expressions. There's some additional complexity involved
//! in handling aggregations with `DISTINCT` correctly so that we can efficiently suppress
//! duplicate updates.
//!
//! In order to optimize the performance of our rendered dataflow, we divide all aggregations
//! into three distinct types. Each type gets rendered separately, with its own specialized plan
//! and dataflow. The three types are as follows:
//!
//! 1. Accumulable:
//!    Accumulable reductions can be computed inline in a Differential update's `difference`
//!    field because they basically boil down to tracking counts of things. `sum()` is an
//!    example of an accumulable reduction, and when some element `x` is removed from the set
//!    of elements being summed, we can introduce `-x` to incrementally maintain the sum. More
//!    formally, accumulable reductions correspond to instances of commutative Abelian groups.
//! 2. Hierarchical:
//!    Hierarchical reductions don't have a meaningful negation like accumulable reductions do, but
//!    they are still commutative and associative, which lets us compute the reduction over subsets
//!    of the input, and then compute the reduction again on those results. For example:
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
//! When we render these reductions we want to limit the number of arrangements we produce. When we build a
//! dataflow for a reduction containing multiple types of reductions, we have no choice but to divide up the
//! requested aggregations by type, render each type separately and then take those results and collate them
//! back in the requested output order. However, if we only need to perform aggregations of a single reduction
//! type, we can specialize and render the dataflow to compute those aggregations in the correct order, and
//! return the output arrangement directly and avoid the extra collation arrangement.

use mz_expr::permutation_for_arrangement;
use mz_expr::AggregateExpr;
use mz_expr::AggregateFunc;
use mz_expr::MirScalarExpr;
use mz_ore::soft_assert_or_log;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;

use super::AvailableCollections;

/// This enum represents the three potential types of aggregations.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ReductionType {
    /// Accumulable functions can be subtracted from (are invertible), and associative.
    /// We can compute these results by moving some data to the diff field under arbitrary
    /// changes to inputs. Examples include sum or count.
    Accumulable,
    /// Hierarchical functions are associative, which means we can split up the work of
    /// computing them across subsets. Note that hierarchical reductions should also
    /// reduce the data in some way, as otherwise rendering them hierarchically is not
    /// worth it. Examples include min or max.
    Hierarchical,
    /// Basic, for lack of a better word, are functions that are neither accumulable
    /// nor hierarchical. Examples include jsonb_agg.
    Basic,
}

/// A `ReducePlan` provides a concise description for how we will
/// execute a given reduce expression.
///
/// The provided reduce expression can have no
/// aggregations, in which case its just a `Distinct` and otherwise
/// it's composed of a combination of accumulable, hierarchical and
/// basic aggregations.
///
/// We want to try to centralize as much decision making about the
/// shape / general computation of the rendered dataflow graph
/// in this plan, and then make actually rendering the graph
/// be as simple (and compiler verifiable) as possible.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ReducePlan {
    /// Plan for not computing any aggregations, just determining the set of
    /// distinct keys.
    Distinct,
    /// Plan for not computing any aggregations, just determining the set of distinct keys. A
    /// specialization of [ReducePlan::Distinct] maintaining rows not in the output.
    #[allow(dead_code)]
    DistinctNegated,
    /// Plan for computing only accumulable aggregations.
    Accumulable(AccumulablePlan),
    /// Plan for computing only hierarchical aggregations.
    Hierarchical(HierarchicalPlan),
    /// Plan for computing only basic aggregations.
    Basic(BasicPlan),
    /// Plan for computing a mix of different kinds of aggregations.
    /// We need to do extra work here to reassemble results back in the
    /// requested order.
    Collation(CollationPlan),
}

/// Plan for computing a set of accumulable aggregations.
///
/// We fuse all of the accumulable aggregations together
/// and compute them with one dataflow fragment. We need to
/// be careful to separate out the aggregations that
/// apply only to the distinct set of values. We need
/// to apply a distinct operator to those before we
/// combine them with everything else.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AccumulablePlan {
    /// All of the aggregations we were asked to compute, stored
    /// in order.
    pub full_aggrs: Vec<AggregateExpr>,
    /// All of the non-distinct accumulable aggregates.
    /// Each element represents:
    /// (index of the aggregation among accumulable aggregations,
    ///  index of the datum among inputs, aggregation expr)
    /// These will all be rendered together in one dataflow fragment.
    pub simple_aggrs: Vec<(usize, usize, AggregateExpr)>,
    /// Same as above but for all of the `DISTINCT` accumulable aggregations.
    pub distinct_aggrs: Vec<(usize, usize, AggregateExpr)>,
}

/// Plan for computing a set of hierarchical aggregations.
///
/// In the append-only setting we can render them in-place
/// with monotonic plans, but otherwise, we need to render
/// them with a reduction tree that splits the inputs into
/// small, and then progressively larger, buckets
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum HierarchicalPlan {
    /// Plan hierarchical aggregations under monotonic inputs.
    Monotonic(MonotonicPlan),
    /// Plan for hierarchical aggregations under non-monotonic inputs.
    Bucketed(BucketedPlan),
}

/// Plan for computing a set of hierarchical aggregations with a
/// monotonic input.
///
/// Here, the aggregations will be rendered in place. We don't
/// need to worry about retractions because the inputs are
/// append only, so we can change our computation to
/// only retain the "best" value in the diff field, instead
/// of holding onto all values.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MonotonicPlan {
    /// All of the aggregations we were asked to compute.
    pub aggr_funcs: Vec<AggregateFunc>,
    /// Set of "skips" or calls to `nth()` an iterator needs to do over
    /// the input to extract the relevant datums.
    pub skips: Vec<usize>,
}

/// Plan for computing a set of hierarchical aggregations
/// with non-monotonic inputs.
///
/// To perform hierarchical aggregations with stable runtimes
/// under updates we'll subdivide the group key into buckets, compute
/// the reduction in each of those subdivided buckets and then combine
/// the results into a coarser bucket (one that represents a larger
/// fraction of the original input) and redo the reduction in another
/// layer. Effectively, we'll construct a min / max heap out of a series
/// of reduce operators (each one is a separate layer).
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BucketedPlan {
    /// All of the aggregations we were asked to compute.
    pub aggr_funcs: Vec<AggregateFunc>,
    /// Set of "skips" or calls to `nth()` an iterator needs to do over
    /// the input to extract the relevant datums.
    pub skips: Vec<usize>,
    /// The number of buckets in each layer of the reduction tree. Should
    /// be decreasing, and ideally, a power of two so that we can easily
    /// distribute values to buckets with `value.hashed() % bucketes[layer]`.
    pub buckets: Vec<u64>,
}

/// Plan for computing a set of basic aggregations.
///
/// There's much less complexity when rendering basic aggregations.
/// Each aggregation corresponds to one Differential reduce operator.
/// That's it. However, we still want to present one final arrangement
/// so basic aggregations present results with the same interface
/// (one arrangement containing a row with all results) that accumulable
/// and hierarchical aggregations do. To provide that, we render an
/// additional reduce operator whenever we have multiple reduce aggregates
/// to combine and present results in the appropriate order. If we
/// were only asked to compute a single aggregation, we can skip
/// that step and return the arrangement provided by computing the aggregation
/// directly.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum BasicPlan {
    /// Plan for rendering a single basic aggregation. Here, the
    /// first element denotes the index in the set of inputs
    /// that we are aggregating over.
    Single(usize, AggregateExpr),
    /// Plan for rendering multiple basic aggregations.
    /// These need to then be collated together in an additional
    /// reduction. Each element represents the:
    /// `(index of the set of the input we are aggregating over,
    ///   the aggregation function)`
    Multiple(Vec<(usize, AggregateExpr)>),
}

/// Plan for collating the results of computing multiple aggregation
/// types.
///
/// TODO: could we express this as a delta join
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct CollationPlan {
    /// Accumulable aggregation results to collate, if any.
    pub accumulable: Option<AccumulablePlan>,
    /// Hierarchical aggregation results to collate, if any.
    pub hierarchical: Option<HierarchicalPlan>,
    /// Basic aggregation results to collate, if any.
    pub basic: Option<BasicPlan>,
    /// When we get results back from each of the different
    /// aggregation types, they will be subsequences of
    /// the sequence aggregations in the original reduce expression.
    /// We keep a map from output position -> reduction type
    /// to easily merge results back into the requested order.
    pub aggregate_types: Vec<ReductionType>,
}

impl ReducePlan {
    /// Generate a plan for computing the supplied aggregations.
    ///
    /// The resulting plan summarizes what the dataflow to be created
    /// and how the aggregations will be executed.
    pub fn create_from(
        aggregates: Vec<AggregateExpr>,
        monotonic: bool,
        expected_group_size: Option<usize>,
    ) -> Self {
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

        // Convert each grouped list of reductions into a plan.
        let plan: Vec<_> = reduction_types
            .into_iter()
            .map(|(typ, aggregates_list)| {
                ReducePlan::create_inner(typ, aggregates_list, monotonic, expected_group_size)
            })
            .collect();

        // If we only have a single type of aggregation present we can
        // render that directly
        if plan.len() == 1 {
            return plan[0].clone();
        }

        // Otherwise, we have to stitch reductions together.

        // First, lets sanity check that we don't have an impossible number
        // of reduction types.
        assert!(plan.len() <= 3);

        let mut collation: CollationPlan = Default::default();

        // Construct a mapping from output_position -> reduction that we can
        // use to reconstruct the output in the correct order.
        let aggregate_types = aggregates
            .iter()
            .map(|a| reduction_type(&a.func))
            .collect::<Vec<_>>();

        collation.aggregate_types = aggregate_types;

        for expr in plan.into_iter() {
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
                ReducePlan::Distinct | ReducePlan::DistinctNegated | ReducePlan::Collation(_) => {
                    panic!("Inner reduce plan was unsupported type!")
                }
            }
        }

        ReducePlan::Collation(collation)
    }

    /// Generate a plan for computing the specified type of aggregations.
    ///
    /// This function assumes that all of the supplied aggregates are
    /// actually of the correct reduction type, and are a subsequence
    /// of the total list of requested aggregations.
    fn create_inner(
        typ: ReductionType,
        aggregates_list: Vec<(usize, AggregateExpr)>,
        monotonic: bool,
        expected_group_size: Option<usize>,
    ) -> Self {
        assert!(
            aggregates_list.len() > 0,
            "error: tried to render a reduce dataflow with no aggregates"
        );
        match typ {
            ReductionType::Accumulable => {
                let mut simple_aggrs = vec![];
                let mut distinct_aggrs = vec![];
                let full_aggrs: Vec<_> = aggregates_list
                    .iter()
                    .cloned()
                    .map(|(_, aggr)| aggr)
                    .collect();
                for (accumulable_index, (datum_index, aggr)) in
                    aggregates_list.into_iter().enumerate()
                {
                    // Accumulable aggregations need to do extra per-aggregate work
                    // for aggregations with the distinct bit set, so we'll separate
                    // those out now.
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

                // We don't have random access over Rows so we can simplify the
                // task of grabbing the inputs we are aggregating over by
                // generating a list of "skips" an iterator over the Row needs
                // to do to get the desired indexes.
                let skips = convert_indexes_to_skips(indexes);
                if monotonic {
                    let monotonic = MonotonicPlan { aggr_funcs, skips };
                    ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(monotonic))
                } else {
                    let mut buckets = vec![];
                    let mut current = 16;

                    // Plan for 4B records in the expected case if the user
                    // didn't specify a group size.
                    let limit = expected_group_size.unwrap_or(4_000_000_000);

                    // Distribute buckets in powers of 16, so that we can strike
                    // a balance between how many inputs each layer gets from
                    // the preceding layer, while also limiting the number of
                    // layers.
                    while current < limit {
                        buckets.push(current as u64);
                        current *= 16;
                    }

                    // We need to store the bucket numbers in decreasing order.
                    buckets.reverse();

                    let bucketed = BucketedPlan {
                        aggr_funcs,
                        skips,
                        buckets,
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
    }

    /// Reports all keys of produced arrangements.
    ///
    /// This is likely either an empty vector, for no arrangement,
    /// or a singleton vector containing the list of expressions
    /// that key a single arrangement.
    pub fn keys(&self, key_arity: usize, arity: usize) -> AvailableCollections {
        match self {
            ReducePlan::DistinctNegated => AvailableCollections::new_raw(),
            _ => {
                let key = (0..key_arity)
                    .map(mz_expr::MirScalarExpr::Column)
                    .collect::<Vec<_>>();
                let (permutation, thinning) = permutation_for_arrangement(&key, arity);
                AvailableCollections::new_arranged(vec![(key, permutation, thinning)])
            }
        }
    }
}

/// Plan for extracting keys and values in preparation for a reduction.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KeyValPlan {
    /// Extracts the columns used as the key.
    pub key_plan: mz_expr::SafeMfpPlan,
    /// Extracts the columns used to feed the aggregations.
    pub val_plan: mz_expr::SafeMfpPlan,
}

impl KeyValPlan {
    /// Create a new [KeyValPlan] from aggregation arguments.
    pub fn new(
        input_arity: usize,
        group_key: &[MirScalarExpr],
        aggregates: &[AggregateExpr],
        input_permutation_and_new_arity: Option<(HashMap<usize, usize>, usize)>,
    ) -> Self {
        // Form an operator for evaluating key expressions.
        let mut key_mfp = mz_expr::MapFilterProject::new(input_arity)
            .map(group_key.iter().cloned())
            .project(input_arity..(input_arity + group_key.len()));
        if let Some((input_permutation, new_arity)) = input_permutation_and_new_arity.clone() {
            key_mfp.permute(input_permutation, new_arity);
        }

        // Form an operator for evaluating value expressions.
        let mut val_mfp = mz_expr::MapFilterProject::new(input_arity)
            .map(aggregates.iter().map(|a| a.expr.clone()))
            .project(input_arity..(input_arity + aggregates.len()));
        if let Some((input_permutation, new_arity)) = input_permutation_and_new_arity {
            val_mfp.permute(input_permutation, new_arity);
        }

        key_mfp.optimize();
        let key_plan = key_mfp.into_plan().unwrap().into_nontemporal().unwrap();
        val_mfp.optimize();
        let val_plan = val_mfp.into_plan().unwrap().into_nontemporal().unwrap();

        Self { key_plan, val_plan }
    }

    /// The arity of the key plan
    pub fn key_arity(&self) -> usize {
        self.key_plan.projection.len()
    }
}

/// Transforms a vector containing indexes of needed columns into one containing
/// the "skips" an iterator over a Row would need to perform to see those values.
///
/// This function requires that all of the elements in `indexes` are strictly
/// increasing.
/// E.g. [3, 6, 10, 15] turns into [3, 3, 4, 5]
pub fn convert_indexes_to_skips(mut indexes: Vec<usize>) -> Vec<usize> {
    for i in 1..indexes.len() {
        soft_assert_or_log!(
            indexes[i - 1] < indexes[i],
            "convert_indexes_to_skip needs indexes to be strictly increasing. Received: {:?}",
            indexes,
        );
    }

    for i in (1..indexes.len()).rev() {
        indexes[i] -= indexes[i - 1];
        indexes[i] -= 1;
    }

    indexes
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
        AggregateFunc::SumInt16
        | AggregateFunc::SumInt32
        | AggregateFunc::SumInt64
        | AggregateFunc::SumFloat32
        | AggregateFunc::SumFloat64
        | AggregateFunc::SumNumeric
        | AggregateFunc::Count
        | AggregateFunc::Any
        | AggregateFunc::All
        | AggregateFunc::Dummy => ReductionType::Accumulable,
        AggregateFunc::MaxNumeric
        | AggregateFunc::MaxInt16
        | AggregateFunc::MaxInt32
        | AggregateFunc::MaxInt64
        | AggregateFunc::MaxFloat32
        | AggregateFunc::MaxFloat64
        | AggregateFunc::MaxBool
        | AggregateFunc::MaxString
        | AggregateFunc::MaxDate
        | AggregateFunc::MaxTimestamp
        | AggregateFunc::MaxTimestampTz
        | AggregateFunc::MinNumeric
        | AggregateFunc::MinInt16
        | AggregateFunc::MinInt32
        | AggregateFunc::MinInt64
        | AggregateFunc::MinFloat32
        | AggregateFunc::MinFloat64
        | AggregateFunc::MinBool
        | AggregateFunc::MinString
        | AggregateFunc::MinDate
        | AggregateFunc::MinTimestamp
        | AggregateFunc::MinTimestampTz => ReductionType::Hierarchical,
        AggregateFunc::JsonbAgg { .. }
        | AggregateFunc::JsonbObjectAgg { .. }
        | AggregateFunc::ArrayConcat { .. }
        | AggregateFunc::ListConcat { .. }
        | AggregateFunc::StringAgg { .. }
        | AggregateFunc::RowNumber { .. }
        | AggregateFunc::DenseRank { .. }
        | AggregateFunc::LagLead { .. } => ReductionType::Basic,
    }
}
