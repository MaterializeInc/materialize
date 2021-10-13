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

use std::collections::BTreeMap;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::{Consolidate, Reduce, Threshold};
use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::DataflowError;
use dec::OrderedDecimal;
use expr::{AggregateExpr, AggregateFunc};
use ore::cast::CastFrom;
use ore::soft_assert_or_log;
use repr::adt::numeric::{self, Numeric, NumericAgg};
use repr::{Datum, DatumList, Row, RowArena};

use super::context::Context;
use crate::render::context::Arrangement;
use crate::render::context::CollectionBundle;
use crate::render::datum_vec::DatumVec;
use crate::render::ArrangementFlavor;

use crate::arrangement::manager::RowSpine;

/// This enum represents the three potential types of aggregations.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
enum ReductionType {
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccumulablePlan {
    /// All of the aggregations we were asked to compute, stored
    /// in order.
    full_aggrs: Vec<AggregateExpr>,
    /// All of the non-distinct accumulable aggregates.
    /// Each element represents:
    /// (index of the aggregation among accumulable aggregations,
    ///  index of the datum among inputs, aggregation expr)
    /// These will all be rendered together in one dataflow fragment.
    simple_aggrs: Vec<(usize, usize, AggregateExpr)>,
    // Same as above but for all of the `DISTINCT` accumulable aggregations.
    distinct_aggrs: Vec<(usize, usize, AggregateExpr)>,
}

/// Plan for computing a set of hierarchical aggregations.
///
/// In the append-only setting we can render them in-place
/// with monotonic plans, but otherwise, we need to render
/// them with a reduction tree that splits the inputs into
/// small, and then progressively larger, buckets
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonotonicPlan {
    /// All of the aggregations we were asked to compute.
    aggr_funcs: Vec<AggregateFunc>,
    /// Set of "skips" or calls to `nth()` an iterator needs to do over
    /// the input to extract the relevant datums.
    skips: Vec<usize>,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketedPlan {
    /// All of the aggregations we were asked to compute.
    aggr_funcs: Vec<AggregateFunc>,
    /// Set of "skips" or calls to `nth()` an iterator needs to do over
    /// the input to extract the relevant datums.
    skips: Vec<usize>,
    // The number of buckets in each layer of the reduction tree. Should
    // be decreasing, and ideally, a power of two so that we can easily
    // distribute values to buckets with `value.hashed() % bucketes[layer]`.
    buckets: Vec<u64>,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CollationPlan {
    /// Accumulable aggregation results to collate, if any.
    accumulable: Option<AccumulablePlan>,
    /// Hierarchical aggregation results to collate, if any.
    hierarchical: Option<HierarchicalPlan>,
    /// Basic aggregation results to collate, if any.
    basic: Option<BasicPlan>,
    /// When we get results back from each of the different
    /// aggregation types, they will be subsequences of
    /// the sequence aggregations in the original reduce expression.
    /// We keep a map from output position -> reduction type
    /// to easily merge results back into the requested order.
    aggregate_types: Vec<ReductionType>,
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
    pub fn keys(&self, key_arity: usize) -> Vec<Vec<expr::MirScalarExpr>> {
        // Accumulate keys into this vector, and return it.
        let mut keys = Vec::new();
        match self {
            ReducePlan::DistinctNegated => {}
            _ => {
                keys.push(
                    (0..key_arity)
                        .map(|column| expr::MirScalarExpr::Column(column))
                        .collect(),
                );
            }
        }
        keys
    }

    /// Render a dataflow based on the provided plan.
    ///
    /// The output will be an arrangements that looks the same as if
    /// we just had a single reduce operator computing everything together, and
    /// this arrangement can also be re-used.
    fn render<G, T>(
        self,
        collection: Collection<G, (Row, Row)>,
        err_input: Collection<G, DataflowError>,
        key_arity: usize,
    ) -> CollectionBundle<G, Row, T>
    where
        G: Scope,
        G::Timestamp: Lattice + Refines<T>,
        T: Timestamp + Lattice,
    {
        // Convenience wrapper to render the right kind of hierarchical plan.
        let build_hierarchical = |collection: Collection<G, (Row, Row)>,
                                  expr: HierarchicalPlan,
                                  top_level: bool| match expr {
            HierarchicalPlan::Monotonic(expr) => build_monotonic(collection, expr, top_level),
            HierarchicalPlan::Bucketed(expr) => build_bucketed(collection, expr, top_level),
        };

        // Convenience wrapper to render the right kind of basic plan.
        let build_basic =
            |collection: Collection<G, (Row, Row)>, expr: BasicPlan, top_level: bool| match expr {
                BasicPlan::Single(index, aggr) => {
                    build_basic_aggregate(collection, index, &aggr, top_level)
                }
                BasicPlan::Multiple(aggrs) => build_basic_aggregates(collection, aggrs, top_level),
            };

        let arrangement_or_bundle: ArrangementOrCollection<G> = match self {
            // If we have no aggregations or just a single type of reduction, we
            // can go ahead and render them directly.
            ReducePlan::Distinct => build_distinct(collection).into(),
            ReducePlan::DistinctNegated => build_distinct_retractions(collection).into(),
            ReducePlan::Accumulable(expr) => build_accumulable(collection, expr, true).into(),
            ReducePlan::Hierarchical(expr) => build_hierarchical(collection, expr, true).into(),
            ReducePlan::Basic(expr) => build_basic(collection, expr, true).into(),
            // Otherwise, we need to render something different for each type of
            // reduction, and then stitch them together.
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
                build_collation(to_collate, expr.aggregate_types, &mut collection.scope()).into()
            }
        };
        arrangement_or_bundle.into_bundle(key_arity, err_input)
    }
}

/// A type wrapping either an arrangement or a single collection.
enum ArrangementOrCollection<G>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    /// Wrap an arrangement
    Arrangement(Arrangement<G, Row>),
    /// Wrap a collection
    Collection(Collection<G, Row>),
}

impl<G> ArrangementOrCollection<G>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    /// Convert to a [CollectionBundle] by either supplying the arrangement or construction from
    /// collections.
    ///
    /// * `key_arity` - The number of columns in the key. Only used for arrangement variants.
    /// * `err_input` - A collection containing the error stream.
    fn into_bundle<T>(
        self,
        key_arity: usize,
        err_input: Collection<G, DataflowError>,
    ) -> CollectionBundle<G, Row, T>
    where
        G::Timestamp: Lattice + Refines<T>,
        T: Timestamp + Lattice,
    {
        match self {
            ArrangementOrCollection::Arrangement(arrangement) => CollectionBundle::from_columns(
                0..key_arity,
                ArrangementFlavor::Local(arrangement, err_input.arrange()),
            ),
            ArrangementOrCollection::Collection(oks) => {
                CollectionBundle::from_collections(oks, err_input)
            }
        }
    }
}

impl<G> From<Arrangement<G, Row>> for ArrangementOrCollection<G>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    fn from(arrangement: Arrangement<G, Row>) -> Self {
        ArrangementOrCollection::Arrangement(arrangement)
    }
}

impl<G> From<Collection<G, Row>> for ArrangementOrCollection<G>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    fn from(collection: Collection<G, Row>) -> Self {
        ArrangementOrCollection::Collection(collection)
    }
}

/// Plan for extracting keys and values in preparation for a reduction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValPlan {
    /// Extracts the columns used as the key.
    key_plan: expr::SafeMfpPlan,
    /// Extracts the columns used to feed the aggregations.
    val_plan: expr::SafeMfpPlan,
    /// Steps to take over the columns of the input row.
    skips: Vec<usize>,
}

impl KeyValPlan {
    pub fn new(
        input_arity: usize,
        group_key: &[expr::MirScalarExpr],
        aggregates: &[AggregateExpr],
    ) -> Self {
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
        let key_plan = key_mfp.into_plan().unwrap().into_nontemporal().unwrap();
        val_mfp.permute(&demand_map, demand_map.len());
        val_mfp.optimize();
        let val_plan = val_mfp.into_plan().unwrap().into_nontemporal().unwrap();

        let skips = convert_indexes_to_skips(demand);

        Self {
            key_plan,
            val_plan,
            skips,
        }
    }
}

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders a `MirRelationExpr::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    pub fn render_reduce(
        &mut self,
        input: CollectionBundle<G, Row, T>,
        key_val_plan: KeyValPlan,
        reduce_plan: ReducePlan,
    ) -> CollectionBundle<G, Row, T> {
        let KeyValPlan {
            key_plan,
            val_plan,
            skips,
        } = key_val_plan;
        let key_arity = key_plan.projection.len();
        let mut row_packer = Row::default();
        let mut row_mfp = Row::default();
        let mut datums = DatumVec::new();
        let (key_val_input, err_input): (
            timely::dataflow::Stream<_, (Result<(Row, Row), DataflowError>, _, _)>,
            _,
        ) = input.flat_map(None, move |row, time, diff| {
            let temp_storage = RowArena::new();

            // Unpack only the demanded columns.
            let mut datums_local = datums.borrow();
            let mut row_iter = row.iter();
            for skip in skips.iter() {
                datums_local.push((&mut row_iter).nth(*skip).unwrap());
            }

            // Evaluate the key expressions.
            let key = match key_plan.evaluate_into(&mut datums_local, &temp_storage, &mut row_mfp) {
                Err(e) => return Some((Err(DataflowError::from(e)), time.clone(), diff.clone())),
                Ok(key) => key.expect("Row expected as no predicate was used"),
            };
            // Evaluate the value expressions.
            // The prior evaluation may have left additional columns we should delete.
            datums_local.truncate(skips.len());
            let val = match val_plan.evaluate_iter(&mut datums_local, &temp_storage) {
                Err(e) => return Some((Err(DataflowError::from(e)), time.clone(), diff.clone())),
                Ok(val) => val.expect("Row expected as no predicate was used"),
            };
            row_packer.clear();
            row_packer.extend(val);
            drop(datums_local);

            // Mint the final row, ideally re-using resources.
            // TODO(mcsherry): This can perhaps be extracted for
            // re-use if it seems to be a common pattern.
            use timely::communication::message::RefOrMut;
            let row = match row {
                RefOrMut::Ref(_) => row_packer.finish_and_reuse(),
                RefOrMut::Mut(row) => {
                    row.clone_from(&row_packer);
                    row_packer.clear();
                    std::mem::take(row)
                }
            };
            return Some((Ok((key, row)), time.clone(), diff.clone()));
        });

        // Demux out the potential errors from key and value selector evaluation.
        use crate::operator::CollectionExt;
        use differential_dataflow::operators::consolidate::ConsolidateStream;
        let (ok, mut err) = key_val_input
            .as_collection()
            .consolidate_stream()
            .flat_map_fallible("OkErrDemux", |x| Some(x));

        err = err.concat(&err_input);

        // Render the reduce plan
        reduce_plan.render(ok, err, key_arity)
    }
}

/// Build the dataflow to combine arrangements containing results of different
/// aggregation types into a single arrangement.
///
/// This computes the same thing as a join on the group key followed by shuffling
/// the values into the correct order. This implementation assumes that all input
/// arrangements present values in a way that respects the desired output order,
/// so we can do a linear merge to form the output.
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

    // First, lets collect all results into a single collection.
    for (reduction_type, arrangement) in arrangements.into_iter() {
        let collection =
            arrangement.as_collection(move |key, val| (key.clone(), (reduction_type, val.clone())));
        to_concat.push(collection);
    }

    use differential_dataflow::collection::concatenate;
    concatenate(scope, to_concat)
        .reduce_abelian::<_, RowSpine<_, _, _, _>>("ReduceCollation", {
            let mut row_packer = Row::default();
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
                    soft_assert_or_log!(
                        *cnt >= 0,
                        "[customer-data] Negative accumulation in ReduceCollation: {:?} with count {:?}",
                        val, cnt,
                    );
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

/// Build the dataflow to compute the set of distinct keys.
fn build_distinct<G>(collection: Collection<G, (Row, Row)>) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    collection.reduce_abelian::<_, RowSpine<_, _, _, _>>("DistinctBy", {
        |key, _input, output| {
            output.push((key.clone(), 1));
        }
    })
}

/// Build the dataflow to compute the set of distinct keys.
///
/// This implementation maintains the rows that don't appear in the output.
fn build_distinct_retractions<G, T>(collection: Collection<G, (Row, Row)>) -> Collection<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    let negated_result = collection.reduce_named("DistinctBy Retractions", {
        |key, input, output| {
            output.push((key.clone(), -1));
            output.extend(
                input
                    .iter()
                    .map(|(values, count)| ((*values).clone(), *count)),
            );
        }
    });
    use timely::dataflow::operators::Map;
    negated_result
        .negate()
        .concat(&collection)
        .consolidate()
        .inner
        .map(|((k, _), time, count)| (k, time, count))
        .as_collection()
}

/// Build the dataflow to compute and arrange multiple non-accumulable,
/// non-hierarchical aggregations on `input`.
///
/// This function assumes that we are explicitly rendering multiple basic aggregations.
/// For each aggregate, we render a different reduce operator, and then fuse
/// results together into a final arrangement that presents all the results
/// in the order specified by `aggrs`. `prepend_keys` is true if the arrangement
/// produced by this function needs to be reused by other views.
fn build_basic_aggregates<G>(
    input: Collection<G, (Row, Row)>,
    aggrs: Vec<(usize, AggregateExpr)>,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // We are only using this function to render multiple basic aggregates and
    // stitch them together. If that's not true we should complain.
    soft_assert_or_log!(
        aggrs.len() > 1,
        "Unexpectedly computing {} basic aggregations together but we expected to be doing more than one",
        aggrs.len(),
    );
    let mut to_collect = Vec::new();
    for (index, aggr) in aggrs {
        let result = build_basic_aggregate(input.clone(), index, &aggr, false);
        to_collect.push(result.as_collection(move |key, val| (key.clone(), (index, val.clone()))));
    }
    differential_dataflow::collection::concatenate(&mut input.scope(), to_collect)
        .reduce_abelian::<_, RowSpine<_, _, _, _>>("ReduceFuseBasic", {
            let mut row_packer = Row::default();
            move |key, input, output| {
                // First, fill our output row with key information if requested.
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

/// Build the dataflow to compute a single basic aggregation.
///
/// This method also applies distinctness if required. `prepend_keys` is true if
/// the arrangement produced by this function needs to be reused by other views.
fn build_basic_aggregate<G>(
    input: Collection<G, (Row, Row)>,
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

    // Extract the value we were asked to aggregate over.
    let mut partial = if !prepend_key {
        let mut packer = Row::default();
        input.map(move |(key, row)| {
            let value = row.iter().nth(index).unwrap();
            packer.push(value);
            (key, packer.finish_and_reuse())
        })
    } else {
        // If the arrangement produced by this function is going to be exported
        // for reuse that implies that theres only a single aggregation in the
        // whole reduce, and only one value in the values row. Let's complain if
        // we're trying to aggregate over anything else.
        if index != 0 {
            log::error!("Computing single basic aggregate on index {} with prepend-keys=true. Expected index 0", index);
        }
        input
    };

    // If `distinct` is set, we restrict ourselves to the distinct `(key, val)`.
    if distinct {
        partial = partial.distinct();
    }

    partial.reduce_abelian::<_, RowSpine<_, _, _, _>>("ReduceInaccumulable", {
        let mut row_packer = Row::default();
        move |key, source, target| {
            // Negative counts would be surprising, but until we are 100% certain we wont
            // see them, we should report when we do. We may want to bake even more info
            // in here in the future.
            if source.iter().any(|(_val, cnt)| cnt < &0) {
                // XXX: This reports user data, which we perhaps should not do!
                for (val, cnt) in source.iter() {
                    soft_assert_or_log!(
                        *cnt >= 0,
                        "[customer-data] Negative accumulation in ReduceInaccumulable: {:?} with count {:?}",
                        val, cnt,
                    );
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

/// Build the dataflow to compute and arrange multiple hierarchical aggregations
/// on non-monotonic inputs.
///
/// This function renders a single reduction tree that computes aggregations with
/// a priority queue implemented with a series of reduce operators that partition
/// the input into buckets, and compute the aggregation over very small buckets
/// and feed the results up to larger buckets. `prepend_keys` is true if the
/// arrangement produced by this function needs to be reused by other views.
///
/// Note that this implementation currently ignores the distinct bit because we
/// currently only perform min / max hierarchically and the reduction tree
/// efficiently suppresses non-distinct updates.
fn build_bucketed<G>(
    input: Collection<G, (Row, Row)>,
    BucketedPlan {
        aggr_funcs,
        skips,
        buckets,
    }: BucketedPlan,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Gather the relevant values into a vec of rows ordered by aggregation_index
    let mut packer = Row::default();
    let input = input.map(move |(key, row)| {
        let mut values = Vec::with_capacity(skips.len());
        let mut row_iter = row.iter();
        for skip in skips.iter() {
            packer.push((&mut row_iter).nth(*skip).unwrap());
            values.push(packer.finish_and_reuse());
        }

        (key, values)
    });

    // Repeatedly apply hierarchical reduction with a progressively coarser key.
    let mut stage = input.map(move |(key, values)| ((key, values.hashed()), values));
    for b in buckets.into_iter() {
        stage = build_bucketed_stage(stage, aggr_funcs.clone(), b);
    }

    // Discard the hash from the key and return to the format of the input data.
    let partial = stage.map(|((key, _hash), values)| (key, values));

    // Build a series of stages for the reduction
    // Arrange the final result into (key, Row)
    partial.reduce_abelian::<_, RowSpine<_, _, _, _>>("ReduceMinsMaxes", {
        let mut row_packer = Row::default();
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

/// Build the dataflow for one stage of a reduction tree for multiple hierarchical
/// aggregates.
///
/// `buckets` indicates the number of buckets in this stage. We do some non
/// obvious trickery here to limit the memory usage per layer by internally
/// holding only the elements that were rejected by this stage. However, the
/// output collection maintains the `((key, bucket), (passing value)` for this
/// stage.
fn build_bucketed_stage<G>(
    input: Collection<G, ((Row, u64), Vec<Row>)>,
    aggrs: Vec<AggregateFunc>,
    buckets: u64,
) -> Collection<G, ((Row, u64), Vec<Row>)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let input = input.map(move |((key, hash), values)| ((key, hash % buckets), values));

    let negated_output = input
        .reduce_named("MinsMaxesHierarchical", {
            move |key, source, target| {
                // Should negative accumulations reach us, we should loudly complain.
                if source.iter().any(|(_val, cnt)| cnt <= &0) {
                    for (val, cnt) in source.iter() {
                        // XXX: This reports user data, which we perhaps should not do!
                        soft_assert_or_log!(
                            *cnt > 0,
                            "[customer-data] Non-positive accumulation in MinsMaxesHierarchical: key: {:?}\tvalue: {:?}\tcount: {:?}",
                            key, val, cnt,
                        );
                    }
                } else {
                    let mut output = Vec::with_capacity(aggrs.len());
                    for (aggr_index, func) in aggrs.iter().enumerate() {
                        let iter = source.iter().map(|(values, _cnt)| values[aggr_index].iter().next().unwrap());
                        output.push(Row::pack_slice(&[func.eval(iter, &RowArena::new())]));
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

/// Build the dataflow to compute and arrange multiple hierarchical aggregations
/// on monotonic inputs.
///
/// `prepend_keys` is true if the arrangement produced by this function needs to
/// be reused by other views.
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
    let mut packer = Row::default();
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

            (key, time, output)
        })
        .as_collection();
    partial
        .arrange_by_self()
        .reduce_abelian::<_, RowSpine<_, _, _, _>>("ReduceMonotonic", {
            let mut row_packer = Row::default();
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

/// Accumulates values for the various types of accumulable aggregations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum AccumInner {
    /// Accumulates boolean values.
    Bool {
        /// The number of `true` values observed.
        trues: isize,
        /// The number of `false` values observed.
        falses: isize,
    },
    /// Accumulates simple numeric values.
    SimpleNumber {
        /// The accumulation of all non-NULL values observed.
        accum: i128,
        /// The number of non-NULL values observed.
        non_nulls: isize,
    },
    /// Accumulates float values.
    Float {
        /// Accumulates non-special float values, mapped to a fixed presicion i128 domain to
        /// preserve associativity and commutativity
        accum: i128,
        /// Counts +inf
        pos_infs: isize,
        /// Counts -inf
        neg_infs: isize,
        /// Counts NaNs
        nans: isize,
        /// Counts non-NULL values
        non_nulls: isize,
    },
    /// Accumulates arbitrary precision decimals.
    Numeric {
        /// Accumulates non-special values
        accum: OrderedDecimal<NumericAgg>,
        /// Counts +inf
        pos_infs: isize,
        /// Counts -inf
        neg_infs: isize,
        /// Counts NaNs
        nans: isize,
        /// Counts non-NULL values
        non_nulls: isize,
    },
}

impl Semigroup for AccumInner {
    fn is_zero(&self) -> bool {
        match self {
            AccumInner::Bool { trues, falses } => trues.is_zero() && falses.is_zero(),
            AccumInner::SimpleNumber { accum, non_nulls } => accum.is_zero() && non_nulls.is_zero(),
            AccumInner::Float {
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
            AccumInner::Numeric {
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

    fn plus_equals(&mut self, other: &AccumInner) {
        match (&mut *self, other) {
            (
                AccumInner::Bool { trues, falses },
                AccumInner::Bool {
                    trues: other_trues,
                    falses: other_falses,
                },
            ) => {
                *trues += other_trues;
                *falses += other_falses;
            }
            (
                AccumInner::SimpleNumber { accum, non_nulls },
                AccumInner::SimpleNumber {
                    accum: other_accum,
                    non_nulls: other_non_nulls,
                },
            ) => {
                *accum += other_accum;
                *non_nulls += other_non_nulls;
            }
            (
                AccumInner::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls,
                },
                AccumInner::Float {
                    accum: other_accum,
                    pos_infs: other_pos_infs,
                    neg_infs: other_neg_infs,
                    nans: other_nans,
                    non_nulls: other_non_nulls,
                },
            ) => {
                *accum += other_accum;
                *pos_infs += other_pos_infs;
                *neg_infs += other_neg_infs;
                *nans += other_nans;
                *non_nulls += other_non_nulls;
            }
            (
                AccumInner::Numeric {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls,
                },
                AccumInner::Numeric {
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
                assert!(!cx_agg.status().rounded(), "AccumInner::Numeric overflow");
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
                "Accumulator::plus_equals called with non-matching variants: {:?} vs {:?}",
                l, r
            ),
        }
    }
}

impl Multiply<isize> for AccumInner {
    type Output = AccumInner;

    fn multiply(self, factor: &isize) -> AccumInner {
        let factor = *factor;
        match self {
            AccumInner::Bool { trues, falses } => AccumInner::Bool {
                trues: trues * factor,
                falses: falses * factor,
            },
            AccumInner::SimpleNumber { accum, non_nulls } => AccumInner::SimpleNumber {
                accum: accum * i128::cast_from(factor),
                non_nulls: non_nulls * factor,
            },
            AccumInner::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => AccumInner::Float {
                accum: accum * i128::cast_from(factor),
                pos_infs: pos_infs * factor,
                neg_infs: neg_infs * factor,
                nans: nans * factor,
                non_nulls: non_nulls * factor,
            },
            AccumInner::Numeric {
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
                assert!(
                    !cx.status().rounded(),
                    "AccumInner::Numeric multiply overflow"
                );
                AccumInner::Numeric {
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

/// Wraps an `AccumInner` with tracking of the total number of records observed,
/// i.e., including null records.
///
/// It is important that `Accum.is_zero()` returns `false` if at least one null
/// record has been observed. Differential dataflow will suppress output when
/// `Accum.is_zero()` returns `true`, but SQL requires that we produce an
/// explicit zero or null record as long as there is *some* input record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Accum {
    inner: AccumInner,
    total: isize,
}

impl Semigroup for Accum {
    fn is_zero(&self) -> bool {
        self.total.is_zero() && self.inner.is_zero()
    }

    fn plus_equals(&mut self, other: &Accum) {
        self.inner.plus_equals(&other.inner);
        self.total += other.total;
    }
}

impl Multiply<isize> for Accum {
    type Output = Accum;

    fn multiply(self, factor: &isize) -> Accum {
        Accum {
            inner: self.inner.multiply(factor),
            total: self.total * *factor,
        }
    }
}

/// Build the dataflow to compute and arrange multiple accumulable aggregations.
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

    let float_scale = f64::from(1 << 24);

    // Instantiate a default vector for diffs with the correct types at each
    // position.
    let zero_diffs: Vec<_> = full_aggrs
        .iter()
        .map(|f| {
            let inner = match f.func {
                AggregateFunc::Any | AggregateFunc::All => AccumInner::Bool {
                    trues: 0,
                    falses: 0,
                },
                AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => AccumInner::Float {
                    accum: 0,
                    pos_infs: 0,
                    neg_infs: 0,
                    nans: 0,
                    non_nulls: 0,
                },
                AggregateFunc::SumNumeric => AccumInner::Numeric {
                    accum: OrderedDecimal(NumericAgg::zero()),
                    pos_infs: 0,
                    neg_infs: 0,
                    nans: 0,
                    non_nulls: 0,
                },
                _ => AccumInner::SimpleNumber {
                    accum: 0,
                    non_nulls: 0,
                },
            };
            Accum { inner, total: 0 }
        })
        .collect();

    // Two aggregation-specific values for each aggregation.
    let datum_to_accumulator = move |datum: Datum, aggr: &AggregateFunc| {
        let inner = match aggr {
            AggregateFunc::Count => AccumInner::SimpleNumber {
                accum: 0, // unused for AggregateFunc::Count
                non_nulls: if datum.is_null() { 0 } else { 1 },
            },
            AggregateFunc::Any | AggregateFunc::All => match datum {
                Datum::True => AccumInner::Bool {
                    trues: 1,
                    falses: 0,
                },
                Datum::Null => AccumInner::Bool {
                    trues: 0,
                    falses: 0,
                },
                Datum::False => AccumInner::Bool {
                    trues: 0,
                    falses: 1,
                },
                x => panic!("Invalid argument to AggregateFunc::Any: {:?}", x),
            },
            AggregateFunc::Dummy => match datum {
                Datum::Dummy => AccumInner::SimpleNumber {
                    accum: 0,
                    non_nulls: 0,
                },
                x => panic!("Invalid argument to AggregateFunc::Dummy: {:?}", x),
            },
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => {
                let n = match datum {
                    Datum::Float32(n) => f64::from(*n),
                    Datum::Float64(n) => *n,
                    Datum::Null => 0f64,
                    x => panic!("Invalid argument to AggregateFunc::{:?}: {:?}", aggr, x),
                };

                let nans = n.is_nan() as isize;
                let pos_infs = (n == f64::INFINITY) as isize;
                let neg_infs = (n == f64::NEG_INFINITY) as isize;
                let non_nulls = (datum != Datum::Null) as isize;

                // Map the floating point value onto a fixed presicion domain
                // All special values should map to zero, since they are tracked separately
                let accum = if nans > 0 || pos_infs > 0 || neg_infs > 0 {
                    0
                } else {
                    (n * float_scale) as i128
                };

                AccumInner::Float {
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

                    AccumInner::Numeric {
                        accum: OrderedDecimal(accum),
                        pos_infs,
                        neg_infs,
                        nans,
                        non_nulls: 1,
                    }
                }
                Datum::Null => AccumInner::Numeric {
                    accum: OrderedDecimal(NumericAgg::zero()),
                    pos_infs: 0,
                    neg_infs: 0,
                    nans: 0,
                    non_nulls: 0,
                },
                x => panic!("Invalid argument to AggregateFunc::SumNumeric: {:?}", x),
            },
            _ => {
                // Other accumulations need to disentangle the accumulable
                // value from its NULL-ness, which is not quite as easily
                // accumulated.
                match datum {
                    Datum::Int16(i) => AccumInner::SimpleNumber {
                        accum: i128::from(i),
                        non_nulls: 1,
                    },
                    Datum::Int32(i) => AccumInner::SimpleNumber {
                        accum: i128::from(i),
                        non_nulls: 1,
                    },
                    Datum::Int64(i) => AccumInner::SimpleNumber {
                        accum: i128::from(i),
                        non_nulls: 1,
                    },
                    Datum::Null => AccumInner::SimpleNumber {
                        accum: 0,
                        non_nulls: 0,
                    },
                    x => panic!("Accumulating non-integer data: {:?}", x),
                }
            }
        };
        Accum { inner, total: 1 }
    };

    let mut to_aggregate = Vec::new();
    // First, collect all non-distinct aggregations in one pass.
    let easy_cases = collection.explode({
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
                diffs[*accumulable_index] = datum_to_accumulator(datum, &aggr.func);
            }
            Some((key, diffs))
        }
    });
    to_aggregate.push(easy_cases);

    // Next, collect all aggregations that require distinctness.
    for (accumulable_index, datum_index, aggr) in distinct_aggrs.into_iter() {
        let mut packer = Row::default();
        let collection = collection
            .map(move |(key, row)| {
                let value = row.iter().nth(datum_index).unwrap();
                packer.push(value);
                (key, packer.finish_and_reuse())
            })
            .distinct()
            .explode({
                let zero_diffs = zero_diffs.clone();
                move |(key, row)| {
                    let datum = row.iter().next().unwrap();
                    let mut diffs = zero_diffs.clone();
                    diffs[accumulable_index] = datum_to_accumulator(datum, &aggr.func);
                    Some((key, diffs))
                }
            });
        to_aggregate.push(collection);
    }
    let collection =
        differential_dataflow::collection::concatenate(&mut collection.scope(), to_aggregate);

    collection
        .arrange_by_self()
        .reduce_abelian::<_, RowSpine<_, _, _, _>>("ReduceAccumulable", {
            let mut row_packer = Row::default();
            move |key, input, output| {
                let accum = &input[0].1;
                // Pack the value with the key as the result.
                if prepend_key {
                    row_packer.extend(key.iter());
                }

                for (aggr, accum) in full_aggrs.iter().zip(accum) {
                    // This should perhaps be un-recoverable, as we risk panicking in the ReduceCollation
                    // operator, when this key is presented but matching aggregates are not found. We will
                    // suppress the output for inputs without net-positive records, which *should* avoid
                    // that panic.
                    soft_assert_or_log!(
                        accum.total != 0 || accum.inner.is_zero(),
                        "[customer-data] ReduceAccumulable observed net-zero records \
                        with non-zero accumulation: {:?}: {:?}",
                        aggr,
                        accum,
                    );

                    // The finished value depends on the aggregation function in a variety of ways.
                    // For all aggregates but count, if only null values were
                    // accumulated, then the output is null.
                    let value = if accum.total > 0
                        && accum.inner.is_zero()
                        && aggr.func != AggregateFunc::Count
                    {
                        Datum::Null
                    } else {
                        match (&aggr.func, &accum.inner) {
                            (AggregateFunc::Count, AccumInner::SimpleNumber { non_nulls, .. }) => {
                                Datum::Int64(i64::cast_from(*non_nulls))
                            }
                            (AggregateFunc::All, AccumInner::Bool { falses, trues }) => {
                                // If any false, else if all true, else must be no false and some nulls.
                                if *falses > 0 {
                                    Datum::False
                                } else if *trues == accum.total {
                                    Datum::True
                                } else {
                                    Datum::Null
                                }
                            }
                            (AggregateFunc::Any, AccumInner::Bool { falses, trues }) => {
                                // If any true, else if all false, else must be no true and some nulls.
                                if *trues > 0 {
                                    Datum::True
                                } else if *falses == accum.total {
                                    Datum::False
                                } else {
                                    Datum::Null
                                }
                            }
                            (AggregateFunc::Dummy, _) => Datum::Dummy,
                            // If any non-nulls, just report the aggregate.
                            (AggregateFunc::SumInt16, AccumInner::SimpleNumber { accum, .. })
                            | (AggregateFunc::SumInt32, AccumInner::SimpleNumber { accum, .. }) => {
                                Datum::Int64(*accum as i64)
                            }
                            (AggregateFunc::SumInt64, AccumInner::SimpleNumber { accum, .. }) => {
                                Datum::from(*accum)
                            }
                            (
                                AggregateFunc::SumFloat32,
                                AccumInner::Float {
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
                                    Datum::from(((*accum as f64) / float_scale) as f32)
                                }
                            }
                            (
                                AggregateFunc::SumFloat64,
                                AccumInner::Float {
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
                                    Datum::from((*accum as f64) / float_scale)
                                }
                            }
                            (
                                AggregateFunc::SumNumeric,
                                AccumInner::Numeric {
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
                                "Unexpected accumulation (aggr={:?}, accum={:?})",
                                aggr.func, accum
                            ),
                        }
                    };

                    row_packer.push(value);
                }
                output.push((row_packer.finish_and_reuse(), 1));
            }
        })
}

/// Transforms a vector containing indexes of needed columns into one containing
/// the "skips" an iterator over a Row would need to perform to see those values.
///
/// This function requires that all of the elements in `indexes` are strictly
/// increasing.
/// E.g. [3, 6, 10, 15] turns into [3, 3, 4, 5]
fn convert_indexes_to_skips(mut indexes: Vec<usize>) -> Vec<usize> {
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
        | AggregateFunc::StringAgg { .. } => ReductionType::Basic,
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

    use differential_dataflow::difference::Semigroup;
    use serde::{Deserialize, Serialize};

    use expr::AggregateFunc;
    use ore::soft_panic_or_log;
    use repr::{Datum, Row};

    /// A monoid containing a single-datum row.
    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
    pub enum ReductionMonoid {
        Min(Row),
        Max(Row),
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
                (lhs, rhs) => {
                    soft_panic_or_log!(
                        "Mismatched monoid variants in reduction! lhs: {:?} rhs: {:?}",
                        lhs,
                        rhs
                    );
                }
            }
        }

        fn is_zero(&self) -> bool {
            false
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
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz => Some(ReductionMonoid::Max(row)),
            AggregateFunc::MinNumeric
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz => Some(ReductionMonoid::Min(row)),
            AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumNumeric
            | AggregateFunc::Count
            | AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::Dummy
            | AggregateFunc::JsonbAgg { .. }
            | AggregateFunc::JsonbObjectAgg { .. }
            | AggregateFunc::ArrayConcat { .. }
            | AggregateFunc::ListConcat { .. }
            | AggregateFunc::StringAgg { .. } => None,
        }
    }
}
