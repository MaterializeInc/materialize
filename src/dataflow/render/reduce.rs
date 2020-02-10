// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use timely::dataflow::Scope;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::{Reduce, Threshold};
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::Collection;

use dataflow_types::Timestamp;
use expr::{AggregateExpr, AggregateFunc, EvalEnv, RelationExpr, ScalarExpr};
use repr::{Datum, Row, RowArena, RowPacker};

use super::context::Context;
use crate::render::context::Arrangement;

impl<G> Context<G, RelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Renders a `RelationExpr::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    pub fn render_reduce(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
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

            let keys_clone = group_key.clone();

            self.ensure_rendered(input, env, scope, worker_index);
            let input = self.collection(input).unwrap();

            // Distinct is a special case, as there are no aggregates to aggregate.
            // In this case, we use a special implementation that does not rely on
            // collating aggregates.
            let arrangement = if aggregates.is_empty() {
                input
                    .map({
                        let env = env.clone();
                        let group_key = group_key.clone();
                        move |row| {
                            let temp_storage = RowArena::new();
                            let datums = row.unpack();
                            (
                                Row::pack(
                                    group_key
                                        .iter()
                                        .map(|i| i.eval(&datums, &env, &temp_storage)),
                                ),
                                (),
                            )
                        }
                    })
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("DistinctBy", {
                        |key, _input, output| {
                            output.push((key.clone(), 1));
                        }
                    })
            } else if aggregates.len() == 1 {
                // If we have a single aggregate, we need not stage aggregations separately.
                build_aggregate_stage(input, group_key, &aggregates[0], env, true)
            } else {
                // We'll accumulate partial aggregates here, where each contains updates
                // of the form `(key, (index, value))`. This is eventually concatenated,
                // and fed into a final reduce to put the elements in order.
                let mut partials = Vec::with_capacity(aggregates.len());
                // Bound the complex dataflow in a region, for better interpretability.
                scope.region(|region| {
                    // Create an iterator over collections, where each is the application
                    // of one aggregation function whose results are annotated with its
                    // position in the final results. To be followed by a merge reduction.
                    for (index, aggr) in aggregates.iter().enumerate() {
                        // Collect the now-aggregated partial result, annotated with its position.
                        partials.push(
                            build_aggregate_stage(input.enter(region), group_key, aggr, env, false)
                                .as_collection(move |key, val| (key.clone(), (index, val.clone())))
                                .leave(),
                        );
                    }
                });

                // Our final action is to collect the partial results into one record.
                //
                // We concatenate the partial results and lay out the fields as indicated by their
                // recorded positions. All keys should contribute exactly one value for each of the
                // aggregates, which we check with assertions; this is true independent of transient
                // change and inconsistency in the inputs; if this is not the case there is a defect
                // in differential dataflow.
                differential_dataflow::collection::concatenate::<_, _, _, _>(scope, partials)
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceCollation", {
                    let aggregates_len = aggregates.len();
                    move |key, input, output| {
                        // The intent, unless things are terribly wrong, is that `input`
                        // contains, in order, the values to drop into `output`.
                        assert_eq!(input.len(), aggregates_len);
                        let mut result = RowPacker::new();
                        result.extend(key.iter());
                        for (index, ((pos, val), cnt)) in input.iter().enumerate() {
                            assert_eq!(*pos, index);
                            assert_eq!(*cnt, 1);
                            result.push(val.unpack().pop().unwrap());
                        }
                        output.push((result.finish(), 1));
                    }
                })
            };
            let index = (0..keys_clone.len()).collect::<Vec<_>>();
            self.set_local_columns(relation_expr, &index[..], arrangement);
        }
    }
}

/// Reduce and arrange `input` by `group_key` and `aggr`.
///
/// This method accommodates in-place aggregations like sums, hierarchical aggregations like min and max,
/// and other aggregations that may be neither of those things. It also applies distinctness if required.
fn build_aggregate_stage<G>(
    input: Collection<G, Row>,
    group_key: &[ScalarExpr],
    aggr: &AggregateExpr,
    env: &EvalEnv,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let AggregateExpr {
        func,
        expr,
        distinct,
    } = aggr.clone();

    // The `partial` collection contains `(key, val)` pairs.
    let mut partial = input.map({
        let env = env.clone();
        let group_key = group_key.to_vec();
        move |row| {
            let temp_storage = RowArena::new();
            let datums = row.unpack();
            (
                Row::pack(
                    group_key
                        .iter()
                        .map(|i| i.eval(&datums, &env, &temp_storage)),
                ),
                Row::pack(Some(expr.eval(&datums, &env, &temp_storage))),
            )
        }
    });

    // If `distinct` is set, we restrict ourselves to the distinct `(key, val)`.
    if distinct {
        partial = partial.distinct();
    }

    // Our strategy will depend on whether the function is accumulable in-place,
    // or can be subjected to hierarchical aggregation. At the moment all functions
    // are one of the two, but this should work even with methods that are neither.
    let (accumulable, hierarchical) = accumulable_hierarchical(&func);

    if accumulable {
        build_accumulable(partial, func, prepend_key)
    } else {
        // If hierarchical, we can repeatedly digest the groups, to minimize the incremental
        // update costs on relatively small updates.
        if hierarchical {
            partial = build_hierarchical(partial, &func, env.clone())
        }

        // Perform a final aggregation, on potentially hierarchically reduced data.
        // The same code should work on data that can not be hierarchically reduced.
        partial.reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceInaccumulable", {
            let env = env.clone();
            move |key, source, target| {
                // We respect the multiplicity here (unlike in hierarchical aggregation)
                // because we don't know that the aggregation method is not sensitive
                // to the number of records.
                let iter = source.iter().flat_map(|(v, w)| {
                    std::iter::repeat(v.iter().next().unwrap()).take(*w as usize)
                });
                let mut packer = RowPacker::new();
                if prepend_key {
                    packer.extend(key.iter());
                }
                packer.push(func.eval(iter, &env, &RowArena::new()));
                target.push((packer.finish(), 1));
            }
        })
    }
}

/// Builds the dataflow for a reduction that can be performed in-place.
///
/// The incoming values are moved to the update's "difference" field, at which point
/// they can be accumulated in place. The `count` operator promotes the accumulated
/// values to data, at which point a final map applies operator-specific logic to
/// yield the final aggregate.
fn build_accumulable<G>(
    collection: Collection<G, (Row, Row)>,
    aggr: AggregateFunc,
    prepend_key: bool,
) -> Arrangement<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    use timely::dataflow::operators::map::Map;

    let float_scale = f64::from(1 << 24);

    collection
        .inner
        .map(|(d, t, r)| (d, t, r as i128))
        .as_collection()
        .explode({
            let aggr = aggr.clone();
            move |(key, row)| {
                let datum = row.unpack()[0];
                let (aggs, nonnulls) = match aggr {
                    AggregateFunc::CountAll => {
                        // Nothing beyond the accumulated count is needed.
                        (0i128, 0i128)
                    }
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
                };
                Some((
                    (key, ()),
                    DiffPair::new(1i128, DiffPair::new(aggs, nonnulls)),
                ))
            }
        })
        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
            "ReduceAccumulable",
            move |key, input, output| {
                let accum = &input[0].1;
                let tot = accum.element1;

                // For most aggregations, the first aggregate is the "data" and the second is the number
                // of non-null elements (so that we can determine if we should produce 0 or a Null).
                // For Any and All, the two aggregates are the numbers of true and false records, resp.
                let agg1 = accum.element2.element1;
                let agg2 = accum.element2.element2;

                // The finished value depends on the aggregation function in a variety of ways.
                let value = match (&aggr, agg2) {
                    (AggregateFunc::Count, _) => Datum::Int64(agg2 as i64),
                    (AggregateFunc::CountAll, _) => Datum::Int64(tot as i64),
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
                    // Below this point, anything with only nulls should be null.
                    (_, 0) => Datum::Null,
                    // If any non-nulls, just report the aggregate.
                    (AggregateFunc::SumInt32, _) => Datum::Int32(agg1 as i32),
                    (AggregateFunc::SumInt64, _) => Datum::Int64(agg1 as i64),
                    (AggregateFunc::SumFloat32, _) => {
                        Datum::Float32((((agg1 as f64) / float_scale) as f32).into())
                    }
                    (AggregateFunc::SumFloat64, _) => {
                        Datum::Float64(((agg1 as f64) / float_scale).into())
                    }
                    (AggregateFunc::SumDecimal, _) => Datum::from(agg1),
                    (AggregateFunc::SumNull, _) => Datum::Null,
                    x => panic!("Unexpected accumulable aggregation: {:?}", x),
                };
                // Pack the value with the key as the result.
                let mut packer = RowPacker::new();
                if prepend_key {
                    packer.extend(key.iter());
                }
                packer.push(value);
                output.push((packer.finish(), 1));
            },
        )
}

/// Builds a dataflow for hierarchical aggregation.
///
/// The dataflow repeatedly applies stages of reductions on progressively more coarse
/// groupings, each of which refines the actual key grouping.
fn build_hierarchical<G>(
    collection: Collection<G, (Row, Row)>,
    aggr: &AggregateFunc,
    env: EvalEnv,
) -> Collection<G, (Row, Row)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Repeatedly apply hierarchical reduction with a progressively coarser key.
    let mut stage = collection.map({ move |(key, row)| ((key, row.hashed()), row) });
    for log_modulus in [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4u64].iter() {
        stage = build_hierarchical_stage(stage, aggr.clone(), env.clone(), 1u64 << log_modulus);
    }

    // Discard the hash from the key and return to the format of the input data.
    stage.map(|((key, _hash), val)| (key, val))
}

fn build_hierarchical_stage<G>(
    collection: Collection<G, ((Row, u64), Row)>,
    aggr: AggregateFunc,
    env: EvalEnv,
    modulus: u64,
) -> Collection<G, ((Row, u64), Row)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    collection
        .map(move |((key, hash), row)| ((key, hash % modulus), row))
        .reduce_named("ReduceHierarchical", {
            move |_key, source, target| {
                // We ignore the count here under the belief that it cannot affect
                // hierarchical aggregations; should that belief be incorrect, we
                // should certainly revise this implementation.
                let iter = source.iter().map(|(val, _cnt)| val.iter().next().unwrap());
                target.push((Row::pack(Some(aggr.eval(iter, &env, &RowArena::new()))), 1));
            }
        })
}

/// Determines whether a function can be accumulated in an update's "difference" field,
/// and whether it can be subjected to recursive (hierarchical) aggregation.
///
/// At present, there is a dichotomy, but this is set up to complain if new aggregations
/// are added that perhaps violate these requirement. For example, a "median" aggregation
/// could be neither accumulable nor hierarchical.
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
/// significant input data).
fn accumulable_hierarchical(func: &AggregateFunc) -> (bool, bool) {
    match func {
        AggregateFunc::SumInt32
        | AggregateFunc::SumInt64
        | AggregateFunc::SumFloat32
        | AggregateFunc::SumFloat64
        | AggregateFunc::SumDecimal
        | AggregateFunc::SumNull
        | AggregateFunc::Count
        | AggregateFunc::CountAll
        | AggregateFunc::Any
        | AggregateFunc::All => (true, false),
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
        | AggregateFunc::MaxNull
        | AggregateFunc::MinInt32
        | AggregateFunc::MinInt64
        | AggregateFunc::MinFloat32
        | AggregateFunc::MinFloat64
        | AggregateFunc::MinDecimal
        | AggregateFunc::MinBool
        | AggregateFunc::MinString
        | AggregateFunc::MinDate
        | AggregateFunc::MinTimestamp
        | AggregateFunc::MinTimestampTz
        | AggregateFunc::MinNull => (false, true),
    }
}
