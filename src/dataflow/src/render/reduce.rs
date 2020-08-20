// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::{Reduce, Threshold};
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::Collection;
use timely::dataflow::Scope;

use dataflow_types::DataflowError;
use expr::{AggregateExpr, AggregateFunc, RelationExpr};
use ore::vec::repurpose_allocation;
use repr::{Datum, Row, RowArena, RowPacker};

use super::context::Context;
use crate::render::context::Arrangement;

// The implementation requires integer timestamps to be able to delay feedback for monotonic inputs.
impl<G> Context<G, RelationExpr, Row, dataflow_types::Timestamp>
where
    G: Scope<Timestamp = dataflow_types::Timestamp>,
{
    /// Renders a `RelationExpr::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    pub fn render_reduce(&mut self, relation_expr: &RelationExpr, scope: &mut G) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
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

            let relation_expr_clone = relation_expr.clone();

            // Our first step is to extract `(key, vals)` from `input`.
            // We do this carefully, attempting to avoid unneccesary allocations
            // that would result from cloning rows in input arrangements.
            let group_key_clone = group_key.clone();
            let aggregates_clone = aggregates.clone();

            // Tracks the required number of columns to extract.
            let mut columns_needed = 0;
            for key in group_key.iter() {
                for column in key.support() {
                    columns_needed = std::cmp::max(columns_needed, column + 1);
                }
            }
            for aggr in aggregates.iter() {
                for column in aggr.expr.support() {
                    columns_needed = std::cmp::max(columns_needed, column + 1);
                }
            }

            let mut row_packer = RowPacker::new();
            let mut datums = vec![];
            let (key_val_input, mut err_input): (
                Collection<_, Result<(Row, Row), DataflowError>, _>,
                _,
            ) = self
                .flat_map_ref(input, move |row| {
                    let temp_storage = RowArena::new();
                    let mut results = Vec::new();
                    // First, evaluate the key selector expressions.
                    // If any error we produce their errors as output and note
                    // the fact that the key was not correctly produced.
                    let mut datums_local = std::mem::take(&mut datums);
                    datums_local.extend(row.iter().take(columns_needed));
                    for expr in group_key_clone.iter() {
                        match expr.eval(&datums_local, &temp_storage) {
                            Ok(val) => row_packer.push(val),
                            Err(e) => {
                                results.push(Err(e.into()));
                            }
                        }
                    }

                    // Second, evaluate the value selector.
                    // If any error occurs we produce both the error as output,
                    // but also a `Datum::Null` value to avoid causing the later
                    // "ReduceCollation" operator to panic due to absent aggregates.
                    if results.is_empty() {
                        let key = row_packer.finish_and_reuse();
                        for aggr in aggregates_clone.iter() {
                            match aggr.expr.eval(&datums_local, &temp_storage) {
                                Ok(val) => {
                                    row_packer.push(val);
                                }
                                Err(e) => {
                                    row_packer.push(Datum::Null);
                                    results.push(Err(e.into()));
                                }
                            }
                        }
                        let row = row_packer.finish_and_reuse();
                        results.push(Ok((key, row)));
                    }
                    datums = repurpose_allocation(datums_local);
                    // Return accumulated results.
                    results
                })
                .unwrap();

            // Demux out the potential errors from key and value selector evaluation.
            use timely::dataflow::operators::ok_err::OkErr;
            let (ok, err) = key_val_input.inner.ok_err(|(x, t, d)| match x {
                Ok(x) => Ok((x, t, d)),
                Err(x) => Err((x, t, d)),
            });

            let ok_input = ok.as_collection();
            err_input = err.as_collection().concat(&err_input);

            // Distinct is a special case, as there are no aggregates to aggregate.
            // In this case, we use a special implementation that does not rely on
            // collating aggregates.
            let (oks, errs) = if aggregates.is_empty() {
                (
                    ok_input.reduce_abelian::<_, OrdValSpine<_, _, _, _>>("DistinctBy", {
                        |key, _input, output| {
                            output.push((key.clone(), 1));
                        }
                    }),
                    err_input,
                )
            } else if aggregates.len() == 1 {
                // If we have a single aggregate, we need not stage aggregations separately.
                (
                    build_aggregate_stage(ok_input, 0, &aggregates[0], true, *monotonic),
                    err_input,
                )
            } else {
                // We'll accumulate partial aggregates here, where each contains updates
                // of the form `(key, (index, value))`. This is eventually concatenated,
                // and fed into a final reduce to put the elements in order.
                let mut ok_partials = Vec::with_capacity(aggregates.len());
                // Bound the complex dataflow in a region, for better interpretability.
                scope.region(|region| {
                    // Create an iterator over collections, where each is the application
                    // of one aggregation function whose results are annotated with its
                    // position in the final results. To be followed by a merge reduction.
                    for (index, aggr) in aggregates.iter().enumerate() {
                        // Collect the now-aggregated partial result, annotated with its position.
                        let ok_partial = build_aggregate_stage(
                            ok_input.enter(region),
                            index,
                            aggr,
                            false,
                            *monotonic,
                        );
                        ok_partials.push(
                            ok_partial
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
                let oks = differential_dataflow::collection::concatenate(scope, ok_partials)
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceCollation", {
                    let aggregates_clone = aggregates.clone();
                    let aggregates_len = aggregates.len();
                    let mut row_packer = RowPacker::new();
                    move |key, input, output| {
                        // The intent, unless things are terribly wrong, is that `input`
                        // contains, in order, the values to drop into `output`. If this
                        // is not the case, we should express our specific discontent.
                        if input.len() != aggregates_len || input.iter().enumerate().any(|(i,((p,_),_))| &i != p) {
                            // TODO(frank): Arguably, the absence of one aggregate is evidence
                            // that the key doesn't exist (the others could be phantoms due to
                            // negative input records); we could just suppress the output in that
                            // case, rather than panic, though we surely want to see what is up.
                            // XXX: This panic reports user-supplied data!
                            panic!(
                                "[customer-data] ReduceCollation found unexpected indexes:\n\tExpected:\t{:?}\n\tFound:\t{:?}\n\tFor:\t{:?}\n\tKey:{:?}\nRelationExpr:\n{}",
                                (0..aggregates_len).collect::<Vec<_>>(),
                                input.iter().map(|((p,_),_)| p).collect::<Vec<_>>(),
                                aggregates_clone,
                                key,
                                relation_expr_clone.pretty(),
                            );
                        }
                        row_packer.extend(key.iter());
                        for ((_pos, val), cnt) in input.iter() {
                            assert_eq!(*cnt, 1);
                            row_packer.push(val.unpack_first());
                        }
                        output.push((row_packer.finish_and_reuse(), 1));
                    }
                });
                (oks, err_input)
            };
            let index = (0..group_key.len()).collect::<Vec<_>>();
            self.set_local_columns(relation_expr, &index[..], (oks, errs.arrange()));
        }
    }
}

/// Reduce and arrange `input` by `group_key` and `aggr`.
///
/// This method accommodates in-place aggregations like sums, hierarchical aggregations like min and max,
/// and other aggregations that may be neither of those things. It also applies distinctness if required.
fn build_aggregate_stage<G>(
    ok_input: Collection<G, (Row, Row)>,
    index: usize,
    aggr: &AggregateExpr,
    prepend_key: bool,
    monotonic: bool,
) -> Arrangement<G, Row>
where
    G: Scope<Timestamp = dataflow_types::Timestamp>,
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

    // Our strategy will depend on whether the function is accumulable in-place,
    // or can be subjected to hierarchical aggregation. At the moment all functions
    // are one of the two, but this should work even with methods that are neither.
    let (accumulable, hierarchical) = accumulable_hierarchical(&func);

    let ok_out = if accumulable {
        build_accumulable(partial, func, prepend_key)
    } else {
        // If hierarchical, we can repeatedly digest the groups, to minimize the incremental
        // update costs on relatively small updates.
        if hierarchical {
            if monotonic && is_min_or_max(&func) {
                use differential_dataflow::operators::iterate::Variable;
                let delay = std::time::Duration::from_nanos(10_000_000_000);
                let retractions = Variable::new(&mut partial.scope(), delay.as_millis() as u64);
                let thinned = partial.concat(&retractions.negate());
                let result = build_hierarchical(thinned, &func);
                retractions.set(&partial.concat(&result.negate()));
                partial = result
            } else {
                partial = build_hierarchical(partial, &func)
            }
        }

        // Perform a final aggregation, on potentially hierarchically reduced data.
        // The same code should work on data that can not be hierarchically reduced.
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
    };

    ok_out
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
    use differential_dataflow::operators::consolidate::ConsolidateStream;
    use timely::dataflow::operators::map::Map;

    let float_scale = f64::from(1 << 24);

    collection
        .inner
        .map(|(d, t, r)| (d, t, r as i128))
        .as_collection()
        .explode({
            let aggr = aggr.clone();
            move |(key, row)| {
                let datum = row.unpack_first();
                let (aggs, nonnulls) = match aggr {
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
                    }
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
        .consolidate_stream()
        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
            "ReduceAccumulable", {
            let mut row_packer = RowPacker::new();
            move |key, input, output| {
                let accum = &input[0].1;
                let tot = accum.element1;

                // For most aggregations, the first aggregate is the "data" and the second is the number
                // of non-null elements (so that we can determine if we should produce 0 or a Null).
                // For Any and All, the two aggregates are the numbers of true and false records, resp.
                let agg1 = accum.element2.element1;
                let agg2 = accum.element2.element2;

                if tot == 0 && (agg1 != 0 || agg2 != 0) {
                    // This should perhaps be un-recoverable, as we risk panicking in the ReduceCollation
                    // operator, when this key is presented but matching aggregates are not found. We will
                    // suppress the output for inputs without net-positive records, which *should* avoid
                    // that panic.
                    log::error!("[customer-data] ReduceAccumulable observed net-zero records with non-zero accumulation: {:?}: {:?}, {:?}", aggr, agg1, agg2);
                }

                // The finished value depends on the aggregation function in a variety of ways.
                let value = match (&aggr, agg2) {
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
                    (AggregateFunc::SumInt64, _) => Datum::Int64(agg1 as i64),
                    (AggregateFunc::SumFloat32, _) => {
                        Datum::Float32((((agg1 as f64) / float_scale) as f32).into())
                    }
                    (AggregateFunc::SumFloat64, _) => {
                        Datum::Float64(((agg1 as f64) / float_scale).into())
                    }
                    (AggregateFunc::SumDecimal, _) => Datum::from(agg1),
                    x => panic!("Unexpected accumulable aggregation: {:?}", x),
                };

                // If net zero records, we probably shouldn't be here (negative inputs)
                // but in any case we should suppress the output to attempt to avoid a
                // panic in ReduceCollation.
                if tot != 0 {
                    // Pack the value with the key as the result.
                    if prepend_key {
                        row_packer.extend(key.iter());
                    }
                    row_packer.push(value);
                    output.push((row_packer.finish_and_reuse(), 1));
                }
            }},
        )
}

/// Builds a dataflow for hierarchical aggregation.
///
/// The dataflow repeatedly applies stages of reductions on progressively more coarse
/// groupings, each of which refines the actual key grouping.
fn build_hierarchical<G>(
    collection: Collection<G, (Row, Row)>,
    aggr: &AggregateFunc,
) -> Collection<G, (Row, Row)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Repeatedly apply hierarchical reduction with a progressively coarser key.
    let mut stage = collection.map(move |(key, row)| ((key, row.hashed()), row));
    for log_modulus in [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4u64].iter() {
        stage = build_hierarchical_stage(stage, aggr.clone(), 1u64 << log_modulus);
    }

    // Discard the hash from the key and return to the format of the input data.
    stage.map(|((key, _hash), val)| (key, val))
}

fn build_hierarchical_stage<G>(
    collection: Collection<G, ((Row, u64), Row)>,
    aggr: AggregateFunc,
    modulus: u64,
) -> Collection<G, ((Row, u64), Row)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    collection
        .map(move |((key, hash), row)| ((key, hash % modulus), row))
        .reduce_named("ReduceHierarchical", {
            let mut row_packer = repr::RowPacker::new();
            move |key, source, target| {
                // Should negative accumulations reach us, we should loudly complain.
                if source.iter().any(|(_val, cnt)| cnt <= &0) {
                    for (val, cnt) in source.iter() {
                        if cnt <= &0 {
                            // XXX: This reports user data, which we perhaps should not do!
                            log::error!("[customer-data] Non-positive accumulation in ReduceHierarchical: key: {:?}\tvalue: {:?}\tcount: {:?}", key, val, cnt);
                        }
                    }
                } else {
                    // We ignore the count here under the belief that it cannot affect
                    // hierarchical aggregations; should that belief be incorrect, we
                    // should certainly revise this implementation.
                    let iter = source.iter().map(|(val, _cnt)| val.iter().next().unwrap());
                    target.push((row_packer.pack(Some(aggr.eval(iter, &RowArena::new()))), 1));
                }
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
        | AggregateFunc::Count
        | AggregateFunc::Any
        | AggregateFunc::All
        | AggregateFunc::Dummy => (true, false),
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
        | AggregateFunc::MinTimestampTz => (false, true),
        AggregateFunc::JsonbAgg => (false, false),
    }
}

/// True if the function is min or max.
fn is_min_or_max(func: &AggregateFunc) -> bool {
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
        | AggregateFunc::MinTimestampTz => true,
        _ => false,
    }
}
