// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::DiffVector;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::{Consolidate, Reduce, Threshold};
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
impl<G> Context<G, RelationExpr, Row, repr::Timestamp>
where
    G: Scope<Timestamp = repr::Timestamp>,
{
    /// Renders a `RelationExpr::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    pub fn render_reduce(&mut self, relation_expr: &RelationExpr, scope: &mut G) {
        if let RelationExpr::Reduce {
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
                .flat_map_ref(
                    input,
                    |_expr| None,
                    move |row| {
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
            //   2. If there is a single aggregation function, we can build the dataflow
            //      for that aggregation function.
            //   3. If all aggregation functions are accumulable (sums) we can build
            //      a dataflow for that concludes with their sums arranged.
            //   4. If there are multiple aggregation functions at least one of which
            //      is non-accumulable, we'll need to build dataflows for each group
            //      and then meld the results together in a final finishing reduce.

            // Distinct is a special case, as there are no aggregates to aggregate.
            // In this case, we use a special implementation that does not rely on
            // collating aggregates.
            if aggregates.is_empty() {
                let (oks, errs) = (
                    ok_input.reduce_abelian::<_, OrdValSpine<_, _, _, _>>("DistinctBy", {
                        |key, _input, output| {
                            output.push((key.clone(), 1));
                        }
                    }),
                    err_input,
                );
                let index = (0..group_key.len()).collect::<Vec<_>>();
                self.set_local_columns(relation_expr, &index[..], (oks, errs.arrange()));
            } else {
                // Collect aggregates with their indexes, so they can be sliced and diced.
                let mut accumulable = Vec::new();
                let mut remaining = Vec::new();
                for index in 0..aggregates.len() {
                    if accumulable_hierarchical(&aggregates[index].func).0 {
                        accumulable.push((index, aggregates[index].clone()));
                    } else {
                        remaining.push((index, aggregates[index].clone()));
                    }
                }

                let arrangement = if remaining.is_empty() {
                    // If we have only accumulable aggregations, they can be arranged and returned.
                    build_accumulables(ok_input, accumulable, true)
                } else if remaining.len() == 1 && accumulable.is_empty() {
                    // If we have a single non-accumulable aggregation, it can be arranged and returned.
                    build_aggregate_stage(
                        ok_input,
                        0,
                        &aggregates[0],
                        true,
                        *monotonic,
                        *expected_group_size,
                    )
                } else {
                    // Otherwise we need to stitch things together.
                    let mut to_collect = Vec::new();
                    if !accumulable.is_empty() {
                        let accumulables_collection =
                            build_accumulables(ok_input.clone(), accumulable, false)
                                .as_collection(|key, val| (key.clone(), (None, val.clone())));
                        to_collect.push(accumulables_collection);
                    }
                    for (index, aggr) in remaining {
                        let collection = build_aggregate_stage(
                            ok_input.clone(),
                            index,
                            &aggr,
                            false,
                            *monotonic,
                            *expected_group_size,
                        );
                        to_collect.push(collection.as_collection(move |key, val| {
                            (key.clone(), (Some(index), val.clone()))
                        }));
                    }
                    let is_accumulable = aggregates
                        .iter()
                        .map(|a| accumulable_hierarchical(&a.func).0)
                        .collect::<Vec<_>>();
                    differential_dataflow::collection::concatenate(scope, to_collect)
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceCollation", {
                            let mut row_packer = RowPacker::new();
                            move |key, mut input, output| {
                                // The inputs are pairs of an optional index and row to decode.

                                // There can be at most one `None` index, and it indicates the accumulable aggregates.
                                let new_row = Row::new(Vec::new());
                                let mut accumulable = if (input[0].0).0 == None {
                                    // This input corresponds to our densely packed accumulable aggregates.
                                    let iter = (input[0].0).1.iter();
                                    // Make sure we never try to read from this input again.
                                    input = &input[1..];
                                    iter
                                } else {
                                    new_row.iter()
                                };

                                row_packer.extend(key.iter());
                                for is_accum in is_accumulable.iter() {
                                    if *is_accum {
                                        row_packer.push(accumulable.next().unwrap());
                                    } else {
                                        // Since this is not an accumulable aggregate, we need to grab
                                        // the next result from other reduction dataflows and put them
                                        // in our output.
                                        let elem = input[0].0;
                                        let row = &elem.1;
                                        let datum = row.unpack_first();
                                        row_packer.push(datum);
                                        input = &input[1..];
                                    }
                                }
                                output.push((row_packer.finish_and_reuse(), 1));
                            }
                        })
                };
                let index = (0..group_key.len()).collect::<Vec<_>>();
                self.set_local_columns(
                    relation_expr,
                    &index[..],
                    (arrangement, err_input.arrange()),
                );
            }
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
    expected_group_size: Option<usize>,
) -> Arrangement<G, Row>
where
    G: Scope<Timestamp = repr::Timestamp>,
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
    let (_accumulable, hierarchical) = accumulable_hierarchical(&func);

    // If hierarchical, we can repeatedly digest the groups, to minimize the incremental
    // update costs on relatively small updates.
    if hierarchical {
        if monotonic && is_min_or_max(&func) {
            // At this point, we assert that inputs are never retracted.
            // We could move the datum to the `diff` component, wrapped
            // in a min/max monoid wrapper. This would permit in-place
            // compaction, and a substantially smaller memory footprint.
            // The records in the stream are pairs `(key, row)` where
            // `row` contains a single value that can be minimized or
            // maximized over.

            use differential_dataflow::operators::reduce::Count;
            use timely::dataflow::operators::map::Map;

            // We need two different code paths for min and max, as the
            // monoid wrapper type encodes the logic. In each case, we
            // wrap the value with the monoid wrapper, which will allow
            // in-place accumulation using either "min" or "max".
            // The `count` operator promotes the accumulated value back
            // to data, and we pass along the reduced form to the final
            // operator.
            // TODO(frank): the `count` operator very nearly produces
            // the arrangement we want as output, minus some formatting
            // with prefixed keys and such. But we could fuse them and
            // save an operator.
            if is_min(&func) {
                partial = partial
                    .consolidate()
                    .inner
                    .map(|((key, value), time, diff)| {
                        assert!(diff > 0);
                        (key, time, monoids::MinMonoid { value })
                    })
                    .as_collection()
                    .count()
                    .map(|(key, min)| (key, min.value));
            } else if is_max(&func) {
                partial = partial
                    .consolidate()
                    .inner
                    .map(|((key, value), time, diff)| {
                        assert!(diff > 0);
                        (key, time, monoids::MaxMonoid { value })
                    })
                    .as_collection()
                    .count()
                    .map(|(key, max)| (key, max.value));
            }
        } else {
            partial = build_hierarchical(partial, &func, expected_group_size)
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
fn build_accumulables<G>(
    collection: Collection<G, (Row, Row)>,
    aggrs: Vec<(usize, AggregateExpr)>,
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

    use differential_dataflow::operators::consolidate::ConsolidateStream;
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
    let diffs_len = aggrs.len() * 3;
    // First, collect all non-distinct aggregations in one pass.
    let easy_cases = collection
        .inner
        .map(|(d, t, r)| (d, t, r as i128))
        .as_collection()
        .explode({
            let aggrs = aggrs.clone();
            move |(key, row)| {
                let mut diffs = vec![0i128; diffs_len];
                // Try to unpack only the datums we need. Unfortunately, since we
                // can't random access into a Row, we have to iterate through one by one.
                // TODO: Even though we don't have random access, we could still avoid unpacking
                // everything that we don't care about, and it might be worth it to extend the
                // Row API to do that.
                let mut row_iter = row.iter().enumerate();
                for (index, (datum_index, aggr)) in aggrs.iter().enumerate() {
                    let mut datum = row_iter.next().unwrap();
                    while datum_index != &datum.0 {
                        datum = row_iter.next().unwrap();
                    }
                    let datum = datum.1;
                    if accumulable_hierarchical(&aggr.func).0 {
                        if !aggr.distinct {
                            let (agg1, agg2) = datum_aggr_values(datum, &aggr.func);
                            diffs[3 * index] = 1i128;
                            diffs[3 * index + 1] = agg1;
                            diffs[3 * index + 2] = agg2;
                        }
                    }
                }
                Some(((key, ()), DiffVector::new(diffs)))
            }
        });
    to_aggregate.push(easy_cases);

    // Next, collect all aggregations that require distinctness.
    for (idx, (datum_index, aggr)) in aggrs.iter().cloned().enumerate() {
        if accumulable_hierarchical(&aggr.func).0 && aggr.distinct {
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
                        diffs[3 * idx] = 1i128;
                        diffs[3 * idx + 1] = agg1;
                        diffs[3 * idx + 2] = agg2;
                        Some(((key, ()), DiffVector::new(diffs)))
                    }
                });
            to_aggregate.push(collection);
        }
    }
    let collection =
        differential_dataflow::collection::concatenate(&mut collection.scope(), to_aggregate);

    collection
        .consolidate_stream()
        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
            "ReduceAccumulable", {
            let mut row_packer = RowPacker::new();
            move |key, input, output| {
                let accum = &input[0].1;
                // Pack the value with the key as the result.
                if prepend_key {
                    row_packer.extend(key.iter());
                }

                for (index, (_, aggr)) in aggrs.iter().enumerate() {
                    // For most aggregations, the first aggregate is the "data" and the second is the number
                    // of non-null elements (so that we can determine if we should produce 0 or a Null).
                    // For Any and All, the two aggregates are the numbers of true and false records, resp.
                    // let agg1 = accum.element2.element1;
                    // let agg2 = accum.element2.element2;
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

                    row_packer.push(value);
                }
                output.push((row_packer.finish_and_reuse(), 1));
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
    expected_group_size: Option<usize>,
) -> Collection<G, (Row, Row)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let mut shifts = vec![];
    let mut current = 4u64;

    // We'll plan for an expected 4B records / key in the absense of hints.
    // Note that here we will render what is essentially a 16-ary heap. At each reduce "layer",
    // the reduce operator will take up to 16 inputs, and produce one output. We use the `expected_group_size` hint
    // to figure out how deep we need to make this heap, but the renderer currently locks in the choice of arity.
    // Making the heap wider (higher-arity) reduces the total number of layers we need, which shrinks the
    // memory usage. However, that increases the worst and average case latencies to update results given new inputs.
    // TODO(rkhaitan): move this decision making logic (choosing the overall depth and width of the reduction tree) to
    // the optimizer.
    let limit = expected_group_size.unwrap_or(4_000_000_000);

    while (1 << current) < limit {
        shifts.push(current);
        current += 4;
    }

    shifts.reverse();

    // Repeatedly apply hierarchical reduction with a progressively coarser key.
    let mut stage = collection.map(move |(key, row)| ((key, row.hashed()), row));
    for log_modulus in shifts.iter() {
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
    let input = collection.map(move |((key, hash), row)| ((key, hash % modulus), row));

    let negated_output = input
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

                    // We only want to arrange the parts of the input that are not part of the output.
                    // More specifically, we want to arrange it so that `input.concat(&output.negate())`
                    // gives us the intended value of this aggregate function.
                    // Thankfully, we don't have to do a lot to manage that because we assume that
                    // the output of this aggregation function will be one of the inputs, and we can
                    // let Differential correctly handle compacting away insertions and deletions to the
                    // same key.

                    target.push((row_packer.pack(Some(aggr.eval(iter, &RowArena::new()))), -1));
                    target.extend(source.iter().map(|(val, cnt)| ((*val).clone(), *cnt)));
                }
            }
        });

    negated_output.negate().concat(&input).consolidate()
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
    is_min(func) || is_max(func)
}

/// Is the aggregate function a "min" variant.
fn is_min(func: &AggregateFunc) -> bool {
    matches!(
        func,
        AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinDecimal
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
    )
}

/// Is the aggregate function is a "max" variant.
fn is_max(func: &AggregateFunc) -> bool {
    matches!(
        func,
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
    )
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

    use repr::{Datum, Row};
    use serde::{Deserialize, Serialize};

    /// A monoid containing a single-datum row, that is updated by SQL's `min`.
    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
    pub struct MinMonoid {
        pub value: Row,
    }

    use differential_dataflow::difference::Semigroup;
    use std::ops::AddAssign;

    impl<'a> AddAssign<&'a Self> for MinMonoid {
        fn add_assign(&mut self, rhs: &'a Self) {
            let swap = {
                let lhs_val = self.value.unpack_first();
                let rhs_val = rhs.value.unpack_first();
                // Datum::Null is the identity, not a small element.
                match (lhs_val, rhs_val) {
                    (_, Datum::Null) => false,
                    (Datum::Null, _) => true,
                    (lhs, rhs) => rhs < lhs,
                }
            };
            if swap {
                self.value.clone_from(&rhs.value);
            }
        }
    }

    impl Semigroup for MinMonoid {
        fn is_zero(&self) -> bool {
            false
        }
    }

    /// A monoid containing a single-datum row, that is updated by SQL's `max`.
    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
    pub struct MaxMonoid {
        pub value: Row,
    }

    impl<'a> AddAssign<&'a Self> for MaxMonoid {
        fn add_assign(&mut self, rhs: &'a Self) {
            let swap = {
                let lhs_val = self.value.unpack_first();
                let rhs_val = rhs.value.unpack_first();
                // Datum::Null is the identity, not a large element.
                match (lhs_val, rhs_val) {
                    (_, Datum::Null) => false,
                    (Datum::Null, _) => true,
                    (lhs, rhs) => rhs > lhs,
                }
            };
            if swap {
                self.value.clone_from(&rhs.value);
            }
        }
    }

    impl Semigroup for MaxMonoid {
        fn is_zero(&self) -> bool {
            false
        }
    }
}
