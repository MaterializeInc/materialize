// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{generic::Operator, Concat, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use dataflow_types::{DataflowError, LinearOperator};
use expr::{EvalError, MirScalarExpr};
use log::error;
use repr::{Datum, Diff, Row, RowArena, Timestamp};

use crate::operator::StreamExt;
use crate::source::DecodeResult;
use crate::source::SourceData;

/// Entrypoint to the upsert-specific transformations involved
/// in rendering a stream that came from an upsert source.
/// Upsert-specific operators are different from the rest of
/// the rendering pipeline in that their input is a stream
/// with two components instead of one, and the second component
/// can be null or empty.
pub fn decode_stream<G>(
    stream: &Stream<G, DecodeResult>,
    as_of_frontier: Antichain<Timestamp>,
    operators: &mut Option<LinearOperator>,
    source_arity: usize,
) -> (
    Collection<G, Row, Diff>,
    Option<Collection<G, dataflow_types::DataflowError, Diff>>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Currently, the upsert-specific transformations run in the
    // following order:
    // 1. Applies `as_of` frontier compaction. The compaction is important as
    // downstream upsert preparation can compact away updates for the same keys
    // at the same times, and by advancing times we make more of them the same.
    // 2. compact away updates for the same keys at the same times
    // 3. decoding records
    // 4. prepending the key to the value so that the stream becomes
    //        of the format (key, <entire record>)
    // We may want to consider switching the order of the transformations to
    // optimize performance trade-offs. In the current order, by running
    // deduplicating before decoding/linear operators, we're reducing compute at the
    // cost of requiring more memory.
    //
    // In the future, we may want to have optimization hints that enable people
    // to specify that they believe that they have a large number of unique
    // keys, at which point materialize may be more performant if it runs
    // decoding/linear operators before deduplicating.
    //
    // The method also takes in `operators` which describes predicates that can
    // be applied and columns of the data that can be blanked out. We can apply
    // these operations but must be careful: for example, we can apply predicates
    // before staging the records, but we need to present filtered results as a
    // `None` value to ensure that it prompts dropping any held values with the
    // same key. If the predicates produce errors, we should notice these as well
    // and retract them if non-erroring records overwrite them.

    // TODO: We could move the decoding before the staging grounds, which would
    // allow us to stage potentially less data. The motivation for the current
    // design is to support bulk deduplication without decoding, but we expect
    // this is most likely to happen in the loading of data; consequently, we
    // could use this implementation until `as_of_frontier` is reached, and then
    // switch over to eager decoding. A work-in-progress idea that needs thought.

    // Extract predicates, and "dummy column" information.
    // Predicates are distinguished into temporal and non-temporal,
    // as the non-temporal determine if a record is retained at all,
    // and the temporal indicate how we should transform its timestamps
    // when it is transmitted.
    let mut temporal = Vec::new();
    let mut predicates = Vec::new();
    let mut position_or = (0..source_arity).map(|x| Some(x)).collect::<Vec<_>>();
    if let Some(mut operators) = operators.take() {
        for predicate in operators.predicates.drain(..) {
            if predicate.contains_temporal() {
                temporal.push(predicate);
            } else {
                predicates.push(predicate);
            }
        }
        // Temporal predicates will be applied after blanking out column,
        // so ensure that their support is preserved.
        // TODO: consider blanking out columns added by this processes in
        // the temporal filtering operator.
        for predicate in temporal.iter() {
            operators.projection.extend(predicate.support());
        }
        operators.projection.sort();
        operators.projection.dedup();

        // Overwrite `position_or` to reflect `operators.projection`.
        position_or = (0..source_arity)
            .map(|col| {
                if operators.projection.contains(&col) {
                    Some(col)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
    }
    let temporal_plan = if !temporal.is_empty() {
        let temporal_mfp = expr::MapFilterProject::new(source_arity).filter(temporal);
        Some(temporal_mfp.into_plan().unwrap_or_else(|e| panic!("{}", e)))
    } else {
        None
    };

    let result_stream = stream.unary_frontier(
        Exchange::new(move |DecodeResult { key, .. }| key.hashed()),
        "Upsert",
        move |_cap, _info| {
            // this is a map of (time) -> (capability, ((key) -> (value with max
            // offset))) This is a BTreeMap because we want to ensure that if we
            // receive (key1, value1, time 5) and (key1, value2, time 7) that we
            // send (key1, value1, time 5) before (key1, value2, time 7)
            let mut to_send = BTreeMap::<_, (_, HashMap<_, SourceData>)>::new();
            // this is a map of (decoded key) -> (decoded_value). We store the
            // latest value for a given key that way we know what to retract if
            // a new value with the same key comes along
            let mut current_values = HashMap::new();

            let mut vector = Vec::new();
            let mut row_packer = repr::Row::default();

            move |input, output| {
                // Digest each input, reduce by presented timestamp.
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for DecodeResult {
                        key,
                        value: new_value,
                        position: new_position,
                    } in vector.drain(..)
                    {
                        let mut time = cap.time().clone();
                        time.advance_by(as_of_frontier.borrow());
                        if key.is_none() {
                            error!("Encountered empty key for value {:?}", new_value);
                            continue;
                        }

                        if let Some(new_offset) = new_position {
                            let entry = to_send
                                .entry(time)
                                .or_insert_with(|| (cap.delayed(&time), HashMap::new()))
                                .1
                                .entry(key)
                                .or_insert_with(Default::default);

                            let new_entry = SourceData {
                                value: new_value,
                                position: new_position,
                                upstream_time_millis: None, // upsert sources don't have a column for this, so setting it to `None` is fine.
                            };

                            if let Some(offset) = entry.position {
                                // If the time is equal, toss out the row with the
                                // lower offset
                                if offset < new_offset {
                                    *entry = new_entry;
                                }
                            } else {
                                // If there was not a previous entry, we'll have
                                // inserted a blank default value, and the
                                // offset would be none.
                                // Just insert new entry into the hashmap.
                                *entry = new_entry;
                            }
                        } else {
                            // This case should be unreachable because kafka
                            // records should always come with an offset.
                            error!("Encountered row with empty offset {:?}", key);
                        }
                    }
                });

                let mut removed_times = Vec::new();
                for (time, (cap, map)) in to_send.iter_mut() {
                    if !input.frontier.less_equal(time) {
                        let mut session = output.session(cap);
                        removed_times.push(time.clone());
                        for (key, data) in map.drain() {
                            // decode key and value, and apply predicates/projections to they combined key/value
                            //
                            // TODO(mcsherry): we could record key decoding errors as the value
                            // which would allow us to recover from key decoding errors by a
                            // later retraction of the key (it will never decode correctly, but
                            // we could produce and then remove the error from the output).
                            match key {
                                Some(Ok(decoded_key)) => {
                                    let decoded_value = if data.value.is_none() {
                                        Ok(None)
                                    } else {
                                        let value = data.value.unwrap();
                                        value.and_then(|row| {
                                            let mut datums = Vec::with_capacity(source_arity);
                                            datums.extend(decoded_key.iter());
                                            datums.extend(row.iter());
                                            evaluate(
                                                &datums,
                                                &predicates,
                                                &position_or,
                                                &mut row_packer,
                                            )
                                            .map_err(DataflowError::from)
                                        })
                                    };
                                    // Turns Ok(None) into None, and others into Some(OK) and Some(Err).
                                    // We store errors as well as non-None values, so that they can be
                                    // retracted if new rows show up for the same key.
                                    let new_value = decoded_value.transpose();
                                    let old_value = if let Some(new_value) = &new_value {
                                        current_values.insert(decoded_key, new_value.clone())
                                    } else {
                                        current_values.remove(&decoded_key)
                                    };
                                    if let Some(old_value) = old_value {
                                        // retract old value
                                        session.give((old_value, cap.time().clone(), -1));
                                    }
                                    if let Some(new_value) = new_value {
                                        // give new value
                                        session.give((new_value, cap.time().clone(), 1));
                                    }
                                }
                                None => {}
                                Some(Err(err)) => {
                                    error!("key decoding error: {}", err);
                                }
                            }
                        }
                    } else {
                        // because this is a BTreeMap, the rest of the times in
                        // the map will be greater than this time. So if the
                        // input_frontier is less than or equal to this time,
                        // it will be less than the times in the rest of the map
                        break;
                    }
                }
                // Discard entries, capabilities for complete times.
                for time in removed_times {
                    to_send.remove(&time);
                }
            }
        },
    );

    let (mut oks, mut errs) = result_stream.ok_err(|(data, time, diff)| match data {
        Ok(data) => Ok((data, time, diff)),
        Err(err) => Err((err, time, diff)),
    });

    // If we have temporal predicates do the thing they have to do.
    if let Some(plan) = temporal_plan {
        let (oks2, errs2) = oks.flat_map_fallible({
            let mut datums = crate::render::datum_vec::DatumVec::new();
            move |(row, time, diff)| {
                let arena = repr::RowArena::new();
                let mut datums_local = datums.borrow_with(&row);
                let times_diffs = plan.evaluate(&mut datums_local, &arena, time, diff);
                // Explicitly drop `datums_local` to release the borrow.
                drop(datums_local);
                times_diffs.map(move |time_diff| {
                    time_diff.map_err(|(e, t, d)| (DataflowError::from(e), t, d))
                })
            }
        });

        oks = oks2;
        errs = errs.concat(&errs2);
    }

    (oks.as_collection(), Some(errs.as_collection()))
}

/// Evaluates predicates and dummy column information.
///
/// This method takes decoded datums and prepares as output
/// a row which contains only those positions of `position_or`.
/// If any predicate is failed, no row is produced, and if an
/// error is encounted it is returned instead.
fn evaluate(
    datums: &[Datum],
    predicates: &[MirScalarExpr],
    position_or: &[Option<usize>],
    row_packer: &mut repr::Row,
) -> Result<Option<Row>, EvalError> {
    let arena = RowArena::new();
    // Each predicate is tested in order.
    for predicate in predicates.iter() {
        if predicate.eval(&datums[..], &arena)? != Datum::True {
            return Ok(None);
        }
    }
    // We pack dummy values in locations that do not reference
    // specific columns.
    row_packer.clear();
    row_packer.extend(position_or.iter().map(|x| match x {
        Some(column) => datums[*column],
        None => Datum::Dummy,
    }));
    Ok(Some(row_packer.finish_and_reuse()))
}
