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
use timely::dataflow::operators::{generic::Operator, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use dataflow_types::{DataflowError, DecodeError, LinearOperator};
use expr::{EvalError, MirScalarExpr};
use log::error;
use repr::{Datum, Diff, Row, RowArena, Timestamp};

use crate::decode::DecoderState;
use crate::source::{SourceData, SourceOutput};

/// Entrypoint to the upsert-specific transformations involved
/// in rendering a stream that came from an upsert source.
/// Upsert-specific operators are different from the rest of
/// the rendering pipeline in that their input is a stream
/// with two components instead of one, and the second component
/// can be null or empty.
pub fn decode_stream<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    as_of_frontier: Antichain<Timestamp>,
    mut key_decoder_state: Box<dyn DecoderState>,
    mut value_decoder_state: Box<dyn DecoderState>,
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
    // before staging the records, but we need to record observe the result as a
    // `None` value to ensure that it prompts dropping any held values with the
    // same key. If the predicates produce errors, we should notice these as well
    // and retract them if non-erroring records overwrite them.
    let result_stream = stream.unary_frontier(
        Exchange::new(move |x: &SourceOutput<Vec<u8>, Vec<u8>>| x.key.hashed()),
        "Upsert",
        |_cap, _info| {
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
            let mut row_packer = repr::RowPacker::new();

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

            move |input, output| {
                // Digest each input, reduce by presented timestamp.
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for SourceOutput {
                        key,
                        value: new_value,
                        position: new_position,
                        upstream_time_millis: new_upstream_time_millis,
                    } in vector.drain(..)
                    {
                        let mut time = cap.time().clone();
                        time.advance_by(as_of_frontier.borrow());
                        if key.is_empty() {
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
                                upstream_time_millis: new_upstream_time_millis,
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
                            // decode key and value
                            match key_decoder_state.decode_key(&key) {
                                Ok(decoded_key) => {
                                    let decoded_value = if data.value.is_empty() {
                                        Ok(None)
                                    } else {
                                        match value_decoder_state.decode_upsert_value(
                                            &data.value,
                                            data.position,
                                            data.upstream_time_millis,
                                        ) {
                                            Ok(value) => {
                                                if let Some(value) = value {
                                                    // prepend key to row
                                                    let temp_storage = RowArena::new();
                                                    // Unpack rows to apply predicates and projections.
                                                    // This could be optimized with MapFilterProject.
                                                    let mut datums =
                                                        Vec::with_capacity(source_arity);
                                                    datums.extend(decoded_key.iter());
                                                    datums.extend(value.iter());

                                                    match evaluate_predicates(
                                                        &predicates,
                                                        &datums[..],
                                                        &temp_storage,
                                                    ) {
                                                        Ok(true) => {
                                                            row_packer.clear();
                                                            row_packer.extend(
                                                                position_or.iter().map(
                                                                    |x| match x {
                                                                        Some(column) => {
                                                                            datums[*column]
                                                                        }
                                                                        None => Datum::Dummy,
                                                                    },
                                                                ),
                                                            );
                                                            Ok(Some(row_packer.finish_and_reuse()))
                                                        }
                                                        Ok(false) => Ok(None),
                                                        Err(e) => Err(DataflowError::from(e)),
                                                    }
                                                } else {
                                                    Ok(None)
                                                }
                                            }
                                            Err(err) => Err(DataflowError::DecodeError(
                                                DecodeError::Text(err),
                                            )),
                                        }
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
                                Err(err) => {
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
                key_decoder_state.log_error_count();
                value_decoder_state.log_error_count();
            }
        },
    );

    let (oks, errs) = result_stream.ok_err(|(data, time, diff)| match data {
        Ok(data) => Ok((data, time, diff)),
        Err(err) => Err((err, time, diff)),
    });

    (oks.as_collection(), Some(errs.as_collection()))
}

fn evaluate_predicates<'a>(
    predicates: &[MirScalarExpr],
    datums: &[Datum<'a>],
    arena: &'a RowArena,
) -> Result<bool, EvalError> {
    for predicate in predicates.iter() {
        if predicate.eval(&datums[..], &arena)? != Datum::True {
            return Ok(false);
        }
    }
    return Ok(true);
}
