// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, CapabilityRef, Concat, OkErr, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_dataflow_types::{
    sources::{UpsertEnvelope, UpsertStyle},
    DataflowError, DecodeError, LinearOperator,
};
use mz_expr::{EvalError, MirScalarExpr};
use mz_ore::result::ResultExt;
use mz_repr::{Datum, DatumVec, DatumVecBorrow, Diff, Row, RowArena, Timestamp};
use tracing::error;

use crate::source::DecodeResult;
use mz_timely_util::operator::StreamExt;

#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
struct UpsertSourceData {
    /// The actual value
    value: Option<Result<Row, DataflowError>>,
    /// The source's reported position for this record
    position: i64,
    /// The time that the upstream source believes that the message was created
    /// Currently only applies to Kafka
    upstream_time_millis: Option<i64>,
    /// Metadata for this row
    metadata: Row,
}

/// Entrypoint to the upsert-specific transformations involved
/// in rendering a stream that came from an upsert source.
/// Upsert-specific operators are different from the rest of
/// the rendering pipeline in that their input is a stream
/// with two components instead of one, and the second component
/// can be null or empty.
pub(crate) fn upsert<G>(
    stream: &Stream<G, DecodeResult>,
    as_of_frontier: Antichain<Timestamp>,
    operators: &mut Option<LinearOperator>,
    // Full arity, including the key columns
    source_arity: usize,
    upsert_envelope: UpsertEnvelope,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (mz_dataflow_types::DataflowError, Timestamp, Diff)>,
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
    let mut position_or = (0..source_arity).map(Some).collect::<Vec<_>>();
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
        let temporal_mfp = mz_expr::MapFilterProject::new(source_arity).filter(temporal);
        Some(temporal_mfp.into_plan().unwrap_or_else(|e| panic!("{}", e)))
    } else {
        None
    };

    let upsert_output = upsert_core(
        stream,
        predicates,
        position_or,
        as_of_frontier,
        upsert_envelope,
    );
    let (mut oks, mut errs) = upsert_output.ok_err(|(data, time, diff)| match data {
        Ok(data) => Ok((data, time, diff)),
        Err(err) => Err((err, time, diff)),
    });

    // If we have temporal predicates do the thing they have to do.
    if let Some(plan) = temporal_plan {
        let (oks2, errs2) = oks.flat_map_fallible("UpsertTemporalOperators", {
            let mut datum_vec = mz_repr::DatumVec::new();
            let mut row_builder = Row::default();
            move |(row, time, diff)| {
                let arena = mz_repr::RowArena::new();
                let mut datums_local = datum_vec.borrow_with(&row);
                plan.evaluate(&mut datums_local, &arena, time, diff, &mut row_builder)
            }
        });

        oks = oks2;
        errs = errs.concat(&errs2);
    }

    (oks, errs)
}

/// Evaluates predicates and dummy column information.
///
/// This method takes decoded datums and prepares as output
/// a row which contains only those positions of `position_or`.
/// If any predicate is failed, no row is produced, and if an
/// error is encountered it is returned instead.
fn evaluate(
    datums: &[Datum],
    predicates: &[MirScalarExpr],
    position_or: &[Option<usize>],
    row_buf: &mut Row,
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
    let mut row_packer = row_buf.packer();
    row_packer.extend(position_or.iter().map(|x| match x {
        Some(column) => datums[*column],
        None => Datum::Dummy,
    }));
    Ok(Some(row_buf.clone()))
}

/// Internal core upsert logic.
fn upsert_core<G>(
    stream: &Stream<G, DecodeResult>,
    predicates: Vec<MirScalarExpr>,
    position_or: Vec<Option<usize>>,
    as_of_frontier: Antichain<Timestamp>,
    upsert_envelope: UpsertEnvelope,
) -> Stream<G, (Result<Row, DataflowError>, u64, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let result_stream = stream.unary_frontier(
        Exchange::new(move |DecodeResult { key, .. }| key.hashed()),
        "Upsert",
        move |_cap, _info| {
            // This is a map of (time) -> (capability, ((key) -> (value with max offset)))
            //
            // This is a BTreeMap because we want to ensure that if we receive (key1, value1, time
            // 5) and (key1, value2, time 7) that we send (key1, value1, time 5) before (key1,
            // value2, time 7)
            let mut pending_values =
                BTreeMap::<Timestamp, (Capability<Timestamp>, HashMap<_, UpsertSourceData>)>::new();
            // this is a map of (decoded key) -> (decoded_value). We store the
            // latest value for a given key that way we know what to retract if
            // a new value with the same key comes along
            let mut current_values = HashMap::new();

            // Intermediate structures re-used to limit allocations
            let mut scratch_vector = Vec::new();
            let mut row_packer = mz_repr::Row::default();
            let mut dv = DatumVec::new();

            move |input, output| {
                // Digest each input, reduce by presented timestamp.
                input.for_each(|cap, data| {
                    data.swap(&mut scratch_vector);
                    process_new_data(
                        &mut scratch_vector,
                        &mut pending_values,
                        &cap,
                        &as_of_frontier,
                    );
                });

                let mut removed_times = Vec::new();
                for (time, (cap, map)) in pending_values.iter_mut() {
                    if input.frontier.less_equal(time) {
                        // because this is a BTreeMap, the rest of the times in
                        // the map will be greater than this time. So if the
                        // input_frontier is less than or equal to this time,
                        // it will be less than the times in the rest of the map
                        break;
                    }
                    process_pending_values_batch(
                        time,
                        cap,
                        map,
                        &mut current_values,
                        &mut row_packer,
                        &mut dv,
                        &upsert_envelope,
                        &predicates,
                        &position_or,
                        &mut removed_times,
                        output,
                    )
                }
                // Discard entries, capabilities for complete times.
                for time in removed_times {
                    pending_values.remove(&time);
                }
            }
        },
    );

    result_stream
}

/// This function fills `pending_values` with new data
/// from the timely operator input.
fn process_new_data(
    new_data: &mut Vec<DecodeResult>,
    pending_values: &mut BTreeMap<
        Timestamp,
        (
            Capability<Timestamp>,
            HashMap<Option<Result<Row, DecodeError>>, UpsertSourceData>,
        ),
    >,
    cap: &CapabilityRef<Timestamp>,
    as_of_frontier: &Antichain<Timestamp>,
) {
    for DecodeResult {
        key,
        value: new_value,
        position: new_position,
        upstream_time_millis: _,
        partition: _,
        metadata,
    } in new_data.drain(..)
    {
        let mut time = cap.time().clone();
        time.advance_by(as_of_frontier.borrow());
        if key.is_none() {
            error!(?new_value, "Encountered empty key for value");
            continue;
        }

        let entry = pending_values
            .entry(time)
            .or_insert_with(|| (cap.delayed(&time), HashMap::new()))
            .1
            .entry(key);

        let new_entry = UpsertSourceData {
            value: new_value.map(ResultExt::err_into),
            position: new_position,
            // upsert sources don't have a column for this, so setting it to
            // `None` is fine.
            upstream_time_millis: None,
            metadata,
        };

        match entry {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                // If the time is equal, toss out the row with the
                // lower offset
                if e.get().position < new_position {
                    e.insert(new_entry);
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(new_entry);
            }
        }
    }
}

/// This function processes a batch of ready (i.e. whose time is below the current
/// input frontier) values and evaluate them against the intermediate upsert
/// data (`current_values`) and output issues and retractions for the output timely
/// stream. It is used exclusively by `upsert_core`
fn process_pending_values_batch(
    // The time, capability, and map of data at that time we
    // are processing in this call.
    time: &u64,
    cap: &mut Capability<Timestamp>,
    map: &mut HashMap<Option<Result<Row, DecodeError>>, UpsertSourceData>,
    // The current map of values we use to perform the upsert comparision
    current_values: &mut HashMap<Result<Row, DecodeError>, Result<Row, DataflowError>>,
    // A shared row used to pack new rows for evaluation and output
    row_packer: &mut Row,
    // A shared row used to build a Vec<Datum<'_>> for evaluation
    dv: &mut DatumVec,
    // Additional source properties used to correctly inter
    upsert_envelope: &UpsertEnvelope,
    // Additional information used to pre-evaluate predicates that reduces the output
    // stream size
    predicates: &[MirScalarExpr],
    position_or: &[Option<usize>],
    // An out parameter of times that must be removed from the `to_send` map
    // as we are done processing them.
    removed_times: &mut Vec<Timestamp>,
    // The output handle to output processed values into the output timely stream.
    output: &mut timely::dataflow::operators::generic::OutputHandle<
        '_,
        Timestamp,
        (Result<Row, DataflowError>, u64, Diff),
        timely::dataflow::channels::pushers::tee::Tee<
            Timestamp,
            (Result<Row, DataflowError>, u64, Diff),
        >,
    >,
) {
    let mut session = output.session(cap);
    removed_times.push(time.clone());
    for (key, data) in map.drain() {
        // decode key and value, and apply predicates/projections to they combined key/value
        //
        // TODO(mcsherry): we could record key decoding errors as the value
        // which would allow us to recover from key decoding errors by a
        // later retraction of the key (it will never decode correctly, but
        // we could produce and then remove the error from the output).
        if let Some(decoded_key) = key {
            let (decoded_key, decoded_value): (_, Result<_, DataflowError>) =
                match (decoded_key, data.value) {
                    (Err(key_decode_error), _) => {
                        (
                            Err(key_decode_error.clone()),
                            // `DecodeError` converted to a `DataflowError`
                            // that we will eventually emit later below
                            Err(key_decode_error.into()),
                        )
                    }
                    (Ok(decoded_key), None) => (Ok(decoded_key), Ok(None)),
                    (Ok(decoded_key), Some(value)) => {
                        let decoded_value = value.and_then(|row| {
                            build_datum_vec_for_evaluation(
                                dv,
                                &upsert_envelope.style,
                                &row,
                                &decoded_key,
                            )
                            .map_or(Ok(None), |mut datums| {
                                datums.extend(data.metadata.iter());
                                evaluate(&datums, &predicates, &position_or, row_packer)
                                    .map_err(DataflowError::from)
                            })
                        });
                        (Ok(decoded_key), decoded_value)
                    }
                };

            // Turns Ok(None) into None, and others into Some(OK) and Some(Err).
            // We store errors as well as non-None values, so that they can be
            // retracted if new rows show up for the same key.
            let new_value = decoded_value.transpose();

            let old_value = if let Some(new_value) = &new_value {
                // Thin out the row to not contain a copy of the
                // key columns, cloning when need-be
                let thinned_value = new_value
                    .as_ref()
                    .map(|full_row| thin(&upsert_envelope.key_indices, &full_row, row_packer))
                    .map_err(|e| e.clone());
                current_values
                    .insert(decoded_key.clone(), thinned_value)
                    .map(|res| {
                        res.map(|v| {
                            rehydrate(
                                &upsert_envelope.key_indices,
                                // The value is never `Ok`
                                // unless the key is also
                                decoded_key.as_ref().unwrap(),
                                &v,
                                row_packer,
                            )
                        })
                    })
            } else {
                current_values.remove(&decoded_key).map(|res| {
                    res.map(|v| {
                        rehydrate(
                            &upsert_envelope.key_indices,
                            // The value is never `Ok`
                            // unless the key is also
                            decoded_key.as_ref().unwrap(),
                            &v,
                            row_packer,
                        )
                    })
                })
            };

            if let Some(old_value) = old_value {
                // Ensure we put the source in a permanently error'd state
                // than to keep on trucking with wrong results.
                //
                // TODO(guswynn): consider changing the key-type of
                // the `current_values` map to allow us to retract
                // errors. Currently, the `DecodeError` key type would
                // retract unrelated errors with the same message.
                if !decoded_key.is_err() {
                    // retract old value
                    session.give((old_value, cap.time().clone(), -1));
                }
            }
            if let Some(new_value) = new_value {
                // give new value
                session.give((new_value, cap.time().clone(), 1));
            }
        }
    }
}

fn build_datum_vec_for_evaluation<'row>(
    dv: &'row mut DatumVec,
    upsert_style: &UpsertStyle,
    row: &'row Row,
    key: &'row Row,
) -> Option<DatumVecBorrow<'row>> {
    let mut datums = dv.borrow();
    match upsert_style {
        UpsertStyle::Debezium { after_idx } => match row.iter().nth(*after_idx).unwrap() {
            Datum::List(after) => {
                datums.extend(after.iter());
                Some(datums)
            }
            Datum::Null => None,
            d => panic!("type error: expected record, found {:?}", d),
        },
        UpsertStyle::Default(_) => {
            datums.extend(key.iter());
            datums.extend(row.iter());
            Some(datums)
        }
    }
}

/// `thin` uses information from the source description to find which indexes in the row
/// are keys and skip them.
fn thin(key_indices: &[usize], value: &Row, row_buf: &mut Row) -> Row {
    let mut row_packer = row_buf.packer();
    let values = &mut value.iter();
    let mut next_idx = 0;
    for &key_idx in key_indices {
        // First, push the datums that are before `key_idx`
        row_packer.extend(values.take(key_idx - next_idx));
        // Then, skip this key datum
        values.next().unwrap();
        next_idx = key_idx + 1;
    }
    // Finally, push any columns after the last key index
    row_packer.extend(values);

    row_buf.clone()
}

/// `rehydrate` uses information from the source description to find which indexes in the row
/// are keys and add them back in in the right places.
fn rehydrate(key_indices: &[usize], key: &Row, thinned_value: &Row, row_buf: &mut Row) -> Row {
    let mut row_packer = row_buf.packer();
    let values = &mut thinned_value.iter();
    let mut next_idx = 0;
    for (&key_idx, key_datum) in key_indices.iter().zip(key.iter()) {
        // First, push the datums that are before `key_idx`
        row_packer.extend(values.take(key_idx - next_idx));
        // Then, push this key datum
        row_packer.push(key_datum);
        next_idx = key_idx + 1;
    }
    // Finally, push any columns after the last key index
    row_packer.extend(values);

    row_buf.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rehydrate_thin_first() {
        let mut packer = Row::default();

        let key_indices = vec![0];
        let key = Row::pack([Datum::String("key")]);

        let thinned = Row::pack([Datum::String("two")]);

        let rehydrated = rehydrate(&key_indices, &key, &thinned, &mut packer);

        assert_eq!(
            rehydrated,
            Row::pack([Datum::String("key"), Datum::String("two"),])
        );

        assert_eq!(thin(&key_indices, &rehydrated, &mut packer), thinned);
    }

    #[test]
    fn test_rehydrate_thin_middle() {
        let mut packer = Row::default();

        let key_indices = vec![2];
        let key = Row::pack([Datum::String("key")]);

        let thinned = Row::pack([
            Datum::String("one"),
            Datum::String("two"),
            Datum::String("four"),
        ]);

        let rehydrated = rehydrate(&key_indices, &key, &thinned, &mut packer);

        assert_eq!(
            rehydrated,
            Row::pack([
                Datum::String("one"),
                Datum::String("two"),
                Datum::String("key"),
                Datum::String("four"),
            ])
        );

        assert_eq!(thin(&key_indices, &rehydrated, &mut packer), thinned);
    }

    #[test]
    fn test_rehydrate_thin_multiple() {
        let mut packer = Row::default();

        let key_indices = vec![2, 4];
        let key = Row::pack([Datum::String("key1"), Datum::String("key2")]);

        let thinned = Row::pack([
            Datum::String("one"),
            Datum::String("two"),
            Datum::String("four"),
            Datum::String("six"),
        ]);

        let rehydrated = rehydrate(&key_indices, &key, &thinned, &mut packer);

        assert_eq!(
            rehydrated,
            Row::pack([
                Datum::String("one"),
                Datum::String("two"),
                Datum::String("key1"),
                Datum::String("four"),
                Datum::String("key2"),
                Datum::String("six"),
            ])
        );

        assert_eq!(thin(&key_indices, &rehydrated, &mut packer), thinned);
    }

    #[test]
    fn test_thin_end() {
        let mut packer = Row::default();

        let key_indices = vec![2];

        assert_eq!(
            thin(
                &key_indices,
                &Row::pack([
                    Datum::String("one"),
                    Datum::String("two"),
                    Datum::String("key"),
                ]),
                &mut packer
            ),
            Row::pack([Datum::String("one"), Datum::String("two")]),
        );
    }
}
