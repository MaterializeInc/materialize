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
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, Map, OkErr, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_dataflow_types::{
    sources::{UpsertEnvelope, UpsertStyle},
    DataflowError, DecodeError, LinearOperator, SourceError, SourceErrorDetails,
};
use mz_expr::{EvalError, MirScalarExpr, SourceInstanceId};
use mz_ore::result::ResultExt;
use mz_persist::operators::upsert::PersistentUpsertConfig;
use mz_repr::{Datum, Diff, Row, RowArena, Timestamp};
use tracing::error;

use crate::operator::StreamExt;
use crate::source::DecodeResult;

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
///
/// When `persist_config` is `Some` this will write upsert state to the configured persistent
/// collection and restore state from it. This does now, however, seal the backing collection. It
/// is the responsibility of the caller to ensure that the collection is sealed up.
pub(crate) fn upsert<G>(
    source_name: &str,
    source_id: SourceInstanceId,
    stream: &Stream<G, DecodeResult>,
    as_of_frontier: Antichain<Timestamp>,
    operators: &mut Option<LinearOperator>,
    // Full arity, including the key columns
    source_arity: usize,
    persist_config: Option<
        PersistentUpsertConfig<Result<Row, DecodeError>, Result<Row, DecodeError>>,
    >,
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

    let (upsert_output, upsert_persist_errs) = match persist_config {
        None => {
            let upsert_output = upsert_core(
                stream,
                predicates,
                position_or,
                as_of_frontier,
                source_arity,
                upsert_envelope,
            );

            let upsert_errs = operator::empty(&stream.scope());

            (upsert_output, upsert_errs)
        }
        Some(upsert_persist_config) => {
            // This is slightly awkward: We don't want to persist full DataflowErrors,so we unpack
            // only DecodeError, which we are more willing to persist. We have to translate to
            // DataflowError again afterwards, such that the returned Streams have the same error
            // type.
            //
            // This also means that we cannot push MFPs into the upsert operatot, as that would
            // mean persisting EvalErrors, which, also icky.
            let mut row_buf = Row::default();
            let stream = stream.flat_map(move |decode_result| {
                if decode_result.key.is_none() {
                    // This is the same behaviour as regular upsert. It's not pretty, though.
                    error!("Encountered empty key in: {:?}", decode_result);
                    return None;
                }
                let offset = decode_result.position;

                // Fold metadata into the value if there is in fact a valid value.
                let value = if let Some(Ok(value)) = decode_result.value {
                    let mut row_packer = row_buf.packer();
                    row_packer.extend(value.iter());
                    row_packer.extend(decode_result.metadata.iter());
                    Some(Ok(row_buf.clone()))
                } else {
                    decode_result.value
                };
                Some((decode_result.key.unwrap(), value, offset))
            });

            let mut row_buf = Row::default();

            let (upsert_output, upsert_persist_errs) = persist::persistent_upsert(
                &stream,
                source_name,
                as_of_frontier,
                upsert_persist_config,
            );

            // Apply Map-Filter-Project and also map back from DecodeError to DataflowError because
            // that's what downstream code expects.
            let mapped_upsert_ok = upsert_output.flat_map(move |((key, value), ts, diff)| {
                match key {
                    Ok(key) => {
                        let result = value
                            .map_err(DataflowError::from)
                            .and_then(|value| {
                                let mut datums = Vec::with_capacity(source_arity);
                                datums.extend(key.iter());
                                datums.extend(value.iter());
                                evaluate(&datums, &predicates, &position_or, &mut row_buf)
                                    .map_err(DataflowError::from)
                            })
                            .transpose();
                        result.map(|result| (result, ts, diff))
                    }
                    Err(err) => {
                        // This can never be retracted! But at least it's better to put the source in a
                        // permanently errored state than to keep on trucking with wrong results.
                        Some((Err(DataflowError::DecodeError(err)), ts, diff))
                    }
                }
            });

            // TODO: It is not ideal that persistence errors end up in the same differential error
            // collection as other errors because they are transient/indefinite errors that we
            // should be treating differently. We do not, however, at the current time have to
            // infrastructure for treating these errors differently, so we're adding them to the
            // same error collections.
            let upsert_persist_errs = upsert_persist_errs.map(move |(err, ts, diff)| {
                let source_error =
                    SourceError::new(source_id, SourceErrorDetails::Persistence(err));
                (source_error.into(), ts, diff)
            });

            (mapped_upsert_ok, upsert_persist_errs)
        }
    };

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

    let errs = errs.concat(&upsert_persist_errs);

    (oks, errs)
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
    source_arity: usize,
    upsert_envelope: UpsertEnvelope,
) -> Stream<G, (Result<Row, DataflowError>, u64, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let result_stream = stream.binary_frontier::<Vec<()>, _, _, _, _, _>(
        &operator::empty(&stream.scope()),
        Exchange::new(move |DecodeResult { key, .. }| key.hashed()),
        Pipeline,
        "Upsert",
        move |_cap, _info| {
            // This is a map of (time) -> (capability, ((key) -> (value with max offset)))
            //
            // This is a BTreeMap because we want to ensure that if we receive (key1, value1, time
            // 5) and (key1, value2, time 7) that we send (key1, value1, time 5) before (key1,
            // value2, time 7)
            let mut to_send = BTreeMap::<_, (_, HashMap<_, UpsertSourceData>)>::new();
            // this is a map of (decoded key) -> (decoded_value). We store the
            // latest value for a given key that way we know what to retract if
            // a new value with the same key comes along
            let mut current_values = HashMap::new();

            let mut vector = Vec::new();
            let mut row_packer = mz_repr::Row::default();

            move |input, _state_input, output| {
                // Digest each input, reduce by presented timestamp.
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for DecodeResult {
                        key,
                        value: new_value,
                        position: new_position,
                        upstream_time_millis: _,
                        partition: _,
                        metadata,
                    } in vector.drain(..)
                    {
                        let mut time = cap.time().clone();
                        time.advance_by(as_of_frontier.borrow());
                        if key.is_none() {
                            error!(?new_value, "Encountered empty key for value");
                            continue;
                        }

                        let entry = to_send
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
                                Some(decoded_key) => {
                                    let (decoded_key, decoded_value): (
                                        _,
                                        Result<_, DataflowError>,
                                    ) = match decoded_key {
                                        Err(key_decode_error) => {
                                            (
                                                Err(key_decode_error.clone()),
                                                // `DecodeError` converted to a `DataflowError`
                                                // that we will eventually emit later below
                                                Err(key_decode_error.into()),
                                            )
                                        }
                                        Ok(decoded_key) => match data.value {
                                            None => (Ok(decoded_key), Ok(None)),
                                            Some(value) => {
                                                let decoded_value = match value {
                                                    Ok(row) => {
                                                        let envelope_value = match upsert_envelope
                                                            .style
                                                        {
                                                            UpsertStyle::Debezium { after_idx } => {
                                                                match row
                                                                    .iter()
                                                                    .nth(after_idx)
                                                                    .unwrap()
                                                                {
                                                                    Datum::List(after) => {
                                                                        let mut datums =
                                                                            Vec::with_capacity(
                                                                                source_arity,
                                                                            );
                                                                        datums.extend(after.iter());
                                                                        Some(datums)
                                                                    }
                                                                    Datum::Null => None,
                                                                    d => panic!(
                                                                    "type error: expected record, \
                                                                        found {:?}",
                                                                    d
                                                                ),
                                                                }
                                                            }
                                                            UpsertStyle::Default(_) => {
                                                                let mut datums = Vec::with_capacity(
                                                                    source_arity,
                                                                );
                                                                datums.extend(decoded_key.iter());
                                                                datums.extend(row.iter());
                                                                Some(datums)
                                                            }
                                                        };

                                                        if let Some(mut datums) = envelope_value {
                                                            datums.extend(data.metadata.iter());
                                                            evaluate(
                                                                &datums,
                                                                &predicates,
                                                                &position_or,
                                                                &mut row_packer,
                                                            )
                                                            .map_err(DataflowError::from)
                                                        } else {
                                                            Ok(None)
                                                        }
                                                    }
                                                    Err(err) => Err(err),
                                                };
                                                (Ok(decoded_key), decoded_value)
                                            }
                                        },
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
                                            .map(|full_row| {
                                                thin(
                                                    &upsert_envelope.key_indices,
                                                    &full_row,
                                                    &mut row_packer,
                                                )
                                            })
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
                                                        &mut row_packer,
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
                                                    &mut row_packer,
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
                                None => {}
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

    result_stream
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

mod persist {
    use super::*;

    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::Hashable;
    use mz_persist::operators::replay::Replay;
    use mz_persist::operators::split_ok_err;
    use mz_persist::operators::stream::{Persist, RetractUnsealed};
    use std::collections::hash_map::Entry;
    use std::collections::{BTreeMap, HashMap};
    use std::fmt::Debug;
    use std::hash::Hash;
    use timely::dataflow::channels::pact::Exchange;
    use timely::dataflow::operators::{Concat, Map, OkErr};
    use timely::dataflow::operators::{Delay, Operator};
    use timely::dataflow::{Scope, Stream};
    use timely::progress::Antichain;
    use tracing::trace;

    /// lalala
    pub fn persistent_upsert<G>(
        stream: &Stream<
            G,
            (
                Result<Row, DecodeError>,
                Option<Result<Row, DecodeError>>,
                i64,
            ),
        >,
        name: &str,
        as_of_frontier: Antichain<u64>,
        persist_config: PersistentUpsertConfig<Result<Row, DecodeError>, Result<Row, DecodeError>>,
    ) -> (
        Stream<
            G,
            (
                (Result<Row, DecodeError>, Result<Row, DecodeError>),
                u64,
                i64,
            ),
        >,
        Stream<G, (String, u64, i64)>,
    )
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let operator_name = format!("persistent_upsert({})", name);

        let (restored_upsert_oks, state_errs) = {
            let snapshot = persist_config.read_handle.snapshot();
            let (restored_oks, restored_errs) = stream
                .scope()
                .replay(snapshot, &as_of_frontier)
                .ok_err(split_ok_err);
            let (restored_upsert_oks, retract_errs) = restored_oks.retract_unsealed(
                name,
                persist_config.write_handle.clone(),
                persist_config.upper_seal_ts,
            );
            let combined_errs = restored_errs.concat(&retract_errs);
            (restored_upsert_oks, combined_errs)
        };

        let mut differential_state_ingester = Some(DifferentialStateIngester::new());

        let upsert_as_of_frontier = as_of_frontier.clone();

        let new_upsert_oks = stream.binary_frontier(
            &restored_upsert_oks,
            Exchange::new(
                move |(key, _value, _ts): &(
                    Result<Row, DecodeError>,
                    Option<Result<Row, DecodeError>>,
                    i64,
                )| key.hashed(),
            ),
            Exchange::new(
                move |((key, _data), _ts, _diff): &((Result<Row, DecodeError>, _), _, _)| {
                    key.hashed()
                },
            ),
            &operator_name.clone(),
            move |_cap, _info| {
                // This is a map of (time) -> (capability, ((key) -> (value with max offset))). This
                // is a BTreeMap because we want to ensure that if we receive (key1, value1, time
                // 5) and (key1, value2, time 7) that we send (key1, value1, time 5) before (key1,
                // value2, time 7).
                //
                // This is a staging area, where we group incoming updates by timestamp (the timely
                // timestamp) and disambiguate by the offset (also called "timestamp" above) if
                // necessary.
                let mut to_send =
                    BTreeMap::<_, (_, HashMap<_, (Option<Result<Row, DecodeError>>, i64)>)>::new();

                // This is a map from key -> value. We store the latest value for a given key that
                // way we know what to retract if a new value with the same key comes along.
                let mut current_values = HashMap::new();

                let mut input_buffer = Vec::new();
                let mut state_input_buffer = Vec::new();

                move |input, state_input, output| {
                    state_input.for_each(|_time, data| {
                        data.swap(&mut state_input_buffer);
                        for state_update in state_input_buffer.drain(..) {
                            trace!("In {}, restored upsert: {:?}", operator_name, state_update);

                            differential_state_ingester
                                .as_mut()
                                .expect("already finished ingesting")
                                .add_update(state_update);
                        }
                    });

                    // An empty frontier signals that we will never receive data from that input
                    // again because no-one upstream holds any capability anymore.
                    if differential_state_ingester.is_some()
                        && state_input.frontier.frontier().is_empty()
                    {
                        let initial_state = differential_state_ingester
                            .take()
                            .expect("already finished ingesting")
                            .finish();
                        current_values.extend(initial_state.into_iter());

                        trace!(
                            "In {}, initial (restored) upsert state: {:?}",
                            operator_name,
                            current_values.iter().take(10).collect::<Vec<_>>()
                        );
                    }

                    // Digest each input, reduce by presented timestamp.
                    input.for_each(|cap, data| {
                        data.swap(&mut input_buffer);
                        for (key, value, offset) in input_buffer.drain(..) {
                            let mut time = cap.time().clone();
                            time.advance_by(upsert_as_of_frontier.borrow());

                            let time_entries = &mut to_send
                                .entry(time)
                                .or_insert_with(|| (cap.delayed(&time), HashMap::new()))
                                .1;

                            let new_entry = (value, offset);

                            match time_entries.entry(key) {
                                Entry::Occupied(mut occupied) => {
                                    let existing_entry = occupied.get_mut();
                                    // If the time is equal, toss out the row with the lower
                                    // offset.
                                    if new_entry.1 > existing_entry.1 {
                                        *existing_entry = new_entry;
                                    }
                                }
                                Entry::Vacant(vacant) => {
                                    // We didn't yet see an entry with the same timestamp. We can
                                    // just insert and don't need to disambiguate by offset.
                                    vacant.insert(new_entry);
                                }
                            }
                        }
                    });

                    let mut removed_times = Vec::new();
                    for (time, (cap, map)) in to_send.iter_mut() {
                        if !input.frontier.less_equal(time)
                            && !state_input.frontier.less_equal(time)
                        {
                            let mut session = output.session(cap);
                            removed_times.push(time.clone());
                            for (key, (value, _offset)) in map.drain() {
                                let old_value = if let Some(new_value) = &value {
                                    current_values.insert(key.clone(), new_value.clone())
                                } else {
                                    current_values.remove(&key)
                                };
                                if let Some(old_value) = old_value {
                                    // Retract old value.
                                    session.give((
                                        (key.clone(), old_value),
                                        cap.time().clone(),
                                        -1,
                                    ));
                                }
                                if let Some(new_value) = value {
                                    // Give new value.
                                    session.give(((key, new_value), cap.time().clone(), 1));
                                }
                            }
                        } else {
                            // Because this is a BTreeMap, the rest of the times in the map will be
                            // greater than this time. So if the input_frontier is less than or
                            // equal to this time, it will be less than the times in the rest of
                            // the map.
                            break;
                        }
                    }

                    // Discard entries, which hold capabilities, for complete times.
                    for time in removed_times {
                        to_send.remove(&time);
                    }
                }
            },
        );

        let (new_upsert_oks, new_upsert_persist_errs) =
            new_upsert_oks.persist(name, persist_config.write_handle);

        // Also pull the timestamp of restored data up to the as_of_frontier. We are doing this in
        // two steps: first, we are modifying the timestamp in the data itself, then we're delaying
        // the timely timestamp. The latter will stash updates while they are not beyond the
        // frontier.
        let retime_as_of_frontier = as_of_frontier.clone();
        let restored_upsert_oks = restored_upsert_oks
            .map(move |(data, mut time, diff)| {
                time.advance_by(retime_as_of_frontier.borrow());
                (data, time, diff)
            })
            .delay_batch(move |time| {
                let mut time = *time;
                time.advance_by(as_of_frontier.borrow());
                time
            });

        (
            new_upsert_oks.concat(&restored_upsert_oks),
            new_upsert_persist_errs.concat(&state_errs),
        )
    }

    /// Ingests differential updates, consolidates them, and emits a final `HashMap` that contains the
    /// consolidated upsert state.
    struct DifferentialStateIngester<K, V> {
        differential_state: HashMap<(K, V), i64>,
    }

    impl<K, V> DifferentialStateIngester<K, V>
    where
        K: Hash + Eq + Clone + Debug,
        V: Hash + Eq + Debug,
    {
        fn new() -> Self {
            DifferentialStateIngester {
                differential_state: HashMap::new(),
            }
        }

        fn add_update(&mut self, update: ((K, V), u64, i64)) {
            let ((k, v), _ts, diff) = update;

            *self.differential_state.entry((k, v)).or_default() += diff;
        }

        fn finish(mut self) -> HashMap<K, V> {
            self.differential_state.retain(|_k, diff| *diff > 0);

            let mut state = HashMap::new();

            for ((key, value), diff) in self.differential_state.into_iter() {
                // our state must be internally consistent
                assert!(diff == 1, "i64 for ({:?}, {:?}) is {}", key, value, diff);
                match state.insert(key.clone(), value) {
                    None => (), // it's all good
                    // we must be internally consistent: there can only be one value per key in the
                    // consolidated state
                    Some(old_value) => {
                        // try_insert() would be perfect here, because we could also report the key
                        // without cloning
                        panic!("Already have a value for key {:?}: {:?}", key, old_value)
                    }
                }
            }

            state
        }
    }
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
