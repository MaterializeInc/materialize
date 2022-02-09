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
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, Map, OkErr, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_dataflow_types::{
    DataflowError, DecodeError, LinearOperator, SourceError, SourceErrorDetails,
};
use mz_expr::{EvalError, MirScalarExpr};
use mz_ore::result::ResultExt;
use mz_persist::operators::upsert::{PersistentUpsert, PersistentUpsertConfig};
use mz_repr::{Datum, Diff, Row, RowArena, Timestamp};
use tracing::error;

use crate::operator::StreamExt;
use crate::source::{DecodeResult, SourceData};

#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
struct UpsertSourceData {
    raw_data: SourceData,
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
    stream: &Stream<G, DecodeResult>,
    as_of_frontier: Antichain<Timestamp>,
    operators: &mut Option<LinearOperator>,
    key_arity: usize,
    // Full arity, including the key columns
    source_arity: usize,
    persist_config: Option<
        PersistentUpsertConfig<Result<Row, DecodeError>, Result<Row, DecodeError>>,
    >,
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
                key_arity,
                source_arity,
                predicates,
                position_or,
                as_of_frontier,
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
            let mut row_packer = mz_repr::Row::default();
            let stream = stream.flat_map(move |decode_result| {
                if decode_result.key.is_none() {
                    // This is the same behaviour as regular upsert. It's not pretty, though.
                    error!("Encountered empty key in: {:?}", decode_result);
                    return None;
                }
                let offset = decode_result.position;

                // Fold metadata into the value if there is in fact a valid value.
                let value = if let Some(Ok(value)) = decode_result.value {
                    row_packer.clear();
                    row_packer.extend(value.iter());
                    row_packer.extend(decode_result.metadata.iter());
                    Some(Ok(row_packer.finish_and_reuse()))
                } else {
                    decode_result.value
                };
                Some((decode_result.key.unwrap(), value, offset))
            });

            let mut row_packer = mz_repr::Row::default();

            let (upsert_output, upsert_persist_errs) =
                stream.persistent_upsert(source_name, as_of_frontier, upsert_persist_config);

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
                                evaluate(&datums, &predicates, &position_or, &mut row_packer)
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
            let source_name = source_name.to_owned();
            let upsert_persist_errs = upsert_persist_errs.map(move |(err, ts, diff)| {
                let source_error =
                    SourceError::new(source_name.clone(), SourceErrorDetails::Persistence(err));
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
    row_packer: &mut mz_repr::Row,
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

/// Internal core upsert logic.
fn upsert_core<G>(
    stream: &Stream<G, DecodeResult>,
    key_arity: usize,
    source_arity: usize,
    predicates: Vec<MirScalarExpr>,
    position_or: Vec<Option<usize>>,
    as_of_frontier: Antichain<Timestamp>,
) -> Stream<G, (Result<Row, DataflowError>, u64, isize)>
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
            let mut to_send = BTreeMap::<_, (_, HashMap<_, UpsertSourceData>)>::new();
            // this is a map of (decoded key) -> (decoded_value). We store the
            // latest value for a given key that way we know what to retract if
            // a new value with the same key comes along
            let mut current_values = HashMap::new();

            let mut vector = Vec::new();
            let mut row_packer = mz_repr::Row::default();

            move |input, output| {
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
                            .entry(key)
                            .or_insert_with(Default::default);

                        let new_entry = UpsertSourceData {
                            raw_data: SourceData {
                                value: new_value.map(ResultExt::err_into),
                                position: new_position,
                                // upsert sources don't have a column for this, so setting it to
                                // `None` is fine.
                                upstream_time_millis: None,
                            },
                            metadata,
                        };

                        if let Some(offset) = entry.raw_data.position {
                            // If the time is equal, toss out the row with the
                            // lower offset
                            if offset < new_position.expect("Kafka must always have an offset") {
                                *entry = new_entry;
                            }
                        } else {
                            // If there was not a previous entry, we'll have
                            // inserted a blank default value, and the
                            // offset would be none.
                            // Just insert new entry into the hashmap.
                            *entry = new_entry;
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
                                    let decoded_value = match data.raw_data.value {
                                        None => Ok(None),
                                        Some(value) => value.and_then(|row| {
                                            let mut datums = Vec::with_capacity(source_arity);

                                            // The datums we send to `evaluate` contain the keys
                                            // and the values in order, so indexing works
                                            datums.extend(decoded_key.iter());

                                            datums.extend(row.iter());
                                            datums.extend(data.metadata.iter());
                                            evaluate(
                                                &datums,
                                                &predicates,
                                                &position_or,
                                                &mut row_packer,
                                            )
                                            .map_err(DataflowError::from)
                                        }),
                                    };
                                    // Turns Ok(None) into None, and others into Some(OK) and Some(Err).
                                    // We store errors as well as non-None values, so that they can be
                                    // retracted if new rows show up for the same key.
                                    let new_value = decoded_value.transpose();

                                    let repack_value = |row: Row, row_packer: &mut Row| {
                                        // Re-use a `Row` to
                                        // rebuild a full `Row` with both keys and values, before we
                                        // send them out of this operator
                                        row_packer.clear();
                                        row_packer.extend(decoded_key.iter());
                                        row_packer.extend(row.iter());
                                        row_packer.finish_and_reuse()
                                    };

                                    let old_value = if let Some(new_value) = &new_value {
                                        // Thin out the row to not contain a copy of the
                                        // key columns, cloning when need-be
                                        let thinned_value = new_value
                                            .as_ref()
                                            .map(|full_row| {
                                                row_packer.clear();
                                                row_packer.extend(full_row.iter().skip(key_arity));
                                                row_packer.finish_and_reuse()
                                            })
                                            .map_err(|e| e.clone());
                                        current_values
                                            .insert(decoded_key.clone(), thinned_value)
                                            .map(|res| {
                                                res.map(|v| repack_value(v, &mut row_packer))
                                            })
                                    } else {
                                        current_values.remove(&decoded_key).map(|res| {
                                            res.map(|v| repack_value(v, &mut row_packer))
                                        })
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
                                    // This can never be retracted! But at least it's better to put the source in a
                                    // permanently errored state than to keep on trucking with wrong results.
                                    session.give((Err(err.into()), cap.time().clone(), 1));
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

    result_stream
}
