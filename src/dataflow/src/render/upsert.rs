// Copyright Materialize, Inc. All rihts reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::{operator, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use dataflow_types::{DataEncoding, DataflowError, LinearOperator, Timestamp};
use repr::{Datum, RelationType, Row, RowArena};

use crate::decode::decode_upsert;
use crate::operator::StreamExt;
use crate::source::SourceOutput;

#[allow(clippy::too_many_arguments)]
pub fn pre_arrange_from_upsert_transforms<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    encoding: DataEncoding,
    key_encoding: DataEncoding,
    debug_name: &str,
    worker_index: usize,
    as_of_frontier: Antichain<Timestamp>,
    linear_operator: &mut Option<LinearOperator>,
    src_type: &RelationType,
) -> (
    Stream<G, (Row, Option<Row>, Timestamp)>,
    Stream<G, DataflowError>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // This operator changes the timestamp from capability to message payload,
    // and applies `as_of` frontier compaction. The compaction is important as
    // downstream upsert preparation can compact away updates for the same keys
    // at the same times, and by advancing times we make more of them the same.
    let stream = stream.unary(Pipeline, "AppendTimestamp", move |_, _| {
        let mut vector = Vec::new();
        move |input, output| {
            input.for_each(|cap, data| {
                data.swap(&mut vector);
                let mut time = cap.time().clone();
                time.advance_by(as_of_frontier.borrow());
                output
                    .session(&cap)
                    .give_iterator(vector.drain(..).map(|x| (x, time.clone())));
            });
        }
    });

    // Deduplicate records by key, decode, and then upsert arrange them.
    let deduplicated = prepare_upsert_by_max_offset(&stream);

    let decoded = decode_upsert(
        &deduplicated,
        encoding,
        key_encoding,
        debug_name,
        worker_index,
    );

    if let Some(operator) = linear_operator {
        // skip applying the linear operator when it is trivial
        if operator.predicates.is_empty()
            && operator.projection == (0..src_type.arity()).collect::<Vec<_>>()
        {
            *linear_operator = None;
        } else {
            return apply_linear_operators(&decoded, &operator, src_type);
        }
    }
    let scope = &decoded.scope();
    (decoded, operator::empty(scope))
}

/// Produces at most one entry for each `(key, time)` pair.
///
/// The incoming stream of `(key, (val, off), time)` records may have many
/// entries with the same `key` and `time`. We are able to reduce this to
/// at most one record for each pair, by retaining only the record with the
/// greatest offset: its action summarizes the sequence of many actions that
/// occur at the same moment and so are not distinguishable.
fn prepare_upsert_by_max_offset<G>(
    stream: &Stream<G, (SourceOutput<Vec<u8>, Vec<u8>>, Timestamp)>,
) -> Stream<G, ((Vec<u8>, (Vec<u8>, Option<i64>)), Timestamp)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary_frontier(
        Exchange::new(move |x: &(SourceOutput<Vec<u8>, Vec<u8>>, Timestamp)| x.0.key.hashed()),
        "UpsertCompaction",
        |_cap, _info| {
            let mut values = HashMap::<_, HashMap<_, (Vec<u8>, Option<i64>)>>::new();
            let mut vector = Vec::new();

            move |input, output| {
                // Digest each input, reduce by presented timestamp.
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for (
                        SourceOutput {
                            key,
                            value: val,
                            position,
                        },
                        time,
                    ) in vector.drain(..)
                    {
                        let value = values
                            .entry(cap.delayed(&time))
                            .or_insert_with(HashMap::new)
                            .entry(key)
                            .or_insert_with(Default::default);

                        if let Some(new_offset) = position {
                            if let Some(offset) = value.1 {
                                if offset < new_offset {
                                    *value = (val, position);
                                }
                            } else {
                                *value = (val, position);
                            }
                        }
                    }
                });

                // Produce (key, val) pairs at any complete times.
                for (cap, map) in values.iter_mut() {
                    if !input.frontier.less_equal(cap.time()) {
                        let mut session = output.session(cap);
                        for (key, val) in map.drain() {
                            session.give(((key, val), cap.time().clone()))
                        }
                    }
                }
                // Discard entries, capabilities for complete times.
                values.retain(|_cap, map| !map.is_empty());
            }
        },
    )
}

fn apply_linear_operators<G>(
    stream: &Stream<G, (Row, Option<Row>, Timestamp)>,
    operator: &LinearOperator,
    src_type: &RelationType,
) -> (
    Stream<G, (Row, Option<Row>, Timestamp)>,
    Stream<G, DataflowError>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Determine replacement values for unused columns.
    // This is copied over from applying linear operators to
    // the non-upsert case.
    // At this point, the stream consists of (key, key+payload, time)
    // so it is ok to delete replace unused values in key+payload
    // but not to replace unused values in key because that would cause
    // errors in arrange_from_upsert
    let position_or = (0..src_type.arity())
        .map(|col| {
            if operator.projection.contains(&col) {
                Ok(col)
            } else {
                Err({
                    // TODO(frank): This could be `Datum::Null` if we
                    // are certain that no readers will consult it and
                    // believe it to be a non-null value. That is the
                    // intent, but it is not yet clear that we ensure
                    // this.
                    let typ = &src_type.column_types[col];
                    if typ.nullable {
                        Datum::Null
                    } else {
                        typ.scalar_type.dummy_datum()
                    }
                })
            }
        })
        .collect::<Vec<_>>();

    // If a row does not match a predicate whose support lies in the key
    // columns, it can be tossed away entirely. If a row does not match
    // a predicate whose support is not contained in the key columns, it
    // must be replaced with (key, None) in case if there was previously a
    // row with the same key that matched the predicate.
    let key_col_len = src_type.keys[0].len();
    let mut predicates = operator.predicates.clone();
    let mut key_predicates = Vec::new();
    predicates.retain(|p| {
        let key_predicate = p.support().into_iter().all(|c| c < key_col_len);
        if key_predicate {
            key_predicates.push(p.clone());
        }
        !key_predicate
    });

    let mut storage = Vec::new();
    let mut filtered_storage = Vec::new();
    let mut row_packer = repr::RowPacker::new();

    stream.unary_fallible(Pipeline, "UpsertLinearFallible", move |_, _| {
        move |input, ok_output, err_output| {
            input.for_each(|time, data| {
                let mut ok_session = ok_output.session(&time);
                let mut err_session = err_output.session(&time);
                data.swap(&mut storage);
                for (key, value, time) in storage.drain(..) {
                    let temp_storage = RowArena::new();
                    let datums = key.unpack();
                    let key_pred_eval = key_predicates
                        .iter()
                        .map(|predicate| predicate.eval(&datums, &temp_storage))
                        .find(|result| result != &Ok(Datum::True));
                    match key_pred_eval {
                        None => {
                            if let Some(value) = value {
                                let datums = value.unpack();
                                let pred_eval = predicates
                                    .iter()
                                    .map(|predicate| predicate.eval(&datums, &temp_storage))
                                    .find(|result| result != &Ok(Datum::True));
                                match pred_eval {
                                    None => {
                                        let value = Some(row_packer.pack(position_or.iter().map(
                                            |pos_or| match pos_or {
                                                Result::Ok(index) => datums[*index],
                                                Result::Err(datum) => *datum,
                                            },
                                        )));
                                        filtered_storage.push((key, value, time));
                                    }
                                    Some(Ok(Datum::False)) => {
                                        filtered_storage.push((key, None, time))
                                    }
                                    Some(Ok(Datum::Null)) => {
                                        filtered_storage.push((key, None, time))
                                    }
                                    Some(Ok(x)) => {
                                        panic!("Predicate evaluated to invalid value: {:?}", x)
                                    }
                                    Some(Err(x)) => {
                                        err_session.give(DataflowError::from(x));
                                    }
                                }
                            } else {
                                filtered_storage.push((key, None, time));
                            }
                        }
                        Some(Ok(Datum::False)) | Some(Ok(Datum::Null)) => {}
                        Some(Ok(x)) => panic!("Predicate evaluated to invalid value: {:?}", x),
                        Some(Err(x)) => {
                            err_session.give(DataflowError::from(x));
                        }
                    };
                }
                if !filtered_storage.is_empty() {
                    ok_session.give_vec(&mut filtered_storage);
                }
            })
        }
    })
}
