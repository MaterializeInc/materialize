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

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::{operator, Operator};
use timely::dataflow::operators::map::Map;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use dataflow_types::{DataEncoding, DataflowError, LinearOperator};
use log::error;
use repr::{Datum, Diff, RelationType, Row, RowArena, Timestamp};

use crate::decode::{decode_upsert, DecoderState};
use crate::operator::StreamExt;
use crate::source::{SourceData, SourceOutput};

/// Entrypoint to the upsert-specific transformations involved
/// in rendering a stream that came from an upsert source.
/// Upsert-specific operators are different from the rest of
/// the rendering pipeline in that their input is a stream
/// with two components instead of one, and the second component
/// can be null or empty.
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
    // Currently, the upsert-specific transformations run in the
    // following order:
    // 1. as_of
    // 2. deduplicating records by key
    // 3. decoding records
    // 4. applying linear operator, which currently consist of
    //     a. filter
    //     b. project
    //     c. prepending the key to the value so that the stream becomes
    //        of the format (key, <entire record>)
    //
    // We may want to consider switching the order of the transformations to
    // optimize performance trade-offs. In the current order, by running
    // deduplicating before decoding/linear operators, we're reducing compute at the
    // cost of requiring more memory.
    //
    // In the future, we may want to have optimization hints that enable people
    // to specify that they believe that they have a large number of unique
    // keys, at which point materialize may be more performant if it runs
    // decoding/linear operators before deduplicating.

    // This operator changes the timestamp from capability to message payload,
    // and applies `as_of` frontier compaction. The compaction is important as
    // downstream upsert preparation can compact away updates for the same keys
    // at the same times, and by advancing times we make more of them the same.
    let stream = apply_as_of_frontier(&stream, as_of_frontier);

    // Deduplicate records by key
    let deduplicated = prepare_upsert_by_max_offset(&stream);

    // Decode
    let decoded = decode_upsert(
        &deduplicated,
        encoding,
        key_encoding,
        debug_name,
        worker_index,
    );

    apply_linear_operators(&decoded, linear_operator, src_type)
}

pub fn apply_as_of_frontier<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    as_of_frontier: Antichain<Timestamp>,
) -> Stream<G, (SourceOutput<Vec<u8>, Vec<u8>>, Timestamp)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary(Pipeline, "AppendTimestamp", move |_, _| {
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
    })
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
) -> Stream<G, ((Vec<u8>, SourceData), Timestamp)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary_frontier(
        Exchange::new(move |x: &(SourceOutput<Vec<u8>, Vec<u8>>, Timestamp)| x.0.key.hashed()),
        "UpsertCompaction",
        |_cap, _info| {
            // this is a map of (time) -> ((key) -> (value with max offset))
            let mut values = HashMap::<_, HashMap<_, SourceData>>::new();
            let mut vector = Vec::new();

            move |input, output| {
                // Digest each input, reduce by presented timestamp.
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for (
                        SourceOutput {
                            key,
                            value: new_value,
                            position: new_position,
                            upstream_time_millis: new_upstream_time_millis,
                        },
                        time,
                    ) in vector.drain(..)
                    {
                        let entry = values
                            .entry(cap.delayed(&time))
                            .or_insert_with(HashMap::new)
                            .entry(key)
                            .or_insert_with(Default::default);

                        if let Some(new_offset) = new_position {
                            if let Some(offset) = entry.position {
                                if offset < new_offset {
                                    *entry = SourceData {
                                        value: new_value,
                                        position: new_position,
                                        upstream_time_millis: new_upstream_time_millis,
                                    };
                                }
                            } else {
                                *entry = SourceData {
                                    value: new_value,
                                    position: new_position,
                                    upstream_time_millis: new_upstream_time_millis,
                                };
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

/// Apply a filter followed by a project to an upsert stream.
/// Also, prepend key columns to the beginning of the value so
/// the return stream is `Stream<G, (key, Option<entire record>, Timestamp)>
/// whereas the input stream is `Stream<G, (key, Option<value>, Timestamp)>
fn apply_linear_operators<G>(
    stream: &Stream<G, (Row, Option<Row>, Timestamp)>,
    _linear_operator: &mut Option<LinearOperator>,
    _src_type: &RelationType,
) -> (
    Stream<G, (Row, Option<Row>, Timestamp)>,
    Stream<G, DataflowError>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let mut row_packer = repr::RowPacker::new();

    // just prepend the key to the value without unpacking rows
    let prepended = stream.map({
        move |(key, value, timestamp)| {
            if let Some(value) = value {
                row_packer.extend_by_row(&key);
                row_packer.extend_by_row(&value);
                (key, Some(row_packer.finish_and_reuse()), timestamp)
            } else {
                (key, None, timestamp)
            }
        }
    });
    let scope = &prepended.scope();
    (prepended, operator::empty(scope))
}

pub fn no_arrangement_upsert<G, K, V>(
    stream: &Stream<G, (SourceOutput<Vec<u8>, Vec<u8>>, Timestamp)>,
    mut key_decoder_state: K,
    mut value_decoder_state: V,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    K: DecoderState + 'static,
    V: DecoderState + 'static,
{
    stream.unary_frontier(Pipeline, "LightUpsert", |_cap, _info| {
        // to_send
        // this is a map of (time) -> (capability, ((key) -> (value with max offset)))
        let mut to_send = BTreeMap::<_, (_, HashMap<_, SourceData>)>::new();
        let mut current_values = HashMap::new();
        let mut vector = Vec::new();
        let mut row_packer = repr::RowPacker::new();

        move |input, output| {
            // Digest each input, reduce by presented timestamp.
            input.for_each(|cap, data| {
                data.swap(&mut vector);
                for (
                    SourceOutput {
                        key,
                        value: new_value,
                        position: new_position,
                        upstream_time_millis: new_upstream_time_millis,
                    },
                    time,
                ) in vector.drain(..)
                {
                    if key.is_empty() {
                        error!("{}", "Encountered empty key");
                        continue;
                    }
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

                    // if the time is equal, toss out the row with the
                    // lower offset
                    if let Some(new_offset) = new_position {
                        if let Some(offset) = entry.position {
                            if offset < new_offset {
                                *entry = new_entry;
                            }
                        } else {
                            *entry = new_entry;
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
                        // decode key and value
                        match key_decoder_state.decode_key(&key) {
                            Ok(decoded_key) => {
                                let decoded_value = if data.value.is_empty() {
                                    Ok(None)
                                } else if let Ok(value) = value_decoder_state.decode_upsert_value(
                                    &data.value,
                                    data.position,
                                    data.upstream_time_millis,
                                ) {
                                    if let Some(value) = value {
                                        // prepend key to row
                                        row_packer.extend_by_row(&decoded_key);
                                        row_packer.extend_by_row(&value);
                                        Ok(Some(row_packer.finish_and_reuse()))
                                    } else {
                                        Ok(None)
                                    }
                                } else {
                                    Err(())
                                };
                                if let Ok(decoded_value) = decoded_value {
                                    // TODO: apply linear operator
                                    let old_value = if let Some(new_value) = &decoded_value {
                                        current_values.insert(decoded_key, new_value.clone())
                                    } else {
                                        current_values.remove(&decoded_key)
                                    };
                                    if let Some(old_value) = old_value {
                                        session.give((old_value, cap.time().clone(), -1));
                                    }
                                    if let Some(new_value) = decoded_value {
                                        session.give((new_value, cap.time().clone(), 1));
                                    }
                                }
                            }
                            Err(err) => {
                                error!("{}", err);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            // Discard entries, capabilities for complete times.
            for time in removed_times {
                to_send.remove(&time);
            }
        }
    })
}
