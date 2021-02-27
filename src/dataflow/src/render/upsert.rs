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
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use log::error;
use repr::{Diff, Row, Timestamp};

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
    let ok_stream = stream.unary_frontier(
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
                                                    row_packer.extend_by_row(&decoded_key);
                                                    row_packer.extend_by_row(&value);
                                                    Ok(Some(row_packer.finish_and_reuse()))
                                                } else {
                                                    Ok(None)
                                                }
                                            }
                                            Err(err) => Err(err),
                                        }
                                    };
                                    if let Ok(decoded_value) = decoded_value {
                                        // TODO: add linear operators such as
                                        // filters and projects?
                                        let old_value = if let Some(new_value) = &decoded_value {
                                            current_values.insert(decoded_key, new_value.clone())
                                        } else {
                                            current_values.remove(&decoded_key)
                                        };
                                        if let Some(old_value) = old_value {
                                            // retract old value
                                            session.give((old_value, cap.time().clone(), -1));
                                        }
                                        if let Some(new_value) = decoded_value {
                                            // give new value
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

    (ok_stream.as_collection(), None)
}
