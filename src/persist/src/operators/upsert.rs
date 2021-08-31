// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that turns a stream of keyed upserts into a stream of differential updates.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

/// Extension trait for [`Stream`].
pub trait PersistentUpsert<G, K, V, T> {
    /// Turns a stream of keyed upserts into a stream of differential updates.
    ///
    /// The input is a stream of `(Key, Option<Val>, timestamp)` tuples, where "timestamp"
    /// expresses a happens-before relationship and could, for example, be a Kafka offset.  The
    /// contents of the collection are defined key-by-key, where each optional value in sequence either
    /// replaces or removes the existing value, should it exist.
    ///
    /// The `as_of_frontier` indicates a frontier that can be used to compact input timestamps
    /// without affecting the results. We *should* apply it, both because it improves performance, and
    /// because potentially incorrect results are visible in sinks.
    ///
    /// This method is only implemented for totally ordered times, as we do not yet understand what
    /// a "sequence" of upserts would mean for partially ordered timestamps.
    fn persistent_upsert(&self, as_of_frontier: Antichain<u64>) -> Stream<G, ((K, V), u64, isize)>
    where
        G: Scope<Timestamp = u64>;
}

impl<G, K, V, T> PersistentUpsert<G, K, V, T> for Stream<G, (K, Option<V>, T)>
where
    G: Scope<Timestamp = u64>,
    K: timely::Data + timely::ExchangeData + Hash + Eq,
    V: timely::Data + timely::ExchangeData,
    T: timely::Data + timely::ExchangeData + Ord,
{
    fn persistent_upsert(&self, as_of_frontier: Antichain<u64>) -> Stream<G, ((K, V), u64, isize)>
    where
        G: Scope<Timestamp = u64>,
    {
        let result_stream = self.unary_frontier(
            Exchange::new(move |(key, _value, _ts): &(K, Option<V>, T)| key.hashed()),
            "Upsert",
            move |_cap, _info| {
                // This is a map of (time) -> (capability, ((key) -> (value with max offset))). This
                // is a BTreeMap because we want to ensure that if we receive (key1, value1, time
                // 5) and (key1, value2, time 7) that we send (key1, value1, time 5) before (key1,
                // value2, time 7).
                //
                // This is a staging area, where we group incoming updates by timestamp (the timely
                // timestamp) and disambiguate by the offset (also called "timestamp" above) if
                // necessary.
                let mut to_send = BTreeMap::<_, (_, HashMap<_, (Option<V>, T)>)>::new();

                // This is a map from key -> value. We store the latest value for a given key that
                // way we know what to retract if a new value with the same key comes along.
                let mut current_values = HashMap::new();

                let mut vector = Vec::new();

                move |input, output| {
                    // Digest each input, reduce by presented timestamp.
                    input.for_each(|cap, data| {
                        data.swap(&mut vector);
                        for (key, value, offset) in vector.drain(..) {
                            let mut time = cap.time().clone();
                            time.advance_by(as_of_frontier.borrow());

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
                        if !input.frontier.less_equal(time) {
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

        result_stream
    }
}

#[cfg(test)]
mod tests {
    // TODO(aljoscha): add tests
}
