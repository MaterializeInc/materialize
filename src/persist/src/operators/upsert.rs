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
use std::fmt::Debug;
use std::hash::Hash;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Concat, Map};
use timely::dataflow::operators::{Delay, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::indexed::runtime::{StreamReadHandle, StreamWriteHandle};
use crate::operators::replay::Replay;
use crate::operators::stream::Persist;
use crate::operators::stream::RetractFutureUpdates;

use persist_types::Codec;

/// Extension trait for [`Stream`].
pub trait PersistentUpsert<G, K: Codec, V: Codec, T> {
    /// Turns a stream of keyed upserts into a stream of differential updates and also persists
    /// upserts to the [`StreamWriteHandle`] given in `persist_config`.
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
    ///
    /// There are important invariants that this method will maintain for the persisted collection
    /// pointed to by `persist_config`. Any other actor that interacts with it must also ensure them.
    /// The invariants only apply to consolidated data, that is when all diffs are summed up for a
    /// given key/value pair. The invariants are:
    ///
    ///  - Each update in the collection must have a diff of `1` or `0`. That is an update either
    ///  exists exactly once or it doesn't exist.
    ///
    ///  - For each key, there can only be one value that it maps to. That is, keys must be unique.
    ///
    /// **Note:** This does only persist upserts but not seal them. Use together with `seal()` to
    /// also seal the persistent collection.
    fn persistent_upsert(
        &self,
        name: &str,
        as_of_frontier: Antichain<u64>,
        persist_config: PersistentUpsertConfig<K, V>,
    ) -> Stream<G, ((K, V), u64, isize)>
    where
        G: Scope<Timestamp = u64>;
}

/// Persist configuration for persistent upsert.
#[derive(Debug)]
pub struct PersistentUpsertConfig<K: Codec, V: Codec> {
    /// The timestamp up to which which data should be read when restoring.
    upper_seal_ts: u64,

    /// [`StreamReadHandle`] for the collection that we should persist to.
    read_handle: StreamReadHandle<K, V>,

    /// [`StreamWriteHandle`] for the collection that we should persist to.
    pub write_handle: StreamWriteHandle<K, V>,
}

impl<K: Codec, V: Codec> PersistentUpsertConfig<K, V> {
    /// Creates a new [`PersistentUpsertConfig`] from the given parts.
    pub fn new(
        upper_seal_ts: u64,
        read_handle: StreamReadHandle<K, V>,
        write_handle: StreamWriteHandle<K, V>,
    ) -> Self {
        PersistentUpsertConfig {
            upper_seal_ts,
            read_handle,
            write_handle,
        }
    }
}

impl<G, K, V, T> PersistentUpsert<G, K, V, T> for Stream<G, (K, Option<V>, T)>
where
    G: Scope<Timestamp = u64>,
    K: timely::Data + timely::ExchangeData + Codec + Debug + Hash + Eq,
    V: timely::Data + timely::ExchangeData + Codec + Debug + Hash + Eq,
    T: timely::Data + timely::ExchangeData + Ord,
{
    fn persistent_upsert(
        &self,
        name: &str,
        as_of_frontier: Antichain<u64>,
        persist_config: PersistentUpsertConfig<K, V>,
    ) -> Stream<G, ((K, V), u64, isize)>
    where
        G: Scope<Timestamp = u64>,
    {
        let operator_name = format!("persistent_upsert({})", name);

        let (restored_upsert_oks, _state_errs) = {
            let snapshot = persist_config
                .read_handle
                .snapshot()
                .expect("cannot take snapshot");
            let (restored_oks, restored_errs) = self.scope().replay(snapshot);
            let restored_upsert_oks = restored_oks.filter_and_retract_future_updates(
                name,
                persist_config.write_handle.clone(),
                persist_config.upper_seal_ts,
            );
            (restored_upsert_oks, restored_errs)
        };

        let mut differential_state_ingester = Some(DifferentialStateIngester::new());

        let upsert_as_of_frontier = as_of_frontier.clone();

        let new_upsert_oks = self.binary_frontier(
            &restored_upsert_oks,
            Exchange::new(move |(key, _value, _ts): &(K, Option<V>, T)| key.hashed()),
            Exchange::new(move |((key, _data), _ts, _diff): &((K, _), _, _)| key.hashed()),
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

                let mut input_buffer = Vec::new();
                let mut state_input_buffer = Vec::new();

                move |input, state_input, output| {
                    state_input.for_each(|_time, data| {
                        data.swap(&mut state_input_buffer);
                        for state_update in state_input_buffer.drain(..) {
                            log::trace!(
                                "In {}, restored upsert: {:?}",
                                operator_name,
                                state_update
                            );

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

                        log::trace!(
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

        let (new_upsert_oks, _new_upsert_persist_errs) =
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

        new_upsert_oks.concat(&restored_upsert_oks)
    }
}

/// Ingests differential updates, consolidates them, and emits a final `HashMap` that contains the
/// consolidated upsert state.
struct DifferentialStateIngester<K, V> {
    differential_state: HashMap<(K, V), isize>,
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

    fn add_update(&mut self, update: ((K, V), u64, isize)) {
        let ((k, v), _ts, diff) = update;

        *self.differential_state.entry((k, v)).or_default() += diff;
    }

    fn finish(mut self) -> HashMap<K, V> {
        self.differential_state.retain(|_k, diff| *diff > 0);

        let mut state = HashMap::new();

        for ((key, value), diff) in self.differential_state.into_iter() {
            // our state must be internally consistent
            assert!(diff == 1, "Diff for ({:?}, {:?}) is {}", key, value, diff);
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

#[cfg(test)]
mod tests {
    // TODO(aljoscha): add tests
}
