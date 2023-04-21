// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use differential_dataflow::consolidation;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use itertools::Itertools;
use mz_storage_client::types::sources::MzOffset;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::Scope;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, Timestamp};

use mz_ore::collections::{CollectionExt, HashMap};
use mz_repr::{Datum, DatumVec, Diff, Row};
use mz_storage_client::types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UpsertKey([u8; 32]);

thread_local! {
    /// A thread-local datum cache used to calculate hashes
    pub static KEY_DATUMS: RefCell<DatumVec> = RefCell::new(DatumVec::new());
}

/// The hash function used to map upsert keys. It is important that this hash is a cryptographic
/// hash so that there is no risk of collisions. Collisions on SHA256 have a probability of 2^128
/// which is many orders of magnitude smaller than many other events that we don't even think about
/// (e.g bit flips). In short, we can safely assume that sha256(a) == sha256(b) iff a == b.
type KeyHash = Sha256;

impl UpsertKey {
    pub fn from_key(key: Result<&Row, &UpsertError>) -> Self {
        Self::from_iter(key.map(|r| r.iter()))
    }

    pub fn from_value(value: Result<&Row, &UpsertError>, mut key_indices: &[usize]) -> Self {
        Self::from_iter(value.map(|value| {
            value.iter().enumerate().flat_map(move |(idx, datum)| {
                let key_idx = key_indices.get(0)?;
                if idx == *key_idx {
                    key_indices = &key_indices[1..];
                    Some(datum)
                } else {
                    None
                }
            })
        }))
    }

    pub fn from_iter<'a, 'b>(
        key: Result<impl Iterator<Item = Datum<'a>> + 'b, &UpsertError>,
    ) -> Self {
        KEY_DATUMS.with(|key_datums| {
            let mut key_datums = key_datums.borrow_mut();
            // Borrowing the DatumVec gives us a temporary buffer to store datums in that will be
            // automatically cleared on Drop. See the DatumVec docs for more details.
            let mut key_datums = key_datums.borrow();
            let key: Result<&[Datum], Datum> = match key {
                Ok(key) => {
                    for datum in key {
                        key_datums.push(datum);
                    }
                    Ok(&*key_datums)
                }
                Err(UpsertError::Value(err)) => {
                    key_datums.extend(err.for_key.iter());
                    Ok(&*key_datums)
                }
                Err(UpsertError::KeyDecode(err)) => Err(Datum::Bytes(&err.raw)),
                Err(UpsertError::NullKey(_)) => Err(Datum::Null),
            };
            let mut hasher = DigestHasher(KeyHash::new());
            key.hash(&mut hasher);
            Self(hasher.0.finalize().into())
        })
    }
}

struct DigestHasher<H: Digest>(H);

impl<H: Digest> Hasher for DigestHasher<H> {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(&self) -> u64 {
        panic!("digest wrapper used to produce a hash");
    }
}

/// A tuple of the row or error, and the order by value.
/// Will be used to store the state for a key in a hashmap in upsert
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct State(Result<Row, UpsertError>, UpsertOrder);

/// Struct containing a list of values to order by
///
/// The following table shows the possible UpsertOrder values
/// for a particular key with different value types where,
/// - p = the UpsertOrder from previously persisted state
/// - k = the UpsertOrder from incoming kafka ingestion
///
/// | value   | order by |p      |k      | k.cmp(p)                      |
/// |---------|----------|-------|-------|-------------------------------|
/// | Ok(row) | Y        |Some(a)|Some(b)| b.cmp(a)                      |
/// | Err()   | Y        |None   |Some() | k > p always since Some > None|
/// | Ok(row) | N        |None   |Some() | k > p always since Some > None|
/// | Err()   | N        |None   |Some() | k > p always since Some > None|
///
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct UpsertOrder(Option<SmallVec<[i128; 2]>>);

impl From<MzOffset> for UpsertOrder {
    fn from(val: MzOffset) -> Self {
        let mut order = SmallVec::new();
        order.push(val.offset.into());
        UpsertOrder(Some(order))
    }
}

/// Gets the ordering information extracting out the values from the row
/// based on the given indices.
/// The `value` can be either the entire source row or a metadata row.
pub(crate) fn get_order(
    value: &Result<Row, UpsertError>,
    order_by_indices: &[usize],
) -> UpsertOrder {
    let order = match value {
        Ok(row) if !order_by_indices.is_empty() => {
            let order_val = order_by_indices
                .iter()
                .map(|idx| {
                    // Row's iterator is not ExactSizeIterator and the nth() call is slightly expensive
                    // TODO(mouli): optimize this
                    let datum = row
                        .iter()
                        .nth(*idx)
                        .expect("Expected a valid order by index");

                    match datum {
                        // Currently only timestamp and u64 types are supported
                        Datum::Timestamp(ts) => ts.timestamp_nanos().into(),
                        Datum::UInt64(num) => num.into(),
                        datum => panic!(
                            "Only timestamp or offset is supported for order by, instead found {:?} at index {:?}",
                            datum, idx
                        ),
                    }
                })
                .collect();
            Some(order_val)
        }
        _ => None,
    };
    UpsertOrder(order)
}

/// Resumes an upsert computation at `resume_upper` given as inputs a collection of upsert commands
/// and the collection of the previous output of this operator.
pub(crate) fn upsert<G: Scope>(
    input: &Collection<G, (UpsertKey, Option<Result<Row, UpsertError>>, UpsertOrder), Diff>,
    mut key_indices: Vec<usize>,
    order_by_indices: SmallVec<[usize; 2]>,
    resume_upper: Antichain<G::Timestamp>,
    previous: Collection<G, Result<Row, DataflowError>, Diff>,
    previous_token: Option<Rc<dyn Any>>,
) -> Collection<G, Result<Row, DataflowError>, Diff>
where
    G::Timestamp: TotalOrder,
{
    // Sort key indices to ensure we can construct the key by iterating over the datums of the row
    key_indices.sort_unstable();

    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    let mut input = builder.new_input(
        &input.inner,
        Exchange::new(move |((key, _, _), _, _)| UpsertKey::hashed(key)),
    );

    // We only care about UpsertValueError since this is the only error that we can retract
    let previous = previous.flat_map(move |result| {
        let value = match result {
            Ok(ok) => Ok(ok),
            Err(DataflowError::EnvelopeError(err)) => match *err {
                EnvelopeError::Upsert(err) => Err(err),
                _ => return None,
            },
            Err(_) => return None,
        };
        Some((UpsertKey::from_value(value.as_ref(), &key_indices), value))
    });
    let mut previous = builder.new_input(
        &previous.inner,
        Exchange::new(|((key, _), _, _)| UpsertKey::hashed(key)),
    );
    let (mut output_handle, output) = builder.new_output();

    builder.build(move |caps| async move {
        let mut output_cap = caps.into_element();

        let mut snapshot = vec![];

        // In the first phase we will collect all the updates from our output that are not beyond
        // resume_upper. This will be the seed state for the command processing below.
        while let Some(event) = previous.next_mut().await {
            match event {
                AsyncEvent::Data(_cap, data) => {
                    snapshot.extend(
                        data.drain(..)
                            .filter(|(_row, ts, _diff)| !resume_upper.less_equal(ts))
                            .map(|(row, _ts, diff)| (row, diff)),
                    );
                }
                AsyncEvent::Progress(upper) => {
                    if PartialOrder::less_equal(&resume_upper, &upper) {
                        break;
                    }
                }
            }
        }
        drop(previous_token);
        while let Some(_event) = previous.next().await {
            // Exchaust the previous input. It is expected to immediately reach the empty
            // antichain since we have dropped its token.
        }
        consolidation::consolidate(&mut snapshot);

        // The main key->value used to store previous values.
        let mut state = HashMap::new();

        // A re-usable buffer of changes, per key. This is a `std`
        // `HashMap` so we can use `HashMap::drain` below.
        #[allow(clippy::disallowed_types)]
        let mut commands_state = std::collections::HashMap::new();

        for ((key, value), diff) in snapshot {
            assert_eq!(diff, 1, "invalid upsert state");
            let order = get_order(&value, &order_by_indices);
            state.insert(key, State(value, order));
        }

        // Now can can resume consuming the collection
        let mut stash = vec![];
        let mut output_updates = vec![];
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());
        while let Some(event) = input.next_mut().await {
            match event {
                AsyncEvent::Data(_cap, data) => {
                    if PartialOrder::less_equal(&input_upper, &resume_upper) {
                        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
                    }

                    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
                        assert!(diff > 0, "invalid upsert input");
                        (time, key, Reverse(order), value)
                    }));
                }
                AsyncEvent::Progress(upper) => {
                    stash.sort_unstable();

                    // Find the prefix that we can emit
                    let idx = stash.partition_point(|(ts, _, _, _)| !upper.less_equal(ts));

                    // Read the previous values _per key_ out of `state`, recording it
                    // along with the value with the _latest timestamp for that key_.
                    commands_state.clear();
                    for (_, key, _, _) in stash.iter().take(idx) {
                        commands_state
                            .entry(*key)
                            .or_insert_with(|| state.get(key).cloned());
                    }

                    // From the prefix that can be emitted we can deduplicate based on (ts, key) in
                    // order to only process the command with the maximum order within the (ts,
                    // key) group. This is achieved by wrapping order in `Reverse(order)` above.
                    let mut commands = stash.drain(..idx).dedup_by(|a, b| {
                        let ((a_ts, a_key, _, _), (b_ts, b_key, _, _)) = (a, b);
                        a_ts == b_ts && a_key == b_key
                    });

                    // Upsert the values into `commands_state`, by recording the latest
                    // value (or deletion). These will be synced at the end to the `state`.
                    while let Some((ts, key, Reverse(new_order), value)) = commands.next() {
                        let command_state = commands_state
                            .get_mut(&key)
                            .expect("key missing from commands_state");
                        match (&command_state, value) {
                            (Some(State(old_value, old_order)), Some(new_value)) => {
                                if new_order.cmp(old_order).is_gt() {
                                    output_updates.push((old_value.to_owned(), ts.clone(), -1));
                                    output_updates.push((new_value.clone(), ts, 1));
                                    command_state.replace(State(new_value, new_order));
                                }
                            }
                            (None, Some(new_value)) => {
                                // No previous state exists, insert new value
                                output_updates.push((new_value.clone(), ts, 1));
                                command_state.replace(State(new_value, new_order));
                            }
                            (Some(State(old_value, old_order)), None) => {
                                if new_order.cmp(old_order).is_gt() {
                                    output_updates.push((old_value.to_owned(), ts, -1));
                                    command_state.take();
                                }
                            }
                            (None, None) => {} // No-op, trying to remove a key which does not exist in state
                        }
                    }

                    // Record the changes in `state`.
                    for (key, command_state) in commands_state.drain() {
                        match command_state {
                            Some(value) => {
                                state.insert(key, value);
                            }
                            None => {
                                state.remove(&key);
                            }
                        }
                    }

                    // Emit the _consolidated_ changes to the output.
                    output_handle
                        .give_container(&output_cap, &mut output_updates)
                        .await;
                    if let Some(ts) = upper.as_option() {
                        output_cap.downgrade(ts);
                    }
                    input_upper = upper;
                }
            }
        }
    });

    output.as_collection().map(|result| match result {
        Ok(ok) => Ok(ok),
        Err(err) => Err(DataflowError::from(EnvelopeError::Upsert(err))),
    })
}
