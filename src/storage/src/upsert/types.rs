// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # State-management for UPSERT.
//!
//! This module and provide structures for use within an UPSERT
//! operator implementation.
//!
//! UPSERT is a effectively a process which transforms a `Stream<(Key, Option<Data>)>`
//! into a differential collection, by indexing the data based on the key.
//!
//! _This module does not implement this transformation, instead exposing APIs designed
//! for use within an UPSERT operator. There is one exception to this: `consolidate_chunk`
//! implements an efficient upsert-like transformation to re-index a collection using the
//! _output collection_ of an upsert transformation. More on this below.
//!
//! ## `UpsertState`
//!
//! Its primary export is `UpsertState`, which wraps an `UpsertStateBackend` and provides 3 APIs:
//!
//! ### `multi_get`
//! `multi_get` returns the current value for a (unique) set of keys. To keep implementations
//! efficient, the set of keys is an iterator, and results are written back into another parallel
//! iterator. In addition to returning the current values, implementations must also return the
//! _size_ of those values _as they are stored within the implementation_. Implementations are
//! required to chunk large iterators if they need to operate over smaller batches.
//!
//! `multi_get` is implemented directly with `UpsertStateBackend::multi_get`.
//!
//! ### `multi_put`
//! Update or delete values for a set of keys. To keep implementations efficient, the set
//! of updates is an iterator. Implementations are also required to return the difference
//! in values and total size after processing the updates. To simplify this (and because
//! in the `upsert` usecase we have this data readily available), the updates are input
//! with the size of the current value (if any) that was returned from a previous `multi_get`.
//! Implementations are required to chunk large iterators if they need to operate over smaller
//! batches.
//!
//! `multi_put` is implemented directly with `UpsertStateBackend::multi_put`.
//!
//! ### `consolidate_chunk`
//!
//! `consolidate_chunk` re-indexes an UPSERT collection based on its _output collection_ (as
//! opposed to its _input `Stream`_. Please see the docs on `consolidate_chunk` and `StateValue`
//! for more information.
//!
//! `consolidate_chunk` is implemented with both `UpsertStateBackend::multi_put` and
//! `UpsertStateBackend::multi_get`
//!
//! ## Order Keys
//!
//! In practice, the input stream for UPSERT collections includes an _order key_. This is used to
//! sort data with the same key occurring in the same timestamp. This module provides support
//! for serializing and deserializing order keys with their associated data. Being able to ingest
//! data on non-frontier boundaries requires this support.
//!
//! A consequence of this is that tombstones with an order key can be stored within the state.
//! There is currently no support for cleaning these tombstones up, as they are considered rare and
//! small enough.
//!
//! Because `consolidate_chunk` handles data that consolidates correctly, it does not handle
//! order keys.
//!
//!
//! ## A note on state size
//!
//! The `UpsertStateBackend` trait requires implementations report _relatively accurate_ information about
//! how the state size changes over time. Note that it does NOT ask the implementations to give
//! accurate information about actual resource consumption (like disk space including space
//! amplification), and instead is just asking about the size of the values, after they have been
//! encoded. For implementations like `RocksDB`, these may be highly accurate (it literally
//! reports the encoded size as written to the RocksDB API, and for others like the
//! `InMemoryHashMap`, they may be rough estimates of actual memory usage. See
//! `StateValue::memory_size` for more information.
//!
//! Note also that after consolidation, additional space may be used if `StateValue` is
//! used.
//!

use std::fmt;
use std::num::Wrapping;
use std::sync::Arc;
use std::time::Instant;

use bincode::Options;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::{Diff, GlobalId};
use serde::{Serialize, de::DeserializeOwned};

use crate::metrics::upsert::{UpsertMetrics, UpsertSharedMetrics};
use crate::statistics::SourceStatistics;
use crate::upsert::{UpsertKey, UpsertValue};

/// The default set of `bincode` options used for consolidating
/// upsert updates (and writing values to RocksDB).
pub type BincodeOpts = bincode::config::DefaultOptions;

/// Build the default `BincodeOpts`.
pub fn upsert_bincode_opts() -> BincodeOpts {
    // We don't allow trailing bytes, for now,
    // and use varint encoding for space saving.
    bincode::DefaultOptions::new()
}

/// The result type for `multi_get`.
/// The value and size are stored in individual `Option`s so callees
/// can reuse this value as they overwrite this value, keeping
/// track of the previous metadata. Additionally, values
/// may be `None` for tombstones.
#[derive(Clone)]
pub struct UpsertValueAndSize<T, O> {
    /// The value, if there was one.
    pub value: Option<StateValue<T, O>>,
    /// The size of original`value` as persisted,
    /// Useful for users keeping track of statistics.
    pub metadata: Option<ValueMetadata<u64>>,
}

impl<T, O> std::fmt::Debug for UpsertValueAndSize<T, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpsertValueAndSize")
            .field("value", &self.value)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<T, O> Default for UpsertValueAndSize<T, O> {
    fn default() -> Self {
        Self {
            value: None,
            metadata: None,
        }
    }
}

/// Metadata about an existing value in the upsert state backend, as returned
/// by `multi_get`.
#[derive(Copy, Clone, Debug)]
pub struct ValueMetadata<S> {
    /// The size of the value.
    pub size: S,
    /// If the value is a tombstone.
    pub is_tombstone: bool,
}

/// A value to put in with `multi_put`.
#[derive(Clone, Debug)]
pub struct PutValue<V> {
    /// The new value, or a `None` to indicate a delete.
    pub value: Option<V>,
    /// The value of the previous value for this key, if known.
    /// Passed into efficiently calculate statistics.
    pub previous_value_metadata: Option<ValueMetadata<i64>>,
}

/// A value to put in with a `multi_merge`.
pub struct MergeValue<V> {
    /// The value of the merge operand to write to the backend.
    pub value: V,
    /// The 'diff' of this merge operand value, used to estimate the overall size diff
    /// of the working set after this merge operand is merged by the backend.
    pub diff: Diff,
}

/// `UpsertState` has 2 modes:
/// - Normal operation
/// - Consolidation.
///
/// This struct and its substructs are helpers to simplify the logic that
/// individual `UpsertState` implementations need to do to manage these 2 modes.
///
/// Normal operation is simple, we just store an ordinary `UpsertValue`, and allow the implementer
/// to store it any way they want. During consolidation, the logic is more complex.
/// See the docs on `StateValue::merge_update` for more information.
///
/// Note also that this type is designed to support _partial updates_. All values are
/// associated with an _order key_ `O` that can be used to determine if a value existing in the
/// `UpsertStateBackend` occurred before or after a value being considered for insertion.
///
/// `O` typically required to be `: Default`, with the default value sorting below all others.
/// During consolidation, values consolidate correctly (as they are actual
/// differential updates with diffs), so order keys are not required.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum StateValue<T, O> {
    Consolidating(Consolidating),
    Value(Value<T, O>),
}

impl<T, O> std::fmt::Debug for StateValue<T, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateValue::Consolidating(c) => std::fmt::Display::fmt(c, f),
            StateValue::Value(_) => write!(f, "Value"),
        }
    }
}

/// A totally consolidated value stored within the `UpsertStateBackend`.
///
/// This type contains support for _tombstones_, that contain an _order key_,
/// and provisional values.
///
/// What is considered finalized and provisional depends on the implementation
/// of the UPSERT operator: it might consider everything that it writes to its
/// state finalized, and assume that what it emits will be written down in the
/// output exactly as presented. Or it might consider everything it writes down
/// provisional, and only consider updates that it _knows_ to be persisted as
/// finalized.
///
/// Provisional values should only be considered while still "working off"
/// updates with the same timestamp at which the provisional update was
/// recorded.
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub enum Value<T, O> {
    FinalizedValue(UpsertValue),
    Tombstone,
    ProvisionalValue {
        // We keep the finalized value around, because the provisional value is
        // only valid when processing updates at the same timestamp. And at any
        // point we might still require access to the finalized value.
        finalized_value: Option<UpsertValue>,
        // A provisional value of `None` is a provisional tombstone.
        //
        // WIP: We can also box this, to keep the size of StateValue as it was
        // previously.
        provisional_value: (Option<UpsertValue>, T, O),
    },
}

/// A value as produced during consolidation.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize, Debug)]
pub struct Consolidating {
    #[serde(with = "serde_bytes")]
    value_xor: Vec<u8>,
    len_sum: Wrapping<i64>,
    checksum_sum: Wrapping<i64>,
    diff_sum: Wrapping<i64>,
}

impl fmt::Display for Consolidating {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Consolidating")
            .field("len_sum", &self.len_sum)
            .field("checksum_sum", &self.checksum_sum)
            .field("diff_sum", &self.diff_sum)
            .finish_non_exhaustive()
    }
}

impl<T, O> StateValue<T, O> {
    /// A finalized, that is (assumed) persistent, value occurring at some order
    /// key.
    pub fn finalized_value(value: UpsertValue) -> Self {
        Self::Value(Value::FinalizedValue(value))
    }

    #[allow(unused)]
    /// A tombstoned value occurring at some order key.
    pub fn tombstone() -> Self {
        Self::Value(Value::Tombstone)
    }

    /// Whether the value is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        match self {
            Self::Value(Value::Tombstone) => true,
            _ => false,
        }
    }

    /// Pull out the `Value` value for a `StateValue`, after `ensure_decoded` has been called.
    pub fn into_decoded(self) -> Value<T, O> {
        match self {
            Self::Value(value) => value,
            _ => panic!("called `into_decoded without calling `ensure_decoded`"),
        }
    }

    /// The size of a `StateValue`, in memory. This is:
    /// 1. only used in the `InMemoryHashMap` implementation.
    /// 2. An estimate (it only looks at value sizes, and not errors)
    ///
    /// Other implementations may use more accurate accounting.
    pub fn memory_size(&self) -> u64 {
        match self {
            // Similar to `Row::byte_len`, we add the heap size and the size of the value itself.
            Self::Consolidating(Consolidating { value_xor, .. }) => {
                u64::cast_from(value_xor.len()) + u64::cast_from(std::mem::size_of::<Self>())
            }
            Self::Value(Value::FinalizedValue(Ok(row), ..)) => {
                // `Row::byte_len` includes the size of `Row`, which is also in `Self`, so we
                // subtract it.
                u64::cast_from(row.byte_len()) - u64::cast_from(std::mem::size_of::<mz_repr::Row>())
                // This assumes the size of any `O` instantiation is meaningful (i.e. not a heap
                // object).
                + u64::cast_from(std::mem::size_of::<Self>())
            }
            Self::Value(Value::ProvisionalValue {
                finalized_value,
                provisional_value,
            }) => {
                finalized_value.as_ref().map(|v| match v.as_ref() {
                    Ok(row) =>
                        // The finalized value is boxed, so the size of Row is
                        // not included in the outer size of Self. We therefore
                        // don't subtract it here like for the other branches.
                        u64::cast_from(row.byte_len())
                        // Add the size of the order, because it's also behind
                        // the box.
                        + u64::cast_from(std::mem::size_of::<O>()),
                    // Assume errors are rare enough to not move the needle.
                    Err(_) => 0,
                }).unwrap_or(0)
                +
                provisional_value.0.as_ref().map(|v| match v{
                    Ok(row) =>
                        // `Row::byte_len` includes the size of `Row`, which is
                        // also in `Self`, so we subtract it.
                        u64::cast_from(row.byte_len()) - u64::cast_from(std::mem::size_of::<mz_repr::Row>()),
                        // The size of order is already included in the outer
                        // size of self.
                    // Assume errors are rare enough to not move the needle.
                    Err(_) => 0,
                }).unwrap_or(0)
                // This assumes the size of any `O` instantiation is meaningful (i.e. not a heap
                // object).
                + u64::cast_from(std::mem::size_of::<Self>())
            }
            Self::Value(Value::Tombstone) => {
                // This assumes the size of any `O` instantiation is meaningful (i.e. not a heap
                // object).
                u64::cast_from(std::mem::size_of::<Self>())
            }
            Self::Value(Value::FinalizedValue(Err(_), ..)) => {
                // Assume errors are rare enough to not move the needle.
                0
            }
        }
    }
}

impl<T: Eq, O> StateValue<T, O> {
    /// Creates a new provisional value, occurring at some order key, observed
    /// at the given timestamp.
    pub fn new_provisional_value(
        provisional_value: UpsertValue,
        provisional_ts: T,
        order: O,
    ) -> Self {
        Self::Value(Value::ProvisionalValue {
            finalized_value: None,
            provisional_value: (Some(provisional_value), provisional_ts, order),
        })
    }

    /// Creates a provisional value, that retains the finalized value along with
    /// its order in this `StateValue`, if any.
    ///
    /// We record the finalized value, so that we can present it when needed or
    /// when trying to read a provisional value at a different timestamp.
    pub fn into_provisional_value(
        self,
        provisional_value: UpsertValue,
        provisional_ts: T,
        provisional_order: O,
    ) -> Self {
        match self {
            StateValue::Value(Value::FinalizedValue(value)) => {
                StateValue::Value(Value::ProvisionalValue {
                    finalized_value: Some(value),
                    provisional_value: (Some(provisional_value), provisional_ts, provisional_order),
                })
            }
            StateValue::Value(Value::Tombstone) => StateValue::Value(Value::ProvisionalValue {
                finalized_value: None,
                provisional_value: (Some(provisional_value), provisional_ts, provisional_order),
            }),
            StateValue::Value(Value::ProvisionalValue {
                finalized_value,
                provisional_value: _,
            }) => StateValue::Value(Value::ProvisionalValue {
                finalized_value,
                provisional_value: (Some(provisional_value), provisional_ts, provisional_order),
            }),
            StateValue::Consolidating(_) => {
                panic!("called `into_provisional_value` without calling `ensure_decoded`")
            }
        }
    }

    /// Creates a new provisional tombstone occurring at some order key,
    /// observed at the given timestamp.
    pub fn new_provisional_tombstone(provisional_ts: T, order: O) -> Self {
        Self::Value(Value::ProvisionalValue {
            finalized_value: None,
            provisional_value: (None, provisional_ts, order),
        })
    }

    /// Creates a provisional tombstone, that retains the finalized value along
    /// with its order in this `StateValue`, if any.
    ///
    /// We record the current finalized value, so that we can present it when
    /// needed or when trying to read a provisional value at a different
    /// timestamp.
    pub fn into_provisional_tombstone(self, provisional_ts: T, provisional_order: O) -> Self {
        match self {
            StateValue::Value(Value::FinalizedValue(value)) => {
                StateValue::Value(Value::ProvisionalValue {
                    finalized_value: Some(value),
                    provisional_value: (None, provisional_ts, provisional_order),
                })
            }
            StateValue::Value(Value::Tombstone) => StateValue::Value(Value::ProvisionalValue {
                finalized_value: None,
                provisional_value: (None, provisional_ts, provisional_order),
            }),
            StateValue::Value(Value::ProvisionalValue {
                finalized_value,
                provisional_value: _,
            }) => StateValue::Value(Value::ProvisionalValue {
                finalized_value,
                provisional_value: (None, provisional_ts, provisional_order),
            }),
            StateValue::Consolidating(_) => {
                panic!("called `into_provisional_tombstone` without calling `ensure_decoded`")
            }
        }
    }

    /// Returns the order of a provisional value at the given timestamp if it exists.
    pub fn provisional_order(&self, ts: &T) -> Option<&O> {
        match self {
            Self::Value(Value::FinalizedValue(_)) => None,
            Self::Value(Value::Tombstone) => None,
            Self::Value(Value::ProvisionalValue {
                finalized_value: _,
                provisional_value: (_, provisional_ts, provisional_order),
            }) if provisional_ts == ts => Some(provisional_order),
            Self::Value(Value::ProvisionalValue { .. }) => None,
            Self::Consolidating(_) => {
                panic!("called `provisional_order` without calling `ensure_decoded`")
            }
        }
    }

    /// Returns the provisional value, if one is present at the given timestamp.
    /// Falls back to the finalized value, or `None` if there is neither.
    pub fn provisional_value_ref(&self, ts: &T) -> Option<&UpsertValue> {
        match self {
            Self::Value(Value::FinalizedValue(value)) => Some(value),
            Self::Value(Value::Tombstone) => None,
            Self::Value(Value::ProvisionalValue {
                finalized_value: _,
                provisional_value: (provisional_value, provisional_ts, _provisional_order),
            }) if provisional_ts == ts => provisional_value.as_ref(),
            Self::Value(Value::ProvisionalValue {
                finalized_value,
                provisional_value: _,
            }) => finalized_value.as_ref(),
            Self::Consolidating(_) => {
                panic!("called `provisional_value_ref` without calling `ensure_decoded`")
            }
        }
    }

    /// Returns the the finalized value, if one is present.
    pub fn into_finalized_value(self) -> Option<UpsertValue> {
        match self {
            Self::Value(Value::FinalizedValue(value)) => Some(value),
            Self::Value(Value::Tombstone) => None,
            Self::Value(Value::ProvisionalValue {
                finalized_value,
                provisional_value: _,
            }) => finalized_value,
            _ => panic!("called `order` without calling `ensure_decoded`"),
        }
    }
}

impl<T: Eq, O> StateValue<T, O> {
    /// We use a XOR trick in order to accumulate the values without having to store the full
    /// unconsolidated history in memory. For all (value, diff) updates of a key we track:
    /// - diff_sum = SUM(diff)
    /// - checksum_sum = SUM(checksum(bincode(value)) * diff)
    /// - len_sum = SUM(len(bincode(value)) * diff)
    /// - value_xor = XOR(bincode(value))
    ///
    /// ## Return value
    /// Returns a `bool` indicating whether or not the current merged value is able to be deleted.
    ///
    /// ## Correctness
    ///
    /// The method is correct because a well formed upsert collection at a given
    /// timestamp will have for each key:
    /// - Zero or one updates of the form (cur_value, +1)
    /// - Zero or more pairs of updates of the form (prev_value, +1), (prev_value, -1)
    ///
    /// We are interested in extracting the cur_value of each key and discard all prev_values
    /// that might be included in the stream. Since the history of prev_values always comes in
    /// pairs, computing the XOR of those is always going to cancel their effects out. Also,
    /// since XOR is commutative this property is true independent of the order. The same is
    /// true for the summations of the length and checksum since the sum will contain the
    /// unrelated values zero times.
    ///
    /// Therefore the accumulators will end up precisely in one of two states:
    /// 1. diff == 0, checksum == 0, value == [0..] => the key is not present
    /// 2. diff == 1, checksum == checksum(cur_value) value == cur_value => the key is present
    ///
    /// ## Robustness
    ///
    /// In the absense of bugs, accumulating the diff and checksum is not required since we know
    /// that a well formed collection always satisfies XOR(bincode(values)) == bincode(cur_value).
    /// However bugs may happen and so storing 16 more bytes per key to have a very high
    /// guarantee that we're not decoding garbage is more than worth it.
    /// The main key->value used to store previous values.
    #[allow(clippy::as_conversions)]
    pub fn merge_update(
        &mut self,
        value: UpsertValue,
        diff: mz_repr::Diff,
        bincode_opts: BincodeOpts,
        bincode_buffer: &mut Vec<u8>,
    ) -> bool {
        match self {
            Self::Consolidating(Consolidating {
                value_xor,
                len_sum,
                checksum_sum,
                diff_sum,
            }) => {
                bincode_buffer.clear();
                bincode_opts
                    .serialize_into(&mut *bincode_buffer, &value)
                    .unwrap();
                let len = i64::try_from(bincode_buffer.len()).unwrap();

                *diff_sum += diff.into_inner();
                *len_sum += len.wrapping_mul(diff.into_inner());
                // Truncation is fine (using `as`) as this is just a checksum
                *checksum_sum +=
                    (seahash::hash(&*bincode_buffer) as i64).wrapping_mul(diff.into_inner());

                // XOR of even diffs cancel out, so we only do it if diff is odd
                if diff.abs() % Diff::from(2) == Diff::ONE {
                    if value_xor.len() < bincode_buffer.len() {
                        value_xor.resize(bincode_buffer.len(), 0);
                    }
                    // Note that if the new value is _smaller_ than the `value_xor`, and
                    // the values at the end are zeroed out, we can shrink the buffer. This
                    // is extremely sensitive code, so we don't (yet) do that.
                    for (acc, val) in value_xor.iter_mut().zip(bincode_buffer.drain(..)) {
                        *acc ^= val;
                    }
                }

                // Returns whether or not the value can be deleted. This allows
                // us to delete values in `UpsertState::consolidate_chunk` (even
                // if they come back later), to minimize space usage.
                diff_sum.0 == 0 && checksum_sum.0 == 0 && value_xor.iter().all(|&x| x == 0)
            }
            StateValue::Value(_value) => {
                // We can turn a Value back into a Consolidating state:
                // `std::mem::take` will leave behind a default value, which
                // happens to be a default `Consolidating` `StateValue`.
                let this = std::mem::take(self);

                let finalized_value = this.into_finalized_value();
                if let Some(finalized_value) = finalized_value {
                    // If we had a value before, merge it into the
                    // now-consolidating state first.
                    let _ =
                        self.merge_update(finalized_value, Diff::ONE, bincode_opts, bincode_buffer);

                    // Then merge the new value in.
                    self.merge_update(value, diff, bincode_opts, bincode_buffer)
                } else {
                    // We didn't have a value before, might have been a
                    // tombstone. So just merge in the new value.
                    self.merge_update(value, diff, bincode_opts, bincode_buffer)
                }
            }
        }
    }

    /// Merge an existing StateValue into this one, using the same method described in `merge_update`.
    /// See the docstring above for more information on correctness and robustness.
    pub fn merge_update_state(&mut self, other: &Self) {
        match (self, other) {
            (
                Self::Consolidating(Consolidating {
                    value_xor,
                    len_sum,
                    checksum_sum,
                    diff_sum,
                }),
                Self::Consolidating(other_consolidating),
            ) => {
                *diff_sum += other_consolidating.diff_sum;
                *len_sum += other_consolidating.len_sum;
                *checksum_sum += other_consolidating.checksum_sum;
                if other_consolidating.value_xor.len() > value_xor.len() {
                    value_xor.resize(other_consolidating.value_xor.len(), 0);
                }
                for (acc, val) in value_xor
                    .iter_mut()
                    .zip(other_consolidating.value_xor.iter())
                {
                    *acc ^= val;
                }
            }
            _ => panic!("`merge_update_state` called with non-consolidating state"),
        }
    }

    /// During and after consolidation, we assume that values in the `UpsertStateBackend` implementation
    /// can be `Self::Consolidating`, with a `diff_sum` of 1 (or 0, if they have been deleted).
    /// Afterwards, if we need to retract one of these values, we need to assert that its in this correct state,
    /// then mutate it to its `Value` state, so the `upsert` operator can use it.
    #[allow(clippy::as_conversions)]
    pub fn ensure_decoded(&mut self, bincode_opts: BincodeOpts, source_id: GlobalId) {
        match self {
            StateValue::Consolidating(consolidating) => {
                match consolidating.diff_sum.0 {
                    1 => {
                        let len = usize::try_from(consolidating.len_sum.0)
                            .map_err(|_| {
                                format!(
                                    "len_sum can't be made into a usize, state: {}, {}",
                                    consolidating, source_id,
                                )
                            })
                            .expect("invalid upsert state");
                        let value = &consolidating
                            .value_xor
                            .get(..len)
                            .ok_or_else(|| {
                                format!(
                                    "value_xor is not the same length ({}) as len ({}), state: {}, {}",
                                    consolidating.value_xor.len(),
                                    len,
                                    consolidating,
                                    source_id,
                                )
                            })
                            .expect("invalid upsert state");
                        // Truncation is fine (using `as`) as this is just a checksum
                        assert_eq!(
                            consolidating.checksum_sum.0,
                            // Hash the value, not the full buffer, which may have extra 0's
                            seahash::hash(value) as i64,
                            "invalid upsert state: checksum_sum does not match, state: {}, {}",
                            consolidating,
                            source_id,
                        );
                        *self = Self::Value(Value::FinalizedValue(
                            bincode_opts.deserialize(value).unwrap(),
                        ));
                    }
                    0 => {
                        assert_eq!(
                            consolidating.len_sum.0, 0,
                            "invalid upsert state: len_sum is non-0, state: {}, {}",
                            consolidating, source_id,
                        );
                        assert_eq!(
                            consolidating.checksum_sum.0, 0,
                            "invalid upsert state: checksum_sum is non-0, state: {}, {}",
                            consolidating, source_id,
                        );
                        assert!(
                            consolidating.value_xor.iter().all(|&x| x == 0),
                            "invalid upsert state: value_xor not all 0s with 0 diff. \
                            Non-zero positions: {:?}, state: {}, {}",
                            consolidating
                                .value_xor
                                .iter()
                                .positions(|&x| x != 0)
                                .collect::<Vec<_>>(),
                            consolidating,
                            source_id,
                        );
                        *self = Self::Value(Value::Tombstone);
                    }
                    other => panic!(
                        "invalid upsert state: non 0/1 diff_sum: {}, state: {}, {}",
                        other, consolidating, source_id
                    ),
                }
            }
            _ => {}
        }
    }
}

impl<T, O> Default for StateValue<T, O> {
    fn default() -> Self {
        Self::Consolidating(Consolidating::default())
    }
}

/// Statistics for a single call to `consolidate_chunk`.
#[derive(Clone, Default, Debug)]
pub struct SnapshotStats {
    /// The number of updates processed.
    pub updates: u64,
    /// The aggregated number of values inserted or deleted into `state`.
    pub values_diff: Diff,
    /// The total aggregated size of values inserted, deleted, or updated in `state`.
    /// If the current call to `consolidate_chunk` deletes a lot of values,
    /// or updates values to smaller ones, this can be negative!
    pub size_diff: i64,
    /// The number of inserts i.e. +1 diff
    pub inserts: u64,
    /// The number of deletes i.e. -1 diffs
    pub deletes: u64,
}

impl std::ops::AddAssign for SnapshotStats {
    fn add_assign(&mut self, rhs: Self) {
        self.updates += rhs.updates;
        self.values_diff += rhs.values_diff;
        self.size_diff += rhs.size_diff;
        self.inserts += rhs.inserts;
        self.deletes += rhs.deletes;
    }
}

/// Statistics for a single call to `multi_merge`.
#[derive(Clone, Default, Debug)]
pub struct MergeStats {
    /// The number of updates written as merge operands to the backend, for the backend
    /// to process async in the `consolidating_merge_function`.
    /// Should be equal to number of inserts + deletes
    pub written_merge_operands: u64,
    /// The total size of values provided to `multi_merge`. The backend will write these
    /// down and then later merge them in the `consolidating_merge_function`.
    pub size_written: u64,
    /// The estimated diff of the total size of the working set after the merge operands
    /// are merged by the backend. This is an estimate since it can't account for the
    /// size overhead of `StateValue` for values that consolidate to 0 (tombstoned-values).
    pub size_diff: i64,
}

/// Statistics for a single call to `multi_put`.
#[derive(Clone, Default, Debug)]
pub struct PutStats {
    /// The number of puts/deletes processed
    /// Should be equal to number of inserts + updates + deletes
    pub processed_puts: u64,
    /// The aggregated number of non-tombstone values inserted or deleted into `state`.
    pub values_diff: i64,
    /// The aggregated number of tombstones inserted or deleted into `state`
    pub tombstones_diff: i64,
    /// The total aggregated size of values inserted, deleted, or updated in `state`.
    /// If the current call to `multi_put` deletes a lot of values,
    /// or updates values to smaller ones, this can be negative!
    pub size_diff: i64,
    /// The number of inserts
    pub inserts: u64,
    /// The number of updates
    pub updates: u64,
    /// The number of deletes
    pub deletes: u64,
}

impl PutStats {
    /// Adjust the `PutStats` based on the new value and the previous metadata.
    ///
    /// The size parameter is separate as its value is backend-dependent. Its optional
    /// as some backends increase the total size after an entire batch is processed.
    ///
    /// This method is provided for implementors of `UpsertStateBackend::multi_put`.
    pub fn adjust<T, O>(
        &mut self,
        new_value: Option<&StateValue<T, O>>,
        new_size: Option<i64>,
        previous_metdata: &Option<ValueMetadata<i64>>,
    ) {
        self.adjust_size(new_value, new_size, previous_metdata);
        self.adjust_values(new_value, previous_metdata);
        self.adjust_tombstone(new_value, previous_metdata);
    }

    fn adjust_size<T, O>(
        &mut self,
        new_value: Option<&StateValue<T, O>>,
        new_size: Option<i64>,
        previous_metdata: &Option<ValueMetadata<i64>>,
    ) {
        match (&new_value, previous_metdata.as_ref()) {
            (Some(_), Some(ps)) => {
                self.size_diff -= ps.size;
                if let Some(new_size) = new_size {
                    self.size_diff += new_size;
                }
            }
            (None, Some(ps)) => {
                self.size_diff -= ps.size;
            }
            (Some(_), None) => {
                if let Some(new_size) = new_size {
                    self.size_diff += new_size;
                }
            }
            (None, None) => {}
        }
    }

    fn adjust_values<T, O>(
        &mut self,
        new_value: Option<&StateValue<T, O>>,
        previous_metdata: &Option<ValueMetadata<i64>>,
    ) {
        let truly_new_value = new_value.map_or(false, |v| !v.is_tombstone());
        let truly_old_value = previous_metdata.map_or(false, |v| !v.is_tombstone);

        match (truly_new_value, truly_old_value) {
            (false, true) => {
                self.values_diff -= 1;
            }
            (true, false) => {
                self.values_diff += 1;
            }
            _ => {}
        }
    }

    fn adjust_tombstone<T, O>(
        &mut self,
        new_value: Option<&StateValue<T, O>>,
        previous_metdata: &Option<ValueMetadata<i64>>,
    ) {
        let new_tombstone = new_value.map_or(false, |v| v.is_tombstone());
        let old_tombstone = previous_metdata.map_or(false, |v| v.is_tombstone);

        match (new_tombstone, old_tombstone) {
            (false, true) => {
                self.tombstones_diff -= 1;
            }
            (true, false) => {
                self.tombstones_diff += 1;
            }
            _ => {}
        }
    }
}

/// Statistics for a single call to `multi_get`.
#[derive(Clone, Default, Debug)]
pub struct GetStats {
    /// The number of gets processed
    pub processed_gets: u64,
    /// The total size in bytes returned
    pub processed_gets_size: u64,
    /// The number of non-empty records returned
    pub returned_gets: u64,
}

/// A trait that defines the fundamental primitives required by a state-backing of
/// `UpsertState`.
///
/// Implementors of this trait are blind maps that associate keys and values. They need
/// not understand the semantics of `StateValue`, tombstones, or anything else related
/// to a correct `upsert` implementation. The singular exception to this is that they
/// **must** produce accurate `PutStats` and `GetStats`. The reasoning for this is two-fold:
/// - efficiency: this avoids additional buffer allocation.
/// - value sizes: only the backend implementation understands the size of values as recorded
///
/// This **must** is not a correctness requirement (we won't panic when emitting statistics), but
/// rather a requirement to ensure the upsert operator is introspectable.
#[async_trait::async_trait(?Send)]
pub trait UpsertStateBackend<T, O>
where
    T: 'static,
    O: 'static,
{
    /// Whether this backend supports the `multi_merge` operation.
    fn supports_merge(&self) -> bool;

    /// Insert or delete for all `puts` keys, prioritizing the last value for
    /// repeated keys.
    ///
    /// The `PutValue` is _guaranteed_ to have an accurate and up-to-date
    /// record of the metadata for existing value for the given key (if one existed),
    /// as reported by a previous call to `multi_get`.
    ///
    /// `PutStats` **must** be populated correctly, according to these semantics:
    /// - `values_diff` must record the difference in number of new non-tombstone values being
    /// inserted into the backend.
    /// - `tombstones_diff` must record the difference in number of tombstone values being
    /// inserted into the backend.
    /// - `size_diff` must record the change in size for the values being inserted/deleted/updated
    /// in the backend, regardless of whether the values are tombstones or not.
    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue<T, O>>)>;

    /// Get the `gets` keys, which must be unique, placing the results in `results_out`.
    ///
    /// Panics if `gets` and `results_out` are not the same length.
    async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<T, O>>;

    /// For each key in `merges` writes a 'merge operand' to the backend. The backend stores these
    /// merge operands and periodically calls the `consolidating_merge_function` to merge them into
    /// any existing value for each key. The backend will merge the merge operands in the order
    /// they are provided, and the merge function will always be run for a given key when a `get`
    /// operation is performed on that key, or when the backend decides to run the merge based
    /// on its own internal logic.
    /// This allows avoiding the read-modify-write method of updating many values to
    /// improve performance.
    ///
    /// The `MergeValue` should include a `diff` field that represents the update diff for the
    /// value. This is used to estimate the overall size diff of the working set
    /// after the merge operands are merged by the backend `sum[merges: m](m.diff * m.size)`.
    ///
    /// `MergeStats` **must** be populated correctly, according to these semantics:
    /// - `written_merge_operands` must record the number of merge operands written to the backend.
    /// - `size_written` must record the total size of values written to the backend.
    ///     Note that the size of the post-merge values are not known, so this is the size of the
    ///     values written to the backend as merge operands.
    /// - `size_diff` must record the estimated diff of the total size of the working set after the
    ///    merge operands are merged by the backend.
    async fn multi_merge<P>(&mut self, merges: P) -> Result<MergeStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, MergeValue<StateValue<T, O>>)>;
}

/// A function that merges a set of updates for a key into the existing value
/// for the key. This is called by the backend implementation when it has
/// accumulated a set of updates for a key, and needs to merge them into the
/// existing value for the key.
///
/// The function is called with the following arguments:
/// - The key for which the merge is being performed.
/// - An iterator over any current value and merge operands queued for the key.
///
/// The function should return the new value for the key after merging all the updates.
pub(crate) fn consolidating_merge_function<T: Eq, O>(
    _key: UpsertKey,
    updates: impl Iterator<Item = StateValue<T, O>>,
) -> StateValue<T, O> {
    let mut current: StateValue<T, O> = Default::default();

    let mut bincode_buf = Vec::new();
    for update in updates {
        match update {
            StateValue::Consolidating(_) => {
                current.merge_update_state(&update);
            }
            StateValue::Value(_) => {
                // This branch is more expensive, but we hopefully rarely hit
                // it.
                if let Some(finalized_value) = update.into_finalized_value() {
                    let mut update = StateValue::default();
                    update.merge_update(
                        finalized_value,
                        Diff::ONE,
                        upsert_bincode_opts(),
                        &mut bincode_buf,
                    );
                    current.merge_update_state(&update);
                }
            }
        }
    }

    current
}

/// An `UpsertStateBackend` wrapper that supports consolidating merging, and
/// reports basic metrics about the usage of the `UpsertStateBackend`.
pub struct UpsertState<'metrics, S, T, O> {
    inner: S,

    // The status, start time, and stats about calls to `consolidate_chunk`.
    pub snapshot_start: Instant,
    snapshot_stats: SnapshotStats,
    snapshot_completed: bool,

    // Metrics shared across all workers running the `upsert` operator.
    metrics: Arc<UpsertSharedMetrics>,
    // Metrics for a specific worker.
    worker_metrics: &'metrics UpsertMetrics,
    // User-facing statistics.
    stats: SourceStatistics,

    // Bincode options and buffer used in `consolidate_chunk`.
    bincode_opts: BincodeOpts,
    bincode_buffer: Vec<u8>,

    // We need to iterate over `updates` in `consolidate_chunk` twice, so we
    // have a scratch vector for this.
    consolidate_scratch: Vec<(UpsertKey, UpsertValue, mz_repr::Diff)>,
    // "mini-upsert" map used in `consolidate_chunk`
    consolidate_upsert_scratch: indexmap::IndexMap<UpsertKey, UpsertValueAndSize<T, O>>,
    // a scratch vector for calling `multi_get`
    multi_get_scratch: Vec<UpsertKey>,
    shrink_upsert_unused_buffers_by_ratio: usize,
}

impl<'metrics, S, T, O> UpsertState<'metrics, S, T, O> {
    pub(crate) fn new(
        inner: S,
        metrics: Arc<UpsertSharedMetrics>,
        worker_metrics: &'metrics UpsertMetrics,
        stats: SourceStatistics,
        shrink_upsert_unused_buffers_by_ratio: usize,
    ) -> Self {
        Self {
            inner,
            snapshot_start: Instant::now(),
            snapshot_stats: SnapshotStats::default(),
            snapshot_completed: false,
            metrics,
            worker_metrics,
            stats,
            bincode_opts: upsert_bincode_opts(),
            bincode_buffer: Vec::new(),
            consolidate_scratch: Vec::new(),
            consolidate_upsert_scratch: indexmap::IndexMap::new(),
            multi_get_scratch: Vec::new(),
            shrink_upsert_unused_buffers_by_ratio,
        }
    }
}

impl<S, T, O> UpsertState<'_, S, T, O>
where
    S: UpsertStateBackend<T, O>,
    T: Eq + Clone + Send + Sync + Serialize + 'static,
    O: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Consolidate the following differential updates into the state. Updates
    /// provided to this method can be assumed to consolidate into a single
    /// value per-key, after all chunks of updates for a given timestamp have
    /// been processed,
    ///
    /// Therefore, after all updates of a given timestamp have been
    /// `consolidated`, all values must be in the correct state (as determined
    /// by `StateValue::ensure_decoded`).
    ///
    /// The `completed` boolean communicates whether or not this is the final
    /// chunk of updates for the initial "snapshot" from persist.
    ///
    /// If the backend supports it, this method will use `multi_merge` to
    /// consolidate the updates to avoid having to read the existing value for
    /// each key first. On some backends (like RocksDB), this can be
    /// significantly faster than the read-then-write consolidation strategy.
    ///
    /// Also note that we use `self.inner.multi_*`, not `self.multi_*`. This is
    /// to avoid erroneously changing metric and stats values.
    pub async fn consolidate_chunk<U>(
        &mut self,
        updates: U,
        completed: bool,
    ) -> Result<(), anyhow::Error>
    where
        U: IntoIterator<Item = (UpsertKey, UpsertValue, mz_repr::Diff)> + ExactSizeIterator,
    {
        fail::fail_point!("fail_consolidate_chunk", |_| {
            Err(anyhow::anyhow!("Error consolidating values"))
        });

        if completed && self.snapshot_completed {
            panic!("attempted completion of already completed upsert snapshot")
        }

        let now = Instant::now();
        let batch_size = updates.len();

        self.consolidate_scratch.clear();
        self.consolidate_upsert_scratch.clear();
        self.multi_get_scratch.clear();

        // Shrinking the scratch vectors if the capacity is significantly more than batch size
        if self.shrink_upsert_unused_buffers_by_ratio > 0 {
            let reduced_capacity =
                self.consolidate_scratch.capacity() / self.shrink_upsert_unused_buffers_by_ratio;
            if reduced_capacity > batch_size {
                // These vectors have already been cleared above and should be empty here
                self.consolidate_scratch.shrink_to(reduced_capacity);
                self.consolidate_upsert_scratch.shrink_to(reduced_capacity);
                self.multi_get_scratch.shrink_to(reduced_capacity);
            }
        }

        // Depending on if the backend supports multi_merge, call the appropriate method.
        // This can change during the lifetime of the `UpsertState` instance (e.g.
        // the Autospill backend will switch from in-memory to rocksdb after a certain
        // number of updates have been processed and begin supporting multi_merge).
        let stats = if self.inner.supports_merge() {
            self.consolidate_merge_inner(updates).await?
        } else {
            self.consolidate_read_write_inner(updates).await?
        };

        // NOTE: These metrics use the term `merge` to refer to the consolidation of values.
        // This is because they were introduced before we the `multi_merge` operation was added.
        self.metrics
            .merge_snapshot_latency
            .observe(now.elapsed().as_secs_f64());
        self.worker_metrics
            .merge_snapshot_updates
            .inc_by(stats.updates);
        self.worker_metrics
            .merge_snapshot_inserts
            .inc_by(stats.inserts);
        self.worker_metrics
            .merge_snapshot_deletes
            .inc_by(stats.deletes);

        self.stats.update_bytes_indexed_by(stats.size_diff);
        self.stats
            .update_records_indexed_by(stats.values_diff.into_inner());

        self.snapshot_stats += stats;

        if !self.snapshot_completed {
            // Updating the metrics
            self.worker_metrics.rehydration_total.set(
                self.snapshot_stats
                    .values_diff
                    .into_inner()
                    .try_into()
                    .unwrap_or_else(|e: std::num::TryFromIntError| {
                        tracing::warn!(
                            "rehydration_total metric overflowed or is negative \
                        and is innacurate: {}. Defaulting to 0",
                            e.display_with_causes(),
                        );

                        0
                    }),
            );
            self.worker_metrics
                .rehydration_updates
                .set(self.snapshot_stats.updates);
        }

        if completed {
            if self.shrink_upsert_unused_buffers_by_ratio > 0 {
                // After rehydration is done, these scratch buffers should now be empty
                // shrinking them entirely
                self.consolidate_scratch.shrink_to_fit();
                self.consolidate_upsert_scratch.shrink_to_fit();
                self.multi_get_scratch.shrink_to_fit();
            }

            self.worker_metrics
                .rehydration_latency
                .set(self.snapshot_start.elapsed().as_secs_f64());

            self.snapshot_completed = true;
        }
        Ok(())
    }

    /// Consolidate the updates into the state. This method requires the backend
    /// has support for the `multi_merge` operation, and will panic if
    /// `self.inner.supports_merge()` was not checked before calling this
    /// method. `multi_merge` will write the updates as 'merge operands' to the
    /// backend, and then the backend will consolidate those updates with any
    /// existing state using the `consolidating_merge_function`.
    ///
    /// This method can have significant performance benefits over the
    /// read-then-write method of `consolidate_read_write_inner`.
    async fn consolidate_merge_inner<U>(
        &mut self,
        updates: U,
    ) -> Result<SnapshotStats, anyhow::Error>
    where
        U: IntoIterator<Item = (UpsertKey, UpsertValue, mz_repr::Diff)> + ExactSizeIterator,
    {
        let mut updates = updates.into_iter().peekable();

        let mut stats = SnapshotStats::default();

        if updates.peek().is_some() {
            let m_stats = self
                .inner
                .multi_merge(updates.map(|(k, v, diff)| {
                    // Transform into a `StateValue<O>` that can be used by the
                    // `consolidating_merge_function` to merge with any existing
                    // value for the key.
                    let mut val: StateValue<T, O> = Default::default();
                    val.merge_update(v, diff, self.bincode_opts, &mut self.bincode_buffer);

                    stats.updates += 1;
                    if diff.is_positive() {
                        stats.inserts += 1;
                    } else if diff.is_negative() {
                        stats.deletes += 1;
                    }

                    // To keep track of the overall `values_diff` we can use the sum of diffs which
                    // should be equal to the number of non-tombstoned values in the backend.
                    // This is a bit misleading as this represents the eventual state after the
                    // `consolidating_merge_function` has been called to merge all the updates,
                    // and not the state after this `multi_merge` call.
                    //
                    // This does not accurately report values that have been consolidated to diff == 0, as tracking that
                    // per-key is extremely difficult.
                    stats.values_diff += diff;

                    (k, MergeValue { value: val, diff })
                }))
                .await?;

            stats.size_diff = m_stats.size_diff;
        }

        Ok(stats)
    }

    /// Consolidates the updates into the state. This method reads the existing
    /// values for each key, consolidates the updates, and writes the new values
    /// back to the state.
    async fn consolidate_read_write_inner<U>(
        &mut self,
        updates: U,
    ) -> Result<SnapshotStats, anyhow::Error>
    where
        U: IntoIterator<Item = (UpsertKey, UpsertValue, mz_repr::Diff)> + ExactSizeIterator,
    {
        let mut updates = updates.into_iter().peekable();

        let mut stats = SnapshotStats::default();

        if updates.peek().is_some() {
            self.consolidate_scratch.extend(updates);
            self.consolidate_upsert_scratch.extend(
                self.consolidate_scratch
                    .iter()
                    .map(|(k, _, _)| (*k, UpsertValueAndSize::default())),
            );
            self.multi_get_scratch
                .extend(self.consolidate_upsert_scratch.iter().map(|(k, _)| *k));
            self.inner
                .multi_get(
                    self.multi_get_scratch.drain(..),
                    self.consolidate_upsert_scratch.iter_mut().map(|(_, v)| v),
                )
                .await?;

            for (key, value, diff) in self.consolidate_scratch.drain(..) {
                stats.updates += 1;
                if diff.is_positive() {
                    stats.inserts += 1;
                } else if diff.is_negative() {
                    stats.deletes += 1;
                }

                // We rely on the diffs in our input instead of the result of
                // multi_put below. This makes sure we report the same stats as
                // `consolidate_merge_inner`, regardless of what values
                // there were in state before.
                stats.values_diff += diff;

                let entry = self.consolidate_upsert_scratch.get_mut(&key).unwrap();
                let val = entry.value.get_or_insert_with(Default::default);

                if val.merge_update(value, diff, self.bincode_opts, &mut self.bincode_buffer) {
                    entry.value = None;
                }
            }

            // Note we do 1 `multi_get` and 1 `multi_put` while processing a _batch of updates_.
            // Within the batch, we effectively consolidate each key, before persisting that
            // consolidated value. Easy!!
            let p_stats = self
                .inner
                .multi_put(self.consolidate_upsert_scratch.drain(..).map(|(k, v)| {
                    (
                        k,
                        PutValue {
                            value: v.value,
                            previous_value_metadata: v.metadata.map(|v| ValueMetadata {
                                size: v.size.try_into().expect("less than i64 size"),
                                is_tombstone: v.is_tombstone,
                            }),
                        },
                    )
                }))
                .await?;

            stats.size_diff = p_stats.size_diff;
        }

        Ok(stats)
    }

    /// Insert or delete for all `puts` keys, prioritizing the last value for
    /// repeated keys.
    pub async fn multi_put<P>(
        &mut self,
        update_per_record_stats: bool,
        puts: P,
    ) -> Result<(), anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<Value<T, O>>)>,
    {
        fail::fail_point!("fail_state_multi_put", |_| {
            Err(anyhow::anyhow!("Error putting values into state"))
        });
        let now = Instant::now();
        let stats = self
            .inner
            .multi_put(puts.into_iter().map(|(k, pv)| {
                (
                    k,
                    PutValue {
                        value: pv.value.map(StateValue::Value),
                        previous_value_metadata: pv.previous_value_metadata,
                    },
                )
            }))
            .await?;

        self.metrics
            .multi_put_latency
            .observe(now.elapsed().as_secs_f64());
        self.worker_metrics
            .multi_put_size
            .inc_by(stats.processed_puts);

        if update_per_record_stats {
            self.worker_metrics.upsert_inserts.inc_by(stats.inserts);
            self.worker_metrics.upsert_updates.inc_by(stats.updates);
            self.worker_metrics.upsert_deletes.inc_by(stats.deletes);

            self.stats.update_bytes_indexed_by(stats.size_diff);
            self.stats.update_records_indexed_by(stats.values_diff);
            self.stats
                .update_envelope_state_tombstones_by(stats.tombstones_diff);
        }

        Ok(())
    }

    /// Get the `gets` keys, which must be unique, placing the results in `results_out`.
    ///
    /// Panics if `gets` and `results_out` are not the same length.
    pub async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<(), anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<T, O>>,
        O: 'r,
    {
        fail::fail_point!("fail_state_multi_get", |_| {
            Err(anyhow::anyhow!("Error getting values from state"))
        });
        let now = Instant::now();
        let stats = self.inner.multi_get(gets, results_out).await?;

        self.metrics
            .multi_get_latency
            .observe(now.elapsed().as_secs_f64());
        self.worker_metrics
            .multi_get_size
            .inc_by(stats.processed_gets);
        self.worker_metrics
            .multi_get_result_count
            .inc_by(stats.returned_gets);
        self.worker_metrics
            .multi_get_result_bytes
            .inc_by(stats.processed_gets_size);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::Row;

    use super::*;
    #[mz_ore::test]
    fn test_merge_update() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<(), ()>::Consolidating(Consolidating::default());

        let small_row = Ok(Row::default());
        let longer_row = Ok(Row::pack([mz_repr::Datum::Null]));
        s.merge_update(small_row, Diff::ONE, opts, &mut buf);
        s.merge_update(longer_row.clone(), Diff::MINUS_ONE, opts, &mut buf);
        // This clears the retraction of the `longer_row`, but the
        // `value_xor` is the length of the `longer_row`. This tests
        // that we are tracking checksums correctly.
        s.merge_update(longer_row, Diff::ONE, opts, &mut buf);

        // Assert that the `Consolidating` value is fully merged.
        s.ensure_decoded(opts, GlobalId::User(1));
    }

    // We guard some of our assumptions. Increasing in-memory size of StateValue
    // has a direct impact on memory usage of in-memory UPSERT sources.
    #[mz_ore::test]
    fn test_memory_size() {
        let finalized_value: StateValue<(), ()> = StateValue::finalized_value(Ok(Row::default()));
        assert!(
            finalized_value.memory_size() <= 88,
            "memory size is {}",
            finalized_value.memory_size(),
        );

        let provisional_value_with_finalized_value: StateValue<(), ()> =
            finalized_value.into_provisional_value(Ok(Row::default()), (), ());
        assert!(
            provisional_value_with_finalized_value.memory_size() <= 112,
            "memory size is {}",
            provisional_value_with_finalized_value.memory_size(),
        );

        let provisional_value_without_finalized_value: StateValue<(), ()> =
            StateValue::new_provisional_value(Ok(Row::default()), (), ());
        assert!(
            provisional_value_without_finalized_value.memory_size() <= 88,
            "memory size is {}",
            provisional_value_without_finalized_value.memory_size(),
        );

        let mut consolidating_value: StateValue<(), ()> = StateValue::default();
        consolidating_value.merge_update(
            Ok(Row::default()),
            Diff::ONE,
            upsert_bincode_opts(),
            &mut Vec::new(),
        );
        assert!(
            consolidating_value.memory_size() <= 90,
            "memory size is {}",
            consolidating_value.memory_size(),
        );
    }

    #[mz_ore::test]
    #[should_panic(
        expected = "invalid upsert state: len_sum is non-0, state: Consolidating { len_sum: 1"
    )]
    fn test_merge_update_len_0_assert() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<(), ()>::Consolidating(Consolidating::default());

        let small_row = Ok(mz_repr::Row::default());
        let longer_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Null]));
        s.merge_update(longer_row.clone(), Diff::ONE, opts, &mut buf);
        s.merge_update(small_row.clone(), Diff::MINUS_ONE, opts, &mut buf);

        s.ensure_decoded(opts, GlobalId::User(1));
    }

    #[mz_ore::test]
    #[should_panic(
        expected = "invalid upsert state: \"value_xor is not the same length (3) as len (4), state: Consolidating { len_sum: 4"
    )]
    fn test_merge_update_len_to_long_assert() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<(), ()>::Consolidating(Consolidating::default());

        let small_row = Ok(mz_repr::Row::default());
        let longer_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Null]));
        s.merge_update(longer_row.clone(), Diff::ONE, opts, &mut buf);
        s.merge_update(small_row.clone(), Diff::MINUS_ONE, opts, &mut buf);
        s.merge_update(longer_row.clone(), Diff::ONE, opts, &mut buf);

        s.ensure_decoded(opts, GlobalId::User(1));
    }

    #[mz_ore::test]
    #[should_panic(expected = "invalid upsert state: checksum_sum does not match")]
    fn test_merge_update_checksum_doesnt_match() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<(), ()>::Consolidating(Consolidating::default());

        let small_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Int64(2)]));
        let longer_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Int64(1)]));
        s.merge_update(longer_row.clone(), Diff::ONE, opts, &mut buf);
        s.merge_update(small_row.clone(), Diff::MINUS_ONE, opts, &mut buf);
        s.merge_update(longer_row.clone(), Diff::ONE, opts, &mut buf);

        s.ensure_decoded(opts, GlobalId::User(1));
    }
}
