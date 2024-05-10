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
//! for use within an UPSERT operator. There is one exception to this: `consolidate_snapshot_chunk`
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
//! ### `consolidate_snapshot_chunk`
//!
//! `consolidate_snapshot_chunk` re-indexes an UPSERT collection based on its _output collection_ (as
//! opposed to its _input `Stream`_. Please see the docs on `consolidate_snapshot_chunk` and `StateValue`
//! for more information.
//!
//! `consolidate_snapshot_chunk` is implemented with both `UpsertStateBackend::multi_put` and
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
//! Because `consolidate_snapshot_chunk` handles data that consolidates correctly, it does not handle
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
//! Note also that after snapshot consolidation, additional space may be used if `StateValue` is
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
use serde::{de::DeserializeOwned, Serialize};

use super::{UpsertKey, UpsertValue};
use crate::metrics::upsert::{UpsertMetrics, UpsertSharedMetrics};
use crate::statistics::SourceStatistics;

/// The default set of `bincode` options used for consolidating
/// upsert snapshots (and writing values to RocksDB).
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
#[derive(Debug, Default, Clone)]
pub struct UpsertValueAndSize<O> {
    /// The value, if there was one.
    pub value: Option<StateValue<O>>,
    /// The size of original`value` as persisted,
    /// Useful for users keeping track of statistics.
    pub metadata: Option<ValueMetadata<u64>>,
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

/// `UpsertState` has 2 modes:
/// - Normal operation
/// - Consolidation of snapshots (during rehydration).
///
/// This struct and its substructs are helpers to simplify the logic that
/// individual `UpsertState` implementations need to do to manage these 2 modes.
///
/// Normal operation is simple, we just store an ordinary `UpsertValue`, and allow the implementer
/// to store it any way they want. During consolidation of snapshots, the logic is more complex.
/// See the docs on `StateValue::merge_update` for more information.
///
///
/// Note also that this type is designed to support _partial updates_. All values are
/// associated with an _order key_ `O` that can be used to determine if a value existing in the
/// `UpsertStateBackend` occurred before or after a value being considered for insertion.
///
/// `O` typically required to be `: Default`, with the default value sorting below all others.
/// Values consolidated during snapshotting consolidate correctly (as they are actual
/// differential updates with diffs), so order keys are not required.
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub enum StateValue<O> {
    Snapshotting(Snapshotting),
    Value(Value<O>),
}

/// A totally consolidated value stored within the `UpsertStateBackend`.
///
/// This type contains support for _tombstones_, that contain an _order key_.
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub enum Value<O> {
    Value(UpsertValue, O),
    Tombstone(O),
}

/// A value as produced during consolidation of a snapshot.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize, Debug)]
pub struct Snapshotting {
    value_xor: Vec<u8>,
    len_sum: Wrapping<i64>,
    checksum_sum: Wrapping<i64>,
    diff_sum: Wrapping<i64>,
}

impl fmt::Display for Snapshotting {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Snapshotting")
            .field("len_sum", &self.len_sum)
            .field("checksum_sum", &self.checksum_sum)
            .field("diff_sum", &self.checksum_sum)
            .finish_non_exhaustive()
    }
}

impl<O> StateValue<O> {
    /// A normal value occurring at some order key.
    pub fn value(value: UpsertValue, order: O) -> Self {
        Self::Value(Value::Value(value, order))
    }

    #[allow(unused)]
    /// A tombstoned value occurring at some order key.
    pub fn tombstone(order: O) -> Self {
        Self::Value(Value::Tombstone(order))
    }

    /// Whether the value is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        match self {
            Self::Value(Value::Tombstone(_)) => true,
            _ => false,
        }
    }

    /// Pull out the order for the given `Value`, assuming `ensure_decoded` has been called.
    pub fn order(&self) -> &O {
        match self {
            Self::Value(Value::Value(_, order)) => order,
            Self::Value(Value::Tombstone(order)) => order,
            _ => panic!("called `order` without calling `ensure_decoded`"),
        }
    }

    /// Pull out the `Value` value for a `StateValue`, after `ensure_decoded` has been called.
    pub fn into_decoded(self) -> Value<O> {
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
            Self::Snapshotting(Snapshotting { value_xor, .. }) => {
                u64::cast_from(value_xor.len()) + u64::cast_from(std::mem::size_of::<Self>())
            }
            Self::Value(Value::Value(Ok(row), ..)) => {
                // `Row::byte_len` includes the size of `Row`, which is also in `Self`, so we
                // subtract it.
                u64::cast_from(row.byte_len())
                // This assumes the size of any `O` instantiation is meaningful (i.e. not a heap
                // object).
                + u64::cast_from(std::mem::size_of::<Self>())
                    - u64::cast_from(std::mem::size_of::<mz_repr::Row>())
            }
            Self::Value(Value::Tombstone(_)) => {
                // This assumes the size of any `O` instantiation is meaningful (i.e. not a heap
                // object).
                u64::cast_from(std::mem::size_of::<Self>())
            }
            Self::Value(Value::Value(Err(_), ..)) => {
                // Assume errors are rare enough to not move the needle.
                0
            }
        }
    }
}

impl<O: Default> StateValue<O> {
    /// We use a XOR trick in order to accumulate the snapshot without having to store the full
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
    /// The method is correct because a well formed upsert snapshot will have for each key:
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
    /// that a well formed snapshot always satisfies XOR(bincode(values)) == bincode(cur_value).
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
        if let Self::Snapshotting(Snapshotting {
            value_xor,
            len_sum,
            checksum_sum,
            diff_sum,
        }) = self
        {
            bincode_buffer.clear();
            bincode_opts
                .serialize_into(&mut *bincode_buffer, &value)
                .unwrap();
            let len = i64::try_from(bincode_buffer.len()).unwrap();

            *diff_sum += diff;
            *len_sum += len.wrapping_mul(diff);
            // Truncation is fine (using `as`) as this is just a checksum
            *checksum_sum += (seahash::hash(&*bincode_buffer) as i64).wrapping_mul(diff);

            // XOR of even diffs cancel out, so we only do it if diff is odd
            if diff.abs() % 2 == 1 {
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

            // Returns whether or not the value can be deleted. This allows us to delete values in
            // `UpsertState::consolidate_snapshot_chunk` (even if they come back later) during snapshotting,
            // to minimize space usage.
            return diff_sum.0 == 0 && checksum_sum.0 == 0 && value_xor.iter().all(|&x| x == 0);
        } else {
            panic!("`merge_update` called after snapshot consolidation")
        }
    }

    /// Merge an existing StateValue into this one, using the same method described in `merge_update`.
    /// See the docstring above for more information on correctness and robustness.
    pub fn merge_update_state(&mut self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Snapshotting(Snapshotting {
                    value_xor,
                    len_sum,
                    checksum_sum,
                    diff_sum,
                }),
                Self::Snapshotting(other_snapshotting),
            ) => {
                *diff_sum += other_snapshotting.diff_sum;
                *len_sum += other_snapshotting.len_sum;
                *checksum_sum += other_snapshotting.checksum_sum;
                if other_snapshotting.value_xor.len() > value_xor.len() {
                    value_xor.resize(other_snapshotting.value_xor.len(), 0);
                }
                for (acc, val) in value_xor
                    .iter_mut()
                    .zip(other_snapshotting.value_xor.iter())
                {
                    *acc ^= val;
                }
                diff_sum.0 == 0 && checksum_sum.0 == 0 && value_xor.iter().all(|&x| x == 0)
            }
            _ => panic!("`merge_update_state` called with non-snapshotting state"),
        }
    }

    /// After consolidation of a snapshot, we assume that all values in the `UpsertStateBackend` implementation
    /// are `Self::Snapshotting`, with a `diff_sum` of 1 (or 0, if they have been deleted).
    /// Afterwards, if we need to retract one of these values, we need to assert that its in this correct state,
    /// then mutate it to its `Value` state, so the `upsert` operator can use it.
    #[allow(clippy::as_conversions)]
    pub fn ensure_decoded(&mut self, bincode_opts: BincodeOpts) {
        match self {
            StateValue::Snapshotting(snapshotting) => {
                match snapshotting.diff_sum.0 {
                    1 => {
                        let len = usize::try_from(snapshotting.len_sum.0)
                            .map_err(|_| {
                                format!(
                                    "len_sum can't be made into a usize, state: {}",
                                    snapshotting
                                )
                            })
                            .expect("invalid upsert state");
                        let value = &snapshotting
                            .value_xor
                            .get(..len)
                            .ok_or_else(|| {
                                format!(
                                    "value_xor is not the same length ({}) as len ({}), state: {}",
                                    snapshotting.value_xor.len(),
                                    len,
                                    snapshotting
                                )
                            })
                            .expect("invalid upsert state");
                        // Truncation is fine (using `as`) as this is just a checksum
                        assert_eq!(
                            snapshotting.checksum_sum.0,
                            // Hash the value, not the full buffer, which may have extra 0's
                            seahash::hash(value) as i64,
                            "invalid upsert state: checksum_sum does not match, state: {}",
                            snapshotting
                        );
                        *self = Self::Value(Value::Value(
                            bincode_opts.deserialize(value).unwrap(),
                            Default::default(),
                        ));
                    }
                    0 => {
                        assert_eq!(
                            snapshotting.len_sum.0, 0,
                            "invalid upsert state: len_sum is non-0, state: {}",
                            snapshotting
                        );
                        assert_eq!(
                            snapshotting.checksum_sum.0, 0,
                            "invalid upsert state: checksum_sum is non-0, state: {}",
                            snapshotting
                        );
                        assert!(
                            snapshotting.value_xor.iter().all(|&x| x == 0),
                            "invalid upsert state: value_xor not all 0s with 0 diff. \
                            Non-zero positions: {:?}, state: {}",
                            snapshotting
                                .value_xor
                                .iter()
                                .positions(|&x| x != 0)
                                .collect::<Vec<_>>(),
                            snapshotting
                        );
                        // TODO(guswynn): This is probably not necessary, as we should have deleted
                        // the value from the state.
                        *self = Self::Value(Value::Tombstone(Default::default()));
                    }
                    other => panic!(
                        "invalid upsert state: non 0/1 diff_sum: {}, state: {}",
                        other, snapshotting
                    ),
                }
            }
            _ => {}
        }
    }
}

impl<O> Default for StateValue<O> {
    fn default() -> Self {
        Self::Snapshotting(Snapshotting::default())
    }
}

/// Statistics for a single call to `consolidate_snapshot_chunk`.
#[derive(Clone, Default, Debug)]
pub struct SnapshotStats {
    /// The number of updates processed.
    pub updates: u64,
    /// The aggregated number of values inserted or deleted into `state`.
    pub values_diff: i64,
    /// The total aggregated size of values inserted, deleted, or updated in `state`.
    /// If the current call to `consolidate_snapshot_chunk` deletes a lot of values,
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
    /// to process async in the `snapshot_merge_function`.
    /// Should be equal to number of inserts + deletes
    pub written_merge_operands: u64,
    /// The total size of values provided to `multi_merge`. The backend will write these
    /// down and then later merge them in the `snapshot_merge_function`. Since we don't
    /// know the size of the future merged value, we can't provide a `size_diff` here.
    pub size_written: u64,
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
    pub fn adjust<O>(
        &mut self,
        new_value: Option<&StateValue<O>>,
        new_size: Option<i64>,
        previous_metdata: &Option<ValueMetadata<i64>>,
    ) {
        self.adjust_size(new_value, new_size, previous_metdata);
        self.adjust_values(new_value, previous_metdata);
        self.adjust_tombstone(new_value, previous_metdata);
    }

    fn adjust_size<O>(
        &mut self,
        new_value: Option<&StateValue<O>>,
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

    fn adjust_values<O>(
        &mut self,
        new_value: Option<&StateValue<O>>,
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

    fn adjust_tombstone<O>(
        &mut self,
        new_value: Option<&StateValue<O>>,
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
pub trait UpsertStateBackend<O>
where
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
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue<O>>)>;

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
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<O>>;

    /// For each key in `merges` writes a 'merge operand' to the backend. The backend stores these
    /// merge operands and periodically calls the `snapshot_merge_function` to merge them into
    /// any existing value for each key. The backend will merge the merge operands in the order
    /// they are provided, and the merge function will always be run for a given key when a `get`
    /// operation is performed on that key, or when the backend decides to run the merge based
    /// on its own internal logic.
    /// This allows avoiding the read-modify-write method of updating many values to
    /// improve performance.
    ///
    /// `MergeStats` **must** be populated correctly, according to these semantics:
    /// - `written_merge_operands` must record the number of merge operands written to the backend.
    /// - `size_written` must record the total size of values written to the backend.
    /// Note that the size of the post-merge values are not known, so this is the size of the values
    /// written to the backend as merge operands.
    async fn multi_merge<P>(&mut self, merges: P) -> Result<MergeStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, StateValue<O>)>;
}

/// A function that merges a set of updates for a key into the existing value for the key, expected
/// to only be used during the snapshotting-phase of an upsert operator. This is called by the
/// backend implementation when it has accumulated a set of updates for a key, and needs to merge
/// them into the existing value for the key.
///
/// The function should return the new value for the key, or None if the key should be deleted.
/// The function is called with the following arguments:
/// - The key for which the merge is being performed.
/// - An iterator over any current value and merge operands queued for the key.
pub(crate) fn snapshot_merge_function<O>(
    _key: UpsertKey,
    updates: impl Iterator<Item = StateValue<O>>,
) -> Option<StateValue<O>>
where
    O: Default,
{
    let mut current = Default::default();
    let mut should_delete = false;
    assert!(
        matches!(current, StateValue::Snapshotting(_)),
        "merge_function called with non-snapshotting default"
    );
    for update in updates {
        assert!(
            matches!(update, StateValue::Snapshotting(_)),
            "merge_function called with non-snapshot update"
        );
        should_delete = current.merge_update_state(&update);
    }

    if should_delete {
        None
    } else {
        Some(current)
    }
}

/// An `UpsertStateBackend` wrapper that supports
/// snapshot merging, and reports basic metrics about the usage of the `UpsertStateBackend`.
pub struct UpsertState<'metrics, S, O> {
    inner: S,

    // The status, start time, and stats about calls to `consolidate_snapshot_chunk`.
    snapshot_start: Instant,
    snapshot_stats: SnapshotStats,
    snapshot_completed: bool,

    // Metrics shared across all workers running the `upsert` operator.
    metrics: Arc<UpsertSharedMetrics>,
    // Metrics for a specific worker.
    worker_metrics: &'metrics UpsertMetrics,
    // User-facing statistics.
    stats: SourceStatistics,

    // Bincode options and buffer used
    // in `consolidate_snapshot_chunk`.
    bincode_opts: BincodeOpts,
    bincode_buffer: Vec<u8>,

    // We need to iterate over `updates` in `consolidate_snapshot_chunk`
    // twice, so we have a scratch vector for this.
    consolidate_scratch: Vec<(UpsertKey, UpsertValue, mz_repr::Diff)>,
    // "mini-upsert" map used in `consolidate_snapshot_chunk`
    consolidate_upsert_scratch: indexmap::IndexMap<UpsertKey, UpsertValueAndSize<O>>,
    // a scratch vector for calling `multi_get`
    multi_get_scratch: Vec<UpsertKey>,
    shrink_upsert_unused_buffers_by_ratio: usize,
}

impl<'metrics, S, O> UpsertState<'metrics, S, O> {
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

impl<S, O> UpsertState<'_, S, O>
where
    S: UpsertStateBackend<O>,
    O: Default + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Consolidate the following differential updates into the state, during snapshotting.
    /// Updates provided to this method can be assumed to consolidate into a single value
    /// per-key, after all chunks have been processed.
    ///
    /// Therefore, after an entire snapshot has been `consolidated`, all values must be in the correct state
    /// (as determined by `StateValue::ensure_decoded`), and `consolidate_snapshot_chunk` must NOT
    /// be called again.
    ///
    /// The `completed` boolean communicates whether or not this is the final chunk of updates
    /// to be consolidated, to assert correct usage.
    ///
    /// If the backend supports it, this method will use `multi_merge` to consolidate the updates
    /// to avoid having to read the existing value for each key first.
    /// On some backends (like RocksDB), this can be significantly faster than the read-then-write
    /// consolidation strategy.
    ///
    /// Also note that we use `self.inner.multi_*`, not `self.multi_*`. This is to avoid
    /// erroneously changing metric and stats values.
    pub async fn consolidate_snapshot_chunk<U>(
        &mut self,
        updates: U,
        completed: bool,
    ) -> Result<(), anyhow::Error>
    where
        U: IntoIterator<Item = (UpsertKey, UpsertValue, mz_repr::Diff)> + ExactSizeIterator,
    {
        fail::fail_point!("fail_consolidate_snapshot_chunk", |_| {
            Err(anyhow::anyhow!("Error consolidating snapshot values"))
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
            self.consolidate_snapshot_merge_inner(updates).await?
        } else {
            self.consolidate_snapshot_read_write_inner(updates).await?
        };

        // NOTE: These metrics use the term `merge` to refer to the consolidation of snapshot values.
        // This is because they were introduced before we the `multi_merge` operation was added, and
        // to differentiate the two separate `merge` notions we renamed `merge_snapshot_chunk` to
        // `consolidate_snapshot_chunk`.
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

        self.snapshot_stats += stats;
        // Updating the metrics
        self.worker_metrics.rehydration_total.set(
            self.snapshot_stats.values_diff.try_into().unwrap_or_else(
                |e: std::num::TryFromIntError| {
                    tracing::warn!(
                        "rehydration_total metric overflowed or is negative \
                        and is innacurate: {}. Defaulting to 0",
                        e.display_with_causes(),
                    );

                    0
                },
            ),
        );
        self.worker_metrics
            .rehydration_updates
            .set(self.snapshot_stats.updates);
        // These `set_` functions also ensure that these values are non-negative.
        self.stats.set_bytes_indexed(self.snapshot_stats.size_diff);
        self.stats
            .set_records_indexed(self.snapshot_stats.values_diff);

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

    /// Consolidate the updates into the state during snapshotting. This method requires the
    /// backend has support for the `multi_merge` operation, and will panic if
    /// `self.inner.supports_merge()` was not checked before calling this method.
    /// `multi_merge` will write the updates as 'merge operands' to the backend, and then the
    /// backend will consolidate those updates with any existing state using the
    /// `snapshot_merge_function`.
    ///
    /// This method can have significant performance benefits over the
    /// read-then-write method of `consolidate_snapshot_read_write_inner`.
    async fn consolidate_snapshot_merge_inner<U>(
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
                    // Transform into a `StateValue<O>` that can be used by the `snapshot_merge_function`
                    // to merge with any existing value for the key.
                    let mut val: StateValue<O> = Default::default();
                    val.merge_update(v, diff, self.bincode_opts, &mut self.bincode_buffer);

                    stats.updates += 1;
                    if diff > 0 {
                        stats.inserts += 1;
                    } else if diff < 0 {
                        stats.deletes += 1;
                    }

                    // To keep track of the overall `values_diff` we can use the sum of diffs which
                    // should be equal to the number of non-tombstoned values in the backend.
                    // This is a bit misleading as this represents the eventual state after the
                    // `snapshot_merge_function` has been called to merge all the updates,
                    // and not the state after this `multi_merge` call.
                    stats.values_diff += diff;

                    (k, val)
                }))
                .await?;

            // This is the total size of the merge values written to the backend, so is an accurate
            // view of the size stored by the backend after this `multi_merge` operation, but not
            // representative of the ultimate size of the backend after these merge operands have
            // been mergedd by the `snapshot_merge_function`.
            let size: i64 = m_stats.size_written.try_into().expect("less than i64 size");
            stats.size_diff = size;
        }

        Ok(stats)
    }

    /// Consolidates the updates into the state during snapshotting. This method reads the existing
    /// values for each key, consolidates the updates, and writes the new values back to the state.
    async fn consolidate_snapshot_read_write_inner<U>(
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
                if diff > 0 {
                    stats.inserts += 1;
                } else if diff < 0 {
                    stats.deletes += 1;
                }
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

            stats.values_diff = p_stats.values_diff;
            stats.size_diff = p_stats.size_diff;
        }

        Ok(stats)
    }

    /// Insert or delete for all `puts` keys, prioritizing the last value for
    /// repeated keys.
    pub async fn multi_put<P>(&mut self, puts: P) -> Result<(), anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<Value<O>>)>,
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
        self.worker_metrics.upsert_inserts.inc_by(stats.inserts);
        self.worker_metrics.upsert_updates.inc_by(stats.updates);
        self.worker_metrics.upsert_deletes.inc_by(stats.deletes);

        self.stats.update_bytes_indexed_by(stats.size_diff);
        self.stats.update_records_indexed_by(stats.values_diff);
        self.stats
            .update_envelope_state_tombstones_by(stats.tombstones_diff);

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
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<O>>,
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
    use super::*;
    #[mz_ore::test]
    fn test_merge_update() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<()>::Snapshotting(Snapshotting::default());

        let small_row = Ok(mz_repr::Row::default());
        let longer_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Null]));
        s.merge_update(small_row, 1, opts, &mut buf);
        s.merge_update(longer_row.clone(), -1, opts, &mut buf);
        // This clears the retraction of the `longer_row`, but the
        // `value_xor` is the length of the `longer_row`. This tests
        // that we are tracking checksums correctly.
        s.merge_update(longer_row, 1, opts, &mut buf);

        // Assert that the `Snapshotting` value is fully merged.
        s.ensure_decoded(opts);
    }

    #[mz_ore::test]
    #[should_panic(
        expected = "invalid upsert state: len_sum is non-0, state: Snapshotting { len_sum: 1"
    )]
    fn test_merge_update_len_0_assert() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<()>::Snapshotting(Snapshotting::default());

        let small_row = Ok(mz_repr::Row::default());
        let longer_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Null]));
        s.merge_update(longer_row.clone(), 1, opts, &mut buf);
        s.merge_update(small_row.clone(), -1, opts, &mut buf);

        s.ensure_decoded(opts);
    }

    #[mz_ore::test]
    #[should_panic(
        expected = "invalid upsert state: \"value_xor is not the same length (3) as len (4), state: Snapshotting { len_sum: 4"
    )]
    fn test_merge_update_len_to_long_assert() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<()>::Snapshotting(Snapshotting::default());

        let small_row = Ok(mz_repr::Row::default());
        let longer_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Null]));
        s.merge_update(longer_row.clone(), 1, opts, &mut buf);
        s.merge_update(small_row.clone(), -1, opts, &mut buf);
        s.merge_update(longer_row.clone(), 1, opts, &mut buf);

        s.ensure_decoded(opts);
    }

    #[mz_ore::test]
    #[should_panic(expected = "invalid upsert state: checksum_sum does not match")]
    fn test_merge_update_checksum_doesnt_match() {
        let mut buf = Vec::new();
        let opts = upsert_bincode_opts();

        let mut s = StateValue::<()>::Snapshotting(Snapshotting::default());

        let small_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Int64(2)]));
        let longer_row = Ok(mz_repr::Row::pack([mz_repr::Datum::Int64(1)]));
        s.merge_update(longer_row.clone(), 1, opts, &mut buf);
        s.merge_update(small_row.clone(), -1, opts, &mut buf);
        s.merge_update(longer_row.clone(), 1, opts, &mut buf);

        s.ensure_decoded(opts);
    }
}
