// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines the `UpsertStateBackend` trait and various implementations.
//! This trait is the way the `upsert` operator interacts with various state backings.
//!
//! Because its a complex trait with a somewhat leaky abstraction, it warrants a high-level
//! description, explaining the complexity. The trait has 3 methods:
//!
//! ## `multi_get`
//! `multi_get` returns the current value for a (unique) set of keys. To keep implementations
//! efficient, the set of keys is an iterator, and results are written back into another parallel
//! iterator. In addition to returning the current values, implementations must also return the
//! _size_ of those values _as they are stored within the implementation_. Implementations are
//! required to chunk large iterators if they need to operate over smaller batches.
//!
//! ## `multi_put`
//! Update or delete values for a set of keys. To keep implementations efficient, the set
//! of updates is an iterator. Implementations are also required to return the difference
//! in values and total size after processing the updates. To simplify this (and because
//! in the `upsert` usecase we have this data readily available), the updates are input
//! with the size of the current value (if any) that was returned from a previous `multi_get`.
//! Implementations are required to chunk large iterators if they need to operate over smaller
//! batches.
//!
//! ## `merge_snapshot_chunk`
//! The most complicated method, this method requires implementations to consolidate a _chunk_ of
//! updates into their state. This method effectively asks implementations to implement the logic in
//! <https://docs.rs/differential-dataflow/latest/differential_dataflow/consolidation/fn.consolidate.html>,
//! but under the assumption that the set of updates is a valid upsert `Collection`. Note that this
//! allows implementations to do this a memory-efficient (or even, _memory-bounded) way. Because
//! this is non-trivial, this module provides `StateValue`, which implements some of the core logic
//! required to do this. `StateValue::merge_update` has more information about this.
//!
//! `merge_snapshot_chunk` has to return stats about the number of values and size of the state,
//! just like `multi_put`.
//!
//! Another curiosity is that implementation can assume that `merge_snapshot_chunk` is called with
//! a set of updates with a number of keys not greater than `UpsertStateBackend::SNAPSHOT_BATCH_SIZE`. This
//! is different than `multi_put` and `multi_get` purely because it simplifies the way that the `upsert`
//! operator handles snapshots.
//!
//!
//! # A note on state size
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

use std::num::Wrapping;
use std::sync::Arc;
use std::time::Instant;

use bincode::Options;
use itertools::Itertools;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::HashMap;

use crate::render::upsert::{UpsertKey, UpsertValue};
use crate::source::metrics::UpsertSharedMetrics;

/// The default set of `bincode` options used for consolidating
/// upsert snapshots (and writing values to RocksDB).
pub type BincodeOpts = bincode::config::DefaultOptions;

/// Build the default `BincodeOpts`.
pub fn upsert_bincode_opts() -> BincodeOpts {
    // We don't allow trailing bytes, for now,
    // and use varint encoding for space saving.
    bincode::DefaultOptions::new()
}

/// The result type for individual gets.
// The value and size are stored in individual options,
// so that during upsert processing, we can track the
// _original size of a value we get_ separate from
// the updated value we want to write back to the `UpsertStateBackend`
// implementation.
#[derive(Debug, Default, Clone)]
pub struct UpsertValueAndSize {
    /// The value, if there was one.
    pub value: Option<StateValue>,
    /// The size of original`value` as persisted,
    /// Useful for users keeping track of statistics.
    pub size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct PutValue<V> {
    pub value: Option<V>,
    pub previous_persisted_size: Option<i64>,
}

/// In any `UpsertStateBackend` implementation, we need to support 2 modes:
/// - Normal operation
/// - Consolidation of snapshots (during rehydration).
///
/// This struct (and `Snapshotting`) is effectively a helper to simplify the logic that
/// individual `UpsertStateBackend` implementations need to do to manage these 2 modes.
///
/// Normal operation is easy, we just store an ordinary `UpsertValue`, and allow the implementer
/// to store it any way they want. During consolidation of snapshots, the logic is more complex.
/// See the docs on `StateValue::merge_update` for more information.
///
/// This struct is not part of the `UpsertStateBackend` public API, but implementing that API without
/// using it is considered hard-mode.
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub enum StateValue {
    Snapshotting(Snapshotting),
    Decoded(UpsertValue),
}

/// A value as produced during consolidation of a snapshot.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize, Debug)]
pub struct Snapshotting {
    value_xor: Vec<u8>,
    len_sum: Wrapping<i64>,
    checksum_sum: Wrapping<i64>,
    diff_sum: Wrapping<i64>,
}

impl From<UpsertValue> for StateValue {
    fn from(uv: UpsertValue) -> Self {
        Self::Decoded(uv)
    }
}

impl StateValue {
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
                for (acc, val) in value_xor.iter_mut().zip(bincode_buffer.drain(..)) {
                    *acc ^= val;
                }
            }

            // Returns whether or not the value can be deleted. This allows us to delete values in
            // `UpsertState::merge_snapshot_chunk` (even if they come back later) during snapshotting,
            // to minimize space usage.
            return diff_sum.0 == 0 && checksum_sum.0 == 0 && value_xor.iter().all(|&x| x == 0);
        } else {
            panic!("`merge_update` called after snapshot consolidation")
        }
    }

    /// After consolidation of a snapshot, we assume that all values in the `UpsertStateBackend` implementation
    /// are `Self::Snapshotting`, with a `diff_sum` of 1 (or 0, if they have been deleted).
    /// Afterwards, if we need to retract one of these values, we need to assert that its in this correct state,
    /// then mutate it to its `Decoded` state, so the `upsert` operator can use it.
    #[allow(clippy::as_conversions)]
    pub fn ensure_decoded(&mut self, bincode_opts: BincodeOpts) {
        match self {
            StateValue::Snapshotting(Snapshotting {
                value_xor,
                len_sum,
                checksum_sum,
                diff_sum,
            }) => match diff_sum.0 {
                1 => {
                    let len = usize::try_from(len_sum.0).expect("invalid upsert state");
                    let value = &value_xor[..len];
                    // Truncation is fine (using `as`) as this is just a checksum
                    assert_eq!(
                        checksum_sum.0,
                        seahash::hash(value_xor) as i64,
                        "invalid upsert state"
                    );
                    *self = Self::Decoded(bincode_opts.deserialize(value).unwrap());
                }
                0 => {
                    assert_eq!(len_sum.0, 0, "invalid upsert state");
                    assert_eq!(checksum_sum.0, 0, "invalid upsert state");
                    assert!(value_xor.iter().all(|&x| x == 0), "invalid upsert state");
                }
                _ => panic!("invalid upsert state"),
            },
            _ => {}
        }
    }

    /// Pull out the `Decoded` value for a `StateValue`, after `ensure_decoded` has been called.
    pub fn to_decoded(self) -> UpsertValue {
        match self {
            Self::Decoded(v) => v,
            _ => panic!("called `to_decoded without calling `ensure_decoded`"),
        }
    }

    /// The size of a `StateValue`, in memory. This is:
    /// 1. only used in the `InMemoryHashMap` implementation.
    /// 2. An estimate (it only looks at value sizes, and not errors)
    ///
    /// Other implementations may use more accurate accounting.
    pub fn memory_size(&self) -> u64 {
        match self {
            // similar to `Row::byte_len`, we add the heap size and the size of the value itself.
            Self::Snapshotting(Snapshotting { value_xor, .. }) => {
                u64::cast_from(value_xor.len()) + u64::cast_from(std::mem::size_of::<Self>())
            }

            Self::Decoded(Ok(row)) => {
                // `Row::byte_len` includes the size of `Row`, which is also in `Self`, so we
                // subtract it.
                u64::cast_from(row.byte_len()) + u64::cast_from(std::mem::size_of::<Self>())
                    - u64::cast_from(std::mem::size_of::<mz_repr::Row>())
            }
            Self::Decoded(Err(_)) => {
                // Assume errors are rare enough to not move the needle.
                0
            }
        }
    }
}

impl Default for StateValue {
    fn default() -> Self {
        Self::Snapshotting(Snapshotting::default())
    }
}

/// Statistics for a single call to `merge_snapshot_chunk`.
#[derive(Clone, Default, Debug)]
pub struct MergeStats {
    /// The number of updates processed.
    pub updates: u64,
    /// The aggregated number of values inserted or deleted into `state`
    pub values_diff: i64,
    /// The total aggregated size of values inserted, deleted, or updated in `state`.
    /// If the current call to `merge_snapshot_chunk` deletes a lot of values,
    /// or updates values to smaller ones, this can be negative!
    pub size_diff: i64,
}

impl std::ops::AddAssign for MergeStats {
    fn add_assign(&mut self, rhs: Self) {
        self.updates += rhs.updates;
        self.values_diff += rhs.values_diff;
        self.size_diff += rhs.size_diff;
    }
}

/// Statistics for a single call to `multi_put`.
#[derive(Clone, Default, Debug)]
pub struct PutStats {
    /// The number of puts/deletes processed
    pub processed_puts: u64,
    /// The aggregated number of values inserted or deleted into `state`
    pub values_diff: i64,
    /// The total aggregated size of values inserted, deleted, or updated in `state`.
    /// If the current call to `multi_put` deletes a lot of values,
    /// or updates values to smaller ones, this can be negative!
    pub size_diff: i64,
}

/// Statistics for a single call to `multi_get`.
#[derive(Clone, Default, Debug)]
pub struct GetStats {
    /// The number of puts/deletes processed
    pub processed_gets: u64,
}

/// A trait that defines the fundamental primitives required by a state-backing of
/// the `upsert` operator.
#[async_trait::async_trait(?Send)]
pub trait UpsertStateBackend {
    /// Unlike `multi_get` and `multi_put`, which require the implementer
    /// to chunk large iterators as they please, `merge_snapshot_chunk` allows
    /// the implementer to specify their preferred batch size. This batch size
    /// refers to the number of keys being merged into the state.
    ///
    /// This is different from `multi_get` and `multi_put` because snapshots
    /// are merged from asynchronous, batched timely iterators, as opposed to normal
    /// sync iterators.
    const SNAPSHOT_BATCH_SIZE: usize;

    /// Insert or delete for all `puts` keys, prioritizing the last value for
    /// repeated keys.
    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue>)>;

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
        R: IntoIterator<Item = &'r mut UpsertValueAndSize>;
}

/// A `HashMap` with additional scratch space used in some
/// methods.
pub struct InMemoryHashMap {
    state: HashMap<UpsertKey, StateValue>,
}

impl Default for InMemoryHashMap {
    fn default() -> Self {
        Self {
            state: HashMap::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl UpsertStateBackend for InMemoryHashMap {
    const SNAPSHOT_BATCH_SIZE: usize = 1;

    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue>)>,
    {
        let mut stats = PutStats::default();
        for (key, p_value) in puts {
            stats.processed_puts += 1;
            match p_value.value {
                Some(value) => {
                    let size: i64 = value.memory_size().try_into().expect("less than i64 size");
                    match p_value.previous_persisted_size {
                        Some(previous_size) => {
                            stats.size_diff -= previous_size;
                            stats.size_diff += size;
                        }
                        None => {
                            stats.values_diff += 1;
                            stats.size_diff += size;
                        }
                    }
                    self.state.insert(key, value);
                }
                None => {
                    if let Some(previous_size) = p_value.previous_persisted_size {
                        stats.size_diff -= previous_size;
                        stats.values_diff -= 1;
                    }
                    self.state.remove(&key);
                }
            }
        }
        Ok(stats)
    }

    async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize>,
    {
        let mut stats = GetStats::default();
        for (key, result_out) in gets.into_iter().zip_eq(results_out) {
            stats.processed_gets += 1;
            let value = self.state.get(&key).cloned();
            let size = value.as_ref().map(|v| v.memory_size());
            *result_out = UpsertValueAndSize { value, size };
        }
        Ok(stats)
    }
}

/// An `UpsertStateBackend` wrapper that supports
/// snapshot merging, and reports basic metrics about the usage of the `UpsertStateBackend`.
pub struct UpsertState<S> {
    inner: S,
    metrics: Arc<UpsertSharedMetrics>,

    // Bincode options and buffer used
    // in `merge_snapshot_chunk`.
    bincode_opts: BincodeOpts,
    bincode_buffer: Vec<u8>,

    // We need to iterator over `merges` in `merge_snapshot_chunk`
    // twice, so we have a scratch vector for this.
    merge_scratch: Vec<(UpsertKey, UpsertValue, mz_repr::Diff)>,
    // "mini-upsert" map used in `merge_snapshot_chunk`, plus a
    // scratch vector for calling `multi_get`
    merge_upsert_scratch: indexmap::IndexMap<UpsertKey, UpsertValueAndSize>,
    multi_get_scratch: Vec<UpsertKey>,
}

impl<S> UpsertState<S> {
    pub(crate) fn new(inner: S, metrics: Arc<UpsertSharedMetrics>) -> Self {
        Self {
            inner,
            metrics,
            bincode_opts: upsert_bincode_opts(),
            bincode_buffer: Vec::new(),
            merge_scratch: Vec::new(),
            merge_upsert_scratch: indexmap::IndexMap::new(),
            multi_get_scratch: Vec::new(),
        }
    }
}

impl<S> UpsertState<S>
where
    S: UpsertStateBackend,
{
    /// Merge and consolidate the following updates into the state, during snapshotting.
    ///
    /// After an ensure snapshot has been `merged`, all values must be in the correct state
    /// (as determined by `StateValue::ensure_decoded`, and `merge_snapshot_chunk` must NOT
    /// be called again.
    // Note that this does not allow `UpsertStateBackend` backends to optimize this functionality,
    // (for example, using RocksDB's native merge functionality), which would be more
    // performant. This is because:
    // - We don't have proof we need that performance.
    // - This implementation is simpler (things like the RocksDB merge API are quite difficult to use
    // properly
    //
    // Additionally:
    // - Keeping track of stats is way easier this way
    // - The implementation is uses the exact same "mini-upsert" technique used in the upsert
    // operator.
    pub async fn merge_snapshot_chunk<M>(&mut self, merges: M) -> Result<MergeStats, anyhow::Error>
    where
        M: IntoIterator<Item = (UpsertKey, UpsertValue, mz_repr::Diff)>,
    {
        let now = Instant::now();
        let mut merges = merges.into_iter().peekable();

        self.merge_scratch.clear();
        self.merge_upsert_scratch.clear();
        self.multi_get_scratch.clear();

        let mut stats = MergeStats::default();

        if merges.peek().is_some() {
            self.merge_scratch.extend(merges);
            self.merge_upsert_scratch.extend(
                self.merge_scratch
                    .iter()
                    .map(|(k, _, _)| (*k, UpsertValueAndSize::default())),
            );
            self.multi_get_scratch
                .extend(self.merge_upsert_scratch.iter().map(|(k, _)| *k));
            self.inner
                .multi_get(
                    self.multi_get_scratch.drain(..),
                    self.merge_upsert_scratch.iter_mut().map(|(_, v)| v),
                )
                .await?;

            for (key, value, diff) in self.merge_scratch.drain(..) {
                stats.updates += 1;
                let entry = self.merge_upsert_scratch.get_mut(&key).unwrap();
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
                .multi_put(self.merge_upsert_scratch.drain(..).map(|(k, v)| {
                    (
                        k,
                        PutValue {
                            value: v.value,
                            previous_persisted_size: v
                                .size
                                .map(|v| v.try_into().expect("less than i64 size")),
                        },
                    )
                }))
                .await?;

            stats.values_diff = p_stats.values_diff;
            stats.size_diff = p_stats.size_diff;
        }

        self.metrics
            .merge_snapshot_latency
            .observe(now.elapsed().as_secs_f64());
        self.metrics
            .merge_snapshot_updates
            .observe(f64::cast_lossy(stats.updates));

        Ok(stats)
    }

    /// Insert or delete for all `puts` keys, prioritizing the last value for
    /// repeated keys.
    pub async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<UpsertValue>)>,
    {
        let now = Instant::now();
        let stats = self
            .inner
            .multi_put(puts.into_iter().map(|(k, pv)| {
                (
                    k,
                    PutValue {
                        value: pv.value.map(StateValue::Decoded),
                        previous_persisted_size: pv.previous_persisted_size,
                    },
                )
            }))
            .await?;

        self.metrics
            .multi_put_latency
            .observe(now.elapsed().as_secs_f64());
        self.metrics
            .multi_put_size
            .observe(f64::cast_lossy(stats.processed_puts));

        Ok(stats)
    }

    /// Get the `gets` keys, which must be unique, placing the results in `results_out`.
    ///
    /// Panics if `gets` and `results_out` are not the same length.
    pub async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize>,
    {
        let now = Instant::now();
        let stats = self.inner.multi_get(gets, results_out).await?;

        self.metrics
            .multi_get_latency
            .observe(now.elapsed().as_secs_f64());
        self.metrics
            .multi_get_size
            .observe(f64::cast_lossy(stats.processed_gets));

        Ok(stats)
    }
}
