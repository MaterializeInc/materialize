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

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use mz_ore::error::ErrorExt;
use mz_repr::Diff;
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

/// `UpsertState` stores values associated with keys, supporting both finalized and provisional
/// values.
///
/// Normal operation is simple, we just store an ordinary `UpsertValue`, and allow the implementer
/// to store it any way they want.
///
/// Note also that this type is designed to support _partial updates_. All values are
/// associated with an _order key_ `O` that can be used to determine if a value existing in the
/// `UpsertStateBackend` occurred before or after a value being considered for insertion.
///
/// `O` typically required to be `: Default`, with the default value sorting below all others.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum StateValue<T, O> {
    Value(Value<T, O>),
}

impl<T, O> std::fmt::Debug for StateValue<T, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
pub struct Value<T, O> {
    /// The finalized value of a key is the value we know to be correct for the last complete
    /// timestamp that got processed. A finalized value of None means that the key has been deleted
    /// and acts as a tombstone.
    pub finalized: Option<UpsertValue>,
    /// When `Some(_)` it contains the upsert value has been processed for a yet incomplete
    /// timestamp. When None, no provisional update has been emitted yet.
    pub provisional: Option<ProvisionalValue<T, O>>,
}

/// A provisional value emitted for a timestamp. This struct contains enough information to
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub struct ProvisionalValue<T, O> {
    /// The timestamp at which this provisional value occured at
    pub timestamp: T,
    /// The order of this upsert command *within* the timestamp. Commands that happen at the same
    /// timestamp with lower order get ignored. Commands with higher order override this one. If
    /// there a case of equal order then the value itself is used as a tie breaker.
    pub order: O,
    /// The provisional value. A provisional value of None means that the key has been deleted and
    /// acts as a tombstone.
    pub value: Option<UpsertValue>,
}

impl<T, O> StateValue<T, O> {
    #[allow(unused)]
    /// A tombstoned value.
    pub fn tombstone() -> Self {
        Self::Value(Value {
            finalized: None,
            provisional: None,
        })
    }

    /// Whether the value is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        match self {
            Self::Value(value) => value.finalized.is_none(),
        }
    }

    /// Pull out the `Value` value for a `StateValue`.
    pub fn into_decoded(self) -> Value<T, O> {
        match self {
            Self::Value(value) => value,
        }
    }

    /// The size of a `StateValue`, in memory. This is:
    /// 1. only used in the `InMemoryHashMap` implementation.
    /// 2. An estimate (it only looks at value sizes, and not errors)
    ///
    /// Other implementations may use more accurate accounting.
    #[cfg(test)]
    pub fn memory_size(&self) -> usize {
        use mz_repr::Row;
        use std::mem::size_of;

        let heap_size = match self {
            Self::Value(value) => {
                let finalized_heap_size = match value.finalized {
                    Some(Ok(ref row)) => {
                        // `Row::byte_len` includes the size of `Row`, which is also in `Self`, so we
                        // subtract it.
                        row.byte_len() - size_of::<Row>()
                    }
                    // Assume errors are rare enough to not move the needle.
                    _ => 0,
                };
                let provisional_heap_size = match value.provisional {
                    Some(ref provisional) => match provisional.value {
                        Some(Ok(ref row)) => {
                            // `Row::byte_len` includes the size of `Row`, which is also in `Self`, so we
                            // subtract it.
                            row.byte_len() - size_of::<Row>()
                        }
                        // Assume errors are rare enough to not move the needle.
                        _ => 0,
                    },
                    None => 0,
                };
                finalized_heap_size + provisional_heap_size
            }
        };
        heap_size + size_of::<Self>()
    }
}

impl<T: Eq, O> StateValue<T, O> {
    /// Creates a new provisional value, occurring at some order key, observed
    /// at the given timestamp.
    pub fn new_provisional_value(value: UpsertValue, timestamp: T, order: O) -> Self {
        Self::Value(Value {
            finalized: None,
            provisional: Some(ProvisionalValue {
                value: Some(value),
                timestamp,
                order,
            }),
        })
    }

    /// Creates a provisional value, that retains the finalized value in this `StateValue`.
    pub fn into_provisional_value(self, value: UpsertValue, timestamp: T, order: O) -> Self {
        match self {
            StateValue::Value(finalized) => Self::Value(Value {
                finalized: finalized.finalized,
                provisional: Some(ProvisionalValue {
                    value: Some(value),
                    timestamp,
                    order,
                }),
            }),
        }
    }

    /// Creates a new provisional tombstone occurring at some order key,
    /// observed at the given timestamp.
    pub fn new_provisional_tombstone(timestamp: T, order: O) -> Self {
        Self::Value(Value {
            finalized: None,
            provisional: Some(ProvisionalValue {
                value: None,
                timestamp,
                order,
            }),
        })
    }

    /// Creates a provisional tombstone, that retains the finalized value in this `StateValue`.
    ///
    /// We record the current finalized value, so that we can present it when
    /// needed or when trying to read a provisional value at a different
    /// timestamp.
    pub fn into_provisional_tombstone(self, timestamp: T, order: O) -> Self {
        match self {
            StateValue::Value(finalized) => Self::Value(Value {
                finalized: finalized.finalized,
                provisional: Some(ProvisionalValue {
                    value: None,
                    timestamp,
                    order,
                }),
            }),
        }
    }

    /// Returns the order of a provisional value at the given timestamp, if any.
    pub fn provisional_order(&self, ts: &T) -> Option<&O> {
        match self {
            Self::Value(value) => match &value.provisional {
                Some(p) if &p.timestamp == ts => Some(&p.order),
                _ => None,
            },
        }
    }

    /// Returns the provisional value, if one is present at the given timestamp.
    /// Falls back to the finalized value, or `None` if there is neither.
    pub fn provisional_value_ref(&self, ts: &T) -> Option<&UpsertValue> {
        match self {
            Self::Value(value) => match &value.provisional {
                Some(p) if &p.timestamp == ts => p.value.as_ref(),
                _ => value.finalized.as_ref(),
            },
        }
    }

}

impl<T, O> Default for StateValue<T, O> {
    fn default() -> Self {
        Self::Value(Value {
            finalized: None,
            provisional: None,
        })
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
}

/// An `UpsertStateBackend` wrapper that reports basic metrics about the usage
/// of the `UpsertStateBackend`.
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
    /// been processed.
    ///
    /// The `completed` boolean communicates whether or not this is the final
    /// chunk of updates for the initial "snapshot" from persist.
    ///
    /// Since persist delivers fully consolidated data, each (key, value) update
    /// has diff exactly +1 or -1. This method reads existing state, applies the
    /// updates, and writes the results back.
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

        let phase = if !self.snapshot_completed {
            "rehydration"
        } else {
            "steady-state"
        };

        let now = Instant::now();
        let batch_size = updates.len();

        tracing::trace!(
            %phase,
            batch_size,
            completed,
            "consolidate_chunk: processing batch"
        );

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

        let stats = self.consolidate_read_write_inner(updates).await?;

        // NOTE: These metrics use the term `merge` to refer to the consolidation of values.
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

    /// Consolidates the updates into the state. This method reads the existing
    /// values for each key, applies the consolidated updates (each with diff +1
    /// or -1), and writes the new values back to the state.
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
                // multi_put below. This makes sure we report the same stats
                // regardless of what values there were in state before.
                stats.values_diff += diff;

                let entry = self.consolidate_upsert_scratch.get_mut(&key).unwrap();
                let val = entry
                    .value
                    .get_or_insert_with(|| StateValue::tombstone());

                // Since persist delivers fully consolidated data, each update
                // has diff exactly +1 or -1.
                if diff.is_positive() {
                    // Insertion: set the finalized value.
                    match val {
                        StateValue::Value(v) => {
                            v.finalized = Some(value);
                        }
                    }
                } else {
                    // Retraction: clear the finalized value (tombstone).
                    match val {
                        StateValue::Value(v) => {
                            v.finalized = None;
                        }
                    }
                }
            }

            // If a key ended up as a tombstone (finalized == None), remove it
            // from state entirely (value = None in PutValue means delete).
            // Note we do 1 `multi_get` and 1 `multi_put` while processing a _batch of updates_.
            // Within the batch, we effectively consolidate each key, before persisting that
            // consolidated value. Easy!!
            let p_stats = self
                .inner
                .multi_put(self.consolidate_upsert_scratch.drain(..).map(|(k, v)| {
                    let is_tombstone = v
                        .value
                        .as_ref()
                        .map_or(true, |sv| sv.is_tombstone());
                    (
                        k,
                        PutValue {
                            value: if is_tombstone { None } else { v.value },
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

    // We guard some of our assumptions. Increasing in-memory size of StateValue
    // has a direct impact on memory usage of in-memory UPSERT sources.
    #[mz_ore::test]
    fn test_memory_size() {
        let finalized_value: StateValue<(), ()> = StateValue::Value(Value {
            finalized: Some(Ok(Row::default())),
            provisional: None,
        });
        assert!(
            finalized_value.memory_size() <= 64,
            "memory size is {}",
            finalized_value.memory_size(),
        );

        let provisional_value_with_finalized_value: StateValue<(), ()> =
            finalized_value.into_provisional_value(Ok(Row::default()), (), ());
        assert!(
            provisional_value_with_finalized_value.memory_size() <= 64,
            "memory size is {}",
            provisional_value_with_finalized_value.memory_size(),
        );

        let provisional_value_without_finalized_value: StateValue<(), ()> =
            StateValue::new_provisional_value(Ok(Row::default()), (), ());
        assert!(
            provisional_value_without_finalized_value.memory_size() <= 64,
            "memory size is {}",
            provisional_value_without_finalized_value.memory_size(),
        );
    }
}
