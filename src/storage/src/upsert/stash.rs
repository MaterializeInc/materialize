// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A hybrid in-memory / RocksDB stash for buffering upsert updates.
//!
//! [`UpsertStash`] buffers pending upsert input updates in memory, spilling to
//! a RocksDB instance when the estimated memory usage exceeds a configurable
//! threshold. This avoids the I/O overhead of RocksDB for workloads that fit in
//! memory while still bounding memory usage for large snapshots.
//!
//! ## Architecture
//!
//! The stash has two tiers:
//!
//! 1. **In-memory buffer** ([`InMemoryStash`]): A `Vec` of [`StashEntry`]s with
//!    lightweight size tracking. All writes go here first.
//!
//! 2. **RocksDB backend** ([`RocksDbStash`]): When the in-memory buffer exceeds
//!    a configurable byte threshold, it is flushed here. RocksDB uses a merge
//!    operator to deduplicate entries by `(time, key)`, keeping the one with the
//!    largest `from_time`.
//!
//! [`UpsertStash`] coordinates between the two tiers. Once data has been
//! spilled, all subsequent writes go directly to RocksDB. When RocksDB is
//! fully drained, the stash switches back to in-memory mode.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::mem::size_of;

use bincode::Options;
use mz_repr::Row;
use mz_rocksdb::{KeyUpdate, RocksDBInstance};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{UpsertKey, UpsertValue};

/// A single stash entry: an upsert command buffered for later processing.
pub struct StashEntry<T, FromTime> {
    pub time: T,
    pub key: UpsertKey,
    /// Wrapped in [`Reverse`] so that the natural sort order puts the *largest*
    /// `from_time` first within a `(time, key)` group.
    pub from_time: Reverse<FromTime>,
    pub value: Option<UpsertValue>,
}

impl<T, FromTime> StashEntry<T, FromTime> {
    /// Estimated heap size of this entry (fixed struct size + any Row
    /// allocation).
    fn estimated_size(&self) -> usize {
        let row_heap = match &self.value {
            Some(Ok(row)) => row.byte_len().saturating_sub(size_of::<Row>()),
            _ => 0,
        };
        size_of::<Self>() + row_heap
    }
}

/// The value persisted in the RocksDB stash instance.
///
/// Contains both the ordering key (`from_time`) and the upsert value so the
/// merge operator can compare entries and keep the winner.
#[derive(Serialize, Deserialize, Clone)]
pub struct StashValue<FromTime> {
    pub from_time: FromTime,
    pub value: Option<UpsertValue>,
}

/// Merge function for the stash: keeps the entry with the largest `from_time`.
///
/// RocksDB calls this with all pending merge operands (and possibly a base
/// value) for a given key. We pick the maximum `from_time`.
pub fn stash_merge_function<FromTime: Ord>(
    _key: &[u8],
    values: impl Iterator<Item = StashValue<FromTime>>,
) -> StashValue<FromTime> {
    values
        .max_by(|a, b| a.from_time.cmp(&b.from_time))
        .expect("RocksDB merge called with no values")
}

/// A simple in-memory buffer of stash entries with byte-level size tracking.
struct InMemoryStash<T, FromTime> {
    entries: Vec<StashEntry<T, FromTime>>,
    estimated_bytes: usize,
}

impl<T, FromTime> InMemoryStash<T, FromTime> {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            estimated_bytes: 0,
        }
    }

    fn push(&mut self, entry: StashEntry<T, FromTime>) {
        self.estimated_bytes += entry.estimated_size();
        self.entries.push(entry);
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Partition entries: those satisfying `eligible` are returned, the rest
    /// are kept.
    fn drain_eligible(&mut self, eligible: impl Fn(&T) -> bool) -> Vec<StashEntry<T, FromTime>> {
        let mut kept = Vec::new();
        let mut drained = Vec::new();
        let mut kept_bytes = 0usize;

        for entry in self.entries.drain(..) {
            if eligible(&entry.time) {
                drained.push(entry);
            } else {
                kept_bytes += entry.estimated_size();
                kept.push(entry);
            }
        }

        self.entries = kept;
        self.estimated_bytes = kept_bytes;
        drained
    }

    /// Take all entries, resetting the buffer.
    fn take_all(&mut self) -> Vec<StashEntry<T, FromTime>> {
        self.estimated_bytes = 0;
        std::mem::take(&mut self.entries)
    }
}

impl<T: Ord, FromTime> InMemoryStash<T, FromTime> {
    fn min_time(&self) -> Option<&T> {
        self.entries.iter().map(|e| &e.time).min()
    }
}

/// Encapsulates all RocksDB-specific logic: key encoding, writes, prefix scans,
/// and per-timestamp bookkeeping.
struct RocksDbStash<T, FromTime> {
    rocksdb: RocksDBInstance<Vec<u8>, StashValue<FromTime>>,
    /// Per-timestamp metadata: count of entries and the serialized time prefix
    /// used for key encoding and prefix scanning.
    time_info: BTreeMap<T, RocksDbTimeInfo>,
}

struct RocksDbTimeInfo {
    count: usize,
    /// Bincode serialization of the timestamp, used as the key prefix.
    prefix: Vec<u8>,
}

impl<T, FromTime> RocksDbStash<T, FromTime>
where
    T: Ord + Clone + Serialize + DeserializeOwned,
    FromTime: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn new(rocksdb: RocksDBInstance<Vec<u8>, StashValue<FromTime>>) -> Self {
        Self {
            rocksdb,
            time_info: BTreeMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.time_info.is_empty()
    }

    fn min_time(&self) -> Option<&T> {
        self.time_info.keys().next()
    }

    fn entry_count(&self) -> usize {
        self.time_info.values().map(|info| info.count).sum()
    }

    /// Encode a RocksDB key: `[time_prefix | upsert_key_bytes]`.
    fn encode_key(prefix: &[u8], key: &UpsertKey) -> Vec<u8> {
        let mut buf = Vec::with_capacity(prefix.len() + UpsertKey::SIZE);
        buf.extend_from_slice(prefix);
        buf.extend_from_slice(key.as_ref());
        buf
    }

    /// Decode an `UpsertKey` from a raw RocksDB key, given the prefix length.
    fn decode_key(raw_key: &[u8], prefix_len: usize) -> UpsertKey {
        debug_assert!(
            raw_key.len() >= prefix_len + UpsertKey::SIZE,
            "stash key too short: expected at least {} bytes, got {}",
            prefix_len + UpsertKey::SIZE,
            raw_key.len()
        );
        UpsertKey::from(&raw_key[prefix_len..prefix_len + UpsertKey::SIZE])
    }

    /// Write a batch of entries to RocksDB using merge operations.
    ///
    /// Returns the number of entries written.
    async fn write_batch(
        &mut self,
        entries: impl IntoIterator<Item = (T, UpsertKey, FromTime, Option<UpsertValue>)>,
    ) -> Result<usize, anyhow::Error> {
        let bincode_opts = bincode::DefaultOptions::new();
        let mut batch = Vec::new();

        for (time, key, from_time, value) in entries {
            let info = self
                .time_info
                .entry(time)
                .or_insert_with_key(|t| RocksDbTimeInfo {
                    count: 0,
                    prefix: bincode_opts
                        .serialize(t)
                        .expect("timestamp serialization should not fail"),
                });
            info.count += 1;

            let stash_key = Self::encode_key(&info.prefix, &key);
            batch.push((
                stash_key,
                KeyUpdate::Merge(StashValue { from_time, value }),
                None,
            ));
        }

        let count = batch.len();
        if !batch.is_empty() {
            self.rocksdb.multi_update(batch).await?;
        }
        Ok(count)
    }

    /// Scan and delete all entries for timestamps satisfying `eligible`.
    ///
    /// Returns the drained entries and the count of entries removed.
    async fn drain_eligible(
        &mut self,
        eligible: impl Fn(&T) -> bool,
    ) -> Result<(Vec<StashEntry<T, FromTime>>, usize), anyhow::Error> {
        let eligible_times: Vec<(T, Vec<u8>)> = self
            .time_info
            .iter()
            .filter(|(t, _)| eligible(t))
            .map(|(t, info)| (t.clone(), info.prefix.clone()))
            .collect();

        if eligible_times.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut results = Vec::new();
        let mut to_delete = Vec::new();

        for (time, prefix) in &eligible_times {
            for (raw_key, stash_value) in self.rocksdb.prefix_scan(prefix.clone()).await? {
                let key = Self::decode_key(&raw_key, prefix.len());
                results.push(StashEntry {
                    time: time.clone(),
                    key,
                    from_time: Reverse(stash_value.from_time),
                    value: stash_value.value,
                });
                to_delete.push((raw_key, KeyUpdate::Delete, None));
            }
        }

        let drained_count = results.len();

        if !to_delete.is_empty() {
            self.rocksdb.multi_update(to_delete).await?;
        }
        for (time, _) in &eligible_times {
            self.time_info.remove(time);
        }

        Ok((results, drained_count))
    }
}

/// A hybrid in-memory / RocksDB stash for buffering upsert input updates.
///
/// Updates are buffered in memory until estimated memory usage exceeds
/// `spill_threshold`, at which point the buffer is flushed to RocksDB.
/// Draining collects from both tiers and deduplicates.
pub struct UpsertStash<T, FromTime> {
    mem: InMemoryStash<T, FromTime>,
    rocks: RocksDbStash<T, FromTime>,
    /// Byte threshold before spilling to RocksDB.
    spill_threshold: usize,
    /// Once data has been spilled, subsequent writes go directly to RocksDB to
    /// maintain deduplication invariants with the merge operator.
    has_spilled: bool,
}

impl<T, FromTime> UpsertStash<T, FromTime>
where
    T: Ord + Clone + Serialize + DeserializeOwned,
    FromTime: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a new stash.
    ///
    /// `spill_threshold` is the estimated byte budget for the in-memory buffer.
    /// Use `usize::MAX` to never spill, or `0` to always use RocksDB.
    pub fn new(
        rocksdb: RocksDBInstance<Vec<u8>, StashValue<FromTime>>,
        spill_threshold: usize,
    ) -> Self {
        Self {
            mem: InMemoryStash::new(),
            rocks: RocksDbStash::new(rocksdb),
            spill_threshold,
            has_spilled: false,
        }
    }

    /// Buffer a batch of updates. Spills to RocksDB if the memory threshold is
    /// exceeded.
    pub async fn stage_batch(
        &mut self,
        updates: impl IntoIterator<Item = (T, UpsertKey, FromTime, Option<UpsertValue>)>,
    ) -> Result<(), anyhow::Error> {
        if self.has_spilled {
            self.rocks.write_batch(updates).await?;
            return Ok(());
        }

        for (time, key, from_time, value) in updates {
            self.mem.push(StashEntry {
                time,
                key,
                from_time: Reverse(from_time),
                value,
            });
        }

        if self.mem.estimated_bytes > self.spill_threshold {
            self.flush_mem_to_rocks().await?;
        }

        Ok(())
    }

    /// Drain all entries whose timestamp satisfies `eligible`.
    ///
    /// Returns deduplicated entries (one per `(time, key)`, with the largest
    /// `from_time`).
    pub async fn drain(
        &mut self,
        eligible: impl Fn(&T) -> bool,
    ) -> Result<Vec<StashEntry<T, FromTime>>, anyhow::Error> {
        let mut results = self.mem.drain_eligible(&eligible);

        let (rocks_results, _) = self.rocks.drain_eligible(&eligible).await?;
        results.extend(rocks_results);

        // Dedup by (time, key), keeping the largest from_time (smallest
        // Reverse<FromTime>). Always needed: the in-memory buffer doesn't
        // deduplicate on insert, and mixed-tier results need merging too.
        if results.len() > 1 {
            results.sort_unstable_by(|a, b| {
                (&a.time, &a.key, &a.from_time).cmp(&(&b.time, &b.key, &b.from_time))
            });
            results.dedup_by(|a, b| a.time == b.time && a.key == b.key);
        }

        // Once RocksDB is fully drained, switch back to the in-memory fast
        // path for future writes.
        if self.rocks.is_empty() {
            self.has_spilled = false;
        }

        Ok(results)
    }

    pub fn is_empty(&self) -> bool {
        self.mem.is_empty() && self.rocks.is_empty()
    }

    pub fn min_time(&self) -> Option<&T> {
        match (self.mem.min_time(), self.rocks.min_time()) {
            (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
            (some @ Some(_), None) | (None, some @ Some(_)) => some,
            (None, None) => None,
        }
    }

    /// Approximate number of buffered entries. May overcount when the same
    /// `(time, key)` appears in both tiers (dedup happens at drain time).
    pub fn len(&self) -> usize {
        self.mem.entries.len() + self.rocks.entry_count()
    }

    /// Flush the in-memory buffer into RocksDB.
    async fn flush_mem_to_rocks(&mut self) -> Result<(), anyhow::Error> {
        if self.mem.is_empty() {
            return Ok(());
        }
        self.has_spilled = true;

        let entries = self.mem.take_all();
        let updates = entries
            .into_iter()
            .map(|e| (e.time, e.key, e.from_time.0, e.value));
        self.rocks.write_batch(updates).await?;
        Ok(())
    }
}
