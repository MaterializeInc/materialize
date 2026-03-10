// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A RocksDB-backed stash for buffering upsert updates that cannot yet be applied.
//!
//! [`UpsertStash`] replaces the in-memory `Vec` stash used in the upsert operator
//! with a RocksDB instance, allowing the stash to spill to disk when the volume of
//! buffered updates exceeds available memory.
//!
//! ## Key encoding
//!
//! Each stash entry is keyed by a `Vec<u8>` composed of two concatenated parts:
//!
//! 1. **Time prefix**: the bincode serialization of `T` (the timestamp type).
//!    All entries for the same timestamp share the same prefix bytes, enabling
//!    efficient prefix scanning.
//! 2. **UpsertKey bytes**: the 32-byte `UpsertKey` hash.
//!
//! Because a RocksDB merge operator keeps only the entry with the largest
//! `FromTime` for each `(T, UpsertKey)`, no sort or dedup is needed at drain
//! time.
//!
//! ## Value encoding
//!
//! The value stored is [`StashValue<FromTime>`], containing both the `from_time`
//! ordering key and the `Option<UpsertValue>`. The merge operator compares by
//! `from_time` and keeps the maximum.
//!
//! ## Draining
//!
//! [`UpsertStash::drain`] scans entries by time prefix for all eligible times,
//! deletes them from RocksDB, and returns them as
//! `(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)`. Since the merge
//! operator already deduplicates by `(T, UpsertKey)`, no post-scan sort or dedup
//! is required.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use bincode::Options;
use mz_rocksdb::{KeyUpdate, RocksDBInstance};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{UpsertKey, UpsertValue};

/// The value stored in the stash RocksDB instance. Contains both the ordering
/// key (`from_time`) and the upsert value, so the merge operator can compare
/// entries and keep the one with the largest `from_time`.
#[derive(Serialize, Deserialize, Clone)]
pub struct StashValue<FromTime> {
    pub from_time: FromTime,
    pub value: Option<UpsertValue>,
}

/// Merge function for the stash: keeps the entry with the largest `from_time`.
///
/// This follows the pattern of `consolidating_merge_function` in `types.rs`.
/// RocksDB calls this with all pending merge operands (and possibly a base
/// value) for a given key. We simply pick the one with the maximum `from_time`.
pub fn stash_merge_function<FromTime: Ord>(
    _key: &[u8],
    values: impl Iterator<Item = StashValue<FromTime>>,
) -> StashValue<FromTime> {
    values
        .max_by(|a, b| a.from_time.cmp(&b.from_time))
        .expect("RocksDB merge called with no values")
}

/// Metadata tracked per distinct timestamp in the stash.
struct TimeInfo {
    /// Number of entries stashed for this timestamp.
    count: usize,
    /// The bincode-serialized bytes of the timestamp, used as the prefix for
    /// RocksDB key encoding and prefix scanning.
    prefix: Vec<u8>,
}

/// A RocksDB-backed stash for buffering upsert input updates.
///
/// This is a typed wrapper around [`RocksDBInstance`] that stores pending upsert
/// updates keyed by `(time, upsert_key)`. It maintains an in-memory
/// [`BTreeMap`] of per-timestamp entry counts for efficient capability management
/// and emptiness checks.
pub struct UpsertStash<T, FromTime> {
    /// The underlying RocksDB instance. Keys are raw `Vec<u8>` (the concatenated
    /// encoding described in the module docs). Values are `StashValue<FromTime>`.
    rocksdb: RocksDBInstance<Vec<u8>, StashValue<FromTime>>,
    /// Per-timestamp bookkeeping: entry count and serialized prefix bytes.
    time_info: BTreeMap<T, TimeInfo>,
}

impl<T, FromTime> UpsertStash<T, FromTime>
where
    T: Ord + Clone + Serialize + DeserializeOwned,
    FromTime: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a new `UpsertStash` wrapping the given RocksDB instance.
    pub fn new(rocksdb: RocksDBInstance<Vec<u8>, StashValue<FromTime>>) -> Self {
        Self {
            rocksdb,
            time_info: BTreeMap::new(),
        }
    }

    /// Stage a batch of upsert updates into the stash in a single RocksDB write.
    ///
    /// Each update is written as a `Merge` operation so that the merge operator
    /// keeps only the entry with the largest `from_time` per `(time, key)`.
    pub async fn stage_batch(
        &mut self,
        updates: impl IntoIterator<Item = (T, UpsertKey, FromTime, Option<UpsertValue>)>,
    ) -> Result<(), anyhow::Error> {
        let bincode_opts = bincode::DefaultOptions::new();
        let mut batch = Vec::new();

        for (time, key, from_time, value) in updates {
            let time_info = self.time_info.entry(time.clone()).or_insert_with(|| {
                let prefix = bincode_opts
                    .serialize(&time)
                    .expect("timestamp serialization should not fail");
                TimeInfo { count: 0, prefix }
            });
            time_info.count += 1;

            // Key: time_prefix ++ upsert_key (32 bytes). No from_time in key.
            let mut stash_key = Vec::with_capacity(time_info.prefix.len() + 32);
            stash_key.extend_from_slice(&time_info.prefix);
            stash_key.extend_from_slice(key.as_ref());

            batch.push((
                stash_key,
                KeyUpdate::Merge(StashValue { from_time, value }),
                None,
            ));
        }

        if !batch.is_empty() {
            self.rocksdb.multi_update(batch).await?;
        }

        Ok(())
    }

    /// Drain all stashed entries whose timestamp satisfies `eligible`.
    ///
    /// Returns entries as `(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)`.
    /// Because the merge operator already keeps only one entry per `(time, key)`,
    /// no sort or dedup is needed.
    ///
    /// Drained entries are deleted from RocksDB and removed from the in-memory
    /// bookkeeping.
    pub async fn drain(
        &mut self,
        eligible: impl Fn(&T) -> bool,
    ) -> Result<Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>, anyhow::Error> {
        // Collect the eligible times and their prefixes. We collect up front to
        // avoid borrowing `self.time_info` while we mutate the RocksDB instance.
        let eligible_times: Vec<(T, Vec<u8>)> = self
            .time_info
            .iter()
            .filter(|(t, _)| eligible(t))
            .map(|(t, info)| (t.clone(), info.prefix.clone()))
            .collect();

        if eligible_times.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut to_delete: Vec<(Vec<u8>, KeyUpdate<StashValue<FromTime>>, Option<_>)> = Vec::new();

        for (time, prefix) in &eligible_times {
            let entries = self.rocksdb.prefix_scan(prefix.clone()).await?;
            let prefix_len = prefix.len();

            for (raw_key, stash_value) in entries {
                // Parse the key: time_prefix ++ 32 bytes upsert_key.
                debug_assert!(
                    raw_key.len() >= prefix_len + 32,
                    "stash key too short: expected at least {} bytes, got {}",
                    prefix_len + 32,
                    raw_key.len()
                );

                let upsert_key_bytes = &raw_key[prefix_len..prefix_len + 32];
                let upsert_key = UpsertKey::from(upsert_key_bytes);

                results.push((
                    time.clone(),
                    upsert_key,
                    Reverse(stash_value.from_time),
                    stash_value.value,
                ));
                to_delete.push((raw_key, KeyUpdate::Delete, None));
            }
        }

        // Delete all drained entries from RocksDB.
        if !to_delete.is_empty() {
            self.rocksdb.multi_update(to_delete).await?;
        }

        // Remove drained times from the in-memory bookkeeping.
        for (time, _) in &eligible_times {
            self.time_info.remove(time);
        }

        Ok(results)
    }

    /// Returns `true` if there are no stashed entries.
    pub fn is_empty(&self) -> bool {
        self.time_info.is_empty()
    }

    /// Returns a reference to the minimum timestamp that has stashed entries, or
    /// `None` if the stash is empty.
    pub fn min_time(&self) -> Option<&T> {
        self.time_info.keys().next()
    }

    /// Returns the total number of stashed entries across all timestamps.
    pub fn len(&self) -> usize {
        self.time_info.values().map(|info| info.count).sum()
    }
}
