// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting data structure of `(Key, Value, Time, Diff)`
//! updates, indexed by time.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use timely::progress::Antichain;
use timely::PartialOrder;
use uuid::Uuid;

use crate::error::Error;
use crate::future::Future;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{UnsealedBatchMeta, UnsealedMeta};
use crate::indexed::{BlobUnsealedBatch, Id, Snapshot};
use crate::storage::{Blob, SeqNo};

/// A persistent, compacting data structure containing `(Key, Value, Time,
/// Diff)` entries indexed by `(time, key, value)`.
///
/// Unsealed exists to hold data that has been drained out of a [crate::storage::Log]
/// but not yet "seal"ed into a [crate::indexed::trace::Trace]. We store incoming
/// data as immutable batches of updates, corresponding to non-empty, sorted intervals
/// of [crate::storage::Log] sequence numbers.
///
/// As times get sealed and the corresponding updates get moved into the trace,
/// Unsealed can remove those updates, and eventually, entire batches. The approach
/// to removing sealed data optimizes for the common case, for which we assume that:
/// - data arrives roughly in order,
/// - unsealed batches contain data for a small range of distinct times.
/// Every unsealed batch tracks the minimum and maximum update timestamp contained within
/// its list of updates, and we eagerly drop batches that only contain data prior
/// to the sealed frontier whenever possible. In the expected case, this should be
/// sufficient to ensure that Unsealed maintains a bounded storage footprint. If
/// either of the two assumptions are violated, either because updates arrive out
/// of order, or batches contain data at many distinct timestamps, we periodically
/// try to remove the updates strictly behind the current sealed frontier from a
/// given unsealed batch and replace it with a "trimmed" batch that uses less storage.
///
/// This approach intentionally does nothing to physically coalesce multiple
/// unsealed batches into a single unsealed batch. Doing so has many potential downsides;
/// for example physically merging a batch containing updates 5 seconds ahead of
/// the current sealed frontier with another batch containing updates 5 hours ahead
/// of the current sealed frontier would only hurt 5 seconds later, when the previously
/// unmerged batch would have been dropped. Instead, the merged batch has to be trimmed,
/// which requires an extra read and write. If we end up having significant amounts
/// of data far ahead of the current sealed frontier we likely will need a different
/// structure that can hold batches of updates organized by overlapping ranges
/// of times and physically merge unsealed batches using an approach similar to
/// trace physical compaction.
///
/// Invariants:
/// - All entries are after or equal to some time frontier and less than some
///   SeqNo.
/// - TODO: Space usage.
pub struct Unsealed {
    id: Id,
    // NB: This is a closed lower bound. When Indexed seals a time, only data
    // strictly before that time gets moved into the trace.
    ts_lower: Antichain<u64>,
    batches: Vec<UnsealedBatchMeta>,

    // TODO: next_blob_id is deprecated, remove this once we can safely bump
    // BlobMeta::CURRENT_VERSION.
    deprecated_next_blob_id: u64,
}

impl Unsealed {
    /// Returns an Unsealed re-instantiated with the previously serialized
    /// state.
    pub fn new(meta: UnsealedMeta) -> Self {
        Unsealed {
            id: meta.id,
            ts_lower: meta.ts_lower,
            batches: meta.batches,
            deprecated_next_blob_id: meta.next_blob_id,
        }
    }

    // Get a new key to write to the Blob store for this unsealed.
    fn new_blob_key() -> String {
        Uuid::new_v4().to_string()
    }

    /// Serializes the state of this Unsealed for later re-instantiation.
    pub fn meta(&self) -> UnsealedMeta {
        UnsealedMeta {
            id: self.id,
            ts_lower: self.ts_lower.clone(),
            batches: self.batches.clone(),
            next_blob_id: self.deprecated_next_blob_id,
        }
    }

    /// An open upper bound on the seqnos of contained updates.
    pub fn seqno_upper(&self) -> Antichain<SeqNo> {
        self.batches.last().map_or_else(
            || Antichain::from_elem(SeqNo(0)),
            |meta| meta.desc.upper().clone(),
        )
    }

    /// Write a [BlobUnsealedBatch] to [Blob] storage and return the corresponding
    /// [UnsealedBatchMeta].
    ///
    /// The input batch is expected to satisfy all [BlobUnsealedBatch] invariants.
    fn write_batch<L: Blob>(
        &mut self,
        batch: BlobUnsealedBatch,
        blob: &mut BlobCache<L>,
    ) -> Result<UnsealedBatchMeta, Error> {
        let key = Unsealed::new_blob_key();
        let desc = batch.desc.clone();
        let ts_upper = match batch.updates.last() {
            Some(upper) => upper.1,
            None => {
                return Err(Error::from(
                    "invalid unsealed batch: trying to write empty batch",
                ))
            }
        };
        let ts_lower = match batch.updates.first() {
            Some(lower) => lower.1,
            None => {
                return Err(Error::from(
                    "invalid unsealed batch: trying to write empty batch",
                ))
            }
        };
        let size_bytes = blob.set_unsealed_batch(key.clone(), batch)?;
        Ok(UnsealedBatchMeta {
            key,
            desc,
            ts_upper,
            ts_lower,
            size_bytes,
        })
    }

    /// Writes the given batch to [Blob] storage and logically adds the contained
    /// updates to this unsealed.
    pub fn append<L: Blob>(
        &mut self,
        batch: BlobUnsealedBatch,
        blob: &mut BlobCache<L>,
    ) -> Result<(), Error> {
        if batch.desc.lower() != &self.seqno_upper() {
            return Err(Error::from(format!(
                "batch lower doesn't match seqno_upper {:?}: {:?}",
                self.seqno_upper(),
                batch.desc
            )));
        }
        if cfg!(any(debug_assertions, test)) {
            // Batches being appended to this unsealed come from data being
            // drained out of the log. Indexed should have prevented this
            // write to the log, so this should never happen. Hopefully any
            // regressions in maintaining this invariant will be caught by this
            // debug/test check.
            for (_, ts, _) in batch.updates.iter() {
                if !self.ts_lower.less_equal(ts) {
                    return Err(Error::from(format!(
                        "batch contains timestamp {:?} before ts_lower: {:?}",
                        ts, self.ts_lower
                    )));
                }
            }
        }

        let meta = self.write_batch(batch, blob)?;
        self.batches.push(meta);
        Ok(())
    }

    /// Returns a consistent read of the updates contained in this unsealed
    /// matching the given filters (in practice, everything not in Trace).
    pub fn snapshot<L: Blob>(
        &self,
        ts_lower: Antichain<u64>,
        ts_upper: Antichain<u64>,
        blob: &BlobCache<L>,
    ) -> Result<UnsealedSnapshot, Error> {
        if PartialOrder::less_than(&ts_upper, &ts_lower) {
            return Err(Error::from(format!(
                "invalid snapshot request: ts_upper {:?} is less than ts_lower {:?}",
                ts_upper, ts_lower
            )));
        }

        let mut batches = Vec::with_capacity(self.batches.len());
        for meta in self.batches.iter() {
            // We want to read this batch as long as it contains times [lo, hi] s.t.
            // they overlap with the requested [ts_lower, ts_upper).
            // More specifically, we can want to read this batch as long as both:
            // - ts_lower <= hi
            // - ts_upper > lo
            if ts_lower.less_equal(&meta.ts_upper) && !ts_upper.less_equal(&meta.ts_lower) {
                batches.push(blob.get_unsealed_batch_async(&meta.key));
            }
        }

        Ok(UnsealedSnapshot {
            ts_lower,
            ts_upper,
            batches,
        })
    }

    /// Removes all updates contained in this unsealed before the given bound.
    ///
    /// Returns a list of batches that can safely be deleted after the eviction is
    /// committed to durable storage.
    pub fn truncate(
        &mut self,
        new_ts_lower: Antichain<u64>,
    ) -> Result<Vec<UnsealedBatchMeta>, Error> {
        if PartialOrder::less_than(&new_ts_lower, &self.ts_lower) {
            return Err(format!(
                "cannot regress ts_lower from {:?} to {:?}",
                self.ts_lower, new_ts_lower
            )
            .into());
        }
        self.ts_lower = new_ts_lower;
        Ok(self.evict())
    }

    /// Remove all batches containing only data strictly before the Unsealed's time
    /// lower bound.
    ///
    /// Returns a list of batches that can safely be deleted after the eviction is
    /// committed to durable storage.
    fn evict(&mut self) -> Vec<UnsealedBatchMeta> {
        // TODO: actually physically free the old batches.
        let ts_lower = self.ts_lower.clone();
        let evicted = self
            .batches
            .iter()
            .filter(|b| !ts_lower.less_equal(&b.ts_upper))
            .cloned()
            .collect();
        self.batches.retain(|b| ts_lower.less_equal(&b.ts_upper));

        evicted
    }

    /// Create a new [BlobUnsealedBatch] from `batch` containing only the subset of
    /// updates at or in advance of the Unsealed's time lower bound.
    ///
    /// `batch` is assumed not be eligible for eviction at the time of this function
    /// call, and to satisy all [BlobUnsealedBatch] invariants.
    fn trim<L: Blob>(
        &mut self,
        batch: UnsealedBatchMeta,
        blob: &mut BlobCache<L>,
    ) -> Result<UnsealedBatchMeta, Error> {
        // Sanity check that batch cannot be evicted
        debug_assert!(self.ts_lower.less_equal(&batch.ts_upper));
        let mut updates = vec![];

        updates.extend(
            blob.get_unsealed_batch_async(&batch.key)
                .recv()?
                .updates
                .iter()
                .filter(|(_, ts, _)| self.ts_lower.less_equal(ts))
                .cloned(),
        );
        debug_assert!(!updates.is_empty());
        let new_batch = BlobUnsealedBatch {
            desc: batch.desc,
            updates,
        };

        self.write_batch(new_batch, blob)
    }

    /// Take one step towards shrinking the representation of this unsealed.
    ///
    /// Returns true if the trace was modified, false otherwise.
    pub fn step<L: Blob>(&mut self, blob: &mut BlobCache<L>) -> Result<bool, Error> {
        self.evict();

        for (idx, batch) in self.batches.iter_mut().enumerate() {
            // We can trim data out of the batch if it contains data at times < ts_lower.
            if !self.ts_lower.less_equal(&batch.ts_lower) {
                let batch = batch.clone();
                let new_batch = self.trim(batch, blob)?;
                self.batches[idx] = new_batch;
                return Ok(true);
            }
        }

        Ok(false)
    }
}

/// A consistent snapshot of the data currently in a persistent [Unsealed].
#[derive(Debug)]
pub struct UnsealedSnapshot {
    /// A closed lower bound on the times of contained updates.
    pub ts_lower: Antichain<u64>,
    /// An open upper bound on the times of the contained updates.
    pub ts_upper: Antichain<u64>,
    batches: Vec<Future<Arc<BlobUnsealedBatch>>>,
}

impl Snapshot<Vec<u8>, Vec<u8>> for UnsealedSnapshot {
    type Iter = UnsealedSnapshotIter;

    fn into_iters(self, num_iters: NonZeroUsize) -> Vec<Self::Iter> {
        let mut iters = Vec::with_capacity(num_iters.get());
        iters.resize_with(num_iters.get(), || UnsealedSnapshotIter {
            ts_lower: self.ts_lower.clone(),
            ts_upper: self.ts_upper.clone(),
            current_batch: Vec::new(),
            batches: VecDeque::new(),
        });
        // TODO: This should probably distribute batches based on size, but for
        // now it's simpler to round-robin them.
        for (i, batch) in self.batches.into_iter().enumerate() {
            let iter_idx = i % num_iters;
            iters[iter_idx].batches.push_back(batch);
        }
        iters
    }
}

/// An [Iterator] representing one part of the data in a [UnsealedSnapshot].
//
// This intentionally stores the batches as a VecDeque so we can return the data
// in roughly increasing timestamp order, but it's unclear if this is in any way
// important.
#[derive(Debug)]
pub struct UnsealedSnapshotIter {
    /// A closed lower bound on the times of contained updates.
    ts_lower: Antichain<u64>,
    /// An open upper bound on the times of the contained updates.
    ts_upper: Antichain<u64>,

    current_batch: Vec<((Vec<u8>, Vec<u8>), u64, isize)>,
    batches: VecDeque<Future<Arc<BlobUnsealedBatch>>>,
}

impl Iterator for UnsealedSnapshotIter {
    type Item = Result<((Vec<u8>, Vec<u8>), u64, isize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.current_batch.is_empty() {
                let update = self.current_batch.pop().unwrap();
                return Some(Ok(update));
            } else {
                // current_batch is empty, find a new one.
                let b = match self.batches.pop_front() {
                    None => return None,
                    Some(b) => b,
                };
                match b.recv() {
                    Ok(b) => {
                        // Reverse the updates so we can pop them off the back
                        // in roughly increasing time order. At the same time,
                        // enforce our filter before we clone them.
                        let ts_lower = self.ts_lower.borrow();
                        let ts_upper = self.ts_upper.borrow();
                        self.current_batch.extend(
                            b.updates
                                .iter()
                                .rev()
                                .filter(|(_, ts, _)| {
                                    ts_lower.less_equal(&ts) && !ts_upper.less_equal(&ts)
                                })
                                .cloned(),
                        );
                        continue;
                    }
                    Err(err) => return Some(Err(err)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::Description;

    use crate::indexed::metrics::Metrics;
    use crate::indexed::SnapshotExt;
    use crate::mem::MemBlob;

    use super::*;

    fn desc_from(lower: u64, upper: u64, since: u64) -> Description<SeqNo> {
        Description::new(
            Antichain::from_elem(SeqNo(lower)),
            Antichain::from_elem(SeqNo(upper)),
            Antichain::from_elem(SeqNo(since)),
        )
    }

    // Generate a list of ((k, v), t, 1) updates at all of the specified times.
    fn unsealed_updates(update_times: Vec<u64>) -> Vec<((Vec<u8>, Vec<u8>), u64, isize)> {
        update_times
            .into_iter()
            .map(|t| (("k".into(), "v".into()), t, 1))
            .collect()
    }

    // Generate an unsealed batch spanning the specified sequence numbers with
    // updates at the specified times.
    fn unsealed_batch(lower: u64, upper: u64, update_times: Vec<u64>) -> BlobUnsealedBatch {
        BlobUnsealedBatch {
            desc: Description::new(
                Antichain::from_elem(SeqNo(lower)),
                Antichain::from_elem(SeqNo(upper)),
                Antichain::from_elem(SeqNo(0)),
            ),
            updates: unsealed_updates(update_times),
        }
    }

    fn unsealed_batch_meta(
        key: &str,
        lower: u64,
        upper: u64,
        since: u64,
        ts_lower: u64,
        ts_upper: u64,
        size_bytes: u64,
    ) -> UnsealedBatchMeta {
        UnsealedBatchMeta {
            key: key.to_string(),
            desc: Description::new(
                Antichain::from_elem(SeqNo(lower)),
                Antichain::from_elem(SeqNo(upper)),
                Antichain::from_elem(SeqNo(since)),
            ),
            ts_upper,
            ts_lower,
            size_bytes,
        }
    }

    // Attempt to read every update in `unsealed` at times in [lo, hi)
    fn slurp_from<L: Blob>(
        unsealed: &Unsealed,
        blob: &BlobCache<L>,
        lo: u64,
        hi: Option<u64>,
    ) -> Result<Vec<((Vec<u8>, Vec<u8>), u64, isize)>, Error> {
        let hi = hi.map_or_else(|| Antichain::new(), |e| Antichain::from_elem(e));
        let snapshot = unsealed.snapshot(Antichain::from_elem(lo), hi, &blob)?;
        let updates = snapshot.read_to_end()?;
        Ok(updates)
    }

    // Keys are randomly generated, so clear them before we do any comparisons.
    fn cleared_keys(batches: &[UnsealedBatchMeta]) -> Vec<UnsealedBatchMeta> {
        batches
            .iter()
            .cloned()
            .map(|mut b| {
                b.key = "KEY".to_string();
                b
            })
            .collect()
    }

    #[test]
    fn append_ts_lower_invariant() {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("append_ts_lower_invariant"),
        );
        let mut f = Unsealed::new(UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(2),
            batches: vec![],
            next_blob_id: 0,
        });

        // ts < ts_lower.data()[0] is disallowed
        let batch = BlobUnsealedBatch {
            desc: Description::new(
                Antichain::from_elem(SeqNo(0)),
                Antichain::from_elem(SeqNo(1)),
                Antichain::from_elem(SeqNo(0)),
            ),
            updates: vec![(("k".into(), "v".into()), 1, 1)],
        };
        assert_eq!(
            f.append(batch, &mut blob),
            Err(Error::from(
                "batch contains timestamp 1 before ts_lower: Antichain { elements: [2] }"
            ))
        );

        // ts == ts_lower.data()[0] is allowed
        let batch = BlobUnsealedBatch {
            desc: Description::new(
                Antichain::from_elem(SeqNo(0)),
                Antichain::from_elem(SeqNo(1)),
                Antichain::from_elem(SeqNo(0)),
            ),
            updates: vec![(("k".into(), "v".into()), 2, 1)],
        };
        assert_eq!(f.append(batch, &mut blob), Ok(()));
    }

    #[test]
    fn truncate_regress() {
        let mut f: Unsealed = Unsealed::new(UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(2),
            batches: vec![],
            next_blob_id: 0,
        });
        assert_eq!(f.truncate(Antichain::from_elem(2)), Ok(vec![]));
        assert_eq!(
            f.truncate(Antichain::from_elem(1)),
            Err(Error::from(
                "cannot regress ts_lower from Antichain { elements: [2] } to Antichain { elements: [1] }"
            ))
        );
    }

    #[test]
    fn unsealed_evict() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("unsealed_evict"),
        );
        let mut f: Unsealed = Unsealed::new(UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![],
            next_blob_id: 0,
        });

        f.append(unsealed_batch(0, 1, vec![0]), &mut blob)?;
        f.append(unsealed_batch(1, 2, vec![1]), &mut blob)?;
        f.append(unsealed_batch(2, 3, vec![0, 1]), &mut blob)?;

        let snapshot_updates = slurp_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, unsealed_updates(vec![0, 0, 1, 1]));
        assert_eq!(
            cleared_keys(&f.batches),
            vec![
                unsealed_batch_meta("KEY", 0, 1, 0, 0, 0, 186),
                unsealed_batch_meta("KEY", 1, 2, 0, 1, 1, 186),
                unsealed_batch_meta("KEY", 2, 3, 0, 0, 1, 252),
            ],
        );

        // Check that truncate doesn't do anything when no batches can be removed.
        assert_eq!(f.truncate(Antichain::from_elem(0)), Ok(vec![]));

        // Check that truncate correctly returns the list of batches that can be
        // physically deleted.
        assert_eq!(
            f.truncate(Antichain::from_elem(1))?
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![desc_from(0, 1, 0)]
        );

        // Check that repeatedly truncating the same time bound does not modify the unsealed.
        assert_eq!(f.truncate(Antichain::from_elem(1)), Ok(vec![]));

        let snapshot_updates = slurp_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, unsealed_updates(vec![0, 1, 1]));
        assert_eq!(
            cleared_keys(&f.batches),
            vec![
                unsealed_batch_meta("KEY", 1, 2, 0, 1, 1, 186),
                unsealed_batch_meta("KEY", 2, 3, 0, 0, 1, 252),
            ],
        );

        // Check that truncate correctly handles removing all data in the unsealed.
        assert_eq!(
            f.truncate(Antichain::from_elem(2))?
                .into_iter()
                .map(|b| b.desc)
                .collect::<Vec<_>>(),
            vec![desc_from(1, 2, 0), desc_from(2, 3, 0)]
        );

        // Check that truncate correctly handles the case where there are no more batches.
        assert_eq!(f.truncate(Antichain::from_elem(2)), Ok(vec![]));

        Ok(())
    }

    #[test]
    fn unsealed_snapshot() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("unsealed_snapshot"),
        );
        let mut f: Unsealed = Unsealed::new(UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![],
            next_blob_id: 0,
        });

        // Construct a batch holding updates for times [3, 5].
        let updates = vec![
            (("k".into(), "v".into()), 3, 1),
            (("k".into(), "v".into()), 5, 1),
        ];
        let batch = BlobUnsealedBatch {
            desc: Description::new(
                Antichain::from_elem(SeqNo(0)),
                Antichain::from_elem(SeqNo(2)),
                Antichain::from_elem(SeqNo(0)),
            ),
            updates: updates.clone(),
        };

        f.append(batch, &mut blob)?;

        assert_eq!(slurp_from(&f, &blob, 0, None)?, updates);
        assert_eq!(slurp_from(&f, &blob, 0, Some(6))?, updates);

        assert_eq!(slurp_from(&f, &blob, 0, Some(2))?, vec![]);
        assert_eq!(slurp_from(&f, &blob, 6, None)?, vec![]);
        assert_eq!(slurp_from(&f, &blob, 6, Some(8))?, vec![]);

        // hi == lo
        assert_eq!(slurp_from(&f, &blob, 3, Some(3))?, vec![]);

        // invalid args: hi < lo
        assert_eq!(
            slurp_from(&f, &blob, 4, Some(3)),
            Err(Error::from(
                    "invalid snapshot request: ts_upper Antichain { elements: [3] } is less than ts_lower Antichain { elements: [4] }"
            ))
        );

        // lo == batch_min, hi == batch_max + 1
        assert_eq!(slurp_from(&f, &blob, 3, Some(6))?, updates);

        assert_eq!(slurp_from(&f, &blob, 3, Some(4))?, updates[..1]);
        assert_eq!(slurp_from(&f, &blob, 4, Some(5))?, vec![]);
        assert_eq!(slurp_from(&f, &blob, 5, Some(6))?, updates[1..]);

        Ok(())
    }

    #[test]
    fn unsealed_batch_trim() -> Result<(), Error> {
        let mut blob = BlobCache::new(
            Metrics::default(),
            MemBlob::new_no_reentrance("unsealed_batch_trim"),
        );
        let mut f: Unsealed = Unsealed::new(UnsealedMeta {
            id: Id(0),
            ts_lower: Antichain::from_elem(0),
            batches: vec![],
            next_blob_id: 0,
        });

        // Construct a batch holding updates for times [0, 2].
        let updates = vec![
            (("k".into(), "v".into()), 0, 1),
            (("k".into(), "v".into()), 1, 1),
            (("k".into(), "v".into()), 2, 1),
        ];
        let batch = BlobUnsealedBatch {
            desc: Description::new(
                Antichain::from_elem(SeqNo(0)),
                Antichain::from_elem(SeqNo(2)),
                Antichain::from_elem(SeqNo(0)),
            ),
            updates: updates.clone(),
        };

        f.append(batch, &mut blob)?;

        f.truncate(Antichain::from_elem(1))?;

        // Check that no data is evicted after the truncate.
        let snapshot_updates = slurp_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, updates);

        // Take a step to trim the batch
        assert!(f.step(&mut blob)?);

        let snapshot_updates = slurp_from(&f, &blob, 0, None)?;
        assert_eq!(snapshot_updates, updates[1..]);

        assert_eq!(
            cleared_keys(&f.batches),
            vec![unsealed_batch_meta("KEY", 0, 2, 0, 1, 2, 252)],
        );

        Ok(())
    }
}
