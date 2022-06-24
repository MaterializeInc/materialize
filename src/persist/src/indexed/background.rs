// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A runtime for background asynchronous maintenance of stored data.

use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_ore::task::RuntimeExt;
use mz_persist_types::Codec64;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::cache::{BlobCache, CacheHint};
use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsVecBuilder};
use crate::indexed::encoding::{BlobTraceBatchPart, TraceBatchMeta};
use crate::indexed::metrics::Metrics;
use crate::location::Blob;

/// A request to merge two trace batches and write the results to blob storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactTraceReq {
    /// One of the batches to be merged.
    pub b0: TraceBatchMeta,
    /// One of the batches to be merged.
    pub b1: TraceBatchMeta,
    /// The since frontier to be used for the output batch. This must be at or
    /// in advance of the since frontier for both of the input batch.
    pub since: Antichain<u64>,
}

/// A successful merge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactTraceRes {
    /// The original request, so the caller doesn't have to do this matching.
    pub req: CompactTraceReq,
    /// The compacted batch.
    pub merged: TraceBatchMeta,
}

/// A runtime for background asynchronous maintenance of stored data.
//
// TODO: Add migrating records from unsealed to trace as well as deletion of
// batches.
#[derive(Debug)]
pub struct Maintainer<B> {
    // TODO: It feels like a smell to wrap BlobCache in an Arc when most of its
    // internals are already wrapped in Arcs. As of when this was written, the
    // only exception is prev_meta_len, which really is only used from a single
    // thread. Perhaps we should split the Meta parts out of BlobCache.
    blob: Arc<BlobCache<B>>,
    metrics: Arc<Metrics>,
    // Bound the maximum size of any [ColumnarRecordsVec], only used in testing.
    key_val_data_max_len: Option<usize>,
}

impl<B: Blob> Maintainer<B> {
    /// Returns a new [Maintainer].
    pub fn new(blob: BlobCache<B>, metrics: Arc<Metrics>) -> Self {
        Maintainer {
            blob: Arc::new(blob),
            metrics,
            key_val_data_max_len: None,
        }
    }

    /// Returns a new [Maintainer].
    #[cfg(test)]
    fn new_for_testing(
        blob: BlobCache<B>,
        metrics: Arc<Metrics>,
        key_val_data_max_len: usize,
    ) -> Self {
        Maintainer {
            blob: Arc::new(blob),
            metrics,
            key_val_data_max_len: Some(key_val_data_max_len),
        }
    }
}

impl<B: Blob + Send + Sync + 'static> Maintainer<B> {
    /// Asynchronously runs the requested compaction on the tokio blocking work
    /// pool.
    pub async fn compact_trace(&self, req: CompactTraceReq) -> Result<CompactTraceRes, Error> {
        let blob = Arc::clone(&self.blob);
        let metrics = Arc::clone(&self.metrics);
        let key_val_data_max_len = self.key_val_data_max_len.clone();
        Handle::current()
            .spawn_blocking_named(
                || "persist_trace_compaction",
                move || {
                    let handle = Handle::current();
                    Self::compact_trace_blocking(&handle, blob, metrics, req, key_val_data_max_len)
                },
            )
            .await
            .map_err(|err| Error::from(err.to_string()))?
    }

    /// Physically and logically compact two trace batches together
    ///
    /// This function performs trace compaction with bounded memory usage by:
    ///
    /// 1. Only ever keeping one BlobTraceBatchPart in memory from each of
    ///    the two batches being compacted at a time.
    /// 2. Only keeping one ColumnarRecords worth of merged data in memory at
    ///    a time.
    /// 3. Performing roughly a linear merge on the two trace batch parts and
    ///    as we read data from each trace batch, figuring out the upper bound
    ///    on data that can be compacted, consolidating and merging that, and
    ///    then placing that in a ColumnarRecords to await being written out as
    ///    an output BlobTraceBatchPart.
    ///
    /// (3). is best explained with an example. If we are compacting two trace
    /// batches, A which has two parts, one with keys [0, 10) and one with keys
    /// [10, 20) and B has three parts, with keys [0, 15), [15, 30) and [30, 45)
    /// respectively then while compacting when we observe the first batches from
    /// A and B which have keys [0, 10) and [0, 15) respectively, we cannot merge
    /// all of both batches together.
    ///
    /// We can only merge the subset of keys in [0, 10), as its possible that
    /// subsequent parts in A have relevant keys in [10, 15).
    ///
    /// TODO(rkhaitan): We don't do the real linear merge as compaction requires
    /// us to both forward times and consolidate multiple updates at the
    /// forwarded times. I believe that doing so is possible in linear time, but
    /// doing so in a single pass was complicated enough that it is left for
    /// future work.
    fn compact_trace_blocking(
        handle: &Handle,
        blob: Arc<BlobCache<B>>,
        metrics: Arc<Metrics>,
        req: CompactTraceReq,
        key_val_data_max_len: Option<usize>,
    ) -> Result<CompactTraceRes, Error> {
        let start = Instant::now();
        let (first, second) = (&req.b0, &req.b1);
        if first.desc.upper() != second.desc.lower() {
            return Err(Error::from(format!(
                "invalid merge of non-consecutive batches {:?} and {:?}",
                first, second
            )));
        }

        if PartialOrder::less_than(&req.since, first.desc.since()) {
            return Err(Error::from(format!(
                "output since {:?} must be at or in advance of input since {:?}",
                &req.since,
                first.desc.since()
            )));
        }
        if PartialOrder::less_than(&req.since, second.desc.since()) {
            return Err(Error::from(format!(
                "output since {:?} must be at or in advance of input since {:?}",
                &req.since,
                second.desc.since()
            )));
        }

        // Sanity check that both batches being merged are at identical compaction
        // levels.
        debug_assert_eq!(first.level, second.level);

        debug_assert_eq!(handle.block_on(first.validate_data(&blob)), Ok(()));
        debug_assert_eq!(handle.block_on(second.validate_data(&blob)), Ok(()));

        let desc = Description::new(
            first.desc.lower().clone(),
            second.desc.upper().clone(),
            req.since.clone(),
        );

        // Iterators over all of the keys in the first and second batch, respectively.
        let mut first_batch_iter = first.keys.iter();
        let mut second_batch_iter = second.keys.iter();

        // Buffer for data from the first and second batch, respectively.
        let mut first_updates = vec![];
        let mut second_updates = vec![];

        // Buffer for data that needs to be advanced to since, and consolidated before
        // being sent to a ColumnarRecordsVecBuilder.
        let mut consolidation_buffer = vec![];

        // Buffer for consolidated data that is ready to be moved into storage.
        let mut builder = match key_val_data_max_len {
            None => ColumnarRecordsVecBuilder::default(),
            Some(key_val_data_max_len) => {
                ColumnarRecordsVecBuilder::new_with_len(key_val_data_max_len)
            }
        };

        let mut new_size_bytes = 0;
        let mut keys = vec![];

        loop {
            // Loop invariant: we don't have any data that needs consolidation across iterations.
            debug_assert!(consolidation_buffer.is_empty());

            // Pull more data if either of the holding pens from first or second are empty
            //
            // TODO: we can simplify this logic once empty trace batch parts are disallowed.
            while first_updates.is_empty() {
                match first_batch_iter.next() {
                    Some(key) => handle.block_on(Self::load_trace_batch_part(
                        &blob,
                        key,
                        &mut first_updates,
                    ))?,
                    None => break,
                }
            }

            while second_updates.is_empty() {
                match second_batch_iter.next() {
                    Some(key) => handle.block_on(Self::load_trace_batch_part(
                        &blob,
                        key,
                        &mut second_updates,
                    ))?,
                    None => break,
                }
            }

            // Figure out the stopping point for records we can safely consolidate.
            // This is the min max value across the two batches we have seen so far.
            //
            // If both input buffers are empty we can exit the compaction loop.
            let threshold_key_val = match (first_updates.last(), second_updates.last()) {
                (Some((f, _, _)), Some((s, _, _))) => {
                    if f < s {
                        f.clone()
                    } else {
                        s.clone()
                    }
                }
                (Some((f, _, _)), None) => f.clone(),
                (None, Some((s, _, _))) => s.clone(),
                (None, None) => break,
            };

            // Move all of the records <= threshold out of the various holding
            // pens into a common buffer so that they can be consolidated.
            first_updates = Self::drain_leq_threshold(
                first_updates,
                &mut consolidation_buffer,
                &threshold_key_val,
            );
            second_updates = Self::drain_leq_threshold(
                second_updates,
                &mut consolidation_buffer,
                &threshold_key_val,
            );

            // Advance timestamps and consolidate records.
            for ((_, _), ts, _) in consolidation_buffer.iter_mut() {
                ts.advance_by(desc.since().borrow());
            }
            differential_dataflow::consolidation::consolidate_updates(&mut consolidation_buffer);

            for ((key, val), time, diff) in consolidation_buffer.iter() {
                builder.push(((key, val), u64::encode(time), i64::encode(diff)));

                // Move data from builder into storage as we fill up ColumnarRecords.
                for part in builder.take_filled() {
                    debug_assert!(part.len() > 0);
                    let (key, part_size_bytes) = handle.block_on(Self::write_trace_batch_part(
                        &blob,
                        desc.clone(),
                        part,
                        u64::cast_from(keys.len()),
                    ))?;
                    keys.push(key);
                    new_size_bytes += part_size_bytes;
                }
            }

            consolidation_buffer.clear();
        }

        // Grab the last ColumnarRecords if nonempty ones remain.
        let finished = builder.finish();
        for part in finished {
            if part.len() > 0 {
                let (key, part_size_bytes) = handle.block_on(Self::write_trace_batch_part(
                    &blob,
                    desc.clone(),
                    part,
                    u64::cast_from(keys.len()),
                ))?;
                keys.push(key);
                new_size_bytes += part_size_bytes;
            }
        }

        // Only upgrade the compaction level if we know this new batch represents
        // an increase in data over both of its parents so that we know we need
        // even more additional batches to amortize the cost of compacting it in
        // the future.
        let merged_level =
            if new_size_bytes > first.size_bytes && new_size_bytes > second.size_bytes {
                first.level + 1
            } else {
                first.level
            };

        let merged = TraceBatchMeta {
            keys,
            format: ProtoBatchFormat::ParquetKvtd,
            desc,
            level: merged_level,
            size_bytes: new_size_bytes,
        };

        debug_assert_eq!(handle.block_on(merged.validate_data(&blob)), Ok(()));
        metrics
            .compaction_seconds
            .inc_by(start.elapsed().as_secs_f64());
        Ok(CompactTraceRes { req, merged })
    }

    /// Read the data from the trace batch part at `key` into `updates`.
    async fn load_trace_batch_part(
        blob: &BlobCache<B>,
        key: &str,
        updates: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>,
    ) -> Result<(), Error> {
        let batch_part = blob.get_trace_batch_async(key, CacheHint::NeverAdd).await?;
        updates.extend(batch_part.updates.iter().flat_map(|u| {
            u.iter()
                .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), u64::decode(t), i64::decode(d)))
        }));

        Ok(())
    }

    /// Drain all records from `updates` with a (key, val) <= `threshold` into
    /// `buffer`.
    ///
    /// TODO: this could be replaced with a drain_filter if that wasn't
    /// experimental.
    fn drain_leq_threshold(
        mut updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)>,
        buffer: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>,
        threshold: &(Vec<u8>, Vec<u8>),
    ) -> Vec<((Vec<u8>, Vec<u8>), u64, i64)> {
        let mut i = 0;
        while i < updates.len() {
            if &updates[i].0 <= threshold {
                i += 1;
            } else {
                break;
            }
        }

        if i > 0 {
            let updates_new = updates.split_off(i);
            buffer.extend(updates.into_iter());
            updates_new
        } else {
            updates
        }
    }

    /// Write a [BlobTraceBatchPart] containing `updates` into [Blob].
    ///
    /// Returns the key and size in bytes for the trace batch part.
    async fn write_trace_batch_part(
        blob: &BlobCache<B>,
        desc: Description<u64>,
        updates: ColumnarRecords,
        index: u64,
    ) -> Result<(String, u64), Error> {
        let batch_part = BlobTraceBatchPart {
            desc,
            updates: vec![updates],
            index,
        };
        let key = Uuid::new_v4().to_string();
        let size_bytes = blob
            .set_trace_batch(key.clone(), batch_part, ProtoBatchFormat::ParquetKvtd)
            .await?;
        Ok((key, size_bytes))
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::Description;

    use crate::gen::persist::ProtoBatchFormat;
    use crate::indexed::cache::CacheHint;
    use crate::indexed::columnar::ColumnarRecordsVec;
    use crate::indexed::metrics::Metrics;
    use crate::mem::{MemBlob, MemBlobConfig};

    use super::*;

    fn desc_from(lower: u64, upper: u64, since: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(since),
        )
    }

    struct CompactionTestCase<'a> {
        b0: Vec<Vec<((&'a [u8], &'a [u8]), u64, i64)>>,
        b1: Vec<Vec<((&'a [u8], &'a [u8]), u64, i64)>>,
        b0_desc: Description<u64>,
        b1_desc: Description<u64>,
        since: u64,
        merged_normal: TraceBatchMeta,
        merged_small_batches: TraceBatchMeta,
        expected_updates: Vec<((&'a [u8], &'a [u8]), u64, i64)>,
    }

    #[tokio::test]
    async fn compact_trace_errors() -> Result<(), Error> {
        let metrics = Arc::new(Metrics::default());
        let blob = BlobCache::new(
            Arc::new(Metrics::default()),
            MemBlob::open(MemBlobConfig::default()),
            None,
        );
        let maintainer = Maintainer::new(blob, metrics);

        // Non-contiguous batch descs
        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(0, 2, 0),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(3, 4, 0),
                level: 0,
                size_bytes: 0,
            },
            since: Antichain::from_elem(0),
        };
        assert_eq!(maintainer.compact_trace(req).await, Err(Error::from("invalid merge of non-consecutive batches TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 } and TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [3] }, upper: Antichain { elements: [4] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 }")));

        // Overlapping batch descs
        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(0, 2, 0),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(1, 4, 0),
                level: 0,
                size_bytes: 0,
            },
            since: Antichain::from_elem(0),
        };
        assert_eq!(maintainer.compact_trace(req).await, Err(Error::from("invalid merge of non-consecutive batches TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 } and TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [4] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 }")));

        // Since not at or in advance of b0's since
        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(0, 2, 1),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(2, 4, 0),
                level: 0,
                size_bytes: 0,
            },
            since: Antichain::from_elem(0),
        };
        assert_eq!(maintainer.compact_trace(req).await, Err(Error::from("output since Antichain { elements: [0] } must be at or in advance of input since Antichain { elements: [1] }")));

        // Since not at or in advance of b1's since
        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(0, 2, 0),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                keys: vec![],
                format: ProtoBatchFormat::Unknown,
                desc: desc_from(2, 4, 1),
                level: 0,
                size_bytes: 0,
            },
            since: Antichain::from_elem(0),
        };
        assert_eq!(maintainer.compact_trace(req).await, Err(Error::from("output since Antichain { elements: [0] } must be at or in advance of input since Antichain { elements: [1] }")));

        Ok(())
    }

    async fn compact_trace_test_case<
        'a,
        B: Blob + Send + Sync + 'static,
        F: FnMut() -> Result<(Maintainer<B>, BlobCache<B>), Error>,
    >(
        test: &CompactionTestCase<'a>,
        merged: &TraceBatchMeta,
        mut new_fn: F,
    ) -> Result<(), Error> {
        let (maintainer, blob) = new_fn()?;
        let format = ProtoBatchFormat::ParquetKvtd;
        let mut b0_keys = vec![];
        let mut b0_size_bytes = 0;

        for (idx, update) in test.b0.iter().enumerate() {
            let b = BlobTraceBatchPart {
                desc: test.b0_desc.clone(),
                index: u64::cast_from(idx),
                updates: update
                    .clone()
                    .into_iter()
                    .collect::<ColumnarRecordsVec>()
                    .into_inner(),
            };
            let key = format!("b0-{}", idx);
            b0_size_bytes += blob.set_trace_batch(key.clone(), b.clone(), format).await?;
            b0_keys.push(key)
        }

        let mut b1_keys = vec![];
        let mut b1_size_bytes = 0;

        for (idx, update) in test.b1.iter().enumerate() {
            let b = BlobTraceBatchPart {
                desc: test.b1_desc.clone(),
                index: u64::cast_from(idx),
                updates: update
                    .clone()
                    .into_iter()
                    .collect::<ColumnarRecordsVec>()
                    .into_inner(),
            };
            let key = format!("b1-{}", idx);
            b1_size_bytes += blob.set_trace_batch(key.clone(), b.clone(), format).await?;
            b1_keys.push(key)
        }

        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                keys: b0_keys,
                format,
                desc: test.b0_desc.clone(),
                level: 0,
                size_bytes: b0_size_bytes,
            },
            b1: TraceBatchMeta {
                keys: b1_keys,
                format,
                desc: test.b1_desc.clone(),
                level: 0,
                size_bytes: b1_size_bytes,
            },
            since: Antichain::from_elem(test.since),
        };

        let expected_res = CompactTraceRes {
            req: req.clone(),
            merged: merged.clone(),
        };
        let mut res = maintainer.compact_trace(req).await?;

        // Grab the list of newly created trace batch keys so we can check the
        // contents of the merged batch.
        let merged_keys = res.merged.keys.clone();
        // Replace the list of keys with a known set of keys so that we can assert
        // on the trace batch metadata.
        res.merged.keys = merged_keys
            .iter()
            .enumerate()
            .map(|(idx, _)| format!("MERGED-KEY-{}", idx))
            .collect();
        res.merged.size_bytes = 0;
        assert_eq!(res, expected_res);

        let mut updates = vec![];
        let mut batch_parts = vec![];
        for key in merged_keys {
            let batch_part = blob
                .get_trace_batch_async(&key, CacheHint::NeverAdd)
                .await?;
            assert_eq!(&batch_part.desc, &expected_res.merged.desc);
            batch_parts.push(batch_part);
        }

        for batch_part in batch_parts.iter() {
            updates.extend(
                batch_part
                    .updates
                    .iter()
                    .flat_map(|u| u.iter())
                    .map(|(kv, t, d)| (kv, u64::decode(t), i64::decode(d))),
            );
        }

        assert_eq!(updates, test.expected_updates);
        Ok(())
    }

    #[tokio::test]
    async fn compact_trace() -> Result<(), Error> {
        let new_fn = || {
            let metrics = Arc::new(Metrics::default());
            let blob = BlobCache::new(
                Arc::new(Metrics::default()),
                MemBlob::open(MemBlobConfig::default()),
                None,
            );
            let maintainer = Maintainer::new(blob.clone(), metrics);
            Ok((maintainer, blob))
        };

        let test_cases = vec![
            CompactionTestCase {
                b0: vec![vec![
                    (("k".as_bytes(), "v".as_bytes()), 0, 1),
                    (("k2".as_bytes(), "v2".as_bytes()), 0, 1),
                ]],
                b0_desc: desc_from(0, 1, 0),
                b1: vec![vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ]],
                b1_desc: desc_from(1, 3, 0),
                since: 2,
                merged_normal: TraceBatchMeta {
                    keys: vec!["MERGED-KEY-0".into()],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 1,
                    size_bytes: 0,
                },
                merged_small_batches: TraceBatchMeta {
                    keys: vec![
                        "MERGED-KEY-0".into(),
                        "MERGED-KEY-1".into(),
                        "MERGED-KEY-2".into(),
                    ],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 1,
                    size_bytes: 0,
                },
                expected_updates: vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ],
            },
            CompactionTestCase {
                b0: vec![
                    vec![(("k".as_bytes(), "v".as_bytes()), 0, 1)],
                    vec![(("k2".as_bytes(), "v2".as_bytes()), 0, 1)],
                ],
                b0_desc: desc_from(0, 1, 0),
                b1: vec![vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ]],
                b1_desc: desc_from(1, 3, 0),
                since: 2,
                merged_normal: TraceBatchMeta {
                    keys: vec!["MERGED-KEY-0".into()],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 0,
                    size_bytes: 0,
                },
                merged_small_batches: TraceBatchMeta {
                    keys: vec![
                        "MERGED-KEY-0".into(),
                        "MERGED-KEY-1".into(),
                        "MERGED-KEY-2".into(),
                    ],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 1,
                    size_bytes: 0,
                },
                expected_updates: vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ],
            },
            CompactionTestCase {
                b0: vec![
                    vec![(("k".as_bytes(), "v".as_bytes()), 0, 1)],
                    vec![(("k2".as_bytes(), "v2".as_bytes()), 0, 1)],
                ],
                b0_desc: desc_from(0, 1, 0),
                b1: vec![
                    vec![(("k".as_bytes(), "v".as_bytes()), 2, 1)],
                    vec![(("k3".as_bytes(), "v3".as_bytes()), 2, 1)],
                ],
                b1_desc: desc_from(1, 3, 0),
                since: 2,
                merged_normal: TraceBatchMeta {
                    keys: vec!["MERGED-KEY-0".into()],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 0,
                    size_bytes: 0,
                },
                merged_small_batches: TraceBatchMeta {
                    keys: vec![
                        "MERGED-KEY-0".into(),
                        "MERGED-KEY-1".into(),
                        "MERGED-KEY-2".into(),
                    ],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 1,
                    size_bytes: 0,
                },
                expected_updates: vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ],
            },
            CompactionTestCase {
                b0: vec![
                    vec![],
                    vec![(("k".as_bytes(), "v".as_bytes()), 0, 1)],
                    vec![],
                    vec![(("k2".as_bytes(), "v2".as_bytes()), 0, 1)],
                    vec![],
                ],
                b0_desc: desc_from(0, 1, 0),
                b1: vec![
                    vec![(("k".as_bytes(), "v".as_bytes()), 2, 1)],
                    vec![(("k3".as_bytes(), "v3".as_bytes()), 2, 1)],
                ],
                b1_desc: desc_from(1, 3, 0),
                since: 2,
                merged_normal: TraceBatchMeta {
                    keys: vec!["MERGED-KEY-0".into()],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 0,
                    size_bytes: 0,
                },
                merged_small_batches: TraceBatchMeta {
                    keys: vec![
                        "MERGED-KEY-0".into(),
                        "MERGED-KEY-1".into(),
                        "MERGED-KEY-2".into(),
                    ],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 0,
                    size_bytes: 0,
                },
                expected_updates: vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ],
            },
            CompactionTestCase {
                b0: vec![vec![], vec![], vec![]],
                b0_desc: desc_from(0, 1, 0),
                b1: vec![vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ]],
                b1_desc: desc_from(1, 3, 0),
                since: 2,
                merged_normal: TraceBatchMeta {
                    keys: vec!["MERGED-KEY-0".into()],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 0,
                    size_bytes: 0,
                },
                merged_small_batches: TraceBatchMeta {
                    keys: vec![
                        "MERGED-KEY-0".into(),
                        "MERGED-KEY-1".into(),
                        "MERGED-KEY-2".into(),
                    ],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 1,
                    size_bytes: 0,
                },
                expected_updates: vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ],
            },
            CompactionTestCase {
                b0: vec![],
                b0_desc: desc_from(0, 1, 0),
                b1: vec![vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ]],
                b1_desc: desc_from(1, 3, 0),
                since: 2,
                merged_normal: TraceBatchMeta {
                    keys: vec!["MERGED-KEY-0".into()],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 0,
                    size_bytes: 0,
                },
                merged_small_batches: TraceBatchMeta {
                    keys: vec![
                        "MERGED-KEY-0".into(),
                        "MERGED-KEY-1".into(),
                        "MERGED-KEY-2".into(),
                    ],
                    format: ProtoBatchFormat::ParquetKvtd,
                    desc: desc_from(0, 3, 2),
                    level: 1,
                    size_bytes: 0,
                },
                expected_updates: vec![
                    (("k".as_bytes(), "v".as_bytes()), 2, 2),
                    (("k2".as_bytes(), "v2".as_bytes()), 2, 1),
                    (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
                ],
            },
        ];

        for test_case in test_cases.iter() {
            compact_trace_test_case(test_case, &test_case.merged_normal, new_fn.clone()).await?;
        }

        let new_fn = || {
            let metrics = Arc::new(Metrics::default());
            let blob = BlobCache::new(
                Arc::new(Metrics::default()),
                MemBlob::open(MemBlobConfig::default()),
                None,
            );
            let maintainer = Maintainer::new_for_testing(blob.clone(), metrics, 10);
            Ok((maintainer, blob))
        };

        for test_case in test_cases.iter() {
            compact_trace_test_case(test_case, &test_case.merged_small_batches, new_fn.clone())
                .await?;
        }

        Ok(())
    }
}
