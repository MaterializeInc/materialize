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
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::runtime::Runtime as AsyncRuntime;

use mz_ore::task::RuntimeExt;

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::arrangement::Arrangement;
use crate::indexed::cache::{BlobCache, CacheHint};
use crate::indexed::columnar::ColumnarRecordsVec;
use crate::indexed::encoding::{BlobTraceBatchPart, TraceBatchMeta, UnsealedSnapshotMeta};
use crate::indexed::metrics::Metrics;
use crate::pfuture::PFuture;
use crate::storage::{Blob, BlobRead};

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

/// A request to copy part of unsealed into a trace batch and write the results
/// to blob storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainUnsealedReq {
    /// The description of the trace batch to create.
    pub desc: Description<u64>,
    /// A consistent view of data in sealed as of some time.
    pub snap: UnsealedSnapshotMeta,
}

/// A successful drain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainUnsealedRes {
    /// The original request, so the caller doesn't have to do this matching.
    pub req: DrainUnsealedReq,
    /// The compacted batch.
    pub drained: Option<TraceBatchMeta>,
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
    async_runtime: Arc<AsyncRuntime>,
    metrics: Arc<Metrics>,
}

impl<B: BlobRead> Maintainer<B> {
    /// Returns a new [Maintainer].
    pub fn new(
        blob: BlobCache<B>,
        async_runtime: Arc<AsyncRuntime>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Maintainer {
            blob: Arc::new(blob),
            async_runtime,
            metrics,
        }
    }
}

impl<B: Blob> Maintainer<B> {
    /// Asynchronously runs the requested compaction on the work pool provided
    /// at construction time.
    pub fn compact_trace(&self, req: CompactTraceReq) -> PFuture<CompactTraceRes> {
        let (tx, rx) = PFuture::new();
        let blob = Arc::clone(&self.blob);
        let metrics = Arc::clone(&self.metrics);
        // Ignore the spawn_blocking response since we communicate
        // success/failure through the returned future.
        //
        // TODO: Push the spawn_blocking down into the cpu-intensive bits and
        // use spawn here once the storage traits are made async.
        //
        // TODO(guswynn): consider adding more info to the task name here
        let _ = self.async_runtime.spawn_blocking_named(
            || "persist_trace_compaction",
            move || tx.fill(Self::compact_trace_blocking(blob, metrics, req)),
        );
        rx
    }

    fn compact_trace_blocking(
        blob: Arc<BlobCache<B>>,
        metrics: Arc<Metrics>,
        req: CompactTraceReq,
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

        let desc = Description::new(
            first.desc.lower().clone(),
            second.desc.upper().clone(),
            req.since.clone(),
        );

        let mut updates = vec![];
        let mut batches = vec![];

        for key in first.keys.iter() {
            batches.push(
                blob.get_trace_batch_async(key, CacheHint::NeverAdd)
                    .recv()?,
            );
        }

        for key in second.keys.iter() {
            batches.push(
                blob.get_trace_batch_async(key, CacheHint::NeverAdd)
                    .recv()?,
            );
        }

        for batch in batches.iter() {
            updates.extend(batch.updates.iter().flat_map(|u| u.iter()));
        }

        for ((_, _), ts, _) in updates.iter_mut() {
            ts.advance_by(desc.since().borrow());
        }

        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        let updates = updates.iter().collect::<ColumnarRecordsVec>().into_inner();
        let new_batch = BlobTraceBatchPart {
            desc: desc.clone(),
            index: 0,
            updates,
        };

        let merged_key = Arrangement::new_blob_key();
        let format = ProtoBatchFormat::ParquetKvtd;
        let size_bytes = blob.set_trace_batch(merged_key.clone(), new_batch, format)?;

        // Only upgrade the compaction level if we know this new batch represents
        // an increase in data over both of its parents so that we know we need
        // even more additional batches to amortize the cost of compacting it in
        // the future.
        let merged_level = if size_bytes > first.size_bytes && size_bytes > second.size_bytes {
            first.level + 1
        } else {
            first.level
        };

        let merged = TraceBatchMeta {
            keys: vec![merged_key],
            format,
            desc,
            level: merged_level,
            size_bytes,
        };

        metrics
            .compaction_seconds
            .inc_by(start.elapsed().as_secs_f64());
        Ok(CompactTraceRes { req, merged })
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::Description;
    use tokio::runtime::Runtime as AsyncRuntime;

    use crate::gen::persist::ProtoBatchFormat;
    use crate::indexed::columnar::ColumnarRecordsVec;
    use crate::indexed::metrics::Metrics;
    use crate::mem::MemRegistry;

    use super::*;

    fn desc_from(lower: u64, upper: u64, since: u64) -> Description<u64> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(since),
        )
    }

    #[test]
    fn compact_trace() -> Result<(), Error> {
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let metrics = Arc::new(Metrics::default());
        let blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::clone(&async_runtime),
            MemRegistry::new().blob_no_reentrance()?,
            None,
        );
        let maintainer = Maintainer::new(blob.clone(), async_runtime, metrics);

        let b0 = BlobTraceBatchPart {
            desc: desc_from(0, 1, 0),
            index: 0,
            updates: vec![
                (("k".as_bytes(), "v".as_bytes()), 0, 1),
                (("k2".as_bytes(), "v2".as_bytes()), 0, 1),
            ]
            .iter()
            .collect::<ColumnarRecordsVec>()
            .into_inner(),
        };
        let format = ProtoBatchFormat::ParquetKvtd;
        let b0_size_bytes = blob.set_trace_batch("b0".into(), b0.clone(), format)?;

        let b1 = BlobTraceBatchPart {
            desc: desc_from(1, 3, 0),
            index: 0,
            updates: vec![
                (("k".as_bytes(), "v".as_bytes()), 2, 1),
                (("k3".as_bytes(), "v3".as_bytes()), 2, 1),
            ]
            .iter()
            .collect::<ColumnarRecordsVec>()
            .into_inner(),
        };
        let b1_size_bytes = blob.set_trace_batch("b1".into(), b1.clone(), format)?;

        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                keys: vec!["b0".into()],
                format,
                desc: b0.desc,
                level: 0,
                size_bytes: b0_size_bytes,
            },
            b1: TraceBatchMeta {
                keys: vec!["b1".into()],
                format,
                desc: b1.desc,
                level: 0,
                size_bytes: b1_size_bytes,
            },
            since: Antichain::from_elem(2),
        };

        let expected_res = CompactTraceRes {
            req: req.clone(),
            merged: TraceBatchMeta {
                keys: vec!["MERGED_KEY".into()],
                format: ProtoBatchFormat::ParquetKvtd,
                desc: desc_from(0, 3, 2),
                level: 1,
                size_bytes: 0,
            },
        };
        let mut res = maintainer.compact_trace(req).recv()?;

        // Grab the list of newly created trace batch keys so we can check the
        // contents of the merged batch.
        let merged_keys = res.merged.keys.clone();
        // Replace the list of keys with a known set of keys so that we can assert
        // on the trace batch metadata.
        res.merged.keys = vec!["MERGED_KEY".into()];
        res.merged.size_bytes = 0;
        assert_eq!(res, expected_res);

        let mut updates = vec![];
        for key in merged_keys {
            let batch_part = blob
                .get_trace_batch_async(&key, CacheHint::MaybeAdd)
                .recv()?;
            assert_eq!(&batch_part.desc, &expected_res.merged.desc);
            updates.extend(batch_part.updates.iter().flat_map(|u| {
                u.iter()
                    .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), t, d))
            }));
        }

        let expected_updates = vec![
            (("k".as_bytes().to_vec(), "v".as_bytes().to_vec()), 2, 2),
            (("k2".as_bytes().to_vec(), "v2".as_bytes().to_vec()), 2, 1),
            (("k3".as_bytes().to_vec(), "v3".as_bytes().to_vec()), 2, 1),
        ];
        assert_eq!(updates, expected_updates);
        Ok(())
    }

    #[test]
    fn compact_trace_errors() -> Result<(), Error> {
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let metrics = Arc::new(Metrics::default());
        let blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::clone(&async_runtime),
            MemRegistry::new().blob_no_reentrance()?,
            None,
        );
        let maintainer = Maintainer::new(blob, async_runtime, metrics);

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
        assert_eq!(maintainer.compact_trace(req).recv(), Err(Error::from("invalid merge of non-consecutive batches TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 } and TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [3] }, upper: Antichain { elements: [4] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 }")));

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
        assert_eq!(maintainer.compact_trace(req).recv(), Err(Error::from("invalid merge of non-consecutive batches TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 } and TraceBatchMeta { keys: [], format: Unknown, desc: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [4] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 }")));

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
        assert_eq!(maintainer.compact_trace(req).recv(), Err(Error::from("output since Antichain { elements: [0] } must be at or in advance of input since Antichain { elements: [1] }")));

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
        assert_eq!(maintainer.compact_trace(req).recv(), Err(Error::from("output since Antichain { elements: [0] } must be at or in advance of input since Antichain { elements: [1] }")));

        Ok(())
    }
}
