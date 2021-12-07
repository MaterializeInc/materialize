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

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::runtime::Runtime;

use crate::error::Error;
use crate::indexed::arrangement::Arrangement;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{BlobTraceBatch, TraceBatchMeta};
use crate::pfuture::PFuture;
use crate::storage::Blob;

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
pub struct Maintainer<B: Blob> {
    // TODO: It feels like a smell to wrap BlobCache in an Arc when most of its
    // internals are already wrapped in Arcs. As of when this was written, the
    // only exception is prev_meta_len, which really is only used from a single
    // thread. Perhaps we should split the Meta parts out of BlobCache.
    blob: Arc<BlobCache<B>>,
    runtime: Arc<Runtime>,
}

impl<B: Blob> Maintainer<B> {
    /// Returns a new [Maintainer].
    pub fn new(blob: BlobCache<B>, runtime: Arc<Runtime>) -> Self {
        Maintainer {
            blob: Arc::new(blob),
            runtime,
        }
    }

    /// Asynchronously runs the requested compaction on the work pool provided
    /// at construction time.
    pub fn compact_trace(&self, req: CompactTraceReq) -> PFuture<CompactTraceRes> {
        let (tx, rx) = PFuture::new();
        let blob = self.blob.clone();
        // Ignore the spawn_blocking response since we communicate
        // success/failure through the returned future.
        //
        // TODO: Push the spawn_blocking down into the cpu-intensive bits and
        // use spawn here once the storage traits are made async.
        let _ = self
            .runtime
            .spawn_blocking(move || tx.fill(Self::compact_trace_blocking(blob, req)));
        rx
    }

    fn compact_trace_blocking(
        blob: Arc<BlobCache<B>>,
        req: CompactTraceReq,
    ) -> Result<CompactTraceRes, Error> {
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

        updates.extend(
            blob.get_trace_batch_async(&first.key)
                .recv()?
                .updates
                .iter()
                .cloned(),
        );
        updates.extend(
            blob.get_trace_batch_async(&second.key)
                .recv()?
                .updates
                .iter()
                .cloned(),
        );

        for ((_, _), ts, _) in updates.iter_mut() {
            ts.advance_by(desc.since().borrow());
        }

        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        let new_batch = BlobTraceBatch {
            desc: desc.clone(),
            updates,
        };

        let merged_key = Arrangement::new_blob_key();
        let size_bytes = blob.set_trace_batch(merged_key.clone(), new_batch)?;

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
            key: merged_key,
            desc,
            level: merged_level,
            size_bytes,
        };
        Ok(CompactTraceRes { req, merged })
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::Description;
    use tokio::runtime::Runtime;

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
        let blob = BlobCache::new(
            build_info::DUMMY_BUILD_INFO,
            Metrics::default(),
            MemRegistry::new().blob_no_reentrance()?,
        );
        let maintainer = Maintainer::new(blob.clone(), Arc::new(Runtime::new()?));

        let b0 = BlobTraceBatch {
            desc: desc_from(0, 1, 0),
            updates: vec![
                (("k".into(), "v".into()), 0, 1),
                (("k2".into(), "v2".into()), 0, 1),
            ],
        };
        let b0_size_bytes = blob.set_trace_batch("b0".into(), b0.clone())?;

        let b1 = BlobTraceBatch {
            desc: desc_from(1, 3, 0),
            updates: vec![
                (("k".into(), "v".into()), 2, 1),
                (("k3".into(), "v3".into()), 2, 1),
            ],
        };
        let b1_size_bytes = blob.set_trace_batch("b1".into(), b1.clone())?;

        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                key: "b0".into(),
                desc: b0.desc,
                level: 0,
                size_bytes: b0_size_bytes,
            },
            b1: TraceBatchMeta {
                key: "b1".into(),
                desc: b1.desc,
                level: 0,
                size_bytes: b1_size_bytes,
            },
            since: Antichain::from_elem(2),
        };

        let expected_res = CompactTraceRes {
            req: req.clone(),
            merged: TraceBatchMeta {
                key: "MERGED_KEY".into(),
                desc: desc_from(0, 3, 2),
                level: 1,
                size_bytes: 162,
            },
        };
        let mut res = maintainer.compact_trace(req).recv()?;
        let merged_key = res.merged.key.clone();
        res.merged.key = "MERGED_KEY".into();
        assert_eq!(res, expected_res);

        let b2 = blob.get_trace_batch_async(&merged_key).recv()?;
        let expected_updates = vec![
            (("k".into(), "v".into()), 2, 2),
            (("k2".into(), "v2".into()), 2, 1),
            (("k3".into(), "v3".into()), 2, 1),
        ];
        assert_eq!(&b2.desc, &expected_res.merged.desc);
        assert_eq!(&b2.updates, &expected_updates);
        Ok(())
    }

    #[test]
    fn compact_trace_errors() -> Result<(), Error> {
        let blob = BlobCache::new(
            build_info::DUMMY_BUILD_INFO,
            Metrics::default(),
            MemRegistry::new().blob_no_reentrance()?,
        );
        let maintainer = Maintainer::new(blob, Arc::new(Runtime::new()?));

        // Non-contiguous batch descs
        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                key: "".into(),
                desc: desc_from(0, 2, 0),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                key: "".into(),
                desc: desc_from(3, 4, 0),
                level: 0,
                size_bytes: 0,
            },
            since: Antichain::from_elem(0),
        };
        assert_eq!(maintainer.compact_trace(req).recv(), Err(Error::from("invalid merge of non-consecutive batches TraceBatchMeta { key: \"\", desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 } and TraceBatchMeta { key: \"\", desc: Description { lower: Antichain { elements: [3] }, upper: Antichain { elements: [4] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 }")));

        // Overlapping batch descs
        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                key: "".into(),
                desc: desc_from(0, 2, 0),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                key: "".into(),
                desc: desc_from(1, 4, 0),
                level: 0,
                size_bytes: 0,
            },
            since: Antichain::from_elem(0),
        };
        assert_eq!(maintainer.compact_trace(req).recv(), Err(Error::from("invalid merge of non-consecutive batches TraceBatchMeta { key: \"\", desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [2] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 } and TraceBatchMeta { key: \"\", desc: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [4] }, since: Antichain { elements: [0] } }, level: 0, size_bytes: 0 }")));

        // Since not at or in advance of b0's since
        let req = CompactTraceReq {
            b0: TraceBatchMeta {
                key: "".into(),
                desc: desc_from(0, 2, 1),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                key: "".into(),
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
                key: "".into(),
                desc: desc_from(0, 2, 0),
                level: 0,
                size_bytes: 0,
            },
            b1: TraceBatchMeta {
                key: "".into(),
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
