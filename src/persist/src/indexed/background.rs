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
use crate::future::Future;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{BlobTraceBatch, TraceBatchMeta};
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
    /// The blob key to write the resulting batch to.
    pub output_key: String,
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
    pub fn compact_trace(&self, req: CompactTraceReq) -> Future<CompactTraceRes> {
        let (tx, rx) = Future::new();
        let blob = self.blob.clone();
        // Ignore the spawn_blocking response since we communicate
        // success/failure through the returned Future.
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
        if first.desc.upper() != second.desc.lower() {
            return Err(Error::from(format!(
                "invalid merge of non-consecutive batches {:?} and {:?}",
                first, second
            )));
        }

        // Sanity check that both batches being merged are at identical compaction
        // levels.
        debug_assert_eq!(first.level, second.level);

        let desc = Description::new(
            first.desc.lower().clone(),
            second.desc.upper().clone(),
            self.since.clone(),
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

        for ((_, _), t, _) in updates.iter_mut() {
            for since_ts in self.since.elements().iter() {
                if *t < *since_ts {
                    *t = *since_ts;
                }
            }
        }

        differential_dataflow::consolidation::consolidate_updates(&mut updates);

        let new_batch = BlobTraceBatch {
            desc: desc.clone(),
            updates,
        };

        let key = self.new_blob_key();
        // TODO: actually clear the unwanted batches from the blob storage
        let size_bytes = blob.set_trace_batch(key.clone(), new_batch)?;

        // Only upgrade the compaction level if we know this new batch represents
        // an increase in data over both of its parents so that we know we need
        // even more additional batches to amortize the cost of compacting it in
        // the future.
        let merged_level = if size_bytes > first.size_bytes && size_bytes > second.size_bytes {
            first.level + 1
        } else {
            first.level
        };

        Ok(TraceBatchMeta {
            key,
            desc,
            level: merged_level,
            size_bytes,
        })
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
        let blob = BlobCache::new(Metrics::default(), MemRegistry::new().blob_no_reentrance()?);
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
                desc: b0.desc.clone(),
                level: 0,
                size_bytes: b0_size_bytes,
            },
            b1: TraceBatchMeta {
                key: "b1".into(),
                desc: b1.desc.clone(),
                level: 0,
                size_bytes: b1_size_bytes,
            },
            since: Antichain::from_elem(2),
            output_key: "b2".into(),
        };
        let expected_res = CompactTraceRes {
            req: req.clone(),
            merged: TraceBatchMeta {
                key: "b2".into(),
                desc: desc_from(0, 3, 2),
                level: 1,
                size_bytes: 322,
            },
        };
        assert_eq!(
            maintainer.compact_trace(req).recv(),
            Ok(expected_res.clone())
        );

        let b2 = blob.get_trace_batch_async("b2").recv()?;
        let expected_updates = vec![
            (("k".into(), "v".into()), 2, 2),
            (("k2".into(), "v2".into()), 2, 1),
            (("k3".into(), "v3".into()), 2, 1),
        ];
        assert_eq!(&b2.desc, &expected_res.merged.desc);
        assert_eq!(&b2.updates, &expected_updates);
        Ok(())
    }
}
