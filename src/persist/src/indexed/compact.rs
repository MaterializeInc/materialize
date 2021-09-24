// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::runtime::Runtime;

use crate::error::Error;
use crate::future::Future;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::{BlobTraceBatch, Id, TraceBatchMeta};
use crate::indexed::metrics::{metric_duration_ms, Metrics};
use crate::storage::Blob;

/// A request to merge two trace batches and write the results to blob storage.
pub struct CompactReq {
    /// The Id of the collection containing these batches. Not used by
    /// compaction, but used when processing the CompactRes.
    pub id: Id,
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

/// A successful compaction.
pub struct CompactRes {
    /// The original request.
    pub req: CompactReq,
    /// The compacted batch.
    pub merged: TraceBatchMeta,
}

/// A runtime for asynchronous batch compaction.
//
// TODO: Add migrating records from unsealed to trace as well as deletion of
// batches.
pub struct Compacter<B: Blob> {
    // TODO: It feels like a smell to wrap BlobCache in an Arc when most of its
    // internals are already wrapped in Arcs. As of when this was written, the
    // only exception is prev_meta_len, which really is only used from a single
    // thread. Perhaps we should split the Meta parts out of BlobCache.
    blob: Arc<BlobCache<B>>,
    runtime: Arc<Runtime>,
    metrics: Metrics,
}

impl<B: Blob> Compacter<B> {
    pub fn new(blob: BlobCache<B>, runtime: Arc<Runtime>, metrics: Metrics) -> Self {
        Compacter {
            blob: Arc::new(blob),
            runtime,
            metrics,
        }
    }

    /// Asynchronously runs the requested compaction on the work pool provided
    /// at construction time.
    pub fn compact(&self, req: CompactReq) -> Future<CompactRes> {
        let (tx, rx) = Future::new();
        let compaction_start = Instant::now();
        let blob = self.blob.clone();
        // WIP oof this metrics clone is pretty expensive. pass around an
        // Arc<Metrics> everywhere instead.
        let metrics = self.metrics.clone();
        let _ = self.runtime.spawn_blocking(move || {
            tx.fill(Self::compact_blocking(blob, req));
            metrics
                .compaction_ms
                .inc_by(metric_duration_ms(compaction_start.elapsed()));
        });
        rx
    }

    fn compact_blocking(blob: Arc<BlobCache<B>>, req: CompactReq) -> Result<CompactRes, Error> {
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

        let size_bytes = blob.set_trace_batch(req.output_key.clone(), new_batch)?;

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
            key: req.output_key.clone(),
            desc,
            level: merged_level,
            size_bytes,
        };
        Ok(CompactRes { req, merged })
    }
}
