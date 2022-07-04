// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::source::metrics::SourceBaseMetrics;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter};
use prometheus::core::AtomicU64;

pub(super) struct BucketMetrics {
    // public because the normal `inc` flow should *not* increment this, it is
    // specifically used when we aren't incrementing anything else
    pub objects_duplicate: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    objects_downloaded: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    bytes_downloaded: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    messages_ingested: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl BucketMetrics {
    pub(super) fn new(base_metrics: &SourceBaseMetrics, source_id: &str, bucket_id: &str) -> Self {
        let labels = &[bucket_id.to_string(), source_id.to_string()];
        let s3 = &base_metrics.s3;

        Self {
            objects_downloaded: s3
                .objects_downloaded
                .get_delete_on_drop_counter(labels.to_vec()),
            objects_duplicate: s3
                .objects_duplicate
                .get_delete_on_drop_counter(labels.to_vec()),
            bytes_downloaded: s3
                .bytes_downloaded
                .get_delete_on_drop_counter(labels.to_vec()),
            messages_ingested: s3
                .messages_ingested
                .get_delete_on_drop_counter(labels.to_vec()),
        }
    }

    /// Increment counters
    pub(super) fn inc(&self, objects: u64, bytes: u64, messages: u64) {
        self.objects_downloaded.inc_by(objects);
        self.bytes_downloaded.inc_by(bytes);
        self.messages_ingested.inc_by(messages);
    }
}

pub(super) struct ScanBucketMetrics {
    pub(super) objects_discovered: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl ScanBucketMetrics {
    pub(super) fn new(
        base_metrics: &SourceBaseMetrics,
        source_id: &str,
        bucket_id: &str,
    ) -> ScanBucketMetrics {
        let labels = vec![bucket_id.to_string(), source_id.to_string()];
        ScanBucketMetrics {
            objects_discovered: base_metrics
                .s3
                .objects_discovered
                .get_delete_on_drop_counter(labels),
        }
    }
}
