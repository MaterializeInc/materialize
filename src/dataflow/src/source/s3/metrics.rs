// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use lazy_static::lazy_static;
use prometheus::core::AtomicU64;
use prometheus::{register_uint_counter_vec, DeleteOnDropCounter, UIntCounterVec};

pub(super) struct BucketMetrics {
    // public because the normal `inc` flow should *not* increment this, it is
    // specifically used when we aren't incrementing anything else
    pub objects_duplicate: DeleteOnDropCounter<'static, AtomicU64>,
    objects_downloaded: DeleteOnDropCounter<'static, AtomicU64>,
    bytes_downloaded: DeleteOnDropCounter<'static, AtomicU64>,
    messages_ingested: DeleteOnDropCounter<'static, AtomicU64>,
}

impl BucketMetrics {
    pub(super) fn new(source_id: &str, bucket_id: &str) -> BucketMetrics {
        const LABELS: &[&str] = &["bucket_id", "source_id"];
        lazy_static! {
            static ref OBJECTS_DOWNLOADED: UIntCounterVec = register_uint_counter_vec!(
                "mz_s3_objects_downloaded",
                "The number of s3 objects that we have downloaded.",
                LABELS
            )
            .unwrap();
            static ref OBJECTS_DUPLICATE: UIntCounterVec = register_uint_counter_vec!(
                "mz_s3_objects_duplicate_detected",
                "The number of s3 objects that are duplicates, and therefore not downloaded.",
                LABELS
            )
            .unwrap();
            static ref BYTES_DOWNLOADED: UIntCounterVec = register_uint_counter_vec!(
                "mz_s3_bytes_downloaded",
                "The total count of bytes downloaded for this source.",
                LABELS
            )
            .unwrap();
            static ref MESSAGES_INGESTED: UIntCounterVec = register_uint_counter_vec!(
                "mz_s3_messages_ingested",
                "The number of messages ingested for this bucket.",
                LABELS
            )
            .unwrap();
        }

        let labels = &[bucket_id, source_id];

        BucketMetrics {
            objects_downloaded: DeleteOnDropCounter::new_with_error_handler(
                OBJECTS_DOWNLOADED.with_label_values(labels),
                &OBJECTS_DOWNLOADED,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            objects_duplicate: DeleteOnDropCounter::new_with_error_handler(
                OBJECTS_DUPLICATE.with_label_values(labels),
                &OBJECTS_DUPLICATE,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            bytes_downloaded: DeleteOnDropCounter::new_with_error_handler(
                BYTES_DOWNLOADED.with_label_values(labels),
                &BYTES_DOWNLOADED,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
            messages_ingested: DeleteOnDropCounter::new_with_error_handler(
                MESSAGES_INGESTED.with_label_values(labels),
                &MESSAGES_INGESTED,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
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
    pub(super) objects_discovered: DeleteOnDropCounter<'static, AtomicU64>,
}

impl ScanBucketMetrics {
    pub(super) fn new(source_id: &str, bucket_id: &str) -> ScanBucketMetrics {
        const LABELS: &[&str] = &["source_id", "bucket_id"];
        lazy_static! {
            static ref OBJECTS_DISCOVERED: UIntCounterVec = register_uint_counter_vec!(
                "mz_s3_objects_discovered",
                "The number of s3 objects that we have discovered via SCAN or SQS.",
                LABELS
            )
            .unwrap();
        }

        ScanBucketMetrics {
            objects_discovered: DeleteOnDropCounter::new_with_error_handler(
                OBJECTS_DISCOVERED.with_label_values(&[source_id, bucket_id]),
                &OBJECTS_DISCOVERED,
                |e, v| log::debug!("unable to delete metric {}: {}", v.fq_name(), e),
            ),
        }
    }
}
