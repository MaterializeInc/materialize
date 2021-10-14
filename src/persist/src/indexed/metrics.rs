// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence related monitoring metrics.

use std::time::Duration;

use ore::metric;
use ore::metrics::{MetricsRegistry, ThirdPartyMetric, UIntCounter, UIntGauge};

// A helper function so we don't have to litter this as cast all over the place.
//
// TODO: It looks like the other time metrics are using f64 seconds, do that
// instead?
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn metric_duration_ms(d: Duration) -> u64 {
    // (2^64) ms = 584_554_531 years, so if this doesn't fit in a u64, we've got
    // other problems.
    d.as_millis() as u64
}

/// Persistence related monitoring metrics for blob storage.
#[derive(Clone, Debug)]
pub struct BlobMetricsByType {
    pub(crate) blob_write_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_write_bytes: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_write_ms: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_delete_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_delete_bytes: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_delete_ms: ThirdPartyMetric<UIntCounter>,
}

impl BlobMetricsByType {
    pub(crate) fn register_with(blob_type: &str, registry: &MetricsRegistry) -> Self {
        BlobMetricsByType {
            blob_write_count: registry.register_third_party_visible(metric!(
                name: format!("mz_persist_blob_{}_write_count", blob_type),
                help: format!("count of {} blob storage writes", blob_type),
            )),
            blob_write_bytes: registry.register_third_party_visible(metric!(
                name: format!("mz_persist_blob_{}_write_bytes", blob_type),
                help: format!("total size written to {} blob storage", blob_type),
            )),
            blob_write_ms: registry.register_third_party_visible(metric!(
                name: format!("mz_persist_blob_{}_write_ms", blob_type),
                help: format!("time spent writing to {} blob storage", blob_type),
            )),
            blob_delete_bytes: registry.register_third_party_visible(metric!(
                name: format!("mz_persist_blob_{}_delete_bytes", blob_type),
                help: format!("total size deleted from {} blob storage", blob_type),
            )),
            blob_delete_count: registry.register_third_party_visible(metric!(
                name: format!("mz_persist_blob_{}_delete_count", blob_type),
                help: format!("count of {} blob storage deletes", blob_type),
            )),
            blob_delete_ms: registry.register_third_party_visible(metric!(
                name: format!("mz_persist_blob_{}_delete_ms", blob_type),
                help: format!("time spent deleting from {} blob storage", blob_type),
            )),
        }
    }
}

/// Persistence related monitoring metrics.
#[derive(Clone, Debug)]
pub struct Metrics {
    pub(crate) stream_count: ThirdPartyMetric<UIntGauge>,
    // TODO: pub(crate) stream_updated: ThirdPartyMetric<UIntGauge>,
    pub(crate) meta_size_bytes: ThirdPartyMetric<UIntGauge>,

    pub(crate) unsealed_blob_count: ThirdPartyMetric<UIntGauge>,
    pub(crate) unsealed_blob_bytes: ThirdPartyMetric<UIntGauge>,

    pub(crate) trace_blob_count: ThirdPartyMetric<UIntGauge>,
    pub(crate) trace_blob_bytes: ThirdPartyMetric<UIntGauge>,

    // TODO: pub(crate) cmd_queue_ms: ThirdPartyMetric<UIntGauge>,
    pub(crate) cmd_queue_in: ThirdPartyMetric<UIntCounter>,

    pub(crate) cmd_run_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) cmd_run_ms: ThirdPartyMetric<UIntCounter>,
    pub(crate) cmd_failed_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) cmd_step_ms: ThirdPartyMetric<UIntCounter>,
    pub(crate) cmd_step_error_count: ThirdPartyMetric<UIntCounter>,

    // TODO: Break these down by unsealed/trace/meta? We'll have to restructure
    // the code a bit but that seems fine.
    pub(crate) compaction_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) compaction_ms: ThirdPartyMetric<UIntCounter>,
    pub(crate) compaction_write_bytes: ThirdPartyMetric<UIntCounter>,

    // TODO: Tag cmd_process_count with cmd type and remove this?
    pub(crate) cmd_write_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) cmd_write_record_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) cmd_write_record_bytes: ThirdPartyMetric<UIntCounter>,

    pub(crate) unsealed: BlobMetricsByType,
    pub(crate) trace: BlobMetricsByType,
    pub(crate) meta: BlobMetricsByType,
    pub(crate) blob_write_error_quota_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_write_error_other_count: ThirdPartyMetric<UIntCounter>,

    // TODO: pub(crate) blob_read_cache_bytes: ThirdPartyMetric<UIntGauge>,
    // TODO: pub(crate) blob_read_cache_entry_count: ThirdPartyMetric<UIntGauge>,
    pub(crate) blob_read_cache_hit_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_read_cache_miss_count: ThirdPartyMetric<UIntCounter>,
    pub(crate) blob_read_cache_fetch_bytes: ThirdPartyMetric<UIntCounter>,
    // TODO: pub(crate) blob_read_error_count: ThirdPartyMetric<UIntCounter>,
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Metrics {
            stream_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_stream_count",
                help: "count of non-destroyed persistent streams",
            )),
            meta_size_bytes: registry.register_third_party_visible(metric!(
                name: "mz_persist_meta_size_bytes",
                help: "size of the most recently generated META record",
            )),
            unsealed_blob_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_unsealed_blob_count",
                help: "count of all blobs containing unsealed records",
            )),
            unsealed_blob_bytes: registry.register_third_party_visible(metric!(
                name: "mz_persist_unsealed_blob_bytes",
                help: "total size of all blobs containing unsealed records",
            )),
            trace_blob_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_trace_blob_count",
                help: "count of all blobs containing sealed records",
            )),
            trace_blob_bytes: registry.register_third_party_visible(metric!(
                name: "mz_persist_trace_blob_bytes",
                help: "total size of all blobs containing sealed records",
            )),
            cmd_queue_in: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_queue_in",
                help: "count of commands entering the runtime channel",
            )),
            cmd_run_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_run_count",
                help: "count of commands run",
            )),
            cmd_run_ms: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_run_ms",
                help: "time spent running commands",
            )),
            cmd_failed_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_failed_count",
                help: "count of commands run where an error was returned",
            )),
            cmd_step_ms: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_step_ms",
                help: "time spent in step",
            )),
            cmd_step_error_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_step_error_count",
                help: "count of errors returned by cmd step",
            )),
            compaction_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_compaction_count",
                help: "count of times unsealed and trace compaction occurred",
            )),
            compaction_ms: registry.register_third_party_visible(metric!(
                name: "mz_persist_compaction_ms",
                help: "time spent compacting unsealed and trace",
            )),
            compaction_write_bytes: registry.register_third_party_visible(metric!(
                name: "mz_persist_compaction_bytes",
                help: "bytes written compacting unsealed and trace",
            )),
            cmd_write_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_write_count",
                help: "count of write commands run",
            )),
            cmd_write_record_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_write_record_count",
                help: "total count of records written in all streams",
            )),
            cmd_write_record_bytes: registry.register_third_party_visible(metric!(
                name: "mz_persist_cmd_write_record_bytes",
                help: "total count of records written in all streams",
            )),
            unsealed: BlobMetricsByType::register_with("unsealed", registry),
            trace: BlobMetricsByType::register_with("trace", registry),
            meta: BlobMetricsByType::register_with("meta", registry),
            blob_write_error_quota_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_blob_write_error_quota_count",
                help: "count of blob storage writes failing because storage is out of space/quota",
            )),
            blob_write_error_other_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_blob_write_error_other_count",
                help: "count of blob storage writes failing for other reasons",
            )),
            blob_read_cache_hit_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_blob_read_cache_hit_count",
                help: "count of blob reads served by cached data",
            )),
            blob_read_cache_miss_count: registry.register_third_party_visible(metric!(
                name: "mz_persist_blob_read_cache_miss_count",
                help: "count of blob reads that had to be fetched",
            )),
            blob_read_cache_fetch_bytes: registry.register_third_party_visible(metric!(
                name: "mz_persist_blob_read_cache_fetch_bytes",
                help: "total size of blob reads that had to be fetched",
            )),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::register_with(&MetricsRegistry::new())
    }
}
