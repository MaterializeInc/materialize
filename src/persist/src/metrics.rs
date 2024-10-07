// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation-specific metrics for persist blobs and consensus

use std::sync::Arc;
use std::time::Instant;

use mz_dyncfg::ConfigSet;
use mz_ore::lgbytes::{LgBytesMetrics, LgBytesOpMetrics};
use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry};
use prometheus::{CounterVec, IntCounterVec};

/// Metrics specific to S3Blob's internal workings.
#[derive(Debug, Clone)]
pub struct S3BlobMetrics {
    pub(crate) operation_timeouts: IntCounter,
    pub(crate) operation_attempt_timeouts: IntCounter,
    pub(crate) connect_timeouts: IntCounter,
    pub(crate) read_timeouts: IntCounter,
    pub(crate) get_part: IntCounter,
    pub(crate) get_invalid_resp: IntCounter,
    pub(crate) set_single: IntCounter,
    pub(crate) set_multi_create: IntCounter,
    pub(crate) set_multi_part: IntCounter,
    pub(crate) set_multi_complete: IntCounter,
    pub(crate) delete_head: IntCounter,
    pub(crate) delete_object: IntCounter,
    pub(crate) list_objects: IntCounter,

    /// Metrics for all usages of LgBytes. Exposed as public for convenience in
    /// persist boot, we'll have to pull this out and do the plumbing
    /// differently if mz gains a non-persist user of LgBytes.
    pub lgbytes: LgBytesMetrics,
}

impl S3BlobMetrics {
    /// Returns a new [S3BlobMetrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let operations: IntCounterVec = registry.register(metric!(
            name: "mz_persist_s3_operations",
            help: "number of raw s3 calls on behalf of Blob interface methods",
            var_labels: ["op"],
        ));
        Self {
            operation_timeouts: registry.register(metric!(
                name: "mz_persist_s3_operation_timeouts",
                help: "number of operation timeouts (including retries)",
            )),
            operation_attempt_timeouts: registry.register(metric!(
                name: "mz_persist_s3_operation_attempt_timeouts",
                help: "number of operation attempt timeouts (within a single retry)",
            )),
            connect_timeouts: registry.register(metric!(
                name: "mz_persist_s3_connect_timeouts",
                help: "number of timeouts establishing a connection to S3",
            )),
            read_timeouts: registry.register(metric!(
                name: "mz_persist_s3_read_timeouts",
                help: "number of timeouts waiting on first response byte from S3",
            )),
            get_part: operations.with_label_values(&["get_part"]),
            get_invalid_resp: operations.with_label_values(&["get_invalid_resp"]),
            set_single: operations.with_label_values(&["set_single"]),
            set_multi_create: operations.with_label_values(&["set_multi_create"]),
            set_multi_part: operations.with_label_values(&["set_multi_part"]),
            set_multi_complete: operations.with_label_values(&["set_multi_complete"]),
            delete_head: operations.with_label_values(&["delete_head"]),
            delete_object: operations.with_label_values(&["delete_object"]),
            list_objects: operations.with_label_values(&["list_objects"]),
            lgbytes: LgBytesMetrics::new(registry),
        }
    }
}

/// Metrics specific to our usage of Arrow and Parquet.
#[derive(Debug, Clone)]
pub struct ArrowMetrics {
    pub(crate) key: ArrowColumnMetrics,
    pub(crate) val: ArrowColumnMetrics,
    pub(crate) part_build_seconds: Counter,
    pub(crate) part_build_count: IntCounter,
    pub(crate) concat_bytes: IntCounter,
}

impl ArrowMetrics {
    /// Returns a new [ArrowMetrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let op_count: IntCounterVec = registry.register(metric!(
            name: "mz_persist_columnar_op_count",
            help: "number of rows we've run the specified op on in our structured columnar format",
            var_labels: ["op", "column", "result"],
        ));
        let op_seconds: CounterVec = registry.register(metric!(
            name: "mz_persist_columnar_op_seconds",
            help: "numer of seconds we've spent running the specified op on our structured columnar format",
            var_labels: ["op", "column"],
        ));

        let part_build_seconds: Counter = registry.register(metric!(
            name: "mz_persist_columnar_part_build_seconds",
            help: "number of seconds we've spent encoding our structured columnar format",
        ));
        let part_build_count: IntCounter = registry.register(metric!(
            name: "mz_persist_columnar_part_build_count",
            help: "number of times we've encoded our structured columnar format",
        ));
        let concat_bytes: IntCounter = registry.register(metric!(
            name: "mz_persist_columnar_part_concat_bytes",
            help: "number of bytes we've copied when concatenating updates",
        ));

        ArrowMetrics {
            key: ArrowColumnMetrics::new(&op_count, &op_seconds, "key"),
            val: ArrowColumnMetrics::new(&op_count, &op_seconds, "val"),
            part_build_seconds,
            part_build_count,
            concat_bytes,
        }
    }

    /// Metrics for the top-level 'k_s' column.
    pub fn key(&self) -> &ArrowColumnMetrics {
        &self.key
    }

    /// Metrics for the top-level 'v_s' column.
    pub fn val(&self) -> &ArrowColumnMetrics {
        &self.val
    }

    /// Measure and report how long building a Part takes.
    pub fn measure_part_build<R, F: FnOnce() -> R>(&self, f: F) -> R {
        let start = Instant::now();
        let r = f();
        let duration = start.elapsed();

        self.part_build_count.inc();
        self.part_build_seconds.inc_by(duration.as_secs_f64());

        r
    }
}

/// Metrics for a top-level [`arrow`] column in our structured representation.
#[derive(Debug, Clone)]
pub struct ArrowColumnMetrics {
    decoding_count: IntCounter,
    decoding_seconds: Counter,
    correct_count: IntCounter,
    invalid_count: IntCounter,
}

impl ArrowColumnMetrics {
    fn new(count: &IntCounterVec, duration: &CounterVec, col: &'static str) -> Self {
        ArrowColumnMetrics {
            decoding_count: count.with_label_values(&["decode", col, "success"]),
            decoding_seconds: duration.with_label_values(&["decode", col]),
            correct_count: count.with_label_values(&["validation", col, "correct"]),
            invalid_count: count.with_label_values(&["validation", col, "invalid"]),
        }
    }

    /// Measure and report how long decoding takes.
    pub fn measure_decoding<R, F: FnOnce() -> R>(&self, decode: F) -> R {
        let start = Instant::now();
        let result = decode();
        let duration = start.elapsed();

        self.decoding_count.inc();
        self.decoding_seconds.inc_by(duration.as_secs_f64());

        result
    }

    /// Measure and report statistics for validation.
    pub fn report_valid<F: FnOnce() -> bool>(&self, f: F) -> bool {
        let is_valid = f();
        if is_valid {
            self.correct_count.inc();
        } else {
            self.invalid_count.inc();
        }
        is_valid
    }
}

/// Metrics for a Parquet file that we write to S3.
#[derive(Debug, Clone)]
pub struct ParquetMetrics {
    pub(crate) encoded_size: IntCounterVec,
    pub(crate) num_row_groups: IntCounterVec,
    pub(crate) k_metrics: ParquetColumnMetrics,
    pub(crate) v_metrics: ParquetColumnMetrics,
    pub(crate) t_metrics: ParquetColumnMetrics,
    pub(crate) d_metrics: ParquetColumnMetrics,
    pub(crate) k_s_metrics: ParquetColumnMetrics,
    pub(crate) v_s_metrics: ParquetColumnMetrics,
    pub(crate) elided_null_buffers: IntCounter,
}

impl ParquetMetrics {
    pub(crate) fn new(registry: &MetricsRegistry) -> Self {
        let encoded_size: IntCounterVec = registry.register(metric!(
            name: "mz_persist_parquet_encoded_size",
            help: "encoded size of a parquet file that we write to S3",
            var_labels: ["format"],
        ));
        let num_row_groups: IntCounterVec = registry.register(metric!(
            name: "mz_persist_parquet_row_group_count",
            help: "count of row groups in a parquet file",
            var_labels: ["format"],
        ));

        let column_size: IntCounterVec = registry.register(metric!(
            name: "mz_persist_parquet_column_size",
            help: "size in bytes of a column within a parquet file",
            var_labels: ["col", "compressed"],
        ));

        ParquetMetrics {
            encoded_size,
            num_row_groups,
            k_metrics: ParquetColumnMetrics::new("k", &column_size),
            v_metrics: ParquetColumnMetrics::new("v", &column_size),
            t_metrics: ParquetColumnMetrics::new("t", &column_size),
            d_metrics: ParquetColumnMetrics::new("d", &column_size),
            k_s_metrics: ParquetColumnMetrics::new("k_s", &column_size),
            v_s_metrics: ParquetColumnMetrics::new("v_s", &column_size),
            elided_null_buffers: registry.register(metric!(
                name: "mz_persist_parquet_elided_null_buffer_count",
                help: "times we dropped an unnecessary null buffer returned by parquet decoding",
            )),
        }
    }
}

/// Metrics for a column within a Parquet file that we write to S3.
#[derive(Debug, Clone)]
pub struct ParquetColumnMetrics {
    pub(crate) uncompressed_size: IntCounter,
    pub(crate) compressed_size: IntCounter,
}

impl ParquetColumnMetrics {
    pub(crate) fn new(col: &'static str, size: &IntCounterVec) -> Self {
        ParquetColumnMetrics {
            uncompressed_size: size.with_label_values(&[col, "uncompressed"]),
            compressed_size: size.with_label_values(&[col, "compressed"]),
        }
    }

    pub(crate) fn report_sizes(&self, uncompressed: u64, compressed: u64) {
        self.uncompressed_size.inc_by(uncompressed);
        self.compressed_size.inc_by(compressed);
    }
}

/// Metrics for `ColumnarRecords`.
#[derive(Debug)]
pub struct ColumnarMetrics {
    pub(crate) lgbytes_arrow: LgBytesOpMetrics,
    pub(crate) parquet: ParquetMetrics,
    pub(crate) arrow: ArrowMetrics,
    // TODO: Having these two here isn't quite the right thing to do, but it
    // saves a LOT of plumbing.
    pub(crate) cfg: Arc<ConfigSet>,
    pub(crate) is_cc_active: bool,
}

impl ColumnarMetrics {
    /// Returns a new [ColumnarMetrics].
    pub fn new(
        registry: &MetricsRegistry,
        lgbytes: &LgBytesMetrics,
        cfg: Arc<ConfigSet>,
        is_cc_active: bool,
    ) -> Self {
        ColumnarMetrics {
            parquet: ParquetMetrics::new(registry),
            arrow: ArrowMetrics::new(registry),
            lgbytes_arrow: lgbytes.persist_arrow.clone(),
            cfg,
            is_cc_active,
        }
    }

    /// Returns a reference to the [`arrow`] metrics for our structured data representation.
    pub fn arrow(&self) -> &ArrowMetrics {
        &self.arrow
    }

    /// Returns a reference to the [`parquet`] metrics for our structured data representation.
    pub fn parquet(&self) -> &ParquetMetrics {
        &self.parquet
    }

    /// Returns a [ColumnarMetrics] disconnected from any metrics registry.
    ///
    /// Exposed for testing.
    pub fn disconnected() -> Self {
        let registry = MetricsRegistry::new();
        let lgbytes = LgBytesMetrics::new(&registry);
        let cfg = crate::cfg::all_dyn_configs(ConfigSet::default());

        Self::new(&registry, &lgbytes, Arc::new(cfg), false)
    }
}
