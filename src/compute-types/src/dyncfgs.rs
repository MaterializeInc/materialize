// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the compute layer.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};

/// Whether rendering should use `mz_join_core` rather than DD's `JoinCore::join_core`.
pub const ENABLE_MZ_JOIN_CORE: Config<bool> = Config::new(
    "enable_mz_join_core",
    true,
    "Whether compute should use `mz_join_core` rather than DD's `JoinCore::join_core` to render \
     linear joins.",
);

/// Whether rendering should use the new MV sink implementation.
pub const ENABLE_MATERIALIZED_VIEW_SINK_V2: Config<bool> = Config::new(
    "enable_compute_materialized_view_sink_v2",
    true,
    "Whether compute should use the new MV sink implementation.",
);

/// Whether rendering should use the new MV sink correction buffer implementation.
///
/// Only has an effect when `enable_compute_materialized_view_sink_v2` is enabled.
pub const ENABLE_CORRECTION_V2: Config<bool> = Config::new(
    "enable_compute_correction_v2",
    false,
    "Whether compute should use the new MV sink correction buffer implementation.",
);

/// The yielding behavior with which linear joins should be rendered.
pub const LINEAR_JOIN_YIELDING: Config<&str> = Config::new(
    "linear_join_yielding",
    "work:1000000,time:100",
    "The yielding behavior compute rendering should apply for linear join operators. Either \
     'work:<amount>' or 'time:<milliseconds>' or 'work:<amount>,time:<milliseconds>'. Note \
     that omitting one of 'work' or 'time' will entirely disable join yielding by time or \
     work, respectively, rather than falling back to some default.",
);

/// Enable lgalloc for columnation.
pub const ENABLE_COLUMNATION_LGALLOC: Config<bool> = Config::new(
    "enable_columnation_lgalloc",
    false,
    "Enable allocating regions from lgalloc.",
);

/// Enable lgalloc's eager memory return/reclamation feature.
pub const ENABLE_LGALLOC_EAGER_RECLAMATION: Config<bool> = Config::new(
    "enable_lgalloc_eager_reclamation",
    true,
    "Enable lgalloc's eager return behavior.",
);

/// Enable the chunked stack implementation.
pub const ENABLE_CHUNKED_STACK: Config<bool> = Config::new(
    "enable_compute_chunked_stack",
    false,
    "Enable the chunked stack implementation in compute.",
);

/// The interval at which the compute server performs maintenance tasks.
pub const COMPUTE_SERVER_MAINTENANCE_INTERVAL: Config<Duration> = Config::new(
    "compute_server_maintenance_interval",
    Duration::from_millis(10),
    "The interval at which the compute server performs maintenance tasks. Zero enables maintenance on every iteration.",
);

/// Maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
pub const DATAFLOW_MAX_INFLIGHT_BYTES: Config<Option<usize>> = Config::new(
    "compute_dataflow_max_inflight_bytes",
    None,
    "The maximum number of in-flight bytes emitted by persist_sources feeding \
     compute dataflows in non-cc clusters.",
);

/// The "physical backpressure" of `compute_dataflow_max_inflight_bytes_cc` has
/// been replaced in cc replicas by persist lgalloc and we intend to remove it
/// once everything has switched to cc. In the meantime, this is a CYA to turn
/// it back on if absolutely necessary.
pub const DATAFLOW_MAX_INFLIGHT_BYTES_CC: Config<Option<usize>> = Config::new(
    "compute_dataflow_max_inflight_bytes_cc",
    None,
    "The maximum number of in-flight bytes emitted by persist_sources feeding \
     compute dataflows in cc clusters.",
);

/// The interval at which the background thread wakes.
pub const LGALLOC_BACKGROUND_INTERVAL: Config<Duration> = Config::new(
    "lgalloc_background_interval",
    Duration::from_secs(1),
    "Scheduling interval for lgalloc's background worker.",
);

/// The bytes to reclaim (slow path) per size class, for each background thread activation.
pub const LGALLOC_SLOW_CLEAR_BYTES: Config<usize> = Config::new(
    "lgalloc_slow_clear_bytes",
    32 << 20,
    "Clear byte size per size class for every invocation",
);

/// The term `n` in the growth rate `1 + 1/(n + 1)` for `ConsolidatingVec`.
/// The smallest value `0` corresponds to the greatest allowed growth, of doubling.
pub const CONSOLIDATING_VEC_GROWTH_DAMPENER: Config<usize> = Config::new(
    "consolidating_vec_growth_dampener",
    0,
    "Dampener in growth rate for consolidating vector size",
);

/// The number of dataflows that may hydrate concurrently.
pub const HYDRATION_CONCURRENCY: Config<usize> = Config::new(
    "compute_hydration_concurrency",
    4,
    "Controls how many compute dataflows may hydrate concurrently.",
);

/// See `src/storage-operators/src/s3_oneshot_sink/parquet.rs` for more details.
pub const COPY_TO_S3_PARQUET_ROW_GROUP_FILE_RATIO: Config<usize> = Config::new(
    "copy_to_s3_parquet_row_group_file_ratio",
    20,
    "The ratio (defined as a percentage) of row-group size to max-file-size. \
        Must be <= 100.",
);

/// See `src/storage-operators/src/s3_oneshot_sink/parquet.rs` for more details.
pub const COPY_TO_S3_ARROW_BUILDER_BUFFER_RATIO: Config<usize> = Config::new(
    "copy_to_s3_arrow_builder_buffer_ratio",
    150,
    "The ratio (defined as a percentage) of arrow-builder size to row-group size. \
        Must be >= 100.",
);

/// The size of each part in the multi-part upload to use when uploading files to S3.
pub const COPY_TO_S3_MULTIPART_PART_SIZE_BYTES: Config<usize> = Config::new(
    "copy_to_s3_multipart_part_size_bytes",
    1024 * 1024 * 8,
    "The size of each part in a multipart upload to S3.",
);

/// Main switch to enable or disable replica expiration.
///
/// Changes affect existing replicas only after restart.
pub const ENABLE_COMPUTE_REPLICA_EXPIRATION: Config<bool> = Config::new(
    "enable_compute_replica_expiration",
    true,
    "Main switch to disable replica expiration.",
);

/// The maximum lifetime of a replica configured as an offset to the replica start time.
/// Used in temporal filters to drop diffs generated at timestamps beyond the expiration time.
///
/// A zero duration implies no expiration. Changing this value does not affect existing replicas,
/// even when they are restarted.
pub const COMPUTE_REPLICA_EXPIRATION_OFFSET: Config<Duration> = Config::new(
    "compute_replica_expiration_offset",
    Duration::ZERO,
    "The expiration time offset for replicas. Zero disables expiration.",
);

/// When enabled, applies the column demands from a MapFilterProject onto the RelationDesc used to
/// read out of Persist. This allows Persist to prune unneeded columns as a performance
/// optimization.
pub const COMPUTE_APPLY_COLUMN_DEMANDS: Config<bool> = Config::new(
    "compute_apply_column_demands",
    false,
    "When enabled, passes applys column demands to the RelationDesc used to read out of Persist.",
);

/// Whether to render `as_specific_collection` using a fueled flat-map operator.
pub const ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION: Config<bool> = Config::new(
    "enable_compute_render_fueled_as_specific_collection",
    true,
    "When enabled, renders `as_specific_collection` using a fueled flat-map operator.",
);

/// Adds the full set of all compute `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ENABLE_MZ_JOIN_CORE)
        .add(&ENABLE_MATERIALIZED_VIEW_SINK_V2)
        .add(&ENABLE_CORRECTION_V2)
        .add(&LINEAR_JOIN_YIELDING)
        .add(&ENABLE_COLUMNATION_LGALLOC)
        .add(&ENABLE_LGALLOC_EAGER_RECLAMATION)
        .add(&ENABLE_CHUNKED_STACK)
        .add(&COMPUTE_SERVER_MAINTENANCE_INTERVAL)
        .add(&DATAFLOW_MAX_INFLIGHT_BYTES)
        .add(&DATAFLOW_MAX_INFLIGHT_BYTES_CC)
        .add(&LGALLOC_BACKGROUND_INTERVAL)
        .add(&LGALLOC_SLOW_CLEAR_BYTES)
        .add(&HYDRATION_CONCURRENCY)
        .add(&COPY_TO_S3_PARQUET_ROW_GROUP_FILE_RATIO)
        .add(&COPY_TO_S3_ARROW_BUILDER_BUFFER_RATIO)
        .add(&COPY_TO_S3_MULTIPART_PART_SIZE_BYTES)
        .add(&ENABLE_COMPUTE_REPLICA_EXPIRATION)
        .add(&COMPUTE_REPLICA_EXPIRATION_OFFSET)
        .add(&COMPUTE_APPLY_COLUMN_DEMANDS)
        .add(&CONSOLIDATING_VEC_GROWTH_DAMPENER)
        .add(&ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION)
}
