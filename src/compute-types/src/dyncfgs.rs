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

use mz_dyncfg::{Config, ConfigSet, ParameterScope};

/// Whether rendering should use `half_join2` rather than DD's `half_join` for delta joins.
///
/// `half_join2` avoids quadratic behavior in certain join patterns. This flag exists as an escape
/// hatch to revert to the old implementation if issues arise.
pub const ENABLE_HALF_JOIN2: Config<bool> = Config::new(
    "enable_compute_half_join2",
    true,
    "Whether compute should use `half_join2` rather than DD's `half_join` to render delta joins.",
)
.scoped(ParameterScope::Replica);

/// Use the column-paged merge batcher code path at arrange sites. When
/// `true`, arrange operators use `Col2ValPagedBatcher` (in
/// `mz_timely_util::columnar`) and `RowRowColPagedBuilder` (in
/// `mz_row_spine`) — the columnar-native batcher that the pager can
/// spill (gated by [`ENABLE_COLUMN_PAGED_BATCHER_SPILL`]). When `false`
/// (the default), the same arrange sites use the legacy
/// `Col2ValBatcher` / `RowRowBuilder` (columnation-merger) path that
/// shipped before #36627. Read at operator construction time; flips
/// take effect on dataflows created after the change.
///
/// Disabled by default while the new path is stabilizing.
/// `DifferentialJoinHydration*` feature-benchmark scenarios opt in
/// explicitly so the spill path is measured.
pub const ENABLE_COLUMN_PAGED_BATCHER: Config<bool> = Config::new(
    "enable_column_paged_batcher",
    false,
    "Use the columnar-native paged merge batcher at arrange sites. When `false` (default), \
     arranges fall back to the legacy columnation `Col2ValBatcher` / `RowRowBuilder` path.",
)
.scoped(ParameterScope::Replica);

/// Allow the column-paged batcher's pager to actually evict chunks
/// under memory pressure. Only meaningful when
/// [`ENABLE_COLUMN_PAGED_BATCHER`] is `true`; with the spill flag off
/// the pager keeps every chunk resident regardless of budget.
///
/// Off by default, even when the batcher path itself is on, so the
/// no-pressure case stays a pure resident operation. Tune the budget /
/// backend via [`COLUMN_PAGED_BATCHER_BUDGET_FRACTION`].
pub const ENABLE_COLUMN_PAGED_BATCHER_SPILL: Config<bool> = Config::new(
    "enable_column_paged_batcher_spill",
    false,
    "Allow the column-paged batcher's pager to evict chunks under memory pressure. Only \
     meaningful when `enable_column_paged_batcher = true`.",
)
.scoped(ParameterScope::Replica);

/// Total resident-byte budget the column-paged batcher's tiered policy
/// (`mz_timely_util::column_pager::policy::TieredPolicy`) is allowed to
/// hold across all workers in this process, expressed as a fraction of
/// the replica's announced memory limit. A single
/// process-wide pool tracks all resident chunks; allocations beyond the
/// pool spill to the configured backend.
///
/// `0.05` (5%) is a reasonable starting point: large enough that the
/// per-call ColumnBuilder ship-threshold (~2 MiB) fits multiple chunks
/// per worker, small enough that the merge-batcher's transient state
/// doesn't crowd out the spine. Set lower to spill more aggressively
/// under pressure. The computed budget is floored at 128 MiB so the
/// no-pressure case doesn't page per chunk. Ignored when
/// `enable_column_paged_batcher_spill` is `false`.
pub const COLUMN_PAGED_BATCHER_BUDGET_FRACTION: Config<f64> = Config::new(
    "column_paged_batcher_budget_fraction",
    0.05,
    "Fraction of replica memory the column-paged batcher's tiered policy may hold resident \
     before spilling to the backend. Total budget = max(mem_limit * fraction, 128 MiB).",
)
.scoped(ParameterScope::Replica);

/// Compress chunks the column-paged batcher spills, using lz4. Only
/// meaningful when [`ENABLE_COLUMN_PAGED_BATCHER_SPILL`] is `true`; the codec
/// is applied on the pageout path and reversed on page-in. Trades CPU for a
/// smaller on-storage (and, for the swap backend, resident) footprint.
///
/// Off by default so the spill path's cost stays a pure copy until compression
/// is shown to pay for itself on the target workload.
pub const COLUMN_PAGED_BATCHER_LZ4: Config<bool> = Config::new(
    "column_paged_batcher_lz4",
    false,
    "Compress column-paged batcher chunks with lz4 on the spill path. Only meaningful when \
     `enable_column_paged_batcher_spill = true`.",
)
.scoped(ParameterScope::Replica);

/// Proactively evict the column-paged batcher's lz4-compressed spill chunks
/// from RSS via `MADV_PAGEOUT` when spilling to the swap backend. Only
/// meaningful when [`COLUMN_PAGED_BATCHER_LZ4`] is `true` and the active
/// backend is swap (no scratch directory): on that path the compressed bytes
/// stay resident in the process address space and currently receive no madvise
/// at all, so the kernel reclaims them only lazily under LRU pressure.
/// `MADV_PAGEOUT` instead swaps them out eagerly at spill time, holding RSS at
/// the budget rather than letting it drift up to the pressure cliff. A later
/// page-in re-faults the pages — cheap because lz4 shrank the byte volume,
/// which is what makes eager eviction pay off on this path.
///
/// Off by default: the eager-reclaim syscall is the one kernel interaction the
/// pager design singled out as risky, so it stays gated until proven on the
/// target workload.
pub const COLUMN_PAGED_BATCHER_SWAP_PAGEOUT: Config<bool> = Config::new(
    "column_paged_batcher_swap_pageout",
    false,
    "Eagerly evict the column-paged batcher's lz4-compressed swap-backend spill chunks from RSS \
     via `MADV_PAGEOUT` (they otherwise receive no madvise and are reclaimed only lazily). Only \
     meaningful when `column_paged_batcher_lz4 = true` and the swap backend is active.",
)
.scoped(ParameterScope::Replica);

/// Whether rendering should use `mz_join_core` rather than DD's `JoinCore::join_core`.
pub const ENABLE_MZ_JOIN_CORE: Config<bool> = Config::new(
    "enable_mz_join_core",
    true,
    "Whether compute should use `mz_join_core` rather than DD's `JoinCore::join_core` to render \
     linear joins.",
)
.scoped(ParameterScope::Replica);

/// Use sync Timely operators with Tokio tasks for the MV sink.
pub const ENABLE_SYNC_MV_SINK: Config<bool> = Config::new(
    "enable_compute_sync_mv_sink",
    true,
    "Use sync Timely operators with Tokio tasks for the MV sink.",
)
.scoped(ParameterScope::Replica);

/// Whether rendering should use the new MV sink correction buffer implementation.
pub const ENABLE_CORRECTION_V2: Config<bool> = Config::new(
    "enable_compute_correction_v2",
    true,
    "Whether compute should use the new MV sink correction buffer implementation.",
)
.scoped(ParameterScope::Replica);

/// The size factor of subsequent chains in the correction V2 buffer.
pub const CORRECTION_V2_CHAIN_PROPORTIONALITY: Config<f64> = Config::new(
    "compute_correction_v2_chain_proportionality",
    3.0,
    "The size factor of subsequent chains in the correction V2 buffer.",
)
.scoped(ParameterScope::Replica);

/// The byte size of chunks in the correction V2 buffer.
pub const CORRECTION_V2_CHUNK_SIZE: Config<usize> = Config::new(
    "compute_correction_v2_chunk_size",
    8 * 1024,
    "The byte size of chunks in the correction V2 buffer.",
)
.scoped(ParameterScope::Replica);

/// Whether to enable temporal bucketing in compute.
pub const ENABLE_COMPUTE_TEMPORAL_BUCKETING: Config<bool> = Config::new(
    "enable_compute_temporal_bucketing",
    false,
    "Whether to enable temporal bucketing in compute.",
)
.scoped(ParameterScope::Replica);

/// The summary to apply to the frontier in temporal bucketing in compute.
pub const TEMPORAL_BUCKETING_SUMMARY: Config<Duration> = Config::new(
    "compute_temporal_bucketing_summary",
    Duration::from_secs(2),
    "The summary to apply to frontiers in temporal bucketing in compute.",
)
.scoped(ParameterScope::Replica);

/// The yielding behavior with which linear joins should be rendered.
pub const LINEAR_JOIN_YIELDING: Config<&str> = Config::new(
    "linear_join_yielding",
    "work:1000000,time:100",
    "The yielding behavior compute rendering should apply for linear join operators. Either \
     'work:<amount>' or 'time:<milliseconds>' or 'work:<amount>,time:<milliseconds>'. Note \
     that omitting one of 'work' or 'time' will entirely disable join yielding by time or \
     work, respectively, rather than falling back to some default.",
)
.scoped(ParameterScope::Replica);

/// Enable lgalloc.
pub const ENABLE_LGALLOC: Config<bool> =
    Config::new("enable_lgalloc", true, "Enable lgalloc.").scoped(ParameterScope::Replica);

/// Enable lgalloc's eager memory return/reclamation feature.
pub const ENABLE_LGALLOC_EAGER_RECLAMATION: Config<bool> = Config::new(
    "enable_lgalloc_eager_reclamation",
    true,
    "Enable lgalloc's eager return behavior.",
)
.scoped(ParameterScope::Replica);

/// The interval at which the background thread wakes.
pub const LGALLOC_BACKGROUND_INTERVAL: Config<Duration> = Config::new(
    "lgalloc_background_interval",
    Duration::from_secs(1),
    "Scheduling interval for lgalloc's background worker.",
)
.scoped(ParameterScope::Replica);

/// Enable lgalloc's eager memory return/reclamation feature.
pub const LGALLOC_FILE_GROWTH_DAMPENER: Config<usize> = Config::new(
    "lgalloc_file_growth_dampener",
    2,
    "Lgalloc's file growth dampener parameter.",
)
.scoped(ParameterScope::Replica);

/// Enable lgalloc's eager memory return/reclamation feature.
pub const LGALLOC_LOCAL_BUFFER_BYTES: Config<usize> = Config::new(
    "lgalloc_local_buffer_bytes",
    64 << 20,
    "Lgalloc's local buffer bytes parameter.",
)
.scoped(ParameterScope::Replica);

/// The bytes to reclaim (slow path) per size class, for each background thread activation.
pub const LGALLOC_SLOW_CLEAR_BYTES: Config<usize> = Config::new(
    "lgalloc_slow_clear_bytes",
    128 << 20,
    "Clear byte size per size class for every invocation",
)
.scoped(ParameterScope::Replica);

/// Interval to run the memory limiter. A zero duration disables the limiter.
pub const MEMORY_LIMITER_INTERVAL: Config<Duration> = Config::new(
    "memory_limiter_interval",
    Duration::from_secs(10),
    "Interval to run the memory limiter. A zero duration disables the limiter.",
)
.scoped(ParameterScope::Replica);

/// Bias to the memory limiter usage factor.
pub const MEMORY_LIMITER_USAGE_BIAS: Config<f64> = Config::new(
    "memory_limiter_usage_bias",
    1.,
    "Multiplicative bias to the memory limiter's limit.",
)
.scoped(ParameterScope::Replica);

/// Burst factor to memory limit.
pub const MEMORY_LIMITER_BURST_FACTOR: Config<f64> = Config::new(
    "memory_limiter_burst_factor",
    0.,
    "Multiplicative burst factor to the memory limiter's limit.",
)
.scoped(ParameterScope::Replica);

/// Enable lgalloc for columnation.
pub const ENABLE_COLUMNATION_LGALLOC: Config<bool> = Config::new(
    "enable_columnation_lgalloc",
    true,
    "Enable allocating regions from lgalloc.",
)
.scoped(ParameterScope::Replica);

/// The interval at which the compute server performs maintenance tasks.
pub const COMPUTE_SERVER_MAINTENANCE_INTERVAL: Config<Duration> = Config::new(
    "compute_server_maintenance_interval",
    Duration::from_millis(10),
    "The interval at which the compute server performs maintenance tasks. Zero enables maintenance on every iteration.",
)
.scoped(ParameterScope::Replica);

/// Maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
pub const DATAFLOW_MAX_INFLIGHT_BYTES: Config<Option<usize>> = Config::new(
    "compute_dataflow_max_inflight_bytes",
    None,
    "The maximum number of in-flight bytes emitted by persist_sources feeding \
     compute dataflows in non-cc clusters.",
)
.scoped(ParameterScope::Replica);

/// The "physical backpressure" of `compute_dataflow_max_inflight_bytes_cc` has
/// been replaced in cc replicas by persist lgalloc and we intend to remove it
/// once everything has switched to cc. In the meantime, this is a CYA to turn
/// it back on if absolutely necessary.
pub const DATAFLOW_MAX_INFLIGHT_BYTES_CC: Config<Option<usize>> = Config::new(
    "compute_dataflow_max_inflight_bytes_cc",
    None,
    "The maximum number of in-flight bytes emitted by persist_sources feeding \
     compute dataflows in cc clusters.",
)
.scoped(ParameterScope::Replica);

/// The term `n` in the growth rate `1 + 1/(n + 1)` for `ConsolidatingVec`.
/// The smallest value `0` corresponds to the greatest allowed growth, of doubling.
pub const CONSOLIDATING_VEC_GROWTH_DAMPENER: Config<usize> = Config::new(
    "consolidating_vec_growth_dampener",
    1,
    "Dampener in growth rate for consolidating vector size",
)
.scoped(ParameterScope::Replica);

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
)
.scoped(ParameterScope::Replica);

/// See `src/storage-operators/src/s3_oneshot_sink/parquet.rs` for more details.
pub const COPY_TO_S3_ARROW_BUILDER_BUFFER_RATIO: Config<usize> = Config::new(
    "copy_to_s3_arrow_builder_buffer_ratio",
    150,
    "The ratio (defined as a percentage) of arrow-builder size to row-group size. \
        Must be >= 100.",
)
.scoped(ParameterScope::Replica);

/// The size of each part in the multi-part upload to use when uploading files to S3.
pub const COPY_TO_S3_MULTIPART_PART_SIZE_BYTES: Config<usize> = Config::new(
    "copy_to_s3_multipart_part_size_bytes",
    1024 * 1024 * 8,
    "The size of each part in a multipart upload to S3.",
)
.scoped(ParameterScope::Replica);

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
    true,
    "When enabled, passes applys column demands to the RelationDesc used to read out of Persist.",
)
.scoped(ParameterScope::Replica);

/// The amount of output the flat-map operator produces before yielding. Set to a high value to
/// avoid yielding, or to a low value to yield frequently.
pub const COMPUTE_FLAT_MAP_FUEL: Config<usize> = Config::new(
    "compute_flat_map_fuel",
    1_000_000,
    "The amount of output the flat-map operator produces before yielding.",
)
.scoped(ParameterScope::Replica);

/// Whether to render `as_specific_collection` using a fueled flat-map operator.
pub const ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION: Config<bool> = Config::new(
    "enable_compute_render_fueled_as_specific_collection",
    true,
    "When enabled, renders `as_specific_collection` using a fueled flat-map operator.",
)
.scoped(ParameterScope::Replica);

/// Whether to apply logical backpressure in compute dataflows.
pub const ENABLE_COMPUTE_LOGICAL_BACKPRESSURE: Config<bool> = Config::new(
    "enable_compute_logical_backpressure",
    false,
    "When enabled, compute dataflows will apply logical backpressure.",
)
.scoped(ParameterScope::Replica);

/// Maximal number of capabilities retained by the logical backpressure operator.
///
/// Selecting this value is subtle. If it's too small, it'll diminish the effectiveness of the
/// logical backpressure operators. If it's too big, we can slow down hydration and cause state
/// in the operator's implementation to build up.
///
/// The default value represents a compromise between these two extremes. We retain some metrics
/// for 30 days, and the metrics update every minute. The default is exactly this number.
pub const COMPUTE_LOGICAL_BACKPRESSURE_MAX_RETAINED_CAPABILITIES: Config<Option<usize>> =
    Config::new(
        "compute_logical_backpressure_max_retained_capabilities",
        Some(30 * 24 * 60),
        "The maximum number of capabilities retained by the logical backpressure operator.",
    )
    .scoped(ParameterScope::Replica);

/// The slack to round observed timestamps up to.
///
/// The default corresponds to Mz's default tick interval, but does not need to do so. Ideally,
/// it is not smaller than the tick interval, but it can be larger.
pub const COMPUTE_LOGICAL_BACKPRESSURE_INFLIGHT_SLACK: Config<Duration> = Config::new(
    "compute_logical_backpressure_inflight_slack",
    Duration::from_secs(1),
    "Round observed timestamps to slack.",
)
.scoped(ParameterScope::Replica);

/// Enable per-column dictionary compression for row containers in arrangements.
///
/// The `_alpha` suffix is load-bearing: this feature is not yet considered
/// production-ready, and the name is meant to make that unmissable at the
/// `ALTER SYSTEM SET` call site rather than relying on out-of-band warnings.
///
/// Disposition: added 2026-06-09, default off; solicit feedback for one month
/// and remove in the absence of a positive response.
pub const ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA: Config<bool> = Config::new(
    "enable_arrangement_dictionary_compression_alpha",
    false,
    "Enable arrangement dictionary compression (alpha; not yet production-ready).",
);

/// Whether to enable the peek response stash, for sending back large peek
/// responses. The response stash will only be used for results that exceed
/// `compute_peek_response_stash_threshold_bytes`.
pub const ENABLE_PEEK_RESPONSE_STASH: Config<bool> = Config::new(
    "enable_compute_peek_response_stash",
    true,
    "Whether to enable the peek response stash, for sending back large peek responses. Will only be used for results that exceed compute_peek_response_stash_threshold_bytes.",
)
.scoped(ParameterScope::Replica);

/// The threshold for peek response size above which we should use the peek
/// response stash. Only used if the peek response stash is enabled _and_ if the
/// query is "streamable" (roughly: doesn't have an ORDER BY).
pub const PEEK_RESPONSE_STASH_THRESHOLD_BYTES: Config<usize> = Config::new(
    "compute_peek_response_stash_threshold_bytes",
    1024 * 10, /* 10KB */
    "The threshold above which to use the peek response stash, for sending back large peek responses.",
)
.scoped(ParameterScope::Replica);

/// The target number of maximum runs in the batches written to the stash.
///
/// Setting this reasonably low will make it so batches get consolidated/sorted
/// concurrently with data being written. Which will in turn make it so that we
/// have to do less work when reading/consolidating those batches in
/// `environmentd`.
pub const PEEK_RESPONSE_STASH_BATCH_MAX_RUNS: Config<usize> = Config::new(
    "compute_peek_response_stash_batch_max_runs",
    // The lowest possible setting, do as much work as possible on the
    // `clusterd` side.
    2,
    "The target number of maximum runs in the batches written to the stash.",
)
.scoped(ParameterScope::Replica);

/// The target size for batches of rows we read out of the peek stash.
pub const PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES: Config<usize> = Config::new(
    "compute_peek_response_stash_read_batch_size_bytes",
    1024 * 1024 * 100, /* 100mb */
    "The target size for batches of rows we read out of the peek stash.",
);

/// The memory budget for consolidating stashed peek responses in
/// `environmentd`.
pub const PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES: Config<usize> = Config::new(
    "compute_peek_response_stash_read_memory_budget_bytes",
    1024 * 1024 * 64, /* 64mb */
    "The memory budget for consolidating stashed peek responses in environmentd.",
);

/// The number of batches to pump from the peek result iterator when stashing peek responses.
pub const PEEK_STASH_NUM_BATCHES: Config<usize> = Config::new(
    "compute_peek_stash_num_batches",
    100,
    "The number of batches to pump from the peek result iterator (in one iteration through the worker loop) when stashing peek responses.",
)
.scoped(ParameterScope::Replica);

/// The size of each batch, as number of rows, pumped from the peek result
/// iterator when stashing peek responses.
pub const PEEK_STASH_BATCH_SIZE: Config<usize> = Config::new(
    "compute_peek_stash_batch_size",
    100000,
    "The size, as number of rows, of each batch pumped from the peek result iterator (in one iteration through the worker loop) when stashing peek responses.",
)
.scoped(ParameterScope::Replica);

/// The collection interval for the Prometheus metrics introspection source.
///
/// Set to zero to disable scraping and retract any existing data.
pub const COMPUTE_PROMETHEUS_INTROSPECTION_SCRAPE_INTERVAL: Config<Duration> = Config::new(
    "compute_prometheus_introspection_scrape_interval",
    Duration::from_secs(10),
    "The collection interval for the Prometheus metrics introspection source. Set to zero to disable.",
)
.scoped(ParameterScope::Replica);

/// If set, skip fetching or processing the snapshot data for subscribes when possible.
pub const SUBSCRIBE_SNAPSHOT_OPTIMIZATION: Config<bool> = Config::new(
    "compute_subscribe_snapshot_optimization",
    true,
    "If set, skip fetching or processing the snapshot data for subscribes when possible.",
)
.scoped(ParameterScope::Replica);

/// Temporary flag to de-risk the rollout of a release-blocker fix.
///
/// TODO: Remove after one, or a couple, releases.
pub const MV_SINK_ADVANCE_PERSIST_FRONTIERS: Config<bool> = Config::new(
    "compute_mv_sink_advance_persist_frontiers",
    true,
    "Whether the MV sink's write operator advances its internal persist frontiers to the as_of.",
)
.scoped(ParameterScope::Replica);

/// Adds the full set of all compute `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ENABLE_HALF_JOIN2)
        .add(&ENABLE_MZ_JOIN_CORE)
        .add(&ENABLE_SYNC_MV_SINK)
        .add(&ENABLE_CORRECTION_V2)
        .add(&CORRECTION_V2_CHAIN_PROPORTIONALITY)
        .add(&CORRECTION_V2_CHUNK_SIZE)
        .add(&ENABLE_COMPUTE_TEMPORAL_BUCKETING)
        .add(&TEMPORAL_BUCKETING_SUMMARY)
        .add(&LINEAR_JOIN_YIELDING)
        .add(&ENABLE_LGALLOC)
        .add(&LGALLOC_BACKGROUND_INTERVAL)
        .add(&LGALLOC_FILE_GROWTH_DAMPENER)
        .add(&LGALLOC_LOCAL_BUFFER_BYTES)
        .add(&LGALLOC_SLOW_CLEAR_BYTES)
        .add(&MEMORY_LIMITER_INTERVAL)
        .add(&MEMORY_LIMITER_USAGE_BIAS)
        .add(&MEMORY_LIMITER_BURST_FACTOR)
        .add(&ENABLE_LGALLOC_EAGER_RECLAMATION)
        .add(&ENABLE_COLUMNATION_LGALLOC)
        .add(&COMPUTE_SERVER_MAINTENANCE_INTERVAL)
        .add(&DATAFLOW_MAX_INFLIGHT_BYTES)
        .add(&DATAFLOW_MAX_INFLIGHT_BYTES_CC)
        .add(&HYDRATION_CONCURRENCY)
        .add(&COPY_TO_S3_PARQUET_ROW_GROUP_FILE_RATIO)
        .add(&COPY_TO_S3_ARROW_BUILDER_BUFFER_RATIO)
        .add(&COPY_TO_S3_MULTIPART_PART_SIZE_BYTES)
        .add(&ENABLE_COMPUTE_REPLICA_EXPIRATION)
        .add(&COMPUTE_REPLICA_EXPIRATION_OFFSET)
        .add(&COMPUTE_APPLY_COLUMN_DEMANDS)
        .add(&COMPUTE_FLAT_MAP_FUEL)
        .add(&CONSOLIDATING_VEC_GROWTH_DAMPENER)
        .add(&ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION)
        .add(&ENABLE_COMPUTE_LOGICAL_BACKPRESSURE)
        .add(&COMPUTE_LOGICAL_BACKPRESSURE_MAX_RETAINED_CAPABILITIES)
        .add(&COMPUTE_LOGICAL_BACKPRESSURE_INFLIGHT_SLACK)
        .add(&ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA)
        .add(&ENABLE_PEEK_RESPONSE_STASH)
        .add(&PEEK_RESPONSE_STASH_THRESHOLD_BYTES)
        .add(&PEEK_RESPONSE_STASH_BATCH_MAX_RUNS)
        .add(&PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES)
        .add(&PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES)
        .add(&PEEK_STASH_NUM_BATCHES)
        .add(&PEEK_STASH_BATCH_SIZE)
        .add(&COMPUTE_PROMETHEUS_INTROSPECTION_SCRAPE_INTERVAL)
        .add(&SUBSCRIBE_SNAPSHOT_OPTIMIZATION)
        .add(&MV_SINK_ADVANCE_PERSIST_FRONTIERS)
        .add(&ENABLE_COLUMN_PAGED_BATCHER)
        .add(&ENABLE_COLUMN_PAGED_BATCHER_SPILL)
        .add(&COLUMN_PAGED_BATCHER_BUDGET_FRACTION)
        .add(&COLUMN_PAGED_BATCHER_LZ4)
        .add(&COLUMN_PAGED_BATCHER_SWAP_PAGEOUT)
}
