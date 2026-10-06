// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The process-global settings a compute runtime applies from its configuration.
//!
//! lgalloc, the memory limiter, the columnation lgalloc region, the overflowing behavior, the
//! pager and its buffer pool, arrangement dictionary compression, and the metrics registry's
//! workload class label each have one value per process. A process that runs two
//! compute runtimes applies them from one runtime only: the other would double-apply effects that
//! are not idempotent, or race the first.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use mz_compute_types::dyncfgs::{
    COLUMN_CHUNK_COMPRESS_MIN_DEPTH, COLUMN_PAGED_BATCHER_BUDGET_FRACTION,
    COLUMN_PAGED_BATCHER_EAGER_BACKING, COLUMN_PAGED_BATCHER_LZ4,
    COLUMN_PAGED_BATCHER_OVERFLOW_HANDOFF, COLUMN_PAGED_BATCHER_POOL_RSS_TARGET_FRACTION,
    COLUMN_PAGED_BATCHER_READ_PREFETCH, COLUMN_PAGED_BATCHER_SPILL_WORKER_COUNT,
    COLUMN_PAGED_BATCHER_SWAP_PAGEOUT, CORRECTION_V2_COLUMNAR_QUEUE, CORRECTION_V2_QUEUE_DEPTH,
    CORRECTION_V2_QUEUE_DEPTH_UNIT_BYTES, CORRECTION_V2_QUEUE_GEOMETRIC_DEPTH,
    CORRECTION_V2_SINGLE_READ, ENABLE_COLUMN_PAGED_BATCHER_SPILL, ENABLE_COLUMNATION_LGALLOC,
    ENABLE_CORRECTION_V2_SPILL, ENABLE_LGALLOC, ENABLE_LGALLOC_EAGER_RECLAMATION,
    LGALLOC_BACKGROUND_INTERVAL, LGALLOC_FILE_GROWTH_DAMPENER, LGALLOC_LOCAL_BUFFER_BYTES,
    LGALLOC_SLOW_CLEAR_BYTES,
};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metrics::MetricsRegistry;
use mz_storage_types::dyncfgs::ORE_OVERFLOWING_BEHAVIOR;
use prometheus::proto::LabelPair;
use tracing::{debug, error, info, warn};

/// Whether a runtime applies the process-global settings or inherits them. Chosen once, when the
/// runtime is built.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ProcessGlobals {
    /// The runtime applies the settings.
    Apply,
    /// Another runtime in the process applies the settings, and this one inherits them.
    Inherit,
}

impl ProcessGlobals {
    /// Applies the settings `config` carries.
    pub(crate) fn apply_config(self, config: &ConfigSet, scratch_directory: Option<&PathBuf>) {
        match self {
            ProcessGlobals::Inherit => {}
            ProcessGlobals::Apply => {
                apply_lgalloc(config, scratch_directory);
                crate::memory_limiter::apply_limiter_config(config);
                mz_ore::region::ENABLE_LGALLOC_REGION.store(
                    ENABLE_COLUMNATION_LGALLOC.get(config),
                    std::sync::atomic::Ordering::Relaxed,
                );
                let overflowing_behavior = ORE_OVERFLOWING_BEHAVIOR.get(config);
                match overflowing_behavior.parse() {
                    Ok(behavior) => mz_ore::overflowing::set_behavior(behavior),
                    Err(err) => {
                        error!(
                            err,
                            overflowing_behavior, "Invalid value for ore_overflowing_behavior"
                        );
                    }
                }
                apply_pager(scratch_directory);
                apply_column_pager(config, scratch_directory);
                apply_chunk_spill(config);
            }
        }
    }

    /// Sets arrangement dictionary compression, which is captured once per replica.
    pub(crate) fn apply_dictionary_compression(self, enabled: bool) {
        match self {
            ProcessGlobals::Inherit => {}
            ProcessGlobals::Apply => mz_row_spine::DICTIONARY_COMPRESSION
                .store(enabled, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Registers the postprocessor that labels every metric in `registry` with the workload class
    /// `workload_class` holds, once one is known.
    pub(crate) fn register_workload_class_label(
        self,
        registry: &MetricsRegistry,
        workload_class: &Arc<Mutex<Option<String>>>,
    ) {
        match self {
            // The postprocessor rewrites every metric in the whole registry. A second
            // registration would push the label twice onto each metric and produce a
            // duplicate-label scrape error.
            ProcessGlobals::Inherit => {}
            ProcessGlobals::Apply => registry.register_postprocessor({
                let workload_class = Arc::clone(workload_class);
                move |metrics| {
                    let workload_class: Option<String> =
                        workload_class.lock().expect("lock poisoned").clone();
                    let Some(workload_class) = workload_class else {
                        return;
                    };
                    for metric in metrics {
                        for metric in metric.mut_metric() {
                            let mut label = LabelPair::default();
                            label.set_name("workload_class".into());
                            label.set_value(workload_class.clone());

                            let mut labels = metric.take_label();
                            labels.push(label);
                            metric.set_label(labels);
                        }
                    }
                }
            }),
        }
    }
}

fn apply_lgalloc(config: &ConfigSet, scratch_directory: Option<&PathBuf>) {
    if !ENABLE_LGALLOC.get(config) {
        info!("disabling lgalloc");
        lgalloc::lgalloc_set_config(lgalloc::LgAlloc::new().disable());
        return;
    }
    let Some(path) = scratch_directory else {
        debug!("not enabling lgalloc, scratch directory not specified");
        return;
    };
    let clear_bytes = LGALLOC_SLOW_CLEAR_BYTES.get(config);
    let eager_return = ENABLE_LGALLOC_EAGER_RECLAMATION.get(config);
    let file_growth_dampener = LGALLOC_FILE_GROWTH_DAMPENER.get(config);
    let interval = LGALLOC_BACKGROUND_INTERVAL.get(config);
    let local_buffer_bytes = LGALLOC_LOCAL_BUFFER_BYTES.get(config);
    info!(
        ?path,
        backgrund_interval=?interval,
        clear_bytes,
        eager_return,
        file_growth_dampener,
        local_buffer_bytes,
        "enabling lgalloc"
    );
    let background_worker_config = lgalloc::BackgroundWorkerConfig {
        interval,
        clear_bytes,
    };
    lgalloc::lgalloc_set_config(
        lgalloc::LgAlloc::new()
            .enable()
            .with_path(path.clone())
            .with_background_config(background_worker_config)
            .eager_return(eager_return)
            .file_growth_dampener(file_growth_dampener)
            .local_buffer_bytes(local_buffer_bytes),
    );
}

fn apply_pager(scratch_directory: Option<&PathBuf>) {
    // Pager backend selection follows scratch-directory availability:
    // a scratch dir means the file backend; no scratch dir means swap.
    // `set_scratch_dir` and `set_backend` are both idempotent, so calling
    // on every `apply_worker_config` tick is safe. The pager module is
    // only compiled on Unix targets (`mz_ore::pager` is `cfg(unix)`).
    #[cfg(unix)]
    if let Some(path) = scratch_directory {
        mz_ore::pager::set_scratch_dir(path.clone());
        mz_ore::pager::set_backend(mz_ore::pager::Backend::File);
    } else {
        mz_ore::pager::set_backend(mz_ore::pager::Backend::Swap);
    }
}

fn apply_column_pager(config: &ConfigSet, scratch_directory: Option<&PathBuf>) {
    // Apply column-pager configuration. The arrange batchers spill
    // through the buffer pool below, so the consumers of this budget are
    // the MV sink's correction buffer and storage's paged upsert stash
    // flavor, which share one policy and one underlying `mz_ore::pager`.
    // Routes through `apply_tiered_config`, which reuses a process-wide
    // `TieredPolicy` singleton, so operator-driven tunes mutate the
    // existing atomics rather than installing a fresh policy with a
    // fresh budget atomic that would orphan in-flight resident tickets.
    //
    // Backend selection mirrors the lower-level `mz_ore::pager`
    // already configured above: file when a scratch directory is
    // available, swap otherwise.
    use mz_ore::pager::Backend;
    use mz_timely_util::column_pager::{Codec, apply_tiered_config};

    let enabled = ENABLE_COLUMN_PAGED_BATCHER_SPILL.get(config);
    let codec = COLUMN_PAGED_BATCHER_LZ4.get(config).then_some(Codec::Lz4);
    let swap_pageout = COLUMN_PAGED_BATCHER_SWAP_PAGEOUT.get(config);

    // Budget derivation: fraction × announced memory limit, with a
    // 128 MiB floor so the no-pressure case doesn't page per chunk.
    // Falls back to a 4 GiB assumption if no limit was announced
    // (e.g. dev environments).
    const MIB: usize = 1024 * 1024;
    const DEFAULT_MEM_LIMIT: usize = 4 * 1024 * MIB;
    let mem_limit = crate::memory_limiter::get_memory_limit().unwrap_or(DEFAULT_MEM_LIMIT);
    let fraction = COLUMN_PAGED_BATCHER_BUDGET_FRACTION.get(config).max(0.0);
    let total = usize::cast_lossy(f64::cast_lossy(mem_limit) * fraction).max(128 * MIB);

    let backend = if scratch_directory.is_some() {
        Backend::File
    } else {
        Backend::Swap
    };

    debug!(
        enabled,
        ?backend,
        ?codec,
        swap_pageout,
        fraction,
        mem_limit,
        budget_bytes = total,
        "column-paged batcher: applying tiered config",
    );
    apply_tiered_config(enabled, total, backend, codec, swap_pageout);
}

fn apply_chunk_spill(config: &ConfigSet) {
    // Install and retune the process-wide buffer pool that backs chunk
    // spilling. Installation is the gate. The pool is constructed, and its
    // MAP_NORESERVE address space reserved and spill threads spawned, only
    // when a config apply runs with a spill gate on, so a process that
    // never enables spilling never mmaps the pool. Config application
    // reruns on every UpdateConfiguration, so flipping a gate on installs
    // the pool on the next tick. The pool is a process singleton with no
    // teardown: once installed it stays active for the life of the process.
    // Turning every gate back off makes this block do nothing, so the pool
    // keeps its last-applied budget rather than being uninstalled. Later
    // ticks with a gate on retune the one instance in place.
    //
    // Storage's stash shares the singleton and gates only participation,
    // so its spill gate installs the pool too. The worker config set is
    // the full dyncfg aggregate, which is what makes the storage flag
    // readable here.
    use mz_timely_util::pool_config::{PoolPagerConfig, apply_pool_config};

    let compute_spill = ENABLE_COLUMN_PAGED_BATCHER_SPILL.get(config);
    let storage_spill = mz_storage_types::dyncfgs::ENABLE_UPSERT_PAGED_SPILL.get(config);
    let sink_spill = ENABLE_CORRECTION_V2_SPILL.get(config);
    // Set compute's leg of the process-wide chunk spill gate. The
    // gate ORs this leg with storage's, so chunks spill while either
    // subsystem's flag is set. Storage's config application writes
    // only its own leg, keeping the two flags from clobbering each
    // other. The correction buffer has a gate of its own.
    mz_timely_util::columnar::chunk::set_compute_spill_enabled(compute_spill);
    mz_timely_util::columnar::chunk::set_sink_spill_enabled(sink_spill);
    // The pool budget, when a gate installs the pool: the default unit of the MV sink's
    // geometric queue depth hint.
    let mut pool_budget = 0;
    if !(compute_spill || storage_spill || sink_spill) {
        debug!("chunk spill: gates off, leaving the buffer pool uninstalled");
    } else {
        let spill_threads = COLUMN_PAGED_BATCHER_SPILL_WORKER_COUNT.get(config);
        let eager_backing = COLUMN_PAGED_BATCHER_EAGER_BACKING.get(config);

        // Budget derivation: fraction of physical RAM, with a 128 MiB
        // floor so the no-pressure case doesn't page per chunk.
        // Resident budgets derive from RAM, never from the announced
        // memory limit, which on swap-provisioned nodes deliberately
        // includes swap for the memory limiter's purposes. Falls back
        // to a 4 GiB assumption if detection fails.
        const MIB: usize = 1024 * 1024;
        const DEFAULT_RAM: usize = 4 * 1024 * MIB;
        let ram = mz_ore::memory::physical_memory_bytes().unwrap_or(DEFAULT_RAM);
        let of_ram = |fraction: f64| usize::cast_lossy(f64::cast_lossy(ram) * fraction.max(0.0));
        let fraction = COLUMN_PAGED_BATCHER_BUDGET_FRACTION.get(config);
        let total = of_ram(fraction).max(128 * MIB);
        pool_budget = total;
        // No ordering is enforced between the target and the budget. A
        // target at or below budget + warm cap leaves no compressed-tier
        // headroom, which legally collapses the tier. Every backing
        // write then pages out immediately, the pre-tier behavior.
        let rss_target = of_ram(COLUMN_PAGED_BATCHER_POOL_RSS_TARGET_FRACTION.get(config));

        let applied = apply_pool_config(PoolPagerConfig {
            budget_bytes: total,
            spill_threads,
            eager_backing,
            rss_target_bytes: rss_target,
        });
        if let Some(pool) = mz_timely_util::pool_config::active_pool() {
            pool.set_overflow_handoff(COLUMN_PAGED_BATCHER_OVERFLOW_HANDOFF.get(config));
        }
        if applied {
            info!(
                compute_spill,
                storage_spill,
                fraction,
                ram,
                budget_bytes = total,
                spill_threads,
                eager_backing,
                rss_target_bytes = rss_target,
                "chunk spill: applying buffer-pool config",
            );
        } else {
            warn!("chunk spill: buffer pool unavailable; chunks stay resident");
        }
    }

    // The generational depth floor below which spilled bodies store
    // uncompressed. Subsystem-independent, so applied here alongside
    // the rest of the process-wide chunk configuration.
    let compress_min_depth =
        u8::try_from(COLUMN_CHUNK_COMPRESS_MIN_DEPTH.get(config)).unwrap_or(u8::MAX);
    mz_timely_util::columnar::chunk::set_compress_min_depth(compress_min_depth);
    mz_ore::pool::set_read_prefetch(COLUMN_PAGED_BATCHER_READ_PREFETCH.get(config));
    crate::sink::correction_v2::set_single_read(CORRECTION_V2_SINGLE_READ.get(config));
    let queue_unit = match CORRECTION_V2_QUEUE_DEPTH_UNIT_BYTES.get(config) {
        0 => pool_budget,
        unit => unit,
    };
    crate::sink::correction_v2::set_columnar_queue(
        CORRECTION_V2_COLUMNAR_QUEUE.get(config),
        u8::try_from(CORRECTION_V2_QUEUE_DEPTH.get(config)).unwrap_or(u8::MAX),
        CORRECTION_V2_QUEUE_GEOMETRIC_DEPTH.get(config),
        u64::cast_from(queue_unit),
    );
}
