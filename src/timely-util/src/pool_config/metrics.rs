// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Prometheus metrics for the process-wide buffer pool.
//!
//! Installed once by compute init via [`register`]. All metrics are computed
//! gauges that peek at the pool's stats at scrape time, reporting zero until
//! something initializes the pool — a scrape must observe, not mmap the
//! pool's address space into every process that happens to be monitored.

use std::sync::OnceLock;

use mz_ore::metric;
use mz_ore::metrics::{ComputedUIntGauge, MakeCollectorOpts, MetricsRegistry};

/// Install the buffer-pool metrics into `registry`. Idempotent. Repeated
/// calls after the first one are no-ops.
pub fn register(registry: &MetricsRegistry) {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    REGISTERED.get_or_init(|| {
        // Every name and help string is a literal at the `metric!` call so the
        // metrics-catalog scanner (`bin/gen-metrics-catalog`), which reads the
        // source rather than the expanded macro, can index them.
        //
        // These fields are exposed as computed gauges rather than counters
        // because the pool owns the atomics. Monotonic counters and
        // instantaneous levels are mixed under the one gauge type, so the
        // `_total` name suffix, not the metric type, marks a field as
        // monotonic.
        gauge(registry, metric!(name: "mz_column_pool_resident_bytes", help: "Uncompressed bytes resident in the buffer pool."), |s| s.resident_bytes);
        gauge(registry, metric!(name: "mz_column_pool_oversize_bytes", help: "Bytes held by oversize chunks that bypass pool paging."), |s| s.oversize_bytes);
        gauge(registry, metric!(name: "mz_column_pool_inserts_total", help: "Chunks inserted into the buffer pool."), |s| s.inserts);
        gauge(registry, metric!(name: "mz_column_pool_frees_total", help: "Chunks freed from the buffer pool."), |s| s.frees);
        gauge(registry, metric!(name: "mz_column_pool_writes_elided_total", help: "Backing writes elided: chunks freed while unbacked or while queued for a spill thread, dead before their compression completed."), |s| s.writes_elided);
        gauge(registry, metric!(name: "mz_column_pool_evictions_compress_total", help: "Evictions that compressed a chunk into a new swap-backed extent."), |s| s.evictions_compress);
        gauge(registry, metric!(name: "mz_column_pool_evictions_cheap_total", help: "Evictions of already-backed chunks: physical pages released with no compression or extent write."), |s| s.evictions_cheap);
        gauge(registry, metric!(name: "mz_column_pool_extent_bytes_written_total", help: "Compressed bytes written into swap-backed extents."), |s| s.extent_bytes_written);
        gauge(registry, metric!(name: "mz_column_pool_spill_scheduled_total", help: "Evictions handed to buffer-pool spill threads."), |s| s.spill_scheduled);
        gauge(registry, metric!(name: "mz_column_pool_spill_cancelled_total", help: "Compressions cancelled by a concurrent free, whatever their origin (budget eviction or eager backing), so this can exceed the scheduled count."), |s| s.spill_cancelled);
        gauge(registry, metric!(name: "mz_column_pool_spill_in_flight", help: "Spill entries queued or being processed."), |s| s.spill_in_flight);
        gauge(registry, metric!(name: "mz_column_pool_admissions_budget_total", help: "Evicted chunks re-admitted to compressed-but-resident by an admitting read out of free budget headroom."), |s| s.admissions_budget);
        gauge(registry, metric!(name: "mz_column_pool_admissions_steal_total", help: "Evicted chunks re-admitted by an admitting read stealing the slot of a clean backed victim of the same size class."), |s| s.admissions_steal);
        gauge(registry, metric!(name: "mz_column_pool_admissions_denied_total", help: "Admitting reads that found neither budget headroom nor a clean victim and were served as a plain decompress instead."), |s| s.admissions_denied);
        gauge(registry, metric!(name: "mz_column_pool_extent_pageout_incomplete_total", help: "Pageout passes whose residency observation found pages still resident. Climbing steadily means pages cannot reach the swap device."), |s| s.extent_pageout_incomplete);
        gauge(registry, metric!(name: "mz_column_pool_extent_arena_fallbacks_total", help: "Extent writes that fell back to the heap because their extent-arena class had no free slot. Heap-backed extents are never paged out."), |s| s.extent_arena_fallbacks);
        gauge(registry, metric!(name: "mz_column_pool_slot_exhausted_fallbacks_total", help: "Inserts that fell back to unpageable heap chunks because their size class had no free slot."), |s| s.slot_exhausted_fallbacks);
        gauge(registry, metric!(name: "mz_column_pool_oversize_payloads_total", help: "Inserts that went to unpageable heap chunks because the payload was larger than the largest size class."), |s| s.oversize_payloads);
        gauge(registry, metric!(name: "mz_column_pool_live_chunks", help: "Live pool chunks, whatever their residency: for backlog-shaped consumers, the un-drained backlog in chunks."), |s| s.live_chunks);
        gauge(registry, metric!(name: "mz_column_pool_warm_bytes", help: "Class bytes of free slots kept warm for fault-free reuse; RSS exceeds resident bytes by up to this bounded amount."), |s| s.warm_bytes);
        gauge(registry, metric!(name: "mz_column_pool_warm_reuses_total", help: "Slot allocations served from the warm list: no page faults, no kernel page zeroing."), |s| s.warm_reuses);
        gauge(registry, metric!(name: "mz_column_pool_eager_backs_total", help: "Chunks eagerly compressed to compressed-but-resident by idle spill threads; their later eviction is a pure page release."), |s| s.eager_backs);
        gauge(registry, metric!(name: "mz_column_pool_extent_resident_bytes", help: "Allocation bytes of compressed extents currently resident (the compressed-but-resident tier), bounded by the pool RSS target."), |s| s.extent_resident_bytes);
        gauge(registry, metric!(name: "mz_column_pool_extent_unreclaimable_bytes", help: "Allocation bytes of resident extents the RSS target cannot push out (retry-capped and heap-fallback extents). Climbing steadily means pages cannot reach the swap device."), |s| s.extent_unreclaimable_bytes);
        gauge(registry, metric!(name: "mz_column_pool_extent_pageouts_total", help: "Extents pushed to the swap device by RSS-target enforcement."), |s| s.extent_pageouts);
    });
}

/// Registers one computed gauge over a [`mz_ore::pool::PoolStats`] field.
/// Peeks at the process-wide pool at scrape time. Reports zero until something
/// initializes the pool, or if its reservation failed.
fn gauge(
    registry: &MetricsRegistry,
    opts: MakeCollectorOpts,
    field: fn(&mz_ore::pool::PoolStats) -> u64,
) {
    let _gauge: ComputedUIntGauge = registry.register_computed_gauge(opts, move || {
        crate::pool_config::global_pool_peek()
            .map(|pool| field(&pool.stats()))
            .unwrap_or(0)
    });
}
