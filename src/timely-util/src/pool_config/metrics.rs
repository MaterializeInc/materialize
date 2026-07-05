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
use mz_ore::metrics::{ComputedUIntGauge, MetricsRegistry};

/// Install the buffer-pool metrics into `registry`. Idempotent — repeated
/// calls after the first one are no-ops.
pub fn register(registry: &MetricsRegistry) {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    REGISTERED.get_or_init(|| {
        // The cumulative fields are exposed as computed gauges rather than
        // counters because the pool owns the atomics; all are monotonic
        // except resident/oversize bytes.
        register_pool_gauge(registry, "resident_bytes", "Uncompressed bytes resident in the buffer pool.", |s| s.resident_bytes);
        register_pool_gauge(registry, "oversize_bytes", "Bytes held by oversize chunks that bypass pool paging.", |s| s.oversize_bytes);
        register_pool_gauge(registry, "inserts_total", "Chunks inserted into the buffer pool.", |s| s.inserts);
        register_pool_gauge(registry, "frees_total", "Chunks freed from the buffer pool.", |s| s.frees);
        register_pool_gauge(registry, "writes_elided_total", "Backing writes elided: chunks freed while unbacked, dead before any compression or extent write happened.", |s| s.writes_elided);
        register_pool_gauge(registry, "evictions_compress_total", "Evictions that compressed a chunk into a new swap-backed extent.", |s| s.evictions_compress);
        register_pool_gauge(registry, "evictions_cheap_total", "Evictions of already-backed chunks: physical pages released with no compression or extent write.", |s| s.evictions_cheap);
        register_pool_gauge(registry, "extent_bytes_written_total", "Compressed bytes written into swap-backed extents.", |s| s.extent_bytes_written);
        register_pool_gauge(registry, "spill_scheduled_total", "Evictions handed to buffer-pool spill threads.", |s| s.spill_scheduled);
        register_pool_gauge(registry, "spill_cancelled_total", "Scheduled evictions cancelled because the chunk was freed with the write in flight.", |s| s.spill_cancelled);
        register_pool_gauge(registry, "spill_in_flight", "Spill entries queued or being processed.", |s| s.spill_in_flight);
        register_pool_gauge(registry, "slot_exhausted_fallbacks_total", "Inserts that fell back to unpageable heap chunks because their size class had no free slot.", |s| s.slot_exhausted_fallbacks);
        register_pool_gauge(registry, "live_chunks", "Live pool chunks, whatever their residency: for backlog-shaped consumers, the un-drained backlog in chunks.", |s| s.live_chunks);
        register_pool_gauge(registry, "warm_bytes", "Class bytes of free slots kept warm for fault-free reuse; RSS exceeds resident bytes by up to this bounded amount.", |s| s.warm_bytes);
        register_pool_gauge(registry, "warm_reuses_total", "Slot allocations served from the warm list: no page faults, no kernel page zeroing.", |s| s.warm_reuses);
        register_pool_gauge(registry, "eager_backs_total", "Chunks eagerly compressed to compressed-but-resident by idle spill threads; their later eviction is a pure page release.", |s| s.eager_backs);
        register_pool_gauge(registry, "extent_resident_bytes", "Allocation bytes of compressed extents currently resident (the compressed-but-resident tier), bounded by the pool RSS target.", |s| s.extent_resident_bytes);
        register_pool_gauge(registry, "extent_pageouts_total", "Extents pushed to the swap device by RSS-target enforcement.", |s| s.extent_pageouts);
    });
}

/// Registers one computed gauge over a [`mz_ore::pool::PoolStats`] field,
/// named `mz_column_pool_{suffix}`. Peeks at the process-wide pool at scrape
/// time; zero until something initializes the pool (or if its reservation
/// failed).
fn register_pool_gauge(
    registry: &MetricsRegistry,
    suffix: &str,
    help: &str,
    field: fn(&mz_ore::pool::PoolStats) -> u64,
) {
    let _gauge: ComputedUIntGauge = registry.register_computed_gauge(
        metric!(
            name: format!("mz_column_pool_{suffix}"),
            help: help,
        ),
        move || {
            crate::pool_config::global_pool_peek()
                .map(|pool| field(&pool.stats()))
                .unwrap_or(0)
        },
    );
}
