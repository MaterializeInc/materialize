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

//! Prometheus metrics for the column pager.
//!
//! One process-wide [`PagerMetrics`] singleton, installed by compute init via
//! [`register`]. Counter observers (`observe_*`) are no-ops until that call
//! lands; the lazy initialization keeps tests and benches that don't wire a
//! [`MetricsRegistry`] free of bookkeeping.

use std::sync::OnceLock;

use mz_ore::metric;
use mz_ore::metrics::{ComputedUIntGauge, IntCounter, MetricsRegistry};

use crate::column_pager::policy::TieredPolicy;

/// Process-wide pager metrics. Counters track cumulative observations since
/// process start; gauges read the live policy atomics at scrape time.
#[derive(Debug)]
pub struct PagerMetrics {
    /// Number of decisions that kept the chunk resident.
    pub skip_decisions_total: IntCounter,
    /// Total bytes kept resident by skip decisions.
    pub skip_bytes_total: IntCounter,
    /// Number of decisions that paged the chunk out.
    pub pageouts_total: IntCounter,
    /// Uncompressed body bytes handed to the pager for pageout.
    pub paged_bytes_in_total: IntCounter,
    /// On-storage payload bytes after codec / padding.
    pub paged_bytes_out_total: IntCounter,
    /// Number of page-ins from `ColumnPager::take`.
    pub pageins_total: IntCounter,
    /// Total uncompressed bytes delivered by page-in.
    pub pagein_bytes_total: IntCounter,
    /// Resident-ticket drops returning bytes to the budget.
    pub resident_released_total: IntCounter,
    /// Total bytes returned to the budget by ticket drops.
    pub resident_released_bytes_total: IntCounter,
    // Computed gauges are registered with the registry but not held here —
    // their collectors are owned by the prometheus registry.
}

static METRICS: OnceLock<PagerMetrics> = OnceLock::new();

/// Install the pager metrics into `registry`. Idempotent — repeated calls
/// after the first one are no-ops. Computed gauges read the singleton
/// [`TieredPolicy`] atomics at scrape time; their values reflect the live
/// policy whether or not the column-paged batcher is currently enabled.
pub fn register(registry: &MetricsRegistry) {
    let policy: &'static TieredPolicy = crate::column_pager::tiered_policy();
    let _ = METRICS.get_or_init(|| {
        // Computed gauges: closures hold the &'static policy reference.
        let _budget_remaining: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(
                name: "mz_column_pager_budget_remaining_bytes",
                help: "Bytes the column-pager tiered policy currently has \
                       available for resident columns.",
            ),
            move || u64::try_from(policy.budget_remaining()).unwrap_or(u64::MAX),
        );
        let _budget_configured: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(
                name: "mz_column_pager_budget_configured_bytes",
                help: "Most-recently-configured total budget for the \
                       column-pager tiered policy.",
            ),
            move || u64::try_from(policy.configured_total()).unwrap_or(u64::MAX),
        );

        // Buffer-pool gauges peek at the process-wide pool's stats at scrape
        // time, reporting zero until something else initializes the pool —
        // a scrape must observe, not mmap an 8 TiB reservation into every
        // process that happens to be monitored. The cumulative fields are
        // exposed as computed gauges rather than counters because the pool
        // owns the atomics; all are monotonic except resident/oversize bytes.
        register_pool_gauge(registry, "resident_bytes", "Uncompressed bytes resident in the buffer pool.", |s| s.resident_bytes);
        register_pool_gauge(registry, "oversize_bytes", "Bytes held by oversize chunks that bypass pool paging.", |s| s.oversize_bytes);
        register_pool_gauge(registry, "inserts_total", "Chunks inserted into the buffer pool.", |s| s.inserts);
        register_pool_gauge(registry, "frees_total", "Chunks freed from the buffer pool.", |s| s.frees);
        register_pool_gauge(registry, "writes_elided_total", "Backing writes elided: chunks freed while unbacked, dead before any compression or extent write happened.", |s| s.writes_elided);
        register_pool_gauge(registry, "evictions_compress_total", "Evictions that compressed a chunk into a new swap-backed extent.", |s| s.evictions_compress);
        register_pool_gauge(registry, "evictions_cheap_total", "Evictions of already-backed chunks: physical pages released with no compression or extent write.", |s| s.evictions_cheap);
        register_pool_gauge(registry, "faults_total", "Fault-ins decompressing a chunk from its extent back into its pool slot.", |s| s.faults);
        register_pool_gauge(registry, "extent_bytes_written_total", "Compressed bytes written into swap-backed extents.", |s| s.extent_bytes_written);
        register_pool_gauge(registry, "spill_scheduled_total", "Evictions handed to buffer-pool spill threads.", |s| s.spill_scheduled);
        register_pool_gauge(registry, "spill_cancelled_total", "Scheduled evictions cancelled before completing (chunk freed or pinned).", |s| s.spill_cancelled);
        register_pool_gauge(registry, "spill_in_flight", "Spill entries queued or being processed.", |s| s.spill_in_flight);
        register_pool_gauge(registry, "slot_exhausted_fallbacks_total", "Inserts that fell back to unpageable heap chunks because their size class had no free slot.", |s| s.slot_exhausted_fallbacks);
        register_pool_gauge(registry, "live_chunks", "Live pool chunks, whatever their residency: for backlog-shaped consumers, the un-drained backlog in chunks.", |s| s.live_chunks);

        PagerMetrics {
            skip_decisions_total: registry.register(metric!(
                name: "mz_column_pager_skip_decisions_total",
                help: "Pager decisions that kept the chunk resident.",
            )),
            skip_bytes_total: registry.register(metric!(
                name: "mz_column_pager_skip_bytes_total",
                help: "Total bytes kept resident by skip decisions.",
            )),
            pageouts_total: registry.register(metric!(
                name: "mz_column_pager_pageouts_total",
                help: "Pager decisions that paged the chunk out.",
            )),
            paged_bytes_in_total: registry.register(metric!(
                name: "mz_column_pager_paged_bytes_in_total",
                help: "Total uncompressed bytes handed to the pager for \
                       pageout, before any codec is applied.",
            )),
            paged_bytes_out_total: registry.register(metric!(
                name: "mz_column_pager_paged_bytes_out_total",
                help: "Total on-storage bytes after codec / padding.",
            )),
            pageins_total: registry.register(metric!(
                name: "mz_column_pager_pageins_total",
                help: "Successful page-ins from `ColumnPager::take`.",
            )),
            pagein_bytes_total: registry.register(metric!(
                name: "mz_column_pager_pagein_bytes_total",
                help: "Total uncompressed bytes delivered by page-in.",
            )),
            resident_released_total: registry.register(metric!(
                name: "mz_column_pager_resident_released_total",
                help: "Resident-ticket drops returning budget.",
            )),
            resident_released_bytes_total: registry.register(metric!(
                name: "mz_column_pager_resident_released_bytes_total",
                help: "Total bytes returned to the budget by ticket drops.",
            )),
        }
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
            crate::column_pager::global_pool_peek()
                .map(|pool| field(&pool.stats()))
                .unwrap_or(0)
        },
    );
}

#[inline]
fn metrics() -> Option<&'static PagerMetrics> {
    METRICS.get()
}

pub(crate) fn observe_skip(bytes: usize) {
    if let Some(m) = metrics() {
        m.skip_decisions_total.inc();
        m.skip_bytes_total.inc_by(bytes_to_u64(bytes));
    }
}

pub(crate) fn observe_pageout(bytes_in: usize, bytes_out: usize) {
    if let Some(m) = metrics() {
        m.pageouts_total.inc();
        m.paged_bytes_in_total.inc_by(bytes_to_u64(bytes_in));
        m.paged_bytes_out_total.inc_by(bytes_to_u64(bytes_out));
    }
}

pub(crate) fn observe_pagein(bytes: usize) {
    if let Some(m) = metrics() {
        m.pageins_total.inc();
        m.pagein_bytes_total.inc_by(bytes_to_u64(bytes));
    }
}

pub(crate) fn observe_resident_released(bytes: usize) {
    if let Some(m) = metrics() {
        m.resident_released_total.inc();
        m.resident_released_bytes_total.inc_by(bytes_to_u64(bytes));
    }
}

fn bytes_to_u64(b: usize) -> u64 {
    u64::try_from(b).unwrap_or(u64::MAX)
}
