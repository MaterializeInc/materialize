// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Process-wide work counters for columnar batching and trace maintenance.
//!
//! Counts describe work attempted, including repeated processing of the same
//! updates. Bytes are serialized uncompressed sizes, not extent or device writes.
//! For optional exertion and virtual introductions, rows count fuel units and
//! virtual updates respectively. Zero byte counts indicate an uninstrumented size.
//! Row-size buckets are disjoint, indexed by ceil(log2(rows)), with zero and one
//! sharing bucket zero. Counters at different stages must not be added together.

use std::sync::atomic::{AtomicU64, Ordering};

use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{ComputedUIntGauge, MetricsRegistry};

#[derive(Clone, Copy)]
pub(super) enum Stage {
    Merge,
    Advance,
    Commit,
    Batch,
    InitialSettle,
    AsyncInput,
    OptionalExert,
    VirtualIntroduction,
    TraceMerge,
}

impl Stage {
    #[allow(clippy::as_conversions)]
    fn counters(self) -> &'static Counters {
        &COUNTERS[self as usize]
    }

    const ALL: [(Self, &'static str); 9] = [
        (Self::Merge, "merge"),
        (Self::Advance, "advance"),
        (Self::Commit, "commit"),
        (Self::Batch, "batch"),
        (Self::InitialSettle, "initial_settle"),
        (Self::AsyncInput, "async_input"),
        (Self::OptionalExert, "optional_exert"),
        (Self::VirtualIntroduction, "virtual_introduction"),
        (Self::TraceMerge, "trace_merge"),
    ];
}

struct Counters {
    calls: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
    buckets: [AtomicU64; 32],
}

static COUNTERS: [Counters; 9] = [const {
    Counters {
        calls: AtomicU64::new(0),
        rows: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        buckets: [const { AtomicU64::new(0) }; 32],
    }
}; 9];

pub(super) fn record(stage: Stage, rows: usize, bytes: usize) {
    let counters = stage.counters();
    counters.calls.fetch_add(1, Ordering::Relaxed);
    counters
        .rows
        .fetch_add(u64::cast_from(rows), Ordering::Relaxed);
    counters
        .bytes
        .fetch_add(u64::cast_from(bytes), Ordering::Relaxed);
    let bucket = (usize::BITS - rows.saturating_sub(1).leading_zeros()).min(31);
    counters.buckets[usize::cast_from(bucket)].fetch_add(1, Ordering::Relaxed);
}

/// Record one published batch when its consumer receives it, including empty batches.
pub fn record_batch(rows: usize) {
    record(Stage::Batch, rows, 0);
}

pub(crate) fn register(registry: &MetricsRegistry) {
    for (stage, name) in Stage::ALL {
        let counters = stage.counters();
        let _: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(name: "mz_column_chunk_work_calls_total", help: "Columnar work operations, by stage.", const_labels: {"stage" => name}),
            move || counters.calls.load(Ordering::Relaxed),
        );
        let _: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(name: "mz_column_chunk_work_rows_total", help: "Rows processed by columnar work, including repeated visits.", const_labels: {"stage" => name}),
            move || counters.rows.load(Ordering::Relaxed),
        );
        let _: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(name: "mz_column_chunk_work_bytes_total", help: "Uncompressed serialized bytes processed at instrumented columnar stages.", const_labels: {"stage" => name}),
            move || counters.bytes.load(Ordering::Relaxed),
        );
        for (bucket, count) in counters.buckets.iter().enumerate() {
            let _: ComputedUIntGauge = registry.register_computed_gauge(
                metric!(name: "mz_column_chunk_work_size_total", help: "Disjoint columnar operation row-size buckets, indexed by ceil(log2(rows)).", const_labels: {"stage" => name, "log2_rows" => bucket.to_string()}),
                move || count.load(Ordering::Relaxed),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn all_stages_register_and_scrape() {
        let registry = MetricsRegistry::new();
        register(&registry);
        record_batch(3);
        let families = registry.gather();
        assert_eq!(families.len(), 4);
        assert_eq!(
            families
                .iter()
                .map(|family| family.get_metric().len())
                .sum::<usize>(),
            9 * 35
        );
    }
}
