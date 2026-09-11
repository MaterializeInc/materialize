// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Process-wide work counters for columnar chunk maintenance.
//!
//! Counts describe work attempted, including repeated processing of the same
//! updates. Bytes are serialized uncompressed sizes, not extent or device
//! writes, and only the commit stage records them. Row-size buckets are
//! disjoint, indexed by ceil(log2(rows)) with zero and one sharing bucket zero
//! and the last bucket absorbing everything larger. Counters at different
//! stages must not be added together.

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
}

impl Stage {
    const ALL: [(Self, &'static str); 4] = [
        (Self::Merge, "merge"),
        (Self::Advance, "advance"),
        (Self::Commit, "commit"),
        (Self::Batch, "batch"),
    ];

    #[allow(clippy::as_conversions)]
    fn counters(self) -> &'static Counters {
        &COUNTERS[self as usize]
    }
}

const BUCKETS: usize = 32;

struct Counters {
    calls: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

static COUNTERS: [Counters; Stage::ALL.len()] = [const {
    Counters {
        calls: AtomicU64::new(0),
        rows: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        buckets: [const { AtomicU64::new(0) }; BUCKETS],
    }
}; Stage::ALL.len()];

fn bucket(rows: usize) -> usize {
    let bits = usize::BITS - rows.saturating_sub(1).leading_zeros();
    usize::cast_from(bits).min(BUCKETS - 1)
}

pub(super) fn record(stage: Stage, rows: usize, bytes: usize) {
    let counters = stage.counters();
    counters.calls.fetch_add(1, Ordering::Relaxed);
    counters
        .rows
        .fetch_add(u64::cast_from(rows), Ordering::Relaxed);
    if bytes > 0 {
        counters
            .bytes
            .fetch_add(u64::cast_from(bytes), Ordering::Relaxed);
    }
    counters.buckets[bucket(rows)].fetch_add(1, Ordering::Relaxed);
}

/// Record one batch an upsert feedback arrangement published, including
/// empty batches.
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
                metric!(name: "mz_column_chunk_work_size_total", help: "Disjoint columnar operation row-size buckets, indexed by ceil(log2(rows)), the last bucket unbounded.", const_labels: {"stage" => name, "log2_rows" => bucket.to_string()}),
                move || count.load(Ordering::Relaxed),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn buckets_are_ceil_log2() {
        let cases = [
            (0, 0),
            (1, 0),
            (2, 1),
            (3, 2),
            (4, 2),
            (5, 3),
            (1 << 30, 30),
            ((1 << 30) + 1, 31),
            (usize::MAX, 31),
        ];
        for (rows, expected) in cases {
            assert_eq!(bucket(rows), expected, "bucket for {rows} rows");
        }
    }

    #[mz_ore::test]
    fn all_stages_register_and_scrape() {
        let registry = MetricsRegistry::new();
        register(&registry);
        let families = registry.gather();
        assert_eq!(families.len(), 4);
        assert_eq!(
            families
                .iter()
                .map(|family| family.get_metric().len())
                .sum::<usize>(),
            Stage::ALL.len() * (3 + BUCKETS)
        );
    }
}
