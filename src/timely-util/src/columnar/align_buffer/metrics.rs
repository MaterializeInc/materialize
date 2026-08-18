// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lifetime and footprint instrumentation for
//! [`AlignBuffer`](super::AlignBuffer), keyed by [`Origin`].
//!
//! Recording answers two questions per origin: how long a serialized body
//! stays alive, and how many bytes of them are alive at once.
//!
//! Both are read per origin because the answer differs by producer. Bodies on a
//! dataflow edge ([`Origin::Ship`], [`Origin::Consolidate`]) are the ones the
//! question is about: they are owned by whoever holds the container and belong
//! to no budget. The other origins are recorded so they can be told apart from
//! the edges, not because they share their behavior. [`Origin::Correction`]
//! and [`Origin::Pager`] bodies are retained on purpose, and [`Origin::Fetch`]
//! and [`Origin::Decode`] bodies come from a read rather than a producer.
//!
//! Recording is off until [`set_tracking_enabled`], because it costs an
//! [`Instant::now`] and a handful of atomics per buffer, on a path that mints
//! one buffer per shipped chunk. Consult
//! [`mz_column_align_buffer_tracking_enabled`](register) before reading a zero
//! as an absence of traffic.
//!
//! The gate may flip while buffers are in flight. A buffer minted while
//! recording was off carries no charge and stays uncounted for its whole life,
//! so the in-flight gauges never credit back bytes they were never charged. The
//! consequence is that the gauges undercount until every buffer that predates
//! the flip has died.
//!
//! Tests live in `tests/align_buffer_metrics.rs`: the gate and the registration
//! are process-global, so anything asserting on them needs a binary where it is
//! the only test.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metric;
use mz_ore::metrics::raw::HistogramVec;
use mz_ore::metrics::{ComputedUIntGauge, Histogram, MakeCollectorOpts, MetricsRegistry};
use mz_ore::stats::histogram_seconds_buckets;

use super::Origin;

/// Whether buffers record their life. Read once per mint.
static TRACKING: AtomicBool = AtomicBool::new(false);

/// Per-origin counters, indexed by `Origin::index`.
static COUNTERS: [Counters; Origin::ALL.len()] = [
    Counters::new(),
    Counters::new(),
    Counters::new(),
    Counters::new(),
    Counters::new(),
    Counters::new(),
];

/// The per-origin histogram handles, resolved once by [`register`]. Absent
/// until then, which is the normal state in tests and benches, where the
/// counters still work and only the distributions are skipped.
static HISTOGRAMS: OnceLock<Histograms> = OnceLock::new();

/// Size classes to grade buffers against, in bytes. Powers of two spanning the
/// buffer pool's size classes (64 KiB to 8 MiB) with a rung either side, so a
/// distribution that piles up just under a class boundary is visible as such.
const SIZE_BUCKETS: [f64; 13] = [
    4096.0,
    8192.0,
    16384.0,
    32768.0,
    65536.0,
    131_072.0,
    262_144.0,
    524_288.0,
    1_048_576.0,
    2_097_152.0,
    4_194_304.0,
    8_388_608.0,
    16_777_216.0,
];

/// One origin's counters. Levels and monotonic totals both live here; the
/// `_total` suffix on a metric name, not its type, marks it monotonic.
struct Counters {
    mints: AtomicU64,
    drops: AtomicU64,
    bytes_minted: AtomicU64,
    inflight_bytes: AtomicU64,
    inflight_count: AtomicU64,
    inflight_bytes_peak: AtomicU64,
    lifetime_max_nanos: AtomicU64,
}

impl Counters {
    const fn new() -> Counters {
        Counters {
            mints: AtomicU64::new(0),
            drops: AtomicU64::new(0),
            bytes_minted: AtomicU64::new(0),
            inflight_bytes: AtomicU64::new(0),
            inflight_count: AtomicU64::new(0),
            inflight_bytes_peak: AtomicU64::new(0),
            lifetime_max_nanos: AtomicU64::new(0),
        }
    }
}

/// Pre-resolved per-origin histograms, so recording a sample is an array index
/// rather than a label lookup under the vec's lock.
struct Histograms {
    lifetime: [Histogram; Origin::ALL.len()],
    size: [Histogram; Origin::ALL.len()],
}

/// What a tracked buffer carries for the duration of its life. The byte count
/// is the one charged at mint, so the credit at drop matches it exactly even
/// if the gate flipped in between.
pub(super) struct Charge {
    minted: Instant,
    bytes: u64,
}

fn counters(origin: Origin) -> &'static Counters {
    &COUNTERS[origin.index()]
}

/// Turns recording on or off for this process. Takes effect for buffers minted
/// after the call; buffers already in flight keep the tracking state they were
/// minted with.
pub fn set_tracking_enabled(enabled: bool) {
    TRACKING.store(enabled, Ordering::Relaxed);
}

/// Whether recording is on.
pub fn tracking_enabled() -> bool {
    TRACKING.load(Ordering::Relaxed)
}

/// Charges a newly minted buffer of `capacity_words` words, returning what it
/// must carry to be credited back at drop, or `None` when recording is off.
pub(super) fn record_mint(origin: Origin, capacity_words: usize) -> Option<Charge> {
    if !TRACKING.load(Ordering::Relaxed) {
        return None;
    }
    let bytes = u64::cast_from(capacity_words) * 8;
    let counters = counters(origin);
    counters.mints.fetch_add(1, Ordering::Relaxed);
    counters.bytes_minted.fetch_add(bytes, Ordering::Relaxed);
    counters.inflight_count.fetch_add(1, Ordering::Relaxed);
    // `fetch_add` returns the previous value, so the level after this mint is
    // the sum. Two concurrent mints can each miss the other's contribution to
    // the peak, which understates it by at most one buffer per race.
    let inflight = counters.inflight_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
    counters
        .inflight_bytes_peak
        .fetch_max(inflight, Ordering::Relaxed);
    if let Some(histograms) = HISTOGRAMS.get() {
        histograms.size[origin.index()].observe(f64::cast_lossy(bytes));
    }
    Some(Charge {
        minted: Instant::now(),
        bytes,
    })
}

/// Credits back a buffer whose life just ended, and records how long it lasted.
pub(super) fn record_drop(origin: Origin, charge: Charge) {
    let elapsed = charge.minted.elapsed();
    let counters = counters(origin);
    counters.drops.fetch_add(1, Ordering::Relaxed);
    counters
        .inflight_bytes
        .fetch_sub(charge.bytes, Ordering::Relaxed);
    counters.inflight_count.fetch_sub(1, Ordering::Relaxed);
    // Histogram buckets are capped, and the top of the distribution is the
    // whole question here, so keep an uncapped high-water mark beside them.
    // This is not hypothetical: a 1.9x-oversubscribed replica put 1.8% of its
    // bodies past the top bucket, and only this gauge recovered the 142s peak.
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    counters
        .lifetime_max_nanos
        .fetch_max(nanos, Ordering::Relaxed);
    if let Some(histograms) = HISTOGRAMS.get() {
        histograms.lifetime[origin.index()].observe(elapsed.as_secs_f64());
    }
}

/// Installs the align-buffer metrics into `registry`. Idempotent; repeated
/// calls after the first are no-ops.
pub fn register(registry: &MetricsRegistry) {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    REGISTERED.get_or_init(|| {
        // Every name and help string is a literal at the `metric!` call so the
        // metrics-catalog scanner (`bin/gen-metrics-catalog`), which reads the
        // source rather than the expanded macro, can index them.
        let lifetime: HistogramVec = registry.register(metric!(
            name: "mz_column_align_buffer_lifetime_seconds",
            help: "Time from minting a serialized column body to dropping it.",
            var_labels: ["origin"],
            // Ceiling from measurement, not taste: on a replica whose working
            // set was ~1.9x its RAM, 1.8% of bodies outlived 64s and the
            // longest reached 142s, which censored p99 into `+Inf` and made it
            // unrecoverable from the buckets. 1024s leaves room for a deeper
            // pressure regime to still resolve its tail.
            buckets: histogram_seconds_buckets(0.000_008, 1024.0),
        ));
        let size: HistogramVec = registry.register(metric!(
            name: "mz_column_align_buffer_size_bytes",
            help: "Allocation size of serialized column bodies at mint.",
            var_labels: ["origin"],
            buckets: SIZE_BUCKETS.to_vec(),
        ));
        let histograms = Histograms {
            lifetime: Origin::ALL.map(|origin| lifetime.with_label_values(&[origin.label()])),
            size: Origin::ALL.map(|origin| size.with_label_values(&[origin.label()])),
        };
        // Only `register` writes this, under `get_or_init`, so it cannot lose.
        let _ = HISTOGRAMS.set(histograms);

        let _tracking: ComputedUIntGauge = registry.register_computed_gauge(
            metric!(
                name: "mz_column_align_buffer_tracking_enabled",
                help: "Whether align-buffer lifetime recording is on. Every other align-buffer metric is frozen while this is zero.",
            ),
            || u64::from(tracking_enabled()),
        );

        for origin in Origin::ALL {
            let label = origin.label();
            gauge(registry, metric!(name: "mz_column_align_buffer_mints_total", help: "Serialized column bodies minted.", const_labels: {"origin" => label}), origin, |c| c.mints.load(Ordering::Relaxed));
            gauge(registry, metric!(name: "mz_column_align_buffer_drops_total", help: "Serialized column bodies dropped.", const_labels: {"origin" => label}), origin, |c| c.drops.load(Ordering::Relaxed));
            gauge(registry, metric!(name: "mz_column_align_buffer_bytes_minted_total", help: "Allocation bytes of serialized column bodies minted.", const_labels: {"origin" => label}), origin, |c| c.bytes_minted.load(Ordering::Relaxed));
            gauge(registry, metric!(name: "mz_column_align_buffer_inflight_bytes", help: "Allocation bytes of serialized column bodies currently alive.", const_labels: {"origin" => label}), origin, |c| c.inflight_bytes.load(Ordering::Relaxed));
            gauge(registry, metric!(name: "mz_column_align_buffer_inflight_count", help: "Serialized column bodies currently alive.", const_labels: {"origin" => label}), origin, |c| c.inflight_count.load(Ordering::Relaxed));
            gauge(registry, metric!(name: "mz_column_align_buffer_inflight_bytes_peak", help: "High-water mark of allocation bytes of serialized column bodies alive at once.", const_labels: {"origin" => label}), origin, |c| c.inflight_bytes_peak.load(Ordering::Relaxed));
            gauge(registry, metric!(name: "mz_column_align_buffer_lifetime_max_nanoseconds", help: "High-water mark of a serialized column body's life, uncapped by histogram buckets.", const_labels: {"origin" => label}), origin, |c| c.lifetime_max_nanos.load(Ordering::Relaxed));
        }
    });
}

/// Registers one computed gauge over one origin's counters, read at scrape
/// time.
fn gauge(
    registry: &MetricsRegistry,
    opts: MakeCollectorOpts,
    origin: Origin,
    field: fn(&Counters) -> u64,
) {
    let _gauge: ComputedUIntGauge =
        registry.register_computed_gauge(opts, move || field(counters(origin)));
}
