// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end check that align-buffer recording reaches Prometheus.
//!
//! Its own test binary because registration and the recording gate are
//! process-global: `register` installs into exactly one registry per process,
//! and a sibling test toggling the gate would race this one. Everything here
//! runs in one test function for the same reason.

use mz_ore::cast::CastLossy;
use mz_ore::metrics::MetricsRegistry;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::align_buffer::{AlignBuffer, Origin, metrics};
use mz_timely_util::columnar::builder::ColumnBuilder;
use prometheus::proto::MetricFamily;
use timely::container::{ContainerBuilder, PushInto};

/// Records to push through the builder. Each `(u64, u64)` serializes to 16
/// bytes and the ship threshold is 2 MiB, so this crosses it a few times over
/// and the builder is guaranteed to mint buffers rather than hold one partial
/// chunk.
const RECORDS: u64 = 400_000;

/// The one metric sample matching `name` and `origin`, or `None` if the series
/// is absent.
fn sample(families: &[MetricFamily], name: &str, origin: &str) -> Option<f64> {
    let family = families.iter().find(|f| f.name() == name)?;
    let metric = family.get_metric().iter().find(|m| {
        m.get_label()
            .iter()
            .any(|l| l.name() == "origin" && l.value() == origin)
    })?;
    Some(metric.get_gauge().value())
}

/// Observation count of the histogram series matching `name` and `origin`.
fn histogram_count(families: &[MetricFamily], name: &str, origin: &str) -> Option<u64> {
    let family = families.iter().find(|f| f.name() == name)?;
    let metric = family.get_metric().iter().find(|m| {
        m.get_label()
            .iter()
            .any(|l| l.name() == "origin" && l.value() == origin)
    })?;
    Some(metric.get_histogram().get_sample_count())
}

/// Value of an unlabeled gauge series.
fn plain_gauge(families: &[MetricFamily], name: &str) -> Option<f64> {
    let family = families.iter().find(|f| f.name() == name)?;
    Some(family.get_metric().first()?.get_gauge().value())
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn ship_buffers_reach_prometheus() {
    let registry = MetricsRegistry::new();
    metrics::register(&registry);

    // With the gate off, shipping must leave every series alone. This also
    // pins down that a zero reading means "not recording", not "no traffic".
    assert!(!metrics::tracking_enabled());
    let untracked = ship(RECORDS);
    assert!(untracked > 0, "builder shipped nothing to measure");
    let families = registry.gather();
    assert_eq!(
        plain_gauge(&families, "mz_column_align_buffer_tracking_enabled"),
        Some(0.0)
    );
    assert_eq!(
        sample(&families, "mz_column_align_buffer_mints_total", "ship"),
        Some(0.0),
        "recording is off, so nothing may be counted",
    );

    metrics::set_tracking_enabled(true);
    let shipped = ship(RECORDS);
    let families = registry.gather();

    assert_eq!(
        plain_gauge(&families, "mz_column_align_buffer_tracking_enabled"),
        Some(1.0),
    );

    let mints =
        sample(&families, "mz_column_align_buffer_mints_total", "ship").expect("ship mints series");
    let drops =
        sample(&families, "mz_column_align_buffer_drops_total", "ship").expect("ship drops series");
    assert_eq!(
        mints,
        f64::cast_lossy(shipped),
        "every shipped chunk mints exactly one buffer",
    );
    assert_eq!(mints, drops, "every buffer was dropped by end of scope");

    // Everything minted has been dropped, so the level is back to zero while
    // the high-water mark and the byte total retain what passed through.
    assert_eq!(
        sample(&families, "mz_column_align_buffer_inflight_bytes", "ship"),
        Some(0.0),
    );
    assert_eq!(
        sample(&families, "mz_column_align_buffer_inflight_count", "ship"),
        Some(0.0),
    );
    let peak = sample(
        &families,
        "mz_column_align_buffer_inflight_bytes_peak",
        "ship",
    )
    .expect("ship peak series");
    assert!(peak > 0.0, "peak in-flight bytes stayed at zero");
    let bytes = sample(
        &families,
        "mz_column_align_buffer_bytes_minted_total",
        "ship",
    )
    .expect("ship bytes series");
    assert!(bytes >= peak, "bytes minted {bytes} below peak {peak}");
    assert!(
        sample(
            &families,
            "mz_column_align_buffer_lifetime_max_nanoseconds",
            "ship"
        )
        .expect("ship lifetime max series")
            > 0.0,
    );

    // Both distributions observed once per buffer: sizes at mint, lifetimes at
    // drop.
    assert_eq!(
        histogram_count(&families, "mz_column_align_buffer_size_bytes", "ship"),
        Some(shipped),
    );
    assert_eq!(
        histogram_count(&families, "mz_column_align_buffer_lifetime_seconds", "ship"),
        Some(shipped),
    );

    // Origins with no traffic report zero rather than going missing, so a
    // dashboard panel does not show a gap for an idle producer.
    assert_eq!(
        sample(&families, "mz_column_align_buffer_mints_total", "decode"),
        Some(0.0),
    );

    // A buffer minted with the gate off must stay uncounted even if the gate
    // flips on before it dies. Crediting back a charge that was never made
    // would underflow the unsigned in-flight gauge.
    metrics::set_tracking_enabled(false);
    let untracked = AlignBuffer::from_words(Origin::Decode, vec![0u64; 1024]);
    metrics::set_tracking_enabled(true);
    drop(untracked);
    let families = registry.gather();
    assert_eq!(
        sample(&families, "mz_column_align_buffer_drops_total", "decode"),
        Some(0.0),
        "a buffer minted with the gate off must not be counted at drop",
    );
    assert_eq!(
        sample(&families, "mz_column_align_buffer_inflight_bytes", "decode"),
        Some(0.0),
    );

    metrics::set_tracking_enabled(false);
}

/// Pushes `records` records through a [`ColumnBuilder`], draining every chunk
/// it mints, and returns how many chunks it shipped. Each chunk is dropped
/// before the next is extracted, so all of them are dropped by the time this
/// returns.
fn ship(records: u64) -> u64 {
    let mut builder = ColumnBuilder::<(u64, u64)>::default();
    let mut shipped = 0;
    for i in 0..records {
        builder.push_into((i, i));
        while let Some(container) = builder.extract() {
            assert!(
                matches!(container, Column::Align(_)),
                "a shipped chunk is a serialized body",
            );
            shipped += 1;
        }
    }
    // `finish` hands back the trailing partial chunk as `Typed`, which is not
    // an align buffer; drain it so the builder holds nothing at drop.
    while builder.finish().is_some() {}
    shipped
}
