// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_repr::{Diff, Row, Timestamp};

use super::*;
use crate::sink::correction::{CorrectionV1, LoggingEvent};
use crate::sink::correction_workload::{Pattern, make_batches};

#[mz_ore::test]
fn chain_builder_update_count_matches_items() {
    let mut builder = ChainBuilder::<i64>::default();
    for i in 0..10_u64 {
        let d = i64::try_from(i).expect("fits");
        builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
    }
    let chain = builder.finish();
    assert_eq!(chain.update_count, chain.iter().count());
}

/// Push enough updates to cross at least one `mint()` boundary, forcing the
/// `Align` encode -> `from_bytes` decode roundtrip (the spilling path this data
/// structure exists to support), and assert `iter()` roundtrips values, order,
/// and diffs across the spill boundary.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow: crossing the ~2 MiB mint boundary needs ~200k updates
fn chain_builder_roundtrips_across_mint_boundary() {
    // A single `mint()` fires near the ~2 MiB (`SHIP_WORDS`) serialized boundary. With
    // three 8-byte columns per update that's tens of thousands of updates; pushing 200k
    // comfortably forces multiple mints.
    let count = 200_000_u64;

    let mut builder = ChainBuilder::<i64>::default();
    for i in 0..count {
        let d = i64::try_from(i).expect("fits");
        builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
    }
    let chain = builder.finish();

    // Crossing the mint boundary must have produced more than one chunk; otherwise the spill
    // path (each minted chunk is paged out and read back through the pager) wouldn't be
    // exercised. The chunk payload itself is now behind the pager (see [`Chunk`]), so we
    // assert on chunk count rather than inspecting the column variant directly.
    assert!(
        chain.chunks.len() > 1,
        "expected multiple minted chunks, got {} chunk(s): {:?}",
        chain.chunks.len(),
        chain.chunks,
    );

    // `iter()` must roundtrip every update, in order, with correct diffs.
    assert_eq!(chain.update_count, usize::try_from(count).expect("fits"));
    let mut expected = 0_u64;
    for (d, t, r) in chain.iter() {
        assert_eq!(d, i64::try_from(expected).expect("fits"));
        assert_eq!(t, Timestamp::new(expected));
        assert_eq!(r, Diff::ONE);
        expected += 1;
    }
    assert_eq!(expected, count);
}

fn sink_metrics() -> SinkMetrics {
    let registry = MetricsRegistry::new();
    let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
    metrics.sink.clone()
}

/// Run the same stepwise-drain workload through `CorrectionV1` and `CorrectionV2` and assert
/// that they emit the same updates at every step.
///
/// Models the `write_batches` operator catching up through many distinct timestamps: the
/// desired input runs ahead, batches are written one timestamp at a time, and written updates
/// come back negated through the persist feedback.
#[mz_ore::test]
// Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
// provenance of previously stored items under Miri.
#[cfg_attr(miri, ignore)]
fn equivalence_with_v1() {
    let sink_metrics = sink_metrics();

    let mut v1 = CorrectionV1::<String>::new(sink_metrics.clone(), sink_metrics.for_worker(0), 1);
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    let num_ts = 50;
    let keys = 4;

    // Upsert-style input: every timestamp updates each key, retracting the previous value.
    let batch = |t: u64| -> Vec<(String, Timestamp, Diff)> {
        (0..keys)
            .flat_map(|k| {
                let addition = (format!("{k}-{t}"), Timestamp::from(t), Diff::ONE);
                let retraction = t
                    .checked_sub(1)
                    .map(|p| (format!("{k}-{p}"), Timestamp::from(t), -Diff::ONE));
                std::iter::once(addition).chain(retraction)
            })
            .collect()
    };

    // Pre-fill both with all batches, like a catch-up where the input runs ahead.
    for t in 0..num_ts {
        v1.insert(&mut batch(t));
        v2.insert(&mut batch(t));
    }

    // Drain stepwise, with persist feedback, comparing emissions.
    for t in 0..num_ts {
        let upper = Antichain::from_elem(Timestamp::from(t + 1));

        let mut out1: Vec<_> = v1.updates_before(&upper).collect();
        let mut out2: Vec<_> = v2.updates_before(&upper).collect();
        out1.sort();
        out2.sort();
        assert_eq!(out1, out2, "diverged at t={t}");

        v1.insert_negated(&mut out1.clone());
        v2.insert_negated(&mut out2);
        v1.advance_since(upper.clone());
        v2.advance_since(upper);
    }

    // Compare the final state at the since.
    let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
    v1.consolidate_at_since();
    v2.consolidate_at_since();
    let mut out1: Vec<_> = v1.updates_before(&upper).collect();
    let mut out2: Vec<_> = v2.updates_before(&upper).collect();
    out1.sort();
    out2.sort();
    assert_eq!(out1, out2);
}

/// A since jump across many distinct buffered timestamps must collapse them onto the since.
#[mz_ore::test]
// Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
// provenance of previously stored items under Miri.
#[cfg_attr(miri, ignore)]
fn since_jump() {
    let sink_metrics = sink_metrics();
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    let num_ts = 100;
    for t in 0..num_ts {
        v2.insert(&mut vec![
            (format!("a-{t}"), Timestamp::from(t), Diff::ONE),
            (format!("a-{t}"), Timestamp::from(t), -Diff::ONE),
            (format!("b-{t}"), Timestamp::from(t), Diff::ONE),
        ]);
    }

    v2.advance_since(Antichain::from_elem(Timestamp::from(num_ts)));
    v2.consolidate_at_since();

    let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
    let out: Vec<_> = v2.updates_before(&upper).collect();
    assert_eq!(out.len(), usize::try_from(num_ts).unwrap());
    assert!(
        out.iter()
            .all(|(_, t, r)| *t == Timestamp::from(num_ts) && *r == Diff::ONE)
    );
}

/// Reads must not observe updates at or beyond their `upper`, even when the `upper` is not
/// beyond the `since`.
#[mz_ore::test]
// Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
// provenance of previously stored items under Miri.
#[cfg_attr(miri, ignore)]
fn upper_not_beyond_since() {
    let sink_metrics = sink_metrics();
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    v2.insert(&mut vec![(
        "a".to_owned(),
        Timestamp::from(5_u64),
        Diff::ONE,
    )]);
    v2.advance_since(Antichain::from_elem(Timestamp::from(10_u64)));

    // The update logically lives at time 10 now, so a read before 7 must be empty.
    let upper = Antichain::from_elem(Timestamp::from(7_u64));
    assert_eq!(v2.updates_before(&upper).count(), 0);

    // A read before 11 must emit it, advanced to the since.
    let upper = Antichain::from_elem(Timestamp::from(11_u64));
    let out: Vec<_> = v2.updates_before(&upper).collect();
    assert_eq!(
        out,
        vec![("a".to_owned(), Timestamp::from(10_u64), Diff::ONE)]
    );
}

fn default_v2<D: Data>(logging: Option<ChannelLogging>) -> CorrectionV2<D> {
    let sink_metrics = sink_metrics();
    CorrectionV2::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        logging,
        3.0,
        8 * 1024,
    )
}

/// Structural work performed while running `f`: the number of updates copied into newly built
/// chains, as reported through the introspection logging hook.
///
/// Every merge, split, stage flush, and emitted chain reports the chain it produces, so this
/// tracks the cost of maintaining the buffer without depending on wall-clock time. Cursor
/// stepping and time advancement are not reported, but they only ever run over chains that
/// are subsequently rebuilt and thus counted here.
fn chain_work<D: Data>(f: impl FnOnce(ChannelLogging) -> CorrectionV2<D>) -> usize {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let correction = f(ChannelLogging::new(tx));
    drop(correction);

    let mut created = 0;
    while let Ok(event) = rx.try_recv() {
        if let LoggingEvent::ChainCreated(len) = event {
            created += len;
        }
    }
    created
}

/// Fill a buffer with `num_ts` timestamps of `pattern`, mimicking desired input that ran far
/// ahead of persist, then drain it one timestamp at a time with persist feedback, like the
/// `write_batches` operator does while persist writes catch up step by step.
fn drain_stepwise_work(pattern: Pattern, num_ts: u64) -> usize {
    let batches = make_batches(num_ts, pattern);
    chain_work(|logging| {
        let mut correction = default_v2::<Row>(Some(logging));
        for mut batch in batches {
            correction.insert(&mut batch);
        }
        for t in 0..num_ts {
            let upper = Antichain::from_elem(Timestamp::from(t + 1));
            // Written updates come back negated through the persist input. Without this
            // feedback the buffer would legitimately re-emit everything on every step.
            let mut written: Vec<_> = correction.updates_before(&upper).collect();
            correction.insert_negated(&mut written);
            correction.advance_since(upper);
        }
        correction
    })
}

/// Fill a buffer like [`drain_stepwise_work`], then jump the since across all buffered times
/// at once and read everything, like a sink whose persist shard is already far ahead of the
/// dataflow's as-of.
fn advance_jump_work(pattern: Pattern, num_ts: u64) -> usize {
    let batches = make_batches(num_ts, pattern);
    chain_work(|logging| {
        let mut correction = default_v2::<Row>(Some(logging));
        for mut batch in batches {
            correction.insert(&mut batch);
        }
        correction.advance_since(Antichain::from_elem(Timestamp::from(num_ts)));
        correction.consolidate_at_since();
        let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
        let _ = correction.updates_before(&upper).count();
        correction
    })
}

/// Catching up through `T` distinct timestamps must cost work linear in `T`.
///
/// The failure mode is a sink restarting with an old as-of where every drained step re-touches
/// everything buffered so far, which turns the catch-up quadratic. Compare structural work
/// between `T` and `4T` timestamps: linear scaling gives a ratio near 4 (chain merges add a
/// logarithmic factor on top; all routines measure 4.0 to 4.2 at these sizes), quadratic
/// scaling gives 16. The temporal-filter pattern additionally checks that the growing
/// far-future mass stays out of the drained slices.
#[mz_ore::test]
// Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
// provenance of previously stored items under Miri.
#[cfg_attr(miri, ignore)]
fn catch_up_work_is_linear() {
    const SMALL: u64 = 512;
    const LARGE: u64 = 4 * SMALL;
    const MAX_RATIO: f64 = 8.0;

    let routines: [(&str, fn(Pattern, u64) -> usize); 2] = [
        ("drain_stepwise", drain_stepwise_work),
        ("advance_jump", advance_jump_work),
    ];

    for pattern in Pattern::ALL {
        for (routine, work) in routines {
            let small = work(pattern, SMALL);
            let large = work(pattern, LARGE);
            assert!(small > 0, "{routine}/{}: no work recorded", pattern.name());
            let ratio = f64::cast_lossy(large) / f64::cast_lossy(small);
            assert!(
                ratio <= MAX_RATIO,
                "{routine}/{}: work grew {ratio:.1}x for 4x more timestamps \
                 ({small} -> {large} updates copied), catch-up is no longer linear",
                pattern.name(),
            );
        }
    }
}

/// A [`PagingPolicy`] that always spills to the swap backend, uncompressed.
///
/// The default global pager keeps every chunk resident; installing this drives the actual
/// spill path so the tests exercise [`Chunk::column`]'s page-in through [`mz_ore::pager`].
///
/// [`PagingPolicy`]: column_pager::PagingPolicy
struct ForceSwap;

impl column_pager::PagingPolicy for ForceSwap {
    fn decide(&self, _hint: column_pager::PageHint) -> column_pager::PageDecision {
        column_pager::PageDecision::Page {
            backend: mz_ore::pager::Backend::Swap,
            codec: None,
        }
    }
    fn record(&self, _event: column_pager::PageEvent) {}
}

/// Install a global pager that spills every chunk to swap for the duration of `f`, then
/// restore the default (disabled) pager. The global pager is process-wide; concurrent tests
/// only ever observe a correct round-trip regardless of backend, so racing on it is benign.
fn with_swap_pager<R>(f: impl FnOnce() -> R) -> R {
    use std::sync::Arc;
    column_pager::set_global_pager(column_pager::ColumnPager::new(Arc::new(ForceSwap)));
    let result = f();
    column_pager::set_global_pager(column_pager::ColumnPager::disabled());
    result
}

/// Build a chain crossing the mint boundary while every chunk is spilled to swap, then assert
/// `iter()` (the read path behind `updates_before`) pages each chunk back in and roundtrips
/// values, order, and diffs.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // madvise on the swap backend is unsupported under miri
fn iter_roundtrips_through_swap_backend() {
    let count = 200_000_u64;
    with_swap_pager(|| {
        let mut builder = ChainBuilder::<i64>::default();
        for i in 0..count {
            let d = i64::try_from(i).expect("fits");
            builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
        }
        let chain = builder.finish();
        assert!(chain.chunks.len() > 1, "expected multiple minted chunks");
        assert_eq!(chain.update_count, usize::try_from(count).expect("fits"));

        let mut expected = 0_u64;
        for (d, t, r) in chain.iter() {
            assert_eq!(d, i64::try_from(expected).expect("fits"));
            assert_eq!(t, Timestamp::new(expected));
            assert_eq!(r, Diff::ONE);
            expected += 1;
        }
        assert_eq!(expected, count);
    });
}

/// Drive a [`Cursor`] over a spilled, multi-chunk chain to completion (the access pattern
/// merges use). Each step pages the front chunk back in via [`Chunk::column`]; assert the
/// cursor yields every update in order.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // madvise on the swap backend is unsupported under miri
fn cursor_steps_through_swap_backend() {
    let count = 200_000_u64;
    with_swap_pager(|| {
        let mut builder = ChainBuilder::<i64>::default();
        for i in 0..count {
            let d = i64::try_from(i).expect("fits");
            builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
        }
        let chain = builder.finish();
        assert!(chain.chunks.len() > 1, "expected multiple minted chunks");

        let mut rest = chain.into_cursor();
        let mut expected = 0_u64;
        while let Some(cursor) = rest.take() {
            let (d, t, r) = cursor.get();
            assert_eq!(i64::into_owned(d), i64::try_from(expected).expect("fits"));
            assert_eq!(t, Timestamp::new(expected));
            assert_eq!(r, Diff::ONE);
            expected += 1;
            rest = cursor.step();
        }
        assert_eq!(expected, count);
    });
}
