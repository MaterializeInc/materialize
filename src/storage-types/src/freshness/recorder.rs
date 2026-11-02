// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Recording of arrival-weighted latency histograms in periods.

use std::collections::BTreeMap;

use mz_repr::Timestamp;
use timely::progress::Antichain;

use super::ResolvedTree;
use super::histogram::{BucketScheme, add_span};
use super::period::{ClosedPeriod, PeriodBook};

/// Parameters of one observation. They may change between observations.
#[derive(Debug, Clone, Copy)]
pub struct RecorderConfig {
    /// The bucket scheme of periods opened by this observation.
    pub scheme: BucketScheme,
    /// The virtual arrival interval in ms.
    pub interval_ms: u64,
    /// The period length in ms.
    pub period_ms: u64,
    /// How long the histograms keep periods, in ms. Arrivals older than this are not recorded.
    pub retention_ms: u64,
}

/// One non-empty bucket of a closed period `(period_start_ms, period_end_ms]`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PeriodRow {
    pub period_start_ms: u64,
    pub period_end_ms: u64,
    pub buckets_per_octave: u32,
    pub bucket: i32,
    pub count: u64,
}

/// The histogram of one period together with the scheme it was opened with.
#[derive(Debug)]
pub(super) struct PeriodHistogram {
    pub scheme: BucketScheme,
    pub counts: BTreeMap<i32, u64>,
}

/// Records the latency of every virtual arrival of one object, once, into period histograms.
#[derive(Debug)]
pub struct FreshnessRecorder {
    book: PeriodBook<PeriodHistogram>,
    shown: Shown,
}

/// The most entries [`Shown`] keeps.
pub(super) const MAX_SHOWN: usize = 16;

/// The first observations that reflected the arrivals above a period book's high-water mark.
///
/// The book defers the arrivals of a frontier ahead of the wall clock past the next period
/// boundary to a later observation. They were visible at the first observation that reflected
/// them, and are charged at that one.
#[derive(Debug, Default)]
pub(super) struct Shown {
    /// `(reflected, observed_ms)`, ascending in both: the arrivals above the high-water mark up
    /// to `reflected` were reflected at or before `observed_ms`. At most [`MAX_SHOWN`] entries.
    entries: Vec<(u64, u64)>,
}

impl Shown {
    /// Notes that an observation at `observed_ms` reflected the arrivals up to `reflected`, given
    /// the high-water mark `high_water_ms` of the book.
    pub fn note(&mut self, reflected: u64, observed_ms: u64, high_water_ms: u64) {
        if reflected <= high_water_ms || self.entries.last().is_some_and(|(r, _)| reflected <= *r) {
            return;
        }
        self.entries.push((reflected, observed_ms));
        if self.entries.len() > MAX_SHOWN {
            // A frontier that leads the wall clock by many observations would grow the list
            // without bound. Merging the neighbours `(r1, o1), (r2, o2)` into `(r2, o1)` charges
            // `(r1, r2]` at the earlier `o1`, which understates their latencies by at most
            // `o2 - o1`, so the pair with the closest observations is merged.
            let i = (1..self.entries.len())
                .min_by_key(|i| self.entries[*i].1.saturating_sub(self.entries[*i - 1].1))
                .expect("more than one entry");
            let (reflected, _) = self.entries.remove(i);
            self.entries[i - 1].0 = reflected;
        }
    }

    /// Calls `add` with each part `(lo, hi]` of `(from, to]` and the observation time that
    /// first reflected it. `(from, to]` must lie above the high-water mark of the last
    /// [`Self::forget_through`] and at or below the last noted arrival.
    pub fn split(&self, from: u64, to: u64, mut add: impl FnMut(u64, u64, u64)) {
        let mut lo = from;
        for (reflected, observed_ms) in &self.entries {
            let hi = to.min(*reflected);
            if lo < hi {
                add(lo, hi, *observed_ms);
                lo = hi;
            }
        }
        debug_assert!(lo >= to, "({lo}, {to}] not noted");
    }

    /// Forgets the arrivals at or below `high_water_ms`, which the book recorded.
    pub fn forget_through(&mut self, high_water_ms: u64) {
        self.entries.retain(|(r, _)| *r > high_water_ms);
    }

    /// The last noted arrival that is not forgotten.
    pub fn last(&self) -> Option<u64> {
        self.entries.last().map(|(r, _)| *r)
    }
}

impl FreshnessRecorder {
    /// Starts recording arrivals after `start_ms`: the object's creation time, a restart seed, or
    /// the current frontier.
    pub fn new(start_ms: u64) -> Self {
        FreshnessRecorder {
            book: PeriodBook::new(start_ms),
            shown: Shown::default(),
        }
    }

    /// Records a write frontier observed at `now_ms` and returns the rows of periods that closed.
    ///
    /// Rows of one period are adjacent and sorted by bucket. An empty frontier ends the recording
    /// like [`Self::finish`]. A frontier that reflects nothing new, or nothing at all as for a
    /// refresh view before its first refresh, records nothing.
    ///
    /// Every arrival is charged at the first observation that reflected it, also when the period
    /// book defers it to a later call.
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        config: &RecorderConfig,
    ) -> Vec<PeriodRow> {
        let Some(frontier) = frontier.as_option() else {
            return self.finish(Some(tree), now_ms, config);
        };
        let Some(last_reflected) = tree.last_reflected(*frontier) else {
            return Vec::new();
        };
        self.shown
            .note(last_reflected, now_ms, self.book.high_water_ms());
        rows(self.advance(tree, last_reflected, now_ms, config))
    }

    /// Advances the book to `to_ms` at `now_ms`, charging each arrival at the observation that
    /// first reflected it.
    fn advance(
        &mut self,
        tree: &ResolvedTree,
        to_ms: u64,
        now_ms: u64,
        config: &RecorderConfig,
    ) -> Vec<ClosedPeriod<PeriodHistogram>> {
        let shown = &self.shown;
        let closed = self.book.advance(
            to_ms,
            now_ms,
            config,
            || PeriodHistogram {
                scheme: config.scheme,
                counts: BTreeMap::new(),
            },
            |period, from, to| {
                shown.split(from, to, |lo, hi, observed_ms| {
                    add_part(
                        tree,
                        &period.scheme,
                        config.interval_ms,
                        lo,
                        hi,
                        observed_ms,
                        &mut period.counts,
                    )
                })
            },
        );
        self.shown.forget_through(self.book.high_water_ms());
        closed
    }

    /// Ends the recording, for an object that goes away, and returns the rows of the open
    /// periods.
    ///
    /// The arrivals that the period book deferred are recorded first, up to `now_ms`, at the
    /// observations that reflected them. Without `tree`, for an object whose tree no longer
    /// resolves, they are not recorded.
    pub fn finish(
        &mut self,
        tree: Option<&ResolvedTree>,
        now_ms: u64,
        config: &RecorderConfig,
    ) -> Vec<PeriodRow> {
        let mut closed = match (tree, self.shown.last()) {
            (Some(tree), Some(last)) => self.advance(tree, last.min(now_ms), now_ms, config),
            _ => Vec::new(),
        };
        closed.extend(self.book.finish());
        rows(closed)
    }
}

/// The rows of closed periods, adjacent per period and sorted by bucket.
pub(super) fn rows(closed: Vec<ClosedPeriod<PeriodHistogram>>) -> Vec<PeriodRow> {
    closed
        .into_iter()
        .flat_map(|p| {
            let buckets_per_octave = p.state.scheme.per_octave();
            p.state
                .counts
                .into_iter()
                .map(move |(bucket, count)| PeriodRow {
                    period_start_ms: p.start_ms,
                    period_end_ms: p.end_ms,
                    buckets_per_octave,
                    bucket,
                    count,
                })
        })
        .collect()
}

/// Adds the arrivals in `(from, to]` to `counts`, where the ends may fall inside a window.
///
/// The arrivals of a window carry the latency `advance_ms - s` from the window start `s`,
/// wherever the window is cut. A latency of at most 1 ms, also a negative one, takes the bucket
/// of 1 ms.
pub(super) fn add_part(
    tree: &ResolvedTree,
    scheme: &BucketScheme,
    interval_ms: u64,
    from: u64,
    to: u64,
    advance_ms: u64,
    counts: &mut BTreeMap<i32, u64>,
) {
    if from >= to || interval_ms == 0 {
        return;
    }
    let mut add_window_part = |window_start: u64, lo: u64, hi: u64| {
        let n = hi / interval_ms - lo / interval_ms;
        if n > 0 {
            let latency_ms = advance_ms.saturating_sub(window_start);
            *counts.entry(scheme.bucket(latency_ms)).or_default() += n;
        }
    };
    // The leading part of the window that contains `from`, if `from` is not a boundary.
    let mut inner_from = from;
    let next = tree.next_boundary(from);
    if next != from {
        inner_from = next.min(to);
        add_window_part(tree.prev_boundary(from), from, inner_from);
    }
    // The trailing part of the window that contains `to`, if `to` is not a boundary.
    let last_start = tree.prev_boundary(to);
    let inner_to = last_start.max(inner_from);
    if inner_to < to {
        add_window_part(last_start, inner_to, to);
    }
    // Whole windows in between start and end at boundaries, as `add_span` requires.
    add_span(
        tree,
        scheme,
        interval_ms,
        inner_from,
        inner_to,
        advance_ms,
        counts,
    );
}
