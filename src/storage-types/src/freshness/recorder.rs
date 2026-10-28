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
}

impl FreshnessRecorder {
    /// Starts recording arrivals after `start_ms`: the object's creation time, a restart seed, or
    /// the current frontier.
    pub fn new(start_ms: u64) -> Self {
        FreshnessRecorder {
            book: PeriodBook::new(start_ms),
        }
    }

    /// Records a write frontier observed at `now_ms` and returns the rows of periods that closed.
    ///
    /// Rows of one period are adjacent and sorted by bucket. An empty frontier closes all open
    /// periods and ends the recording. A frontier that reflects nothing new, or nothing at all as
    /// for a refresh view before its first refresh, records nothing.
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        config: &RecorderConfig,
    ) -> Vec<PeriodRow> {
        let closed = match frontier.as_option() {
            None => self.book.finish(),
            Some(frontier) => {
                let Some(last_reflected) = tree.last_reflected(*frontier) else {
                    return Vec::new();
                };
                // All parts of the span share this advance time, so a window cut at a period
                // boundary keeps a single latency.
                let advance_ms = now_ms.max(last_reflected);
                self.book.advance(
                    last_reflected,
                    config.period_ms,
                    || PeriodHistogram {
                        scheme: config.scheme,
                        counts: BTreeMap::new(),
                    },
                    |period, from, to| {
                        add_part(
                            tree,
                            &period.scheme,
                            config.interval_ms,
                            from,
                            to,
                            advance_ms,
                            &mut period.counts,
                        )
                    },
                )
            }
        };
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
/// The arrivals of a window carry the latency of the window start, wherever the window is cut.
/// `advance_ms` must be at least `to`.
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
            *counts
                .entry(scheme.bucket(advance_ms - window_start))
                .or_default() += n;
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
