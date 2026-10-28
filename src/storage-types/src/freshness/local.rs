// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Recording of local delay: how long an object took to make an arrival visible after its
//! slowest direct input had.

use std::collections::BTreeMap;

use mz_repr::Timestamp;
use timely::progress::Antichain;

use super::ResolvedTree;
use super::histogram::BucketScheme;
use super::period::PeriodBook;
use super::recorder::{PeriodHistogram, PeriodRow, RecorderConfig, add_part, rows};

/// Observation times of a collection's visibility advances, kept while a consumer may still need
/// them.
///
/// Visibility is measured in arrival time, the collection's own `last_reflected`, so a refresh
/// collection is visible when its data is, not when its frontier passes.
#[derive(Debug, Default)]
pub struct VisibilityLog {
    /// `(reflected, observed_ms)`, ascending in both.
    entries: Vec<(u64, u64)>,
    /// Arrivals at or below this are no longer answerable because their entries were pruned.
    floor: u64,
}

impl VisibilityLog {
    /// Records that arrivals up to `reflected` were visible at `observed_ms`. A `reflected`
    /// that does not exceed the last recorded one is ignored.
    pub fn record(&mut self, reflected: u64, observed_ms: u64) {
        if self.entries.last().is_none_or(|(r, _)| reflected > *r) {
            self.entries.push((reflected, observed_ms));
        }
    }

    /// The earliest observation time at which arrival time `arrival` was visible, if known.
    pub fn visible_at(&self, arrival: u64) -> Option<u64> {
        self.segment(arrival).0
    }

    /// Drops what is only needed to answer arrivals at or below `arrival`.
    pub fn prune_through(&mut self, arrival: u64) {
        // An entry with `reflected = r` answers the arrivals above its predecessor up to `r`,
        // so it is dead once `r` is at most `arrival`.
        let dead = self.entries.partition_point(|(r, _)| *r <= arrival);
        if dead > 0 {
            self.floor = self.floor.max(self.entries[dead - 1].0);
        }
        self.entries.drain(..dead);
    }

    /// The observation time `visible_at(arrival)` returns, and the inclusive upper bound of the
    /// arrivals that share this answer, `None` if unbounded.
    fn segment(&self, arrival: u64) -> (Option<u64>, Option<u64>) {
        if arrival <= self.floor {
            return (None, Some(self.floor));
        }
        let idx = self.entries.partition_point(|(r, _)| *r < arrival);
        match self.entries.get(idx) {
            Some((r, observed)) => (Some(*observed), Some(*r)),
            None => (None, None),
        }
    }
}

/// Records the local delay of every virtual arrival of one object, once, into period histograms.
///
/// An arrival's local delay is the time the object made it visible minus the time its slowest
/// direct input did. Windows, periods and finality follow `FreshnessRecorder`, so per period the
/// counts agree with it.
#[derive(Debug)]
pub struct LocalDelayRecorder {
    book: PeriodBook<PeriodHistogram>,
}

impl LocalDelayRecorder {
    /// Starts recording arrivals after `start_ms`, like `FreshnessRecorder::new`.
    pub fn new(start_ms: u64) -> Self {
        LocalDelayRecorder {
            book: PeriodBook::new(start_ms),
        }
    }

    /// Records an advance of the object to `frontier` at `now_ms`, given its direct inputs'
    /// logs, and returns the rows of periods that closed.
    ///
    /// An arrival window is ready when every input's log shows its end as visible. With no inputs
    /// the local delay is the end-to-end latency. An input whose log cannot answer, because it
    /// was pruned or the advance was never observed, counts as ready at `now_ms`, which
    /// understates the delay and never overstates it.
    /// Rows have the order and finality of `FreshnessRecorder::observe`.
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        inputs: &[&VisibilityLog],
        config: &RecorderConfig,
    ) -> Vec<PeriodRow> {
        let closed = match frontier.as_option() {
            None => self.book.finish(),
            Some(frontier) => {
                let Some(last_reflected) = tree.last_reflected(*frontier) else {
                    return Vec::new();
                };
                let advance_ms = now_ms.max(last_reflected);
                self.book.advance(
                    last_reflected,
                    config.period_ms,
                    || PeriodHistogram {
                        scheme: config.scheme,
                        counts: BTreeMap::new(),
                    },
                    |period, from, to| {
                        if inputs.is_empty() {
                            add_part(
                                tree,
                                &period.scheme,
                                config.interval_ms,
                                from,
                                to,
                                advance_ms,
                                &mut period.counts,
                            );
                        } else {
                            add_local_part(
                                tree,
                                &period.scheme,
                                config.interval_ms,
                                (from, to),
                                advance_ms,
                                inputs,
                                &mut period.counts,
                            );
                        }
                    },
                )
            }
        };
        rows(closed)
    }
}

/// Adds the arrivals in `(from, to]` with their local delay at `now_ms` to `counts`.
///
/// Walks the windows in groups that share one ready time, so the work is proportional to the log
/// entries consumed and the buckets touched, not to the number of windows.
fn add_local_part(
    tree: &ResolvedTree,
    scheme: &BucketScheme,
    interval_ms: u64,
    (from, to): (u64, u64),
    now_ms: u64,
    inputs: &[&VisibilityLog],
    counts: &mut BTreeMap<i32, u64>,
) {
    if interval_ms == 0 {
        return;
    }
    let mut pos = from;
    while pos < to {
        // The window of the first arrival after `pos`. A `to` inside a window does not change
        // the window's end.
        let window_end = tree.next_boundary(pos + 1);
        let mut ready_ms = 0;
        let mut group_end = to;
        for log in inputs {
            let (observed, last) = log.segment(window_end);
            ready_ms = ready_ms.max(observed.unwrap_or(now_ms));
            // The group ends where this input's answer changes. The bound is at least
            // `window_end` because the entry answers it.
            if let Some(last) = last {
                group_end = group_end.min(last);
            }
        }
        let n = group_end / interval_ms - pos / interval_ms;
        if n > 0 {
            // NOTE: An input observed after the object, which separately observed frontiers
            // allow, makes the delay negative. It is clamped to 0, and a latency of 0 ms has no
            // bucket of its own, so it takes the bucket of 1 ms.
            let latency_ms = now_ms.saturating_sub(ready_ms).max(1);
            *counts.entry(scheme.bucket(latency_ms)).or_default() += n;
        }
        pos = group_end;
    }
}
