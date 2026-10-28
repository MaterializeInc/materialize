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

/// Observation times of a collection's write frontier advances, kept while a consumer may still
/// need them.
#[derive(Debug, Default)]
pub struct FrontierLog {
    /// `(frontier, observed_ms)`, ascending in both.
    entries: Vec<(u64, u64)>,
    /// Timestamps below this are no longer answerable because their entries were pruned.
    floor: u64,
}

impl FrontierLog {
    /// Records that the frontier was observed at `frontier` at `observed_ms`. A frontier that
    /// does not exceed the last recorded one is ignored.
    pub fn record(&mut self, frontier: Timestamp, observed_ms: u64) {
        let frontier = u64::from(frontier);
        if self.entries.last().is_none_or(|(f, _)| frontier > *f) {
            self.entries.push((frontier, observed_ms));
        }
    }

    /// The earliest observation time at which the frontier exceeded `ts`, if observed.
    pub fn passed_at(&self, ts: Timestamp) -> Option<u64> {
        self.segment(u64::from(ts)).0
    }

    /// Drops what is only needed to answer timestamps at or below `ts`.
    pub fn prune_through(&mut self, ts: Timestamp) {
        // An entry with frontier `f` answers timestamps below `f`, so it is dead once `f - 1`
        // is at most `ts`.
        let limit = u64::from(ts).saturating_add(1);
        let dead = self.entries.partition_point(|(f, _)| *f <= limit);
        if dead > 0 {
            self.floor = self.floor.max(self.entries[dead - 1].0);
        }
        self.entries.drain(..dead);
    }

    /// The observation time `passed_at(ts)` returns, and the exclusive upper bound of the
    /// timestamps that share this answer, `None` if unbounded.
    fn segment(&self, ts: u64) -> (Option<u64>, Option<u64>) {
        if ts < self.floor {
            return (None, Some(self.floor));
        }
        let idx = self.entries.partition_point(|(f, _)| *f <= ts);
        match self.entries.get(idx) {
            Some((f, observed)) => (Some(*observed), Some(*f)),
            None => (None, None),
        }
    }
}

/// The output timestamp of the arrivals of the window ending at `end`, the maximum over leaves
/// of the leaf timestamp. All arrivals of a window share every leaf's timestamp.
fn output_timestamp(tree: &ResolvedTree, end: u64) -> u64 {
    let mut ts = end;
    tree.for_each_ticker(&mut |t| {
        if let Some(i) = t.grid_ms() {
            ts = ts.max(end.div_ceil(i).saturating_mul(i));
        }
    });
    ts
}

/// The largest window end whose output timestamp is below `exclusive`.
fn last_window_end_below(tree: &ResolvedTree, exclusive: u64) -> u64 {
    let bound = exclusive.saturating_sub(1);
    let mut end = bound;
    tree.for_each_ticker(&mut |t| {
        if let Some(i) = t.grid_ms() {
            end = end.min(bound - bound % i);
        }
    });
    end
}

/// Records the local delay of every virtual arrival of one object, once, into period histograms.
///
/// An arrival's local delay is the time the object made it visible minus the time its slowest
/// direct input did. Windows, periods and finality follow `FreshnessRecorder`, so per period the
/// counts agree with it.
#[derive(Debug)]
pub struct LocalDelayRecorder {
    book: PeriodBook<PeriodHistogram>,
    /// The latest arrival time recorded so far.
    reflected_ms: u64,
    /// The time of the advance that reached `reflected_ms`, or the start time.
    advanced_ms: u64,
}

impl LocalDelayRecorder {
    /// Starts recording arrivals after `start_ms`, like `FreshnessRecorder::new`.
    pub fn new(start_ms: u64) -> Self {
        LocalDelayRecorder {
            book: PeriodBook::new(start_ms),
            reflected_ms: start_ms,
            advanced_ms: start_ms,
        }
    }

    /// Records an advance of the object to `frontier` at `now_ms`, given its direct inputs'
    /// logs, and returns the rows of periods that closed.
    ///
    /// With no inputs the local delay is the end-to-end latency. An input whose log does not
    /// cover an arrival's output timestamp, because it was pruned or the advance was never
    /// observed, counts as ready at the object's previous advance, which understates the delay.
    /// Rows have the order and finality of `FreshnessRecorder::observe`.
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        inputs: &[&FrontierLog],
        config: &RecorderConfig,
    ) -> Vec<PeriodRow> {
        let closed = match frontier.as_option() {
            None => self.book.finish(),
            Some(frontier) => {
                let Some(last_reflected) = tree.last_reflected(*frontier) else {
                    return Vec::new();
                };
                let advance_ms = now_ms.max(last_reflected);
                let fallback_ms = self.advanced_ms;
                let closed = self.book.advance(
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
                                (advance_ms, fallback_ms),
                                inputs,
                                &mut period.counts,
                            );
                        }
                    },
                );
                if last_reflected > self.reflected_ms {
                    self.reflected_ms = last_reflected;
                    self.advanced_ms = now_ms;
                }
                closed
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
    (now_ms, fallback_ms): (u64, u64),
    inputs: &[&FrontierLog],
    counts: &mut BTreeMap<i32, u64>,
) {
    if interval_ms == 0 {
        return;
    }
    let mut pos = from;
    while pos < to {
        // The window of the first arrival after `pos`. A `to` inside a window does not change
        // the window's output timestamp.
        let window_end = tree.next_boundary(pos + 1);
        let ts = output_timestamp(tree, window_end);
        let mut ready_ms = 0;
        let mut group_end = to;
        for log in inputs {
            let (observed, exclusive) = log.segment(ts);
            ready_ms = ready_ms.max(observed.unwrap_or(fallback_ms));
            // The group ends where this input's answer changes. The bound is at least
            // `window_end` because `ts` is below `exclusive` and grows with the window end.
            if let Some(exclusive) = exclusive {
                group_end = group_end.min(last_window_end_below(tree, exclusive));
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
