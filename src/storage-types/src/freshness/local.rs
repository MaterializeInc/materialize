// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Recording of local delay and skew, the parts of an object's end-to-end latency that the
//! object itself and the alignment of its inputs add.
//!
//! An arrival's end-to-end charge splits into three parts that sum to it. Upstream runs from
//! the window start until the slowest direct input showed the arrival, which is that input's
//! own end-to-end latency and is not recorded. Skew runs from then until every direct input's
//! frontier passed the timestamp the object needs for the arrival. Local runs from then until
//! the object showed the arrival.

use std::collections::BTreeMap;
use std::fmt;

use mz_repr::Timestamp;
use timely::progress::Antichain;

use super::ResolvedTree;
use super::period::{ClosedPeriod, PeriodBook};
use super::recorder::{PeriodHistogram, PeriodRow, RecorderConfig, add_part, rows};

/// One observed advance of a collection.
#[derive(Debug, Clone, Copy)]
struct Entry {
    /// The frontier, `u64::MAX` for an empty one.
    frontier: u64,
    /// The largest arrival the frontier reflects under the collection's own tree, 0 if none.
    reflected: u64,
    observed_ms: u64,
}

/// Observation times of a collection's frontier advances, kept while a consumer may still need
/// them.
///
/// Each entry answers in two domains. In arrival time, the collection's own `last_reflected`,
/// a refresh collection is visible when its data is, not when its frontier passes. In
/// timestamps, an entry tells when the frontier passed a timestamp a consumer needs.
#[derive(Default, Clone)]
pub struct VisibilityLog {
    /// Non-decreasing in frontier and reflected arrival.
    entries: Vec<Entry>,
    /// The fieldwise maximum of the pruned entries, `None` before the first prune. Arrivals at
    /// or below its `reflected` and timestamps below its `frontier` are answered by its
    /// observation time, which is at or after their actual answer.
    floor: Option<Entry>,
}

// NOTE: Summarizes instead of deriving, because controller state dumps format every log, and
// a log can hold an entry per observed frontier advance.
impl fmt::Debug for VisibilityLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VisibilityLog")
            .field("len", &self.entries.len())
            .field("first", &self.entries.first())
            .field("last", &self.entries.last())
            .field("floor", &self.floor)
            .finish()
    }
}

impl VisibilityLog {
    /// Records that the frontier was `frontier`, reflecting the arrivals up to `reflected`, at
    /// `observed_ms`. An entry that advances neither value over the last one is ignored, and a
    /// value that regresses keeps the last one.
    pub fn record(&mut self, frontier: u64, reflected: Option<u64>, observed_ms: u64) {
        let reflected = reflected.unwrap_or(0);
        let last = self.entries.last().or(self.floor.as_ref());
        let (last_frontier, last_reflected) = last.map_or((0, 0), |e| (e.frontier, e.reflected));
        if frontier > last_frontier || reflected > last_reflected {
            self.entries.push(Entry {
                frontier: frontier.max(last_frontier),
                reflected: reflected.max(last_reflected),
                observed_ms,
            });
        }
    }

    /// The earliest observation time at which arrival time `arrival` was visible, if known.
    ///
    /// For an arrival whose entry was pruned it is the observation time of the last pruned
    /// entry, which is at or after the arrival's actual visibility.
    pub fn visible_at(&self, arrival: u64) -> Option<u64> {
        self.segment(arrival).0
    }

    /// The earliest observation time at which the frontier was past `timestamp`, if known.
    ///
    /// Pruned entries answer like for [`Self::visible_at`].
    pub fn passed_at(&self, timestamp: u64) -> Option<u64> {
        self.frontier_segment(timestamp).0
    }

    /// Drops what is only needed to answer arrivals at or below `arrival`.
    ///
    /// The timestamps that later arrivals need under a consumer's tree that contains this
    /// collection's tree stay answerable, because a frontier past such a timestamp reflects
    /// the later arrival.
    pub fn prune_through(&mut self, arrival: u64) {
        // An entry with `reflected = r` answers the arrivals above its predecessor up to `r`,
        // so it is dead once `r` is at most `arrival`.
        let dead = self.entries.partition_point(|e| e.reflected <= arrival);
        self.floor = Self::raise(self.floor, &self.entries[..dead]);
        self.entries.drain(..dead);
    }

    /// Drops the entries observed before `observed_ms`, except the last one, which stays to
    /// answer the arrivals up to it.
    pub fn prune_observed_before(&mut self, observed_ms: u64) {
        let old = self
            .entries
            .partition_point(|e| e.observed_ms < observed_ms);
        let dead = old.min(self.entries.len().saturating_sub(1));
        if dead > 0 {
            self.prune_through(self.entries[dead - 1].reflected);
        }
    }

    /// A copy that answers like this log for every arrival above `arrival`, in both domains as
    /// for [`Self::prune_through`].
    pub fn after(&self, arrival: u64) -> Self {
        let dead = self.entries.partition_point(|e| e.reflected <= arrival);
        VisibilityLog {
            entries: self.entries[dead..].to_vec(),
            floor: Self::raise(self.floor, &self.entries[..dead]),
        }
    }

    /// `floor` raised by the last of the pruned entries `dead`.
    fn raise(floor: Option<Entry>, dead: &[Entry]) -> Option<Entry> {
        let Some(last) = dead.last() else {
            return floor;
        };
        Some(match floor {
            None => *last,
            Some(floor) => Entry {
                frontier: floor.frontier.max(last.frontier),
                reflected: floor.reflected.max(last.reflected),
                observed_ms: floor.observed_ms.max(last.observed_ms),
            },
        })
    }

    /// The observation time `visible_at(arrival)` returns, and the inclusive upper bound of the
    /// arrivals that share this answer, `None` if unbounded.
    fn segment(&self, arrival: u64) -> (Option<u64>, Option<u64>) {
        if let Some(floor) = &self.floor
            && arrival <= floor.reflected
        {
            return (Some(floor.observed_ms), Some(floor.reflected));
        }
        let idx = self.entries.partition_point(|e| e.reflected < arrival);
        match self.entries.get(idx) {
            Some(e) => (Some(e.observed_ms), Some(e.reflected)),
            None => (None, None),
        }
    }

    /// The observation time `passed_at(timestamp)` returns, and the exclusive upper bound of
    /// the timestamps that share this answer, `None` if unbounded.
    fn frontier_segment(&self, timestamp: u64) -> (Option<u64>, Option<u64>) {
        if let Some(floor) = &self.floor
            && timestamp < floor.frontier
        {
            return (Some(floor.observed_ms), Some(floor.frontier));
        }
        let idx = self.entries.partition_point(|e| e.frontier <= timestamp);
        match self.entries.get(idx) {
            Some(e) => (Some(e.observed_ms), Some(e.frontier)),
            None => (None, None),
        }
    }
}

/// The rows of periods that closed, per measure. Rows of one period are adjacent and sorted by
/// bucket.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LocalRows {
    pub local: Vec<PeriodRow>,
    /// Empty for an object with fewer than two direct inputs.
    pub skew: Vec<PeriodRow>,
}

/// The histograms of one period of both measures.
#[derive(Debug)]
struct LocalPeriod {
    local: PeriodHistogram,
    skew: PeriodHistogram,
}

/// Records the local delay and skew of every virtual arrival of one object, once, into period
/// histograms.
///
/// Windows, periods and finality follow `FreshnessRecorder`, so per period the counts agree with
/// it.
#[derive(Debug)]
pub struct LocalDelayRecorder {
    book: PeriodBook<LocalPeriod>,
    /// `(reflected, observed_ms)` of the first observations that reflected arrivals above the
    /// high-water mark, ascending in both. The book defers arrivals of a frontier ahead of the
    /// wall clock past the next period boundary to a later observation, which would charge
    /// them the time until then as local delay.
    shown: Vec<(u64, u64)>,
}

impl LocalDelayRecorder {
    /// Starts recording arrivals after `start_ms`, like `FreshnessRecorder::new`.
    pub fn new(start_ms: u64) -> Self {
        LocalDelayRecorder {
            book: PeriodBook::new(start_ms),
            shown: Vec::new(),
        }
    }

    /// The last arrival recorded so far, or the start. Input logs need to answer only the
    /// arrivals above it.
    pub fn high_water_ms(&self) -> u64 {
        self.book.high_water_ms()
    }

    /// Records an advance of the object to `frontier` at `now_ms`, given its direct inputs'
    /// logs, and returns the rows of periods that closed.
    ///
    /// `needed` is the object's tree without its own refresh node. An arrival needs every
    /// input's frontier to pass the timestamp `needed` assigns it, so a refresh wait stays in
    /// the local delay of the refreshing object. Skew is recorded only with two or more inputs.
    /// With no inputs the local delay is the end-to-end latency.
    ///
    /// Both measures may understate and never overstate. An input whose log cannot answer an
    /// arrival counts as ready at `now_ms`, and one whose answering entry was pruned as ready at
    /// the observation of its last pruned entry. Rows have the order and finality of
    /// `FreshnessRecorder::observe`.
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        needed: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        inputs: &[&VisibilityLog],
        config: &RecorderConfig,
    ) -> LocalRows {
        let closed = match frontier.as_option() {
            None => return self.finish(),
            Some(frontier) => {
                let Some(last_reflected) = tree.last_reflected(*frontier) else {
                    return LocalRows::default();
                };
                let advance_ms = now_ms.max(last_reflected);
                if last_reflected > self.book.high_water_ms()
                    && self.shown.last().is_none_or(|(r, _)| last_reflected > *r)
                {
                    self.shown.push((last_reflected, now_ms));
                }
                let shown = &self.shown;
                let histogram = || PeriodHistogram {
                    scheme: config.scheme,
                    counts: BTreeMap::new(),
                };
                let closed = self.book.advance(
                    last_reflected,
                    now_ms,
                    config,
                    || LocalPeriod {
                        local: histogram(),
                        skew: histogram(),
                    },
                    |period, from, to| {
                        if inputs.is_empty() {
                            add_part(
                                tree,
                                &period.local.scheme,
                                config.interval_ms,
                                from,
                                to,
                                advance_ms,
                                &mut period.local.counts,
                            );
                        } else {
                            // NOTE: Raw observation times, because `advance_ms` exceeds them
                            // by as much as a frontier runs ahead of the wall clock, which
                            // would overstate the delay by that much. The last entry of
                            // `shown` reflects `to`, so the parts cover `(from, to]`.
                            let mut lo = from;
                            for (reflected, observed_ms) in shown {
                                let hi = to.min(*reflected);
                                if lo < hi {
                                    add_local_part(
                                        tree,
                                        needed,
                                        config.interval_ms,
                                        (lo, hi),
                                        *observed_ms,
                                        inputs,
                                        period,
                                    );
                                    lo = hi;
                                }
                            }
                        }
                    },
                );
                let high_water_ms = self.book.high_water_ms();
                self.shown.retain(|(r, _)| *r > high_water_ms);
                closed
            }
        };
        local_rows(closed)
    }

    /// Like [`FreshnessRecorder::finish`](super::recorder::FreshnessRecorder::finish).
    pub fn finish(&mut self) -> LocalRows {
        self.shown.clear();
        local_rows(self.book.finish())
    }
}

fn local_rows(closed: Vec<ClosedPeriod<LocalPeriod>>) -> LocalRows {
    let (mut local, mut skew) = (Vec::new(), Vec::new());
    for p in closed {
        let (start_ms, end_ms) = (p.start_ms, p.end_ms);
        local.push(ClosedPeriod {
            start_ms,
            end_ms,
            state: p.state.local,
        });
        skew.push(ClosedPeriod {
            start_ms,
            end_ms,
            state: p.state.skew,
        });
    }
    LocalRows {
        local: rows(local),
        skew: rows(skew),
    }
}

/// Adds the arrivals in `(from, to]` with their local delay and skew at `now_ms` to `period`.
///
/// Walks the windows in groups in which every input answers both readiness questions with the
/// same entry, so the work is proportional to the log entries consumed and the buckets touched,
/// not to the number of windows.
fn add_local_part(
    tree: &ResolvedTree,
    needed: &ResolvedTree,
    interval_ms: u64,
    (from, to): (u64, u64),
    now_ms: u64,
    inputs: &[&VisibilityLog],
    period: &mut LocalPeriod,
) {
    if interval_ms == 0 {
        return;
    }
    let mut pos = from;
    while pos < to {
        // The window of the first arrival after `pos`. A `to` inside a window does not change
        // the window's end. All arrivals of a window need the same timestamp, because the
        // window boundaries are the grid points of the leaves of `needed`.
        let window_end = tree.next_boundary(pos + 1);
        let timestamp = needed.assigned(window_end);
        // The latest time an input showed the window, and the latest time an input's frontier
        // passed the timestamp the window needs.
        let (mut arrival_ready_ms, mut timestamp_ready_ms) = (0, 0);
        let mut group_end = to;
        for log in inputs {
            let (observed, last) = log.segment(window_end);
            arrival_ready_ms = arrival_ready_ms.max(observed.unwrap_or(now_ms));
            // The group ends where this input's answer changes. The bound is at least
            // `window_end` because the entry answers it.
            if let Some(last) = last {
                // Snapped down to this object's window boundary, because the window that
                // contains `last` ends after it and is answered by a later entry. Snapping up
                // would give that window this entry's earlier ready time and overstate its delay.
                group_end = group_end.min(tree.prev_boundary(last));
            }
            if let Some(timestamp) = timestamp {
                let (observed, passed) = log.frontier_segment(timestamp);
                timestamp_ready_ms = timestamp_ready_ms.max(observed.unwrap_or(now_ms));
                // The entry answers the arrivals that need a timestamp below `passed`, the
                // arrivals `passed` reflects. They include `window_end`.
                if let Some(passed) = passed.filter(|f| *f < u64::MAX) {
                    let last = needed.last_reflected(Timestamp::from(passed));
                    group_end = group_end.min(last.map_or(window_end, |l| tree.prev_boundary(l)));
                }
            }
        }
        if timestamp.is_none() {
            // The object reflects the window, so `needed` assigns it a timestamp unless the
            // trees disagree. Every input then counts as unanswered, which records 0 for both.
            (arrival_ready_ms, timestamp_ready_ms) = (now_ms, now_ms);
        }
        // NOTE: A frontier past the needed timestamp reflects the window if the input's tree is
        // part of `needed`, so the timestamp readiness is at least the arrival readiness. A tick
        // interval that changed between the input's observation and this one breaks that, and
        // the clamp then charges the difference to neither measure.
        timestamp_ready_ms = timestamp_ready_ms.max(arrival_ready_ms);
        debug_assert!(group_end > pos, "group_end={group_end} pos={pos}");
        let n = group_end / interval_ms - pos / interval_ms;
        if n > 0 {
            // NOTE: An input observed after the object, which separately observed frontiers
            // allow, makes the delay negative. It is clamped to 0, and a latency of 0 ms has no
            // bucket of its own, so it takes the bucket of 1 ms.
            let local_ms = now_ms.saturating_sub(timestamp_ready_ms).max(1);
            *period
                .local
                .counts
                .entry(period.local.scheme.bucket(local_ms))
                .or_default() += n;
            if inputs.len() >= 2 {
                let skew_ms = (timestamp_ready_ms - arrival_ready_ms).max(1);
                *period
                    .skew
                    .counts
                    .entry(period.skew.scheme.bucket(skew_ms))
                    .or_default() += n;
            }
        }
        pos = group_end;
    }
}
