// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Bookkeeping of open recording periods shared by the freshness recorders.

use std::collections::BTreeMap;
use std::fmt;

use super::recorder::RecorderConfig;

/// The longest span of arrivals that one advance accounts in periods of the configured length.
/// Longer spans, a catch-up after a stall, are accounted in pieces of at most [`DAY_MS`].
pub(super) const CATCH_UP_MS: u64 = 60 * 60 * 1000;

/// The length of a UTC day in ms, at whose multiples catch-up pieces are cut.
pub(super) const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// A period `(start, end]` that will not change anymore, with the state accumulated for it.
#[derive(Debug)]
pub(super) struct ClosedPeriod<S> {
    pub start_ms: u64,
    pub end_ms: u64,
    pub state: S,
}

#[derive(Debug)]
struct OpenPeriod<S> {
    end_ms: u64,
    state: S,
}

/// Arrivals up to a high-water mark, charged to the periods in which they arrived.
///
/// Periods are half-open `(start, end]` on arrival time and start at multiples of the period
/// length, except for the pieces of a catch-up, the first period, which starts at the recording
/// start, and the periods around a change of the period length, so every arrival time belongs to
/// exactly one period. A period is handed out once,
/// when its end is at most the high-water mark, which makes everything handed out final.
/// Periods are handed out in order, each ending no earlier than the one before, so the largest
/// end handed out bounds every arrival counted so far.
pub(super) struct PeriodBook<S> {
    high_water_ms: u64,
    /// The open periods by start. They tile the arrivals after the last handed-out period.
    open: BTreeMap<u64, OpenPeriod<S>>,
    /// The period length of the open periods, `None` before the first advance.
    period_ms: Option<u64>,
    /// No period starts before this, the recording start or the high-water mark at the last
    /// change of the period length.
    cut_ms: u64,
    /// The end of the last handed-out period.
    handed_out_ms: u64,
    finished: bool,
}

// NOTE: Summarizes instead of deriving, because controller state dumps format every
// collection's recorders and a dump must stay small.
impl<S> fmt::Debug for PeriodBook<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeriodBook")
            .field("high_water_ms", &self.high_water_ms)
            .field("open_periods", &self.open.len())
            .field("period_ms", &self.period_ms)
            .field("finished", &self.finished)
            .finish()
    }
}

impl<S> PeriodBook<S> {
    /// Starts accounting arrivals after `start_ms`, in a first period that starts there.
    pub fn new(start_ms: u64) -> Self {
        // NOTE: A restart can seed `start_ms` at the end of a period cut off the grid. A first
        // period on the grid would start before it and overlap the periods handed out before.
        PeriodBook {
            high_water_ms: start_ms,
            open: BTreeMap::new(),
            period_ms: None,
            cut_ms: start_ms,
            handed_out_ms: start_ms,
            finished: false,
        }
    }

    /// Advances the high-water mark to `to_ms`, observed at `now_ms`, and returns the periods
    /// that closed.
    ///
    /// `add` receives the state of each period and the part `(from, to]` of the new span that
    /// falls into it. `open` creates the state of a period when its first part arrives, so the
    /// state can capture configuration that later calls change. A `to_ms` at or below the
    /// high-water mark, and any call after `finish`, does nothing.
    ///
    /// Arrivals older than the retention of `config` before `now_ms` are skipped. The high-water
    /// mark advances at most to the last period boundary at or before `now_ms` plus one period,
    /// and later calls continue from there. A span longer than [`CATCH_UP_MS`] is accounted in
    /// pieces between UTC day boundaries, one period each, so the number of periods is bounded
    /// by the retention in days instead of the span in periods.
    ///
    /// A changed period length first cuts every open period at the high-water mark and hands
    /// it out, and the periods of the new length start no earlier than the cut.
    pub fn advance(
        &mut self,
        to_ms: u64,
        now_ms: u64,
        config: &RecorderConfig,
        mut open: impl FnMut() -> S,
        mut add: impl FnMut(&mut S, u64, u64),
    ) -> Vec<ClosedPeriod<S>> {
        let period_ms = config.period_ms.max(1);
        if self.finished {
            return Vec::new();
        }
        let mut closed = Vec::new();
        if self.period_ms.is_some_and(|p| p != period_ms) {
            // NOTE: Periods of the new length can end before open periods of the old one, and a
            // restart seeds from the largest end handed out, which would then skip the arrivals
            // the old periods still hold. Their arrivals are all counted up to the high-water
            // mark, so cutting them there hands them out final and in order.
            let cut_ms = self.high_water_ms;
            for (start_ms, p) in std::mem::take(&mut self.open) {
                closed.push(ClosedPeriod {
                    start_ms,
                    end_ms: p.end_ms.min(cut_ms),
                    state: p.state,
                });
            }
            self.cut_ms = cut_ms;
        }
        self.period_ms = Some(period_ms);
        // A frontier far ahead of the wall clock, for example after a clock jump, would
        // otherwise open one period per period length up to it.
        let cap = now_ms.saturating_add(period_ms);
        let to_ms = to_ms.min(cap - cap % period_ms);
        if to_ms <= self.high_water_ms {
            return self.hand_out(closed);
        }
        // Rows of periods that end before the retention would be truncated right away.
        let floor = now_ms.saturating_sub(config.retention_ms);
        let mut from = self.high_water_ms.max(floor);
        if to_ms.saturating_sub(from) > CATCH_UP_MS {
            // The pieces start and end at period boundaries, so the periods after the catch-up
            // sit on the grid of the period length again.
            let inner_from = from.div_ceil(period_ms).saturating_mul(period_ms);
            let inner_to = to_ms - to_ms % period_ms;
            if inner_from < inner_to {
                self.add_periods(from, inner_from, period_ms, &mut open, &mut add);
                from = inner_from;
                while from < inner_to {
                    let end = (from / DAY_MS + 1).saturating_mul(DAY_MS).min(inner_to);
                    self.add_part((from, end), from, end, &mut open, &mut add);
                    from = end;
                }
            }
        }
        self.add_periods(from, to_ms, period_ms, &mut open, &mut add);
        self.high_water_ms = to_ms;
        // The open periods tile the arrivals in order, so those that closed are a prefix.
        while let Some(entry) = self.open.first_entry() {
            if entry.get().end_ms > to_ms {
                break;
            }
            closed.push(Self::close(entry.remove_entry()));
        }
        self.hand_out(closed)
    }

    /// Notes that `closed` are handed out, and returns them.
    fn hand_out(&mut self, closed: Vec<ClosedPeriod<S>>) -> Vec<ClosedPeriod<S>> {
        for p in &closed {
            let in_order = p.start_ms >= self.handed_out_ms && p.end_ms > p.start_ms;
            debug_assert!(
                in_order,
                "period ({}, {}] after one ending at {}",
                p.start_ms, p.end_ms, self.handed_out_ms
            );
            self.handed_out_ms = self.handed_out_ms.max(p.end_ms);
        }
        closed
    }

    /// Adds the arrivals in `(from, to]` to the periods of length `period_ms` they fall into.
    fn add_periods(
        &mut self,
        mut from: u64,
        to: u64,
        period_ms: u64,
        open: &mut impl FnMut() -> S,
        add: &mut impl FnMut(&mut S, u64, u64),
    ) {
        while from < to {
            let grid_start = from - from % period_ms;
            let end = grid_start.saturating_add(period_ms);
            let start = grid_start.max(self.cut_ms);
            let part_end = to.min(end);
            self.add_part((start, end), from, part_end, open, add);
            from = part_end;
        }
    }

    /// Adds the arrivals in `(from, to]` to period `(start, end]`, opening it if needed.
    fn add_part(
        &mut self,
        (start, end): (u64, u64),
        from: u64,
        to: u64,
        open: &mut impl FnMut() -> S,
        add: &mut impl FnMut(&mut S, u64, u64),
    ) {
        let period = self.open.entry(start).or_insert_with(|| OpenPeriod {
            end_ms: end,
            state: open(),
        });
        debug_assert_eq!(period.end_ms, end, "period starting at {start}");
        add(&mut period.state, from, to);
    }

    /// Returns every open period, including those that extend past the high-water mark, and
    /// stops accepting arrivals. The last arrival of the object is the high-water mark.
    pub fn finish(&mut self) -> Vec<ClosedPeriod<S>> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let closed = std::mem::take(&mut self.open)
            .into_iter()
            .map(Self::close)
            .collect();
        self.hand_out(closed)
    }

    fn close((start_ms, p): (u64, OpenPeriod<S>)) -> ClosedPeriod<S> {
        ClosedPeriod {
            start_ms,
            end_ms: p.end_ms,
            state: p.state,
        }
    }
}
