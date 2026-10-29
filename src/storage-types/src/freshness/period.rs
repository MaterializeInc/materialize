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
/// length, except for the pieces of a catch-up, so every arrival time belongs to exactly one
/// period. A period is handed out once, when its end is at most the high-water mark, which makes
/// everything handed out final.
pub(super) struct PeriodBook<S> {
    high_water_ms: u64,
    open: BTreeMap<(u64, u64), OpenPeriod<S>>,
    finished: bool,
}

// NOTE: Summarizes instead of deriving, because controller state dumps format every
// collection's recorders and a dump must stay small.
impl<S> fmt::Debug for PeriodBook<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeriodBook")
            .field("high_water_ms", &self.high_water_ms)
            .field("open_periods", &self.open.len())
            .field("finished", &self.finished)
            .finish()
    }
}

impl<S> PeriodBook<S> {
    /// Starts accounting arrivals after `start_ms`.
    pub fn new(start_ms: u64) -> Self {
        PeriodBook {
            high_water_ms: start_ms,
            open: BTreeMap::new(),
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
    pub fn advance(
        &mut self,
        to_ms: u64,
        now_ms: u64,
        config: &RecorderConfig,
        mut open: impl FnMut() -> S,
        mut add: impl FnMut(&mut S, u64, u64),
    ) -> Vec<ClosedPeriod<S>> {
        let period_ms = config.period_ms.max(1);
        // A frontier far ahead of the wall clock, for example after a clock jump, would
        // otherwise open one period per period length up to it.
        let cap = now_ms.saturating_add(period_ms);
        let to_ms = to_ms.min(cap - cap % period_ms);
        if self.finished || to_ms <= self.high_water_ms {
            return Vec::new();
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
        // NOTE: A changed period length can leave a later-starting period that ends before an
        // earlier one, so every period is checked instead of stopping at the first open one.
        let (closed, open): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut self.open)
            .into_iter()
            .partition(|(_, p)| p.end_ms <= to_ms);
        self.open = open;
        closed.into_iter().map(Self::close).collect()
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
            let start = from - from % period_ms;
            let end = start.saturating_add(period_ms);
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
        // NOTE: Keyed by the end as well, because a changed period length can give a new
        // period the start of an open one with a different end. Sharing the entry would
        // charge arrivals past the open period's end to it.
        let period = self.open.entry((start, end)).or_insert_with(|| OpenPeriod {
            end_ms: end,
            state: open(),
        });
        add(&mut period.state, from, to);
    }

    /// Returns every open period, including those that extend past the high-water mark, and
    /// stops accepting arrivals. The last arrival of the object is the high-water mark.
    pub fn finish(&mut self) -> Vec<ClosedPeriod<S>> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        std::mem::take(&mut self.open)
            .into_iter()
            .map(Self::close)
            .collect()
    }

    fn close(((start_ms, _), p): ((u64, u64), OpenPeriod<S>)) -> ClosedPeriod<S> {
        ClosedPeriod {
            start_ms,
            end_ms: p.end_ms,
            state: p.state,
        }
    }
}
