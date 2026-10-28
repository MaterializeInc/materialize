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
/// length, so every arrival time belongs to exactly one period. A period is handed out once,
/// when its end is at most the high-water mark, which makes everything handed out final.
#[derive(Debug)]
pub(super) struct PeriodBook<S> {
    high_water_ms: u64,
    open: BTreeMap<(u64, u64), OpenPeriod<S>>,
    finished: bool,
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

    /// Advances the high-water mark to `to_ms` and returns the periods that closed.
    ///
    /// `add` receives the state of each period and the part `(from, to]` of the new span that
    /// falls into it. `open` creates the state of a period when its first part arrives, so the
    /// state can capture configuration that later calls change. A `to_ms` at or below the
    /// high-water mark, and any call after `finish`, does nothing.
    pub fn advance(
        &mut self,
        to_ms: u64,
        period_ms: u64,
        mut open: impl FnMut() -> S,
        mut add: impl FnMut(&mut S, u64, u64),
    ) -> Vec<ClosedPeriod<S>> {
        if self.finished || to_ms <= self.high_water_ms {
            return Vec::new();
        }
        let period_ms = period_ms.max(1);
        let mut from = self.high_water_ms;
        while from < to_ms {
            let start = from - from % period_ms;
            let end = start.saturating_add(period_ms);
            let part_end = to_ms.min(end);
            // NOTE: Keyed by the end as well, because a changed period length can give a new
            // period the start of an open one with a different end. Sharing the entry would
            // charge arrivals past the open period's end to it.
            let period = self.open.entry((start, end)).or_insert_with(|| OpenPeriod {
                end_ms: end,
                state: open(),
            });
            add(&mut period.state, from, part_end);
            from = part_end;
        }
        self.high_water_ms = to_ms;
        // NOTE: A changed period length can leave a later-starting period that ends before an
        // earlier one, so every period is checked instead of stopping at the first open one.
        let (closed, open): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut self.open)
            .into_iter()
            .partition(|(_, p)| p.end_ms <= to_ms);
        self.open = open;
        closed.into_iter().map(Self::close).collect()
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
