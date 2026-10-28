// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Controller-side description of how an object's output frontier follows arrival time.

use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, Timestamp};

/// How a wall-clock leaf advances its frontier.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum Ticker {
    /// Ticks at multiples of `interval_ms` since the epoch.
    Aligned { interval_ms: u64 },
    /// Ticks every `interval_ms` at an unknown phase.
    Unaligned { interval_ms: u64 },
    /// Follows wall clock continuously.
    Continuous,
}

impl Ticker {
    /// The interval of the grid this ticker contributes to window boundaries, `None` if every
    /// millisecond is a boundary.
    fn grid_ms(&self) -> Option<u64> {
        match self {
            // NOTE: Unaligned leaves have an unknown phase, so we approximate them with the
            // epoch-aligned grid of their interval. A zero interval degenerates to continuous.
            Ticker::Aligned { interval_ms } | Ticker::Unaligned { interval_ms } => {
                Some(*interval_ms).filter(|i| *i > 0)
            }
            Ticker::Continuous => None,
        }
    }
}

/// A source of arrival-time dependence.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum FreshnessLeaf {
    /// An ingestion whose ticker is looked up at [`FreshnessTree::resolve`] time.
    Ingestion(GlobalId),
    /// A leaf with a known ticker.
    Fixed(Ticker),
}

/// How an object's frontier depends on the leaves it transitively reads.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum FreshnessTree {
    Leaf(FreshnessLeaf),
    /// The frontier is the minimum over the inputs.
    Min(Vec<FreshnessTree>),
    /// Output only changes at refreshes of `schedule`.
    Refresh {
        schedule: RefreshSchedule,
        input: Box<FreshnessTree>,
    },
}

/// Resolves the current ticker of an ingestion.
pub trait TickerLookup {
    fn ingestion_ticker(&self, id: GlobalId) -> Option<Ticker>;
}

impl FreshnessTree {
    /// Combines the trees of an object's inputs under an optional refresh schedule.
    ///
    /// Equal inputs are deduplicated. Returns `None` if there are no inputs.
    pub fn merge(
        mut inputs: Vec<FreshnessTree>,
        schedule: Option<&RefreshSchedule>,
    ) -> Option<FreshnessTree> {
        inputs.sort();
        inputs.dedup();
        let inner = match inputs.len() {
            0 => return None,
            1 => inputs.pop().expect("length checked"),
            _ => FreshnessTree::Min(inputs),
        };
        Some(match schedule {
            Some(schedule) => FreshnessTree::Refresh {
                schedule: schedule.clone(),
                input: Box::new(inner),
            },
            None => inner,
        })
    }

    /// Fixes all ingestion tickers. Returns `None` if any ingestion is unknown to `lookup`.
    pub fn resolve(&self, lookup: &dyn TickerLookup) -> Option<ResolvedTree> {
        Some(match self {
            FreshnessTree::Leaf(FreshnessLeaf::Fixed(t)) => ResolvedTree::Leaf(*t),
            FreshnessTree::Leaf(FreshnessLeaf::Ingestion(id)) => {
                ResolvedTree::Leaf(lookup.ingestion_ticker(*id)?)
            }
            FreshnessTree::Min(inputs) => ResolvedTree::Min(
                inputs
                    .iter()
                    .map(|i| i.resolve(lookup))
                    .collect::<Option<Vec<_>>>()?,
            ),
            FreshnessTree::Refresh { schedule, input } => ResolvedTree::Refresh {
                schedule: schedule.clone(),
                input: Box::new(input.resolve(lookup)?),
            },
        })
    }
}

/// A [`FreshnessTree`] with all tickers fixed.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResolvedTree {
    Leaf(Ticker),
    Min(Vec<ResolvedTree>),
    Refresh {
        schedule: RefreshSchedule,
        input: Box<ResolvedTree>,
    },
}

impl ResolvedTree {
    /// The largest arrival time whose updates are all visible below `frontier`.
    ///
    /// Returns `None` if no arrival time is visible, for example before the first refresh.
    pub fn last_reflected(&self, frontier: Timestamp) -> Option<u64> {
        // The largest output timestamp that is complete.
        let bound = u64::from(frontier).checked_sub(1)?;
        self.last_reflected_at(bound)
    }

    /// `last_reflected` for the inclusive bound `bound` on the complete output timestamps.
    fn last_reflected_at(&self, bound: u64) -> Option<u64> {
        match self {
            // An arrival at `t` is assigned the timestamp `ceil(t, i)`, so it is visible
            // exactly if `t` is at most `bound` rounded down to the grid.
            ResolvedTree::Leaf(ticker) => Some(match ticker.grid_ms() {
                Some(i) => bound - bound % i,
                None => bound,
            }),
            ResolvedTree::Min(inputs) => inputs
                .iter()
                .map(|i| i.last_reflected_at(bound))
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .min(),
            // Updates land at the next refresh, so only those at or before the last refresh
            // at or below `bound` are visible.
            ResolvedTree::Refresh { schedule, input } => {
                let refresh =
                    schedule.round_down_timestamp_m1(Timestamp::from(bound.checked_add(1)?))?;
                // NOTE: `round_down_timestamp_m1` saturates at timestamp 0 when the previous
                // refresh would be negative, so 0 only counts if it is an actual refresh.
                if schedule.round_up_timestamp(refresh) != Some(refresh) {
                    return None;
                }
                input.last_reflected_at(u64::from(refresh))
            }
        }
    }

    /// The smallest window boundary at or after `z`, the union of all aligned and unaligned
    /// leaf grids. Every millisecond is a boundary if a continuous leaf exists.
    pub fn next_boundary(&self, z: u64) -> u64 {
        let mut best: Option<u64> = None;
        let mut continuous = false;
        self.for_each_ticker(&mut |t| match t.grid_ms() {
            Some(i) => {
                let b = z.div_ceil(i).saturating_mul(i);
                best = Some(best.map_or(b, |x| x.min(b)));
            }
            None => continuous = true,
        });
        if continuous { z } else { best.unwrap_or(z) }
    }

    /// The tickers of all leaves in tree order, the cache key for the assignment bound.
    pub fn tickers(&self) -> Vec<Ticker> {
        let mut out = Vec::new();
        self.for_each_ticker(&mut |t| out.push(*t));
        out
    }

    fn for_each_ticker(&self, f: &mut dyn FnMut(&Ticker)) {
        match self {
            ResolvedTree::Leaf(t) => f(t),
            ResolvedTree::Min(inputs) => inputs.iter().for_each(|i| i.for_each_ticker(f)),
            ResolvedTree::Refresh { input, .. } => input.for_each_ticker(f),
        }
    }
}

#[cfg(test)]
mod tests;
