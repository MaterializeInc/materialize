// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Controller-side description of how an object's output frontier follows arrival time.

use std::collections::BTreeSet;
use std::time::Duration;

use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, Timestamp};

/// The most change points [`ResolvedTree::assignment_bound`] scans before it falls back to a
/// conservative bound.
const MAX_CHANGE_POINTS: u128 = 1_000_000;

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

    /// The worst-case delay D in ms from an arrival to its visibility when every tick and refresh
    /// is on time. Never below the true worst case.
    ///
    /// The supremum is not attained in continuous time, so D is one more than the largest
    /// single-arrival latency on the integer millisecond grid.
    pub fn assignment_bound(&self) -> u64 {
        if !self.has_refresh() {
            let mut leaves = Vec::new();
            self.collect_leaves(&mut leaves);
            return closed_form(&leaves);
        }
        self.scan_bound()
            .unwrap_or_else(|| self.conservative_bound())
    }

    fn has_refresh(&self) -> bool {
        match self {
            ResolvedTree::Leaf(_) => false,
            ResolvedTree::Min(inputs) => inputs.iter().any(|i| i.has_refresh()),
            ResolvedTree::Refresh { .. } => true,
        }
    }

    /// Collects the leaves reachable through `Min` nodes only, and returns the refresh nodes
    /// that block the descent.
    fn collect_leaves<'a>(&'a self, leaves: &mut Vec<Ticker>) -> Vec<&'a ResolvedTree> {
        match self {
            ResolvedTree::Leaf(t) => {
                leaves.push(*t);
                Vec::new()
            }
            ResolvedTree::Min(inputs) => inputs
                .iter()
                .flat_map(|i| i.collect_leaves(leaves))
                .collect(),
            ResolvedTree::Refresh { .. } => vec![self],
        }
    }

    /// An upper bound that needs no scan, used if the scan would be too large.
    fn conservative_bound(&self) -> u64 {
        let mut leaves = Vec::new();
        let others = self.collect_leaves(&mut leaves);
        let mut bound = closed_form(&leaves);
        for other in others {
            let ResolvedTree::Refresh { schedule, input } = other else {
                unreachable!("collect_leaves only returns refresh nodes");
            };
            // A refresh delays an arrival by at most one period on top of its input's bound.
            bound = bound
                .saturating_add(input.conservative_bound())
                .saturating_add(max_period_ms(schedule));
        }
        bound
    }

    /// The largest `w - last_reflected(expected_upper(w))` over the points where the expected
    /// frontier changes in one hyperperiod, plus one. Returns `None` if the scan is too large
    /// or a schedule is not periodic.
    fn scan_bound(&self) -> Option<u64> {
        let mut grids = Vec::new();
        let mut unaligned = Vec::new();
        let mut schedules = Vec::new();
        self.for_each_ticker(&mut |t| match t {
            Ticker::Aligned { interval_ms } => grids.extend(Some(*interval_ms).filter(|i| *i > 0)),
            Ticker::Unaligned { interval_ms } => {
                if *interval_ms > 0 {
                    grids.push(*interval_ms);
                    unaligned.push(*interval_ms);
                }
            }
            Ticker::Continuous => {}
        });
        self.for_each_schedule(&mut |s| schedules.push(s));
        // One-shot refreshes break periodicity.
        if schedules.iter().any(|s| !s.ats.is_empty()) {
            return None;
        }
        let periods: Vec<u64> = schedules
            .iter()
            .flat_map(|s| s.everies.iter().map(|e| duration_ms(e.interval)))
            .collect();
        if periods.iter().any(|p| *p == 0) {
            return None;
        }
        let all: Vec<u64> = grids.iter().chain(periods.iter()).copied().collect();
        let mut hyper = 1u128;
        for i in &all {
            hyper = lcm(hyper, u128::from(*i));
            if hyper > u128::from(u64::MAX) / 8 {
                return None;
            }
        }
        // Each unaligned leaf adds two shifted copies of every point.
        let copies = 1 + 2 * u128::try_from(unaligned.len()).ok()?;
        let points: u128 = all.iter().map(|i| hyper / u128::from(*i)).sum::<u128>() * copies;
        if points > MAX_CHANGE_POINTS {
            return None;
        }
        let hyper = u64::try_from(hyper).ok()?;
        // Start far enough from 0 that no `last_reflected` lookback saturates, one hyperperiod
        // for every refresh stage plus one for the leaves.
        let stages = u64::try_from(schedules.len()).ok()?.checked_add(2)?;
        let base = hyper.checked_mul(stages)?;
        let end = base.checked_add(hyper)?;

        let mut change_points = BTreeSet::new();
        change_points.insert(end);
        for i in &grids {
            let mut c = base;
            while c < end {
                c += i;
                change_points.insert(c);
            }
        }
        for schedule in &schedules {
            let mut from = base + 1;
            while let Some(r) = schedule.round_up_timestamp(Timestamp::from(from)) {
                let r = u64::from(r);
                if r > end {
                    break;
                }
                change_points.insert(r);
                from = r + 1;
            }
        }
        // The expected frontier of an unaligned leaf trails the wall clock by its interval, so
        // every change point `c` shows up again at `c + u`. The point before it is kept as well
        // because the bound is read just before a change.
        for u in unaligned {
            let shifted: Vec<u64> = change_points
                .iter()
                .flat_map(|c| [c + u - 1, c + u])
                .collect();
            change_points.extend(shifted);
        }

        change_points
            .into_iter()
            .filter_map(|c| {
                let w = c - 1;
                let reflected = self.last_reflected(self.expected_upper(w)?)?;
                Some(w.saturating_sub(reflected))
            })
            .max()
            .map(|d| d.saturating_add(1))
    }

    /// The frontier of the output at wall clock `now` if every tick and refresh is on time.
    fn expected_upper(&self, now: u64) -> Option<Timestamp> {
        Some(Timestamp::from(match self {
            ResolvedTree::Leaf(Ticker::Aligned { interval_ms }) if *interval_ms > 0 => {
                // A tick at `t` leaves the frontier at `t + 1`.
                now - now % interval_ms + 1
            }
            // NOTE: An unaligned leaf has an unknown phase, so we assume the worst: the last
            // tick was a full interval ago.
            ResolvedTree::Leaf(Ticker::Unaligned { interval_ms }) if *interval_ms > 0 => {
                now.saturating_sub(*interval_ms) + 1
            }
            ResolvedTree::Leaf(_) => now + 1,
            ResolvedTree::Min(inputs) => inputs
                .iter()
                .map(|i| i.expected_upper(now).map(u64::from))
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .min()?,
            ResolvedTree::Refresh { schedule, input } => {
                return schedule.round_up_timestamp(input.expected_upper(now)?);
            }
        }))
    }

    fn for_each_schedule<'a>(&'a self, f: &mut dyn FnMut(&'a RefreshSchedule)) {
        match self {
            ResolvedTree::Leaf(_) => {}
            ResolvedTree::Min(inputs) => inputs.iter().for_each(|i| i.for_each_schedule(f)),
            ResolvedTree::Refresh { schedule, input } => {
                f(schedule);
                input.for_each_schedule(f);
            }
        }
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

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The longest gap between two refreshes of `schedule`.
fn max_period_ms(schedule: &RefreshSchedule) -> u64 {
    let everies = schedule.everies.iter().map(|e| duration_ms(e.interval));
    let mut ats = schedule.ats.clone();
    ats.sort();
    let gaps = ats.windows(2).map(|w| u64::from(w[1]) - u64::from(w[0]));
    everies.chain(gaps).max().unwrap_or(0)
}

fn gcd(a: u128, b: u128) -> u128 {
    if b == 0 { a } else { gcd(b, a % b) }
}

fn lcm(a: u128, b: u128) -> u128 {
    a / gcd(a, b) * b
}

/// D of a minimum over `leaves` without refreshes.
///
/// An arrival is first assigned a timestamp on the grid of one leaf, which costs up to that
/// leaf's interval, and then waits for another leaf's frontier to pass it, which costs that
/// leaf's alignment term. D is the maximum over pairs of leaves, or the interval of the single
/// leaf.
///
/// NOTE: For two aligned leaves the alignment term is `i_m - gcd(i_k, i_m)`. An unaligned leaf
/// has an arbitrary phase, so its alignment term is its full interval. A continuous leaf
/// contributes 0 both ways. The proof for aligned leaves is in the appendix "the assignment bound
/// of a join" of `doc/developer/design/20261028_tick_aware_freshness.md`, and the Lean model
/// `Mz.Freshness.latency_le` covers the two-input case.
fn closed_form(leaves: &[Ticker]) -> u64 {
    // The assignment term of a leaf, 0 for a leaf that follows wall clock.
    fn interval(t: &Ticker) -> u64 {
        t.grid_ms().unwrap_or(0)
    }
    fn pair(a: &Ticker, b: &Ticker) -> u64 {
        let (i, j) = (interval(a), interval(b));
        match (a, b) {
            (Ticker::Aligned { .. }, Ticker::Aligned { .. }) if i > 0 && j > 0 => {
                let g = u64::try_from(gcd(u128::from(i), u128::from(j))).unwrap_or(1);
                i.saturating_add(j) - g
            }
            _ => i.saturating_add(j),
        }
    }
    match leaves {
        [] => 0,
        [one] => interval(one),
        _ => {
            let mut best = 0;
            for (k, a) in leaves.iter().enumerate() {
                for b in &leaves[k + 1..] {
                    best = best.max(pair(a, b));
                }
            }
            best
        }
    }
}

#[cfg(test)]
mod tests;
