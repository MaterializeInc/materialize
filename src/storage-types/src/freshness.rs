// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Controller-side description of how an object's output frontier follows arrival time.

pub mod histogram;
pub mod local;
mod period;
pub mod recorder;

use std::collections::BTreeSet;
use std::time::Duration;

use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, Timestamp};

/// The most change points [`ResolvedTree::assignment_bound`] scans before it falls back to a
/// conservative bound.
const MAX_CHANGE_POINTS: u128 = 1_000_000;

/// Bounds in ms on one subtree when every tick and refresh is on time.
struct Bounds {
    /// Most an arrival's timestamp can exceed the arrival time.
    assign: u64,
    /// Most the frontier can trail the timestamp it covers.
    lag: u64,
    /// Most the visibility of an arrival can trail the arrival.
    delay: u64,
}

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
    /// A value that changes whenever `ingestion_ticker` may return a different ticker for some
    /// ingestion. A tree resolved under an unchanged generation stays valid.
    fn generation(&self) -> u64;
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
                input.last_reflected_at(last_refresh_at_or_before(schedule, bound)?)
            }
        }
    }

    /// The timestamp an arrival at `arrival` is assigned, `None` if it never becomes visible, as
    /// after the last refresh of a schedule without `EVERY`.
    ///
    /// It is the counterpart of `last_reflected`: an arrival is visible below `frontier` exactly
    /// if its assigned timestamp is below `frontier`.
    pub fn assigned(&self, arrival: u64) -> Option<u64> {
        match self {
            ResolvedTree::Leaf(ticker) => Some(match ticker.grid_ms() {
                Some(i) => arrival.div_ceil(i).saturating_mul(i),
                None => arrival,
            }),
            ResolvedTree::Min(inputs) => inputs
                .iter()
                .map(|i| i.assigned(arrival))
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .max(),
            ResolvedTree::Refresh { schedule, input } => schedule
                .round_up_timestamp(Timestamp::from(input.assigned(arrival)?))
                .map(u64::from),
        }
    }

    /// How long after wall clock `timestamp` the frontier first passes `timestamp` when every
    /// tick and refresh is on time, at the earliest for any phase of the unaligned leaves.
    ///
    /// An aligned leaf waits for its next tick at or after `timestamp`, and a continuous one
    /// waits 0. An unaligned leaf ticks at a phase the controller does not know, so it can
    /// tick right at `timestamp` and waits 0. A minimum waits for its slowest input.
    pub fn grid_wait(&self, timestamp: u64) -> u64 {
        match self {
            ResolvedTree::Leaf(Ticker::Aligned { interval_ms }) if *interval_ms > 0 => {
                timestamp
                    .div_ceil(*interval_ms)
                    .saturating_mul(*interval_ms)
                    - timestamp
            }
            ResolvedTree::Leaf(_) => 0,
            ResolvedTree::Min(inputs) => inputs
                .iter()
                .map(|i| i.grid_wait(timestamp))
                .max()
                .unwrap_or(0),
            // The output frontier rounds the input frontier up to the next refresh, so it
            // passes `timestamp` once the input passed the last refresh at or before it. Before
            // the first refresh the output frontier is past `timestamp` already.
            ResolvedTree::Refresh { schedule, input } => {
                let Some(refresh) = last_refresh_at_or_before(schedule, timestamp) else {
                    return 0;
                };
                let passed = refresh.saturating_add(input.grid_wait(refresh));
                passed.saturating_sub(timestamp)
            }
        }
    }

    /// The latest wall clock at which the frontier reflects `arrival` when every tick and
    /// refresh is on time, for any phase of the unaligned leaves. `None` if it never does, as
    /// after the last refresh of a schedule without `EVERY`.
    ///
    /// An unaligned leaf ticks at most one interval after any time, so it shows an arrival at
    /// the latest one interval after it.
    pub fn latest_show(&self, arrival: u64) -> Option<u64> {
        match self {
            ResolvedTree::Leaf(_) => self.latest_assigned(arrival),
            // Every input has to pass the timestamp the minimum assigns, which is at most
            // `latest_assigned`.
            ResolvedTree::Min(inputs) => {
                let timestamp = self.latest_assigned(arrival)?;
                inputs.iter().map(|i| i.latest_pass(timestamp)).max()
            }
            // The output passes a refresh once the input passed it.
            ResolvedTree::Refresh { input, .. } => {
                let refresh = self.latest_assigned(arrival)?;
                Some(input.latest_pass(refresh).max(refresh))
            }
        }
    }

    /// The latest timestamp any phase of the unaligned leaves assigns `arrival`, `None` if no
    /// refresh comes after it.
    fn latest_assigned(&self, arrival: u64) -> Option<u64> {
        match self {
            ResolvedTree::Leaf(Ticker::Unaligned { interval_ms }) => {
                Some(arrival.saturating_add(*interval_ms))
            }
            ResolvedTree::Leaf(_) => self.assigned(arrival),
            ResolvedTree::Min(inputs) => inputs
                .iter()
                .map(|i| i.latest_assigned(arrival))
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .max(),
            ResolvedTree::Refresh { schedule, input } => schedule
                .round_up_timestamp(Timestamp::from(input.latest_assigned(arrival)?))
                .map(u64::from),
        }
    }

    /// The latest wall clock at which the frontier passes `timestamp` when every tick and
    /// refresh is on time, for any phase of the unaligned leaves, at least `timestamp`.
    fn latest_pass(&self, timestamp: u64) -> u64 {
        match self {
            ResolvedTree::Leaf(Ticker::Unaligned { interval_ms }) => {
                timestamp.saturating_add(*interval_ms)
            }
            ResolvedTree::Leaf(_) => timestamp.saturating_add(self.grid_wait(timestamp)),
            ResolvedTree::Min(inputs) => inputs
                .iter()
                .map(|i| i.latest_pass(timestamp))
                .max()
                .unwrap_or(timestamp),
            // See `grid_wait`.
            ResolvedTree::Refresh { schedule, input } => {
                match last_refresh_at_or_before(schedule, timestamp) {
                    None => timestamp,
                    Some(refresh) => input.latest_pass(refresh).max(timestamp),
                }
            }
        }
    }

    /// The input of a top-level refresh node, or the tree itself if it has none.
    pub fn below_refresh(&self) -> &ResolvedTree {
        match self {
            ResolvedTree::Refresh { input, .. } => input,
            _ => self,
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

    /// The largest window boundary at or before `z`, the counterpart of `next_boundary`.
    pub fn prev_boundary(&self, z: u64) -> u64 {
        let mut best: Option<u64> = None;
        let mut continuous = false;
        self.for_each_ticker(&mut |t| match t.grid_ms() {
            Some(i) => {
                let b = z - z % i;
                best = Some(best.map_or(b, |x| x.max(b)));
            }
            None => continuous = true,
        });
        if continuous { z } else { best.unwrap_or(z) }
    }

    /// The worst-case delay D in ms from an arrival to its visibility when every tick and refresh
    /// is on time. Never below the true worst case in the steady state.
    ///
    /// The supremum is not attained in continuous time, so D is one more than the largest
    /// single-arrival latency on the integer millisecond grid. Returns `None` if the delay is
    /// unbounded, which is the case for a refresh node whose schedule has no `EVERY`: arrivals
    /// after its last `AT` never become visible. Where a schedule has both, the `AT`s only add
    /// refreshes and are ignored, which keeps the bound valid.
    pub fn assignment_bound(&self) -> Option<u64> {
        self.assignment_bound_with_cap(MAX_CHANGE_POINTS)
    }

    /// `assignment_bound` with the scan size limit as a parameter, so tests can force the
    /// conservative path.
    fn assignment_bound_with_cap(&self, max_change_points: u128) -> Option<u64> {
        let mut unbounded = false;
        self.for_each_schedule(&mut |s| unbounded |= s.everies.is_empty());
        if unbounded {
            return None;
        }
        if !self.has_refresh() {
            let mut leaves = Vec::new();
            self.collect_leaves(&mut leaves);
            return Some(closed_form(&leaves));
        }
        let periodic = self.without_ats();
        periodic
            .scan_bound(max_change_points)
            .or_else(|| periodic.conservative_bound())
    }

    fn has_refresh(&self) -> bool {
        match self {
            ResolvedTree::Leaf(_) => false,
            ResolvedTree::Min(inputs) => inputs.iter().any(|i| i.has_refresh()),
            ResolvedTree::Refresh { .. } => true,
        }
    }

    fn without_ats(&self) -> ResolvedTree {
        match self {
            ResolvedTree::Leaf(t) => ResolvedTree::Leaf(*t),
            ResolvedTree::Min(inputs) => {
                ResolvedTree::Min(inputs.iter().map(|i| i.without_ats()).collect())
            }
            ResolvedTree::Refresh { schedule, input } => ResolvedTree::Refresh {
                schedule: RefreshSchedule {
                    everies: schedule.everies.clone(),
                    ats: Vec::new(),
                },
                input: Box::new(input.without_ats()),
            },
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

    /// An upper bound on D that needs no scan. `None` if a refresh node has no `EVERY`.
    fn conservative_bound(&self) -> Option<u64> {
        self.bounds().map(|b| b.delay)
    }

    /// The assignment, lag and delay bounds of this subtree, see [`Bounds`].
    ///
    /// Let `T(t)` be the timestamp an arrival at `t` ends up with and `f(x)` the wall time at
    /// which the frontier first covers timestamp `x`. A leaf rounds up to its grid, so both
    /// move by at most its interval, and the delay of a lone leaf is that interval. A minimum
    /// takes the maximum of its inputs for both. A refresh with period `r` over `Y` rounds
    /// `T` up to a refresh point `R <= T_Y + r` and then defers to `Y`, so `T(t) - t <= r + A(Y)`,
    /// `f(x) - x <= r + L(Y)` for any `x`, and, because `f` is then evaluated exactly at the
    /// refresh point `R`, the delay is at most `R - t + L(Y)`, which is `r + A(Y) + L(Y)` with
    /// the period counted once.
    ///
    /// For a minimum, the delay is `f(T) - t` where `T` and `f` each come from one input. Two
    /// leaves combine as in [`closed_form`]. For any other pair, `T` from input `k` and `f` from
    /// input `m` give at most `A(k) + L(m)`, and `k = m` is that input's own delay.
    fn bounds(&self) -> Option<Bounds> {
        match self {
            ResolvedTree::Leaf(_) | ResolvedTree::Min(_) => {
                let mut leaves = Vec::new();
                let others = self.collect_leaves(&mut leaves);
                let others: Vec<Bounds> = others
                    .iter()
                    .map(|o| o.bounds())
                    .collect::<Option<Vec<_>>>()?;
                let leaf_max = leaves.iter().filter_map(|t| t.grid_ms()).max().unwrap_or(0);
                let mut bounds = Bounds {
                    assign: leaf_max,
                    lag: leaf_max,
                    delay: closed_form(&leaves),
                };
                for (j, o) in others.iter().enumerate() {
                    bounds.assign = bounds.assign.max(o.assign);
                    bounds.lag = bounds.lag.max(o.lag);
                    bounds.delay = bounds.delay.max(o.delay);
                    // Every leaf against this refresh node, in both directions.
                    bounds.delay = bounds
                        .delay
                        .max(leaf_max.saturating_add(o.lag.max(o.assign)));
                    for (k, p) in others.iter().enumerate() {
                        if j != k {
                            bounds.delay = bounds.delay.max(p.assign.saturating_add(o.lag));
                        }
                    }
                }
                Some(bounds)
            }
            ResolvedTree::Refresh { schedule, input } => {
                let period = schedule
                    .everies
                    .iter()
                    .map(|e| duration_ms(e.interval))
                    .max()?;
                let inner = input.bounds()?;
                Some(Bounds {
                    assign: period.saturating_add(inner.assign),
                    lag: period.saturating_add(inner.lag),
                    delay: period
                        .saturating_add(inner.assign)
                        .saturating_add(inner.lag),
                })
            }
        }
    }

    /// The largest `w - last_reflected(expected_upper(w))` over the points where the expected
    /// frontier changes in one hyperperiod, plus one. Returns `None` if the tree has unaligned
    /// leaves, the scan has more than `max_change_points` points, or it cannot be set up.
    fn scan_bound(&self, max_change_points: u128) -> Option<u64> {
        let mut grids = Vec::new();
        let mut has_unaligned = false;
        let mut schedules = Vec::new();
        self.for_each_ticker(&mut |t| match t {
            Ticker::Aligned { interval_ms } => grids.extend(Some(*interval_ms).filter(|i| *i > 0)),
            Ticker::Unaligned { .. } => has_unaligned = true,
            Ticker::Continuous => {}
        });
        // NOTE: The scan reads `last_reflected`, which floors an unaligned leaf to the epoch
        // grid of its interval, so it can undershoot the delay of a phase that is off that grid
        // when a refresh sits on top. The conservative bound is the safe choice there.
        if has_unaligned {
            return None;
        }
        self.for_each_schedule(&mut |s| schedules.push(s));
        let periods: Vec<u64> = schedules
            .iter()
            .flat_map(|s| s.everies.iter().map(|e| duration_ms(e.interval)))
            .collect();
        if periods.contains(&0) {
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
        let points: u128 = all.iter().map(|i| hyper / u128::from(*i)).sum();
        if points > max_change_points {
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
            // NOTE: `scan_bound` never runs on unaligned leaves, so the epoch grid here only
            // matters for aligned ones.
            ResolvedTree::Leaf(
                Ticker::Aligned { interval_ms } | Ticker::Unaligned { interval_ms },
            ) if *interval_ms > 0 => {
                // A tick at `t` leaves the frontier at `t + 1`.
                now - now % interval_ms + 1
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

/// The last refresh of `schedule` at or before `timestamp`, if any.
fn last_refresh_at_or_before(schedule: &RefreshSchedule, timestamp: u64) -> Option<u64> {
    let refresh = schedule.round_down_timestamp_m1(Timestamp::from(timestamp.checked_add(1)?))?;
    // NOTE: `round_down_timestamp_m1` saturates at timestamp 0 when the previous refresh would
    // be negative, so 0 only counts if it is an actual refresh.
    (schedule.round_up_timestamp(refresh) == Some(refresh)).then(|| u64::from(refresh))
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
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
/// contributes 0 both ways. The aligned alignment term holds because the multiples of `i_k`
/// reduce modulo `i_m` to the multiples of `g = gcd(i_k, i_m)` (Bezout), so the distance to
/// the next multiple of `i_m` is at most `i_m - g` and some multiple reaches it. The supremum
/// over arrivals then adds the assignment term of up to `i_k` just below the tick.
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
