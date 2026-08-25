// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! What a worker activation may spend walking index peeks before it hands the worker back.

use mz_compute_types::dyncfgs::{
    ENABLE_INDEX_PEEK_OFFLOAD, INDEX_PEEK_ACTIVATION_BUDGET, INDEX_PEEK_INLINE_BUDGET,
};
use mz_dyncfg::ConfigSet;

/// The fuel an activation may spend walking index peeks on the worker, and what one peek may take
/// of it.
///
/// Two counters rather than one, and the second is not optional. The per-peek budget is the
/// placement policy: a peek that outruns it has been measured to be expensive and moves off the
/// worker, which is what lets a point lookup over a skewed hot key offload without being
/// special-cased. The aggregate bounds how long one activation withholds the worker from
/// everything else it serves, because the peek path visits every pending peek on every activation,
/// so a per-peek budget alone would let N pending peeks cost N times that budget in a single pass,
/// unbounded in N.
///
/// Both are counted in cursor positions visited rather than rows returned, matching the unit the
/// scan charges: a selective filter steps the cursor without returning anything, so a row-counted
/// budget is one such a peek never spends.
pub(super) enum InlineBudget {
    /// Every peek walks to completion where it started, and nothing is promoted or passed over.
    ///
    /// This is what the kill switch restores, and it has to be what the worker did before the
    /// offload existed rather than an approximation of it. Unbounded fuel is also what makes
    /// promotion unreachable: the scan suspends only when its fuel runs out or when it holds a
    /// full batch, and the first cannot happen here, so the only suspension left is the one that
    /// goes to the peek stash exactly as it did before.
    Unbounded,
    /// A peek may spend `per_peek` before it is promoted, and all peeks together may spend
    /// `remaining` before the rest of this activation's work gets the worker back.
    Bounded { per_peek: usize, remaining: usize },
}

impl InlineBudget {
    /// Reads the budget in effect now.
    ///
    /// Read once per activation rather than held, so a configuration change reaches the next
    /// activation without disturbing the one under way.
    pub(super) fn for_activation(config: &ConfigSet) -> Self {
        if !ENABLE_INDEX_PEEK_OFFLOAD.get(config) {
            return Self::Unbounded;
        }

        Self::Bounded {
            per_peek: INDEX_PEEK_INLINE_BUDGET.get(config),
            remaining: INDEX_PEEK_ACTIVATION_BUDGET.get(config),
        }
    }

    /// The fuel one peek's slice may spend, or `None` when this activation has none left to give.
    ///
    /// A peek is granted its whole per-peek budget or nothing at all, so promotion keeps meaning
    /// that the peek outran that budget rather than that it arrived late in an activation. The
    /// aggregate is therefore overrun by at most one per-peek budget, which is also what keeps an
    /// inline budget configured above the aggregate from passing over every peek forever.
    ///
    /// A peek granted nothing is passed over rather than stepped with an empty budget. A scan
    /// stepped with no fuel suspends without walking anything, which would promote a peek that
    /// never had its inline turn.
    pub(super) fn grant(&self) -> Option<usize> {
        match self {
            Self::Unbounded => Some(usize::MAX),
            Self::Bounded {
                per_peek,
                remaining,
            } => (*remaining > 0).then_some(*per_peek),
        }
    }

    /// Charges the activation for the positions a slice walked.
    pub(super) fn charge(&mut self, spent: usize) {
        match self {
            Self::Unbounded => {}
            Self::Bounded { remaining, .. } => *remaining = remaining.saturating_sub(spent),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigUpdates;

    use super::*;

    /// With the offload off, every peek is granted unbounded fuel however much the peeks before it
    /// spent, which is what makes the kill switch restore the worker's old behaviour rather than
    /// approximate it.
    #[mz_ore::test]
    fn the_kill_switch_grants_every_peek_an_unbounded_slice() {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut budget = InlineBudget::for_activation(&config);

        for _ in 0..3 {
            assert_eq!(budget.grant(), Some(usize::MAX));
            budget.charge(usize::MAX);
        }
    }

    /// The aggregate is spent by what the peeks walked, not by what they were granted, so cheap
    /// peeks keep getting turns and expensive ones drain it.
    #[mz_ore::test]
    fn the_aggregate_is_spent_by_what_the_peeks_walk() {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&INDEX_PEEK_INLINE_BUDGET, 100);
        updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, 250);
        updates.apply(&config);

        let mut budget = InlineBudget::for_activation(&config);

        // Point-lookup-sized slices barely touch the aggregate.
        for _ in 0..10 {
            assert_eq!(budget.grant(), Some(100));
            budget.charge(2);
        }

        // Slices that spend the whole per-peek budget drain it, and the peeks behind them are
        // passed over rather than served with a partial slice.
        assert_eq!(budget.grant(), Some(100));
        budget.charge(100);
        assert_eq!(budget.grant(), Some(100));
        budget.charge(100);
        assert_eq!(budget.grant(), Some(100));
        budget.charge(100);
        assert_eq!(budget.grant(), None);
    }

    /// An inline budget larger than the aggregate still serves one peek per activation. Without
    /// that, no peek would ever be granted a slice and every peek would hang.
    #[mz_ore::test]
    fn an_inline_budget_above_the_aggregate_still_serves_one_peek() {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&INDEX_PEEK_INLINE_BUDGET, 1_000_000_000);
        updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, 1);
        updates.apply(&config);

        let mut budget = InlineBudget::for_activation(&config);

        assert_eq!(budget.grant(), Some(1_000_000_000));
        budget.charge(1_000_000_000);
        assert_eq!(budget.grant(), None);
    }
}
