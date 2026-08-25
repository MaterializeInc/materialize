// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! What a worker activation may spend walking index peeks before it hands the worker back.

use mz_compute_types::dyncfgs::{
    ENABLE_INDEX_PEEK_OFFLOAD, INDEX_PEEK_ACTIVATION_BUDGET, INDEX_PEEK_INLINE_BUDGET,
};
use mz_dyncfg::{ConfigSet, ConfigValHandle};

/// The parameters an [`InlineBudget`] is read from, each as a handle rather than as a value.
///
/// An activation's fuel is read afresh, so reading the parameters by name would put three lookups
/// against the whole configuration set on the worker's loop. Handles give the property the
/// parameters are documented with at a fraction of that cost: a configuration change reaches the
/// next activation, and so the walks it grants a slice to, without discarding the positions earlier
/// sweeps walked.
struct InlineBudgetConfig {
    enabled: ConfigValHandle<bool>,
    per_peek: ConfigValHandle<usize>,
    aggregate: ConfigValHandle<usize>,
}

impl InlineBudgetConfig {
    fn new(config: &ConfigSet) -> Self {
        Self {
            enabled: ENABLE_INDEX_PEEK_OFFLOAD.handle(config),
            per_peek: INDEX_PEEK_INLINE_BUDGET.handle(config),
            aggregate: INDEX_PEEK_ACTIVATION_BUDGET.handle(config),
        }
    }

    /// Reads the parameters in effect now into the fuel of one activation.
    fn arm(&self) -> ActivationBudget {
        if !self.enabled.get() {
            return ActivationBudget::Unbounded;
        }

        ActivationBudget::Bounded {
            // A per-peek budget of zero would suspend every scan before it walked a position, and
            // a suspension that holds no full batch is a promotion, so every point lookup would
            // cost a task, a permit, and a trace bundle clone while walking nothing. The aggregate
            // is untouched by such a peek, so this is a waste rather than a wedge, but it is also
            // exactly what granting a peek an empty slice is documented to avoid. One position
            // keeps the parameter monotone down to its floor.
            per_peek: self.per_peek.get().max(1),
            // An aggregate of zero would pass every peek over on every activation, and the
            // activation a passed-over peek asks for would arrive to find the same empty budget,
            // so no peek would ever be answered. One position keeps the parameter monotone down
            // to its floor instead of wedging at it.
            remaining: self.aggregate.get().max(1),
        }
    }
}

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
enum ActivationBudget {
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

/// The fuel of the activation under way, armed from the parameters by the first peek that asks for
/// a slice of it.
///
/// Arming is by demand rather than by a caller refilling ahead of one, because the callers that
/// grant a slice are not only the sweep. Commands are drained in full before any sweep runs, and a
/// peek arriving on that path is granted its first slice as it arrives, so a budget that only a
/// sweep armed would hand every peek of a reconnecting controller's re-sent backlog an unbounded
/// slice. Arming at the grant leaves one place that can be wrong and makes a future caller of
/// [`InlineBudget::grant`] bounded by construction.
///
/// Arming cannot happen where the state is built either. `CreateInstance` is followed by the
/// `UpdateConfiguration` that carries the parameters, so a budget armed in the constructor would
/// snapshot the code defaults and hold them for the life of the activation.
pub(super) struct InlineBudget {
    config: InlineBudgetConfig,
    /// The fuel of the activation under way, or `None` while no peek has asked for a slice since
    /// the activation began.
    activation: Option<ActivationBudget>,
}

impl InlineBudget {
    /// A budget reading `config`, whose first grant arms the first activation.
    pub(super) fn new(config: &ConfigSet) -> Self {
        Self {
            config: InlineBudgetConfig::new(config),
            activation: None,
        }
    }

    /// Begins an activation, discarding what the previous one left.
    ///
    /// This is what keeps the aggregate a bound per activation rather than per process. Arming is
    /// lazy, so nothing else ever refills the fuel: a budget armed once and never begun again
    /// would let the first activation's aggregate bound every peek the replica goes on to serve.
    /// The parameters are read at the grant that follows rather than here, so a configuration
    /// change reaches this activation without disturbing the one it interrupted.
    pub(super) fn start_activation(&mut self) {
        self.activation = None;
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
    pub(super) fn grant(&mut self) -> Option<usize> {
        let Self { config, activation } = self;
        match activation.get_or_insert_with(|| config.arm()) {
            ActivationBudget::Unbounded => Some(usize::MAX),
            ActivationBudget::Bounded {
                per_peek,
                remaining,
            } => (*remaining > 0).then_some(*per_peek),
        }
    }

    /// Charges the activation for the positions a slice walked.
    ///
    /// A charge without a grant ahead of it is nothing to charge for, because a slice is only ever
    /// walked out of fuel this granted.
    pub(super) fn charge(&mut self, spent: usize) {
        match &mut self.activation {
            Some(ActivationBudget::Bounded { remaining, .. }) => {
                *remaining = remaining.saturating_sub(spent)
            }
            Some(ActivationBudget::Unbounded) | None => {}
        }
    }

    /// What is left of this activation's aggregate, or `None` when the activation is unarmed or
    /// unbounded.
    #[cfg(test)]
    pub(super) fn remaining(&self) -> Option<usize> {
        match &self.activation {
            Some(ActivationBudget::Bounded { remaining, .. }) => Some(*remaining),
            Some(ActivationBudget::Unbounded) | None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigUpdates;

    use super::*;

    /// The configuration the budget tests read, with the offload on, a per-peek budget of
    /// `per_peek` and an aggregate of `aggregate`.
    fn config(per_peek: usize, aggregate: usize) -> ConfigSet {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&INDEX_PEEK_INLINE_BUDGET, per_peek);
        updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, aggregate);
        updates.apply(&config);
        config
    }

    /// A budget that no activation has begun still grants a bounded slice, because the grant is
    /// what arms it.
    ///
    /// The worker drains the commands it has queued before it sweeps its pending peeks, and a peek
    /// arriving on that path is granted a slice as it arrives. A budget armed only where an
    /// activation begins would grant that peek the unbounded fuel of a budget that had never been
    /// read, and unbounded fuel never suspends, so the peek would walk to its answer on the worker
    /// however the parameters are set.
    #[mz_ore::test]
    fn the_first_grant_arms_the_budget() {
        let config = config(100, 250);
        let mut budget = InlineBudget::new(&config);

        assert_eq!(budget.grant(), Some(100));
    }

    /// Beginning an activation refills the aggregate, which is what makes it a bound on one
    /// activation rather than on the process.
    #[mz_ore::test]
    fn beginning_an_activation_refills_the_aggregate() {
        let config = config(100, 250);
        let mut budget = InlineBudget::new(&config);

        assert_eq!(budget.grant(), Some(100));
        budget.charge(250);
        assert_eq!(budget.grant(), None);

        budget.start_activation();

        assert_eq!(budget.grant(), Some(100));
        assert_eq!(budget.remaining(), Some(250));
    }

    /// With the offload off, every peek is granted unbounded fuel however much the peeks before it
    /// spent, which is what makes the kill switch restore the worker's old behaviour rather than
    /// approximate it.
    #[mz_ore::test]
    fn the_kill_switch_grants_every_peek_an_unbounded_slice() {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut budget = InlineBudget::new(&config);

        for _ in 0..3 {
            assert_eq!(budget.grant(), Some(usize::MAX));
            budget.charge(usize::MAX);
        }
    }

    /// The aggregate is spent by what the peeks walked, not by what they were granted, so cheap
    /// peeks keep getting turns and expensive ones drain it.
    #[mz_ore::test]
    fn the_aggregate_is_spent_by_what_the_peeks_walk() {
        let config = config(100, 250);
        let mut budget = InlineBudget::new(&config);

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

    /// A configuration change reaches the activation that follows it through the handles the
    /// budget holds, which is what keeps a parameter change from waiting on a restart. The
    /// activation under way when the change lands keeps the fuel it was armed with.
    #[mz_ore::test]
    fn a_parameter_change_reaches_the_next_activation() {
        let config = mz_dyncfgs::all_dyncfgs();
        let mut budget = InlineBudget::new(&config);

        assert_eq!(budget.grant(), Some(usize::MAX));

        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        updates.add(&INDEX_PEEK_INLINE_BUDGET, 100);
        updates.apply(&config);

        assert_eq!(budget.grant(), Some(usize::MAX));

        budget.start_activation();

        assert_eq!(budget.grant(), Some(100));
    }

    /// A per-peek budget of zero still walks one position. Without that, every peek would be
    /// promoted having walked nothing, which spends a task and a permit on a point lookup the
    /// worker would have answered outright.
    #[mz_ore::test]
    fn a_zero_per_peek_budget_still_walks_one_position() {
        let config = config(0, *INDEX_PEEK_ACTIVATION_BUDGET.default());
        let mut budget = InlineBudget::new(&config);

        assert_eq!(budget.grant(), Some(1));
    }

    /// An aggregate of zero still serves one peek per activation. Without that, every peek would
    /// be passed over on every activation and none would ever be answered.
    #[mz_ore::test]
    fn a_zero_aggregate_still_serves_one_peek() {
        let config = config(100, 0);
        let mut budget = InlineBudget::new(&config);

        assert_eq!(budget.grant(), Some(100));
        budget.charge(100);
        assert_eq!(budget.grant(), None);
    }

    /// An inline budget larger than the aggregate still serves one peek per activation. Without
    /// that, no peek would ever be granted a slice and every peek would hang.
    #[mz_ore::test]
    fn an_inline_budget_above_the_aggregate_still_serves_one_peek() {
        let config = config(1_000_000_000, 1);
        let mut budget = InlineBudget::new(&config);

        assert_eq!(budget.grant(), Some(1_000_000_000));
        budget.charge(1_000_000_000);
        assert_eq!(budget.grant(), None);
    }
}
