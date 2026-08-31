// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the fuel an activation may spend walking index peeks.

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
/// spent, which is how the kill switch keeps a peek that answers inline on the worker. It says
/// nothing about a peek whose rows belong in the stash, which suspends on its batch rather
/// than on its fuel and leaves the worker either way.
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
/// offloaded having walked nothing, which spends a task and a permit on a point lookup the
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
