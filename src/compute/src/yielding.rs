// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cooperative scheduling of long-running work on a compute worker.
//!
//! A compute worker is a single thread that multiplexes dataflow scheduling,
//! command handling, and peek processing. Any one of those refusing to return
//! for seconds at a time costs the whole worker its interactivity. Work that
//! can run long is therefore expected to run in bounded slices: perform a
//! slice, return, and get called again on the next activation.
//!
//! [`YieldSpec`] says how large a slice may be, and [`Budget`] tracks the
//! remaining allowance within one slice. When several items share an
//! activation, [`Budget::nest`] gives each one its own allowance inside the
//! shared one, so that no single item can hog the activation or keep the
//! others from being reached.

use std::time::{Duration, Instant};

/// Specification of how large a slice of cooperative work may be.
///
/// Both bounds are optional and independent. Omitting one disables that
/// dimension entirely rather than falling back to a default, and omitting both
/// disables yielding.
#[derive(Clone, Copy, Debug)]
pub(crate) struct YieldSpec {
    /// Yield after the given amount of work was performed.
    pub after_work: Option<usize>,
    /// Yield after the given amount of time has elapsed.
    pub after_time: Option<Duration>,
}

impl Default for YieldSpec {
    fn default() -> Self {
        Self {
            after_work: Some(1_000_000),
            after_time: Some(Duration::from_millis(100)),
        }
    }
}

impl YieldSpec {
    /// Parses the dyncfg representation: `work:<amount>`, `time:<millis>`, or
    /// both separated by a comma. Returns `None` if the string does not parse.
    pub fn try_from_str(s: &str) -> Option<Self> {
        let mut after_work = None;
        let mut after_time = None;

        let options = s.split(',').map(|o| o.trim());
        for option in options {
            let mut iter = option.split(':').map(|p| p.trim());
            match std::array::from_fn(|_| iter.next()) {
                [Some("work"), Some(amount), None] => {
                    let amount = amount.parse().ok()?;
                    after_work = Some(amount);
                }
                [Some("time"), Some(millis), None] => {
                    let millis = millis.parse().ok()?;
                    let duration = Duration::from_millis(millis);
                    after_time = Some(duration);
                }
                _ => return None,
            }
        }

        Some(Self {
            after_work,
            after_time,
        })
    }
}

/// The remaining allowance for one slice of cooperative work.
///
/// Work is measured in caller-defined units. The caller asks for an
/// [`allowance`](Budget::allowance), spends up to that much, reports what it
/// spent with [`charge`](Budget::charge), and yields once the budget
/// [`is_spent`](Budget::is_spent).
pub(crate) struct Budget {
    /// Work units left, `None` when the spec imposes no work bound.
    work: Option<usize>,
    deadline: Option<Instant>,
    /// Work units left until we read the clock again. Reading the clock is
    /// cheap but not free, and a slice can be tens of millions of units, so we
    /// only consult the deadline every `CLOCK_INTERVAL` units.
    until_clock_check: usize,
    /// Latched once the deadline has passed.
    expired: bool,
}

impl Budget {
    /// Work units charged between clock reads.
    ///
    /// Reading the clock per unit is measurable once a slice runs to tens of
    /// millions of units, so we amortize it. The cost is resolution: a time
    /// bound cannot be observed sooner than `CLOCK_INTERVAL` units, so at a
    /// few microseconds per unit the deadline can overshoot by milliseconds.
    /// Set a work bound as well if that matters.
    const CLOCK_INTERVAL: usize = 1024;

    pub fn new(spec: &YieldSpec) -> Self {
        Self {
            work: spec.after_work,
            deadline: spec.after_time.map(|d| Instant::now() + d),
            until_clock_check: Self::CLOCK_INTERVAL,
            expired: false,
        }
    }

    /// How much work the caller may perform before it must call
    /// [`charge`](Budget::charge) again.
    ///
    /// Non-zero while the budget is not spent, and zero once it is. A caller
    /// that must make progress regardless has to impose its own floor: a spec
    /// of `work:0` yields a budget that is spent before any work happens.
    pub fn allowance(&self) -> usize {
        match self.work {
            Some(work) => work.min(self.until_clock_check),
            None => self.until_clock_check,
        }
    }

    /// Charges `units` of performed work against the budget.
    pub fn charge(&mut self, units: usize) {
        if let Some(work) = &mut self.work {
            *work = work.saturating_sub(units);
        }
        self.until_clock_check = self.until_clock_check.saturating_sub(units);

        if self.until_clock_check == 0 {
            self.until_clock_check = Self::CLOCK_INTERVAL;
            if let Some(deadline) = self.deadline {
                self.expired |= Instant::now() >= deadline;
            }
        }
    }

    /// Whether the allowance is used up and the caller should yield.
    pub fn is_spent(&self) -> bool {
        self.work == Some(0) || self.expired
    }

    /// Carves a per-item allowance out of this budget.
    pub fn nest(&mut self, spec: &YieldSpec) -> NestedBudget<'_> {
        NestedBudget {
            own: Budget::new(spec),
            shared: self,
        }
    }
}

/// A per-item allowance nested inside one shared by all items.
///
/// Work charged here is charged against both, and the pair is spent as soon as
/// either bound is reached. So an item yields once it has had its turn, and
/// the shared bound still caps what all items together may spend.
pub(crate) struct NestedBudget<'a> {
    own: Budget,
    shared: &'a mut Budget,
}

impl NestedBudget<'_> {
    /// See [`Budget::allowance`].
    pub fn allowance(&self) -> usize {
        self.own.allowance().min(self.shared.allowance())
    }

    /// See [`Budget::charge`].
    pub fn charge(&mut self, units: usize) {
        self.own.charge(units);
        self.shared.charge(units);
    }

    /// See [`Budget::is_spent`].
    pub fn is_spent(&self) -> bool {
        self.own.is_spent() || self.shared.is_spent()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn parses_yield_spec() {
        let spec = YieldSpec::try_from_str("work:100,time:5").unwrap();
        assert_eq!(spec.after_work, Some(100));
        assert_eq!(spec.after_time, Some(Duration::from_millis(5)));

        let spec = YieldSpec::try_from_str("work:100").unwrap();
        assert_eq!(spec.after_work, Some(100));
        assert_eq!(spec.after_time, None);

        let spec = YieldSpec::try_from_str("time:5").unwrap();
        assert_eq!(spec.after_work, None);
        assert_eq!(spec.after_time, Some(Duration::from_millis(5)));

        assert!(YieldSpec::try_from_str("work:banana").is_none());
        assert!(YieldSpec::try_from_str("fuel:100").is_none());
        assert!(YieldSpec::try_from_str("").is_none());
        assert!(YieldSpec::try_from_str("work:1,").is_none());

        // Repeating a key is accepted and the last one wins. Nothing depends on
        // this, it just isn't worth rejecting.
        let spec = YieldSpec::try_from_str("work:1,work:2").unwrap();
        assert_eq!(spec.after_work, Some(2));
    }

    /// A zero work bound produces a budget that is spent before anything
    /// happens. Callers that must make progress have to floor the allowance
    /// themselves, so pin the behavior they are flooring against.
    #[mz_ore::test]
    fn zero_work_budget_is_spent_immediately() {
        let spec = YieldSpec {
            after_work: Some(0),
            after_time: None,
        };
        let mut budget = Budget::new(&spec);
        assert!(budget.is_spent());
        assert_eq!(budget.allowance(), 0);
        assert!(budget.nest(&YieldSpec::default()).is_spent());

        // And the other way around: a spent per-item budget inside a healthy
        // shared one.
        let mut shared = Budget::new(&YieldSpec::default());
        assert!(shared.nest(&spec).is_spent());
    }

    /// Charging more than the allowance is legal and saturates.
    #[mz_ore::test]
    fn overcharging_saturates() {
        let spec = YieldSpec {
            after_work: Some(10),
            after_time: None,
        };
        let mut budget = Budget::new(&spec);
        budget.charge(usize::MAX);
        assert!(budget.is_spent());
        assert_eq!(budget.allowance(), 0);
    }

    /// A per-item bound wider than the shared one is dead: the shared bound
    /// decides. This is what a misconfigured `peek_yielding` looks like.
    #[mz_ore::test]
    fn nested_budget_wider_than_shared_is_dead() {
        let shared_spec = YieldSpec {
            after_work: Some(10),
            after_time: None,
        };
        let own = YieldSpec {
            after_work: Some(usize::MAX),
            after_time: None,
        };
        let mut shared = Budget::new(&shared_spec);
        let mut nested = shared.nest(&own);

        assert_eq!(nested.allowance(), 10);
        nested.charge(10);
        assert!(nested.is_spent());
        assert!(shared.is_spent());
    }

    /// A work bound that is not a multiple of the clock interval still hands
    /// out exactly that much.
    #[mz_ore::test]
    fn work_bound_off_clock_interval() {
        let spec = YieldSpec {
            after_work: Some(Budget::CLOCK_INTERVAL + 7),
            after_time: None,
        };
        let mut budget = Budget::new(&spec);

        let mut spent = 0;
        while !budget.is_spent() {
            let allowance = budget.allowance();
            assert!(allowance > 0);
            budget.charge(allowance);
            spent += allowance;
        }
        assert_eq!(spent, Budget::CLOCK_INTERVAL + 7);
    }

    #[mz_ore::test]
    fn budget_allowance_is_never_zero_before_spent() {
        let spec = YieldSpec {
            after_work: Some(3 * Budget::CLOCK_INTERVAL),
            after_time: None,
        };
        let mut budget = Budget::new(&spec);

        let mut spent = 0;
        while !budget.is_spent() {
            let allowance = budget.allowance();
            assert!(allowance > 0);
            budget.charge(allowance);
            spent += allowance;
        }
        assert_eq!(spent, 3 * Budget::CLOCK_INTERVAL);
    }

    #[mz_ore::test]
    fn nested_budget_is_bounded_by_both() {
        let own = YieldSpec {
            after_work: Some(Budget::CLOCK_INTERVAL),
            after_time: None,
        };
        let shared_spec = YieldSpec {
            after_work: Some(3 * Budget::CLOCK_INTERVAL),
            after_time: None,
        };

        // The shared budget covers three turns of the per-item budget.
        let mut shared = Budget::new(&shared_spec);
        for _ in 0..3 {
            let mut nested = shared.nest(&own);
            let mut spent = 0;
            while !nested.is_spent() {
                let allowance = nested.allowance();
                assert!(allowance > 0);
                nested.charge(allowance);
                spent += allowance;
            }
            assert_eq!(spent, Budget::CLOCK_INTERVAL);
        }

        assert!(shared.is_spent());
        assert!(shared.nest(&own).is_spent());
    }

    #[mz_ore::test]
    fn budget_expires_on_deadline() {
        let spec = YieldSpec {
            after_work: None,
            after_time: Some(Duration::ZERO),
        };
        let mut budget = Budget::new(&spec);

        // The deadline is only consulted once a full clock interval is charged.
        assert!(!budget.is_spent());
        budget.charge(Budget::CLOCK_INTERVAL);
        assert!(budget.is_spent());
    }

    #[mz_ore::test]
    fn unbounded_budget_never_spends() {
        let spec = YieldSpec {
            after_work: None,
            after_time: None,
        };
        let mut budget = Budget::new(&spec);
        budget.charge(usize::MAX);
        assert!(!budget.is_spent());
    }
}
