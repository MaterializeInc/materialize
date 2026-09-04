// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! What a worker activation may spend walking index peeks before it hands the worker back.

use mz_compute_types::dyncfgs::{
    ENABLE_INDEX_PEEK_OFFLOAD, INDEX_PEEK_ACTIVATION_BUDGET, INDEX_PEEK_INLINE_BUDGET,
};
use mz_dyncfg::{ConfigSet, ConfigValHandle};

/// The parameters an [`InlineBudget`] is read from, as handles: every activation reads them, and
/// by-name lookups would put three searches of the configuration set on the worker's loop.
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

        // Both floors keep the parameters monotone down to zero rather than wedging there. A
        // per-peek budget of zero suspends every scan before it walks a position, and a suspension
        // holding no full batch is an offload, so every point lookup would pay for a task and a
        // permit to walk nothing. An aggregate of zero passes every peek over on every activation,
        // and the activation a passed-over peek asks for finds the same empty budget.
        ActivationBudget::Bounded {
            per_peek: self.per_peek.get().max(1),
            remaining: self.aggregate.get().max(1),
        }
    }
}

/// The fuel an activation may spend walking index peeks on the worker, and what one peek may take
/// of it.
///
/// The per-peek budget decides placement: a peek that outruns it moves off the worker. The
/// aggregate exists because the sweep visits every pending peek, so a per-peek budget alone would
/// let N peeks cost N times that budget in one pass.
///
/// Both count cursor positions, the unit the scan charges.
enum ActivationBudget {
    /// Every peek walks to completion where it started, and nothing is offloaded or passed over.
    ///
    /// What the kill switch restores. A scan suspends only out of fuel or holding a full batch,
    /// so unbounded fuel leaves the batch as the only suspension and the peek takes the stash.
    Unbounded,
    /// A peek may spend `per_peek` before it is offloaded, and all peeks together may spend
    /// `remaining` before the rest of this activation's work gets the worker back.
    Bounded { per_peek: usize, remaining: usize },
}

/// The fuel of the activation under way, armed from the parameters by the first peek that asks for
/// a slice of it.
///
/// Arming at the grant is what reads the parameters as late as anything can. Commands drain in
/// full before the sweep that begins an activation, so a peek arriving on that path is granted a
/// slice before any activation has begun, and a budget that only an activation's start armed would
/// hand that peek whatever the last one left. The constructor cannot arm it either: `ConfigSet`
/// handles read the live value, but a value read there is read before `handle_create_instance`
/// applies the controller's snapshot, and an empty snapshot leaves the defaults in place until the
/// first `UpdateConfiguration`. Since the offload's own flag defaults off, an eagerly armed budget
/// is an unbounded one, and it would go to every peek in a reconnecting controller's backlog.
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

    /// Begins an activation, discarding what the previous one left. Nothing else refills the fuel.
    pub(super) fn start_activation(&mut self) {
        self.activation = None;
    }

    /// The fuel one peek's slice may spend, or `None` when this activation has none left to give.
    ///
    /// A peek gets its whole per-peek budget or nothing, so the aggregate can overrun by one
    /// budget.
    ///
    /// A caller granted `None` must pass the peek over. Stepping with no fuel suspends the scan
    /// without walking anything, offloading a peek that never had its inline turn.
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

    /// Charges the activation for the positions a slice walked. An unarmed budget has nothing to
    /// charge, since a slice is only ever walked out of fuel this granted.
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
mod tests;
