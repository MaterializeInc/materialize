// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cutover rule that merges the batch chains of several replicas of one batched subscribe.
//!
//! Each replica sends a chain of batches `[lower, upper)`. The controller keeps the accepted
//! upper `U` and forwards the output of whichever replica extends it, so replicas that
//! re-render or fail over rejoin the output without duplicates or gaps.

use mz_repr::{Timestamp, UpdateCollection};
use timely::PartialOrder;
use timely::progress::Antichain;

/// The decision for one batch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the instance controller consumes decisions")
)]
pub(super) enum CutoverDecision {
    /// The batch ends at or below `U`. Discard it.
    Drop,
    /// The batch extends `U`. Forward its updates at times in `[lower, upper)`, where `lower` is
    /// the previous `U`. If `trim` is set, the batch starts before `lower` and the caller must
    /// remove the updates before it.
    Accept {
        lower: Antichain<Timestamp>,
        trim: bool,
    },
    /// The batch starts beyond `U`. Replica chains are contiguous, so this is a protocol
    /// violation.
    Gap,
}

/// The accepted upper `U` of one subscribe.
///
/// Callers present the batches of each replica in the order that replica sent them. The state
/// holds nothing per replica.
#[derive(Debug)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the instance controller owns the cutover")
)]
pub(super) struct Cutover {
    accepted: Antichain<Timestamp>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the instance controller drives the cutover")
)]
impl Cutover {
    /// Creates a cutover that accepts the chain starting at `start`.
    pub(super) fn new(start: Timestamp) -> Self {
        Self {
            accepted: Antichain::from_elem(start),
        }
    }

    /// Decides the fate of the batch `[lower, upper)` and advances `U` if it is accepted.
    pub(super) fn process(
        &mut self,
        lower: &Antichain<Timestamp>,
        upper: &Antichain<Timestamp>,
    ) -> CutoverDecision {
        // An empty `U` means the subscribe completed, so no later batch is accepted. The empty
        // `upper` of a completion batch is not `<=` a nonempty `U`, so it falls through.
        if self.accepted.is_empty() || PartialOrder::less_equal(upper, &self.accepted) {
            return CutoverDecision::Drop;
        }
        if !PartialOrder::less_equal(lower, &self.accepted) {
            return CutoverDecision::Gap;
        }
        let effective = std::mem::replace(&mut self.accepted, upper.clone());
        CutoverDecision::Accept {
            trim: *lower != effective,
            lower: effective,
        }
    }

    /// The accepted upper `U`. Empty once the subscribe completed.
    pub(super) fn accepted(&self) -> &Antichain<Timestamp> {
        &self.accepted
    }
}

/// Keeps the updates of `updates` at times at or after `from`.
///
/// The updates of a share carry no order, so this filters every one.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the instance controller trims straddling batches")
)]
pub(super) fn trim_inline(
    updates: UpdateCollection,
    from: &Antichain<Timestamp>,
) -> UpdateCollection {
    let mut builder = UpdateCollection::builder(updates.byte_len(), updates.len());
    for (row, time, diff) in updates.iter() {
        if from.less_equal(time) {
            builder.push((row, time, diff));
        }
    }
    builder.build()
}

#[cfg(test)]
mod tests;
