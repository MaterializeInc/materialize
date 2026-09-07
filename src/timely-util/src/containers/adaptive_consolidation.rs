// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A container builder that consolidates its output while consolidation pays off.
//!
//! Consolidating each chunk before it leaves an operator collapses repeated `(data, time)` pairs
//! before they are exchanged or arranged: a large saving when the data has few distinct keys,
//! and a sort that recovers nothing when it has many. [`AdaptiveConsolidatingContainerBuilder`]
//! measures what each consolidation recovers. While the recent recovery stays below
//! `MIN_RECOVERY_PERMILLE` it passes chunks through unsorted, consolidating one chunk in every
//! `PROBE_INTERVAL` so that a change in the data is noticed within that many chunks.
//!
//! Consumers must not rely on the output being consolidated or ordered; like differential's
//! `ConsolidatingContainerBuilder`, this is an optimization and does not maintain FIFO order.

use std::collections::VecDeque;

use differential_dataflow::Data;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use timely::container::{ContainerBuilder, PushInto};

/// Below this recovery, in permille of a chunk's records, consolidation is judged not worth its
/// sort. Two percent is well under the sort's cost relative to the rest of the pipeline.
const MIN_RECOVERY_PERMILLE: u32 = 20;
/// Chunks passed through unsorted between probing consolidations.
const PROBE_INTERVAL: u32 = 32;

/// See the module documentation.
pub struct AdaptiveConsolidatingContainerBuilder<D, T, R> {
    current: Vec<(D, T, R)>,
    empty: Vec<Vec<(D, T, R)>>,
    outbound: VecDeque<Vec<(D, T, R)>>,
    /// Recovery of recent consolidations, in permille, as an exponential moving average that
    /// starts out assuming consolidation pays.
    recovery_permille: u32,
    /// Chunks still to pass through before the next probing consolidation. Zero while
    /// consolidating.
    pass_through_left: u32,
}

impl<D, T, R> Default for AdaptiveConsolidatingContainerBuilder<D, T, R> {
    fn default() -> Self {
        Self {
            current: Vec::new(),
            empty: Vec::new(),
            outbound: VecDeque::new(),
            recovery_permille: 1000,
            pass_through_left: 0,
        }
    }
}

impl<D, T, R> AdaptiveConsolidatingContainerBuilder<D, T, R>
where
    D: Data,
    T: Data,
    R: Semigroup + 'static,
{
    /// Whether chunks currently leave unsorted.
    pub fn is_passing_through(&self) -> bool {
        self.pass_through_left > 0
    }

    /// Consolidates `current` when in consolidating mode, then moves whole containers of the
    /// preferred capacity (or everything, if `all`) to `outbound`.
    #[cold]
    fn flush(&mut self, all: bool) {
        let preferred_capacity = timely::container::buffer::default_capacity::<(D, T, R)>();
        if self.pass_through_left == 0 {
            let before = self.current.len();
            consolidate_updates(&mut self.current);
            let recovered = before - self.current.len();
            let permille = u32::try_from(recovered * 1000 / before.max(1)).unwrap_or(1000);
            self.recovery_permille = (self.recovery_permille * 3 + permille) / 4;
            if self.recovery_permille < MIN_RECOVERY_PERMILLE {
                self.pass_through_left = PROBE_INTERVAL;
            }
        } else {
            self.pass_through_left -= 1;
        }
        let take = if all {
            self.current.len()
        } else {
            (self.current.len() / preferred_capacity) * preferred_capacity
        };
        let mut drain = self.current.drain(..take).peekable();
        while drain.peek().is_some() {
            let mut container = self
                .empty
                .pop()
                .unwrap_or_else(|| Vec::with_capacity(preferred_capacity));
            container.clear();
            container.extend((&mut drain).take(preferred_capacity));
            self.outbound.push_back(container);
        }
    }
}

impl<D, T, R, P> PushInto<P> for AdaptiveConsolidatingContainerBuilder<D, T, R>
where
    D: Data,
    T: Data,
    R: Semigroup + 'static,
    Vec<(D, T, R)>: PushInto<P>,
{
    #[inline]
    fn push_into(&mut self, item: P) {
        let preferred_capacity = timely::container::buffer::default_capacity::<(D, T, R)>();
        if self.current.capacity() < preferred_capacity * 2 {
            self.current
                .reserve(preferred_capacity * 2 - self.current.capacity());
        }
        self.current.push_into(item);
        if self.current.len() == self.current.capacity() {
            self.flush(false);
        }
    }
}

impl<D, T, R> ContainerBuilder for AdaptiveConsolidatingContainerBuilder<D, T, R>
where
    D: Data,
    T: Data,
    R: Semigroup + 'static,
{
    type Container = Vec<(D, T, R)>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(container) = self.outbound.pop_front() {
            self.empty.push(container);
            self.empty.last_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.current.is_empty() {
            self.flush(true);
            // Keep two spare containers at most, so a burst does not pin memory.
            self.empty.truncate(2);
        }
        self.extract()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type B = AdaptiveConsolidatingContainerBuilder<u64, u64, i64>;

    fn drain_all(b: &mut B) -> Vec<(u64, u64, i64)> {
        let mut out = Vec::new();
        while let Some(c) = b.extract() {
            out.append(c);
        }
        while let Some(c) = b.finish() {
            out.append(c);
        }
        out
    }

    #[mz_ore::test]
    fn repeated_keys_stay_consolidated() {
        let mut b = B::default();
        for i in 0..100_000u64 {
            b.push_into((i % 10, 0u64, 1i64));
        }
        let out = drain_all(&mut b);
        assert!(!b.is_passing_through());
        assert!(
            out.len() < 1_000,
            "ten keys collapse each chunk, got {}",
            out.len()
        );
        assert_eq!(out.iter().map(|(_, _, r)| *r).sum::<i64>(), 100_000);
    }

    #[mz_ore::test]
    fn distinct_keys_stop_the_sort_and_lose_nothing() {
        let mut b = B::default();
        for i in 0..200_000u64 {
            b.push_into((i, 0u64, 1i64));
        }
        assert!(
            b.is_passing_through(),
            "nothing recovered, so chunks pass through"
        );
        let out = drain_all(&mut b);
        assert_eq!(out.len(), 200_000);
    }

    #[mz_ore::test]
    fn a_probe_notices_when_keys_start_repeating() {
        let mut b = B::default();
        for i in 0..200_000u64 {
            b.push_into((i, 0u64, 1i64));
        }
        assert!(b.is_passing_through());
        // Enough repeated-key chunks for several probes to raise the recovery estimate.
        for i in 0..400_000u64 {
            b.push_into((i % 10, 0u64, 1i64));
        }
        let out = drain_all(&mut b);
        assert!(
            !b.is_passing_through(),
            "probes saw the recovery and resumed consolidating"
        );
        assert!(
            out.len() < 400_000,
            "later chunks were consolidated, got {}",
            out.len()
        );
        assert_eq!(out.iter().map(|(_, _, r)| *r).sum::<i64>(), 600_000);
    }
}
