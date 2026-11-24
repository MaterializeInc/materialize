// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, btree_map::Entry};

use mz_ore::soft_assert_no_log;
use mz_repr::{Diff, Timestamp};
use timely::progress::Timestamp as _;

/// A compacted catalog trace with buffered updates awaiting consolidation.
///
/// Compacted entries share a timestamp, including when later updates cancel
/// without changing their contents. Keys retain their original representation:
/// migrations need to distinguish raw JSON forms that decode to the same value.
#[derive(Debug)]
pub(crate) struct SnapshotTrace<T> {
    entries: BTreeMap<T, Diff>,
    timestamp: Timestamp,
    pending: Vec<(T, Timestamp, Diff)>,
}

impl<T: Ord> SnapshotTrace<T> {
    pub(crate) fn new() -> Self {
        Self::from(Vec::new())
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len() + self.pending.len()
    }

    pub(crate) fn push(&mut self, update: (T, Timestamp, Diff)) {
        self.pending.push(update);
    }

    /// Iterates the compacted entries followed by any unconsolidated updates.
    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = (&T, &Timestamp, &Diff)> {
        self.entries
            .iter()
            .map(|(kind, diff)| (kind, &self.timestamp, diff))
            .chain(self.pending.iter().map(|(kind, ts, diff)| (kind, ts, diff)))
    }

    pub(crate) fn to_vec(&self) -> Vec<(T, Timestamp, Diff)>
    where
        T: Clone,
    {
        self.iter()
            .map(|(kind, ts, diff)| (kind.clone(), *ts, *diff))
            .collect()
    }

    pub(crate) fn into_updates(self) -> impl Iterator<Item = (T, Timestamp, Diff)> {
        self.entries
            .into_iter()
            .map(move |(kind, diff)| (kind, self.timestamp, diff))
            .chain(self.pending)
    }

    /// Advances all retained entries to the last update timestamp and sums diffs.
    pub(crate) fn consolidate(&mut self) {
        soft_assert_no_log!(
            self.pending.windows(2).all(|pair| pair[0].1 <= pair[1].1)
                && (self.entries.is_empty()
                    || self
                        .pending
                        .first()
                        .is_none_or(|(_, ts, _)| self.timestamp <= *ts)),
            "snapshot should be sorted by timestamp"
        );

        // Take the timestamp before cancellation, including updates that leave
        // no surviving entry. Empty progress does not advance it.
        self.timestamp = self
            .pending
            .last()
            .map(|(_, ts, _)| *ts)
            .or_else(|| (!self.entries.is_empty()).then_some(self.timestamp))
            .unwrap_or_else(Timestamp::minimum);
        // Replay batches can be catalog-sized. Do not retain their backing buffer.
        for (kind, _, diff) in std::mem::take(&mut self.pending) {
            match self.entries.entry(kind) {
                Entry::Vacant(entry) => {
                    if diff != Diff::ZERO {
                        entry.insert(diff);
                    }
                }
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() += diff;
                    if *entry.get() == Diff::ZERO {
                        entry.remove();
                    }
                }
            }
        }
    }
}

impl<T> From<Vec<(T, Timestamp, Diff)>> for SnapshotTrace<T> {
    fn from(pending: Vec<(T, Timestamp, Diff)>) -> Self {
        Self {
            entries: BTreeMap::new(),
            timestamp: Timestamp::minimum(),
            pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn consolidate_full(updates: &mut Vec<(u8, Timestamp, Diff)>) {
        let timestamp = updates
            .last()
            .map(|(_, ts, _)| *ts)
            .unwrap_or_else(Timestamp::minimum);
        for (_, ts, _) in updates.iter_mut() {
            *ts = timestamp;
        }
        differential_dataflow::consolidation::consolidate_updates(updates);
    }

    proptest! {
        #[mz_ore::test]
        fn incremental_matches_full_consolidation(
            batches in prop::collection::vec(
                (
                    prop::collection::vec((0u8..16, -2i64..=2), 0..40),
                    any::<bool>(),
                    any::<bool>(),
                ),
                0..40
            )
        ) {
            let mut incremental = SnapshotTrace::new();
            let mut full = Vec::new();
            for (index, (batch, consolidate, filter)) in batches.into_iter().enumerate() {
                let ts = Timestamp::from(u64::try_from(index).expect("small batch index"));
                for (key, diff) in batch {
                    let update = (key, ts, Diff::from(diff));
                    incremental.push(update);
                    full.push(update);
                }
                if filter {
                    incremental = SnapshotTrace::from(incremental.into_updates()
                        .filter(|(key, _, _)| key % 2 == 0).collect::<Vec<_>>());
                    full.retain(|(key, _, _)| key % 2 == 0);
                }
                prop_assert_eq!(incremental.len(), full.len());
                prop_assert_eq!(incremental.to_vec(), full.clone());
                let reversed: Vec<_> = incremental.iter().rev()
                    .map(|(k, t, d)| (*k, *t, *d)).collect();
                prop_assert_eq!(reversed, full.iter().rev().copied().collect::<Vec<_>>());
                if consolidate {
                    consolidate_full(&mut full);
                    incremental.consolidate();
                    prop_assert_eq!(incremental.to_vec(), full.clone());
                }
            }
            consolidate_full(&mut full);
            incremental.consolidate();
            prop_assert_eq!(incremental.into_updates().collect::<Vec<_>>(), full);
        }
    }

    #[mz_ore::test]
    fn cancellations_advance_surviving_rows_but_empty_progress_does_not() {
        let mut trace = SnapshotTrace::from(vec![(1u8, Timestamp::from(1), Diff::ONE)]);
        trace.consolidate();
        trace.push((2, Timestamp::from(3), Diff::ONE));
        trace.push((2, Timestamp::from(3), Diff::MINUS_ONE));
        trace.consolidate();
        assert_eq!(trace.to_vec(), vec![(1, Timestamp::from(3), Diff::ONE)]);
        trace.consolidate();
        assert_eq!(trace.to_vec(), vec![(1, Timestamp::from(3), Diff::ONE)]);
    }
}
