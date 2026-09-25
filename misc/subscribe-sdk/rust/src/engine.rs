// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The consistency engine: buffer timestamped changes, then release everything
//! strictly below a frontier as one consolidated batch.
//!
//! This one mechanism underlies both the single-view batcher and the multi-view
//! cohort. A single view is the one-member case: its release frontier is its own
//! progress frontier. A cohort of N views feeds N buffers and releases every
//! buffer below the *minimum* frontier across them, which is the latest
//! timestamp closed for every member at once.
//!
//! Whatever the release frontier, the released set is *consolidated* before it
//! leaves the engine: within one batch the net effect per row is computed and
//! rows that cancel to nothing are dropped. A batch is therefore a clean net
//! delta at its frontier, not a replay of the intra-window churn. Consolidation
//! is scoped to a single batch, never across batches, so a consumer that applies
//! batches in order still sees every intermediate settled state.

use std::collections::HashMap;

use crate::envelope::{Change, Envelope, Row};
use crate::error::SubscribeError;

/// A per-member buffer of timestamped changes awaiting release.
///
/// Holds changes until a frontier closes them, enforcing the two ordering
/// invariants the server guarantees: data never arrives below the frontier, and
/// the frontier never regresses. A violation is a [`SubscribeError::Protocol`],
/// since it means the stream is not what the protocol promises.
#[derive(Debug)]
pub(crate) struct ReleaseBuffer {
    envelope: Envelope,
    /// Buffered changes with their timestamp, in arrival order.
    buffered: Vec<(u64, Change)>,
    /// The highest progress frontier observed, or `None` before the first.
    frontier: Option<u64>,
}

impl ReleaseBuffer {
    pub(crate) fn new(envelope: Envelope) -> Self {
        ReleaseBuffer {
            envelope,
            buffered: Vec::new(),
            frontier: None,
        }
    }

    /// The highest frontier observed so far.
    pub(crate) fn frontier(&self) -> Option<u64> {
        self.frontier
    }

    /// The number of changes currently buffered, before consolidation. Used by
    /// the cohort to bound how much a laggard can make its peers buffer.
    pub(crate) fn pending(&self) -> usize {
        self.buffered.len()
    }

    /// Buffers a change. Errors if its timestamp is below the observed frontier,
    /// which the server promises never happens.
    pub(crate) fn push_data(
        &mut self,
        timestamp: u64,
        change: Change,
    ) -> Result<(), SubscribeError> {
        if let Some(frontier) = self.frontier {
            if timestamp < frontier {
                return Err(SubscribeError::Protocol(format!(
                    "change at timestamp {timestamp} arrived after the frontier \
                     advanced to {frontier}"
                )));
            }
        }
        self.buffered.push((timestamp, change));
        Ok(())
    }

    /// Records a progress frontier. Errors if it regresses.
    pub(crate) fn observe_progress(&mut self, frontier: u64) -> Result<(), SubscribeError> {
        if let Some(last) = self.frontier {
            if frontier < last {
                return Err(SubscribeError::Protocol(format!(
                    "frontier went backwards from {last} to {frontier}"
                )));
            }
        }
        self.frontier = Some(frontier);
        Ok(())
    }

    /// Drains and consolidates every buffered change with a timestamp strictly
    /// below `release_frontier`. Changes at or above it stay buffered.
    pub(crate) fn release_below(&mut self, release_frontier: u64) -> Vec<Change> {
        let mut closed = Vec::new();
        let mut still_open = Vec::with_capacity(self.buffered.len());
        for (timestamp, change) in self.buffered.drain(..) {
            if timestamp < release_frontier {
                closed.push(change);
            } else {
                still_open.push((timestamp, change));
            }
        }
        self.buffered = still_open;
        consolidate(&self.envelope, closed)
    }
}

/// Collapses a batch of changes to its net effect, preserving first-appearance
/// order so output is deterministic and close to arrival order.
///
/// The strategy depends on the envelope: diff changes sum their multiplicities
/// per row, upsert changes keep only the last operation per key.
pub(crate) fn consolidate(envelope: &Envelope, changes: Vec<Change>) -> Vec<Change> {
    match envelope {
        Envelope::Diff => consolidate_diff(changes),
        Envelope::Upsert { .. } => consolidate_upsert(changes),
    }
}

/// Sums signed multiplicities per row, dropping rows whose net diff is zero.
fn consolidate_diff(changes: Vec<Change>) -> Vec<Change> {
    let mut acc: Vec<(Row, i64)> = Vec::new();
    let mut index: HashMap<Row, usize> = HashMap::new();
    for change in changes {
        // A diff-envelope stream only ever carries `Change::Diff`; any other
        // variant is silently ignored rather than misreported.
        if let Change::Diff { row, diff } = change {
            match index.get(&row) {
                Some(&i) => acc[i].1 += diff,
                None => {
                    index.insert(row.clone(), acc.len());
                    acc.push((row, diff));
                }
            }
        }
    }
    acc.into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|(row, diff)| Change::Diff { row, diff })
        .collect()
}

/// Keeps the last operation observed for each key, in first-seen key order. A
/// window that upserts then deletes a key nets to the delete, and vice versa.
fn consolidate_upsert(changes: Vec<Change>) -> Vec<Change> {
    let mut order: Vec<Row> = Vec::new();
    let mut last: HashMap<Row, Change> = HashMap::new();
    for change in changes {
        let key = match &change {
            Change::Upsert { key, .. } | Change::Delete { key } | Change::KeyViolation { key } => {
                key.clone()
            }
            // An upsert-envelope stream never carries a bare diff.
            Change::Diff { .. } => continue,
        };
        if last.insert(key.clone(), change).is_none() {
            order.push(key);
        }
    }
    order
        .into_iter()
        .map(|key| {
            last.remove(&key)
                .expect("key recorded in order was inserted")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn diff(id: &str, diff: i64) -> Change {
        Change::Diff {
            row: vec![Some(id.to_string())],
            diff,
        }
    }

    fn upsert(id: &str, value: &str) -> Change {
        Change::Upsert {
            key: vec![Some(id.to_string())],
            value: vec![Some(value.to_string())],
        }
    }

    fn delete(id: &str) -> Change {
        Change::Delete {
            key: vec![Some(id.to_string())],
        }
    }

    #[test]
    fn diff_consolidation_sums_multiplicities_per_row() {
        let out = consolidate_diff(vec![diff("a", 3), diff("b", -1), diff("a", 2)]);
        // `a` sums to 5 in first-seen order, `b` stays -1.
        assert_eq!(out, vec![diff("a", 5), diff("b", -1)]);
    }

    #[test]
    fn diff_consolidation_drops_net_zero_rows() {
        let out = consolidate_diff(vec![diff("a", 1), diff("b", 1), diff("a", -1)]);
        // `a` cancels out and disappears; `b` remains.
        assert_eq!(out, vec![diff("b", 1)]);
    }

    #[test]
    fn diff_consolidation_preserves_first_seen_order() {
        let out = consolidate_diff(vec![diff("c", 1), diff("a", 1), diff("b", 1)]);
        assert_eq!(out, vec![diff("c", 1), diff("a", 1), diff("b", 1)]);
    }

    #[test]
    fn upsert_consolidation_keeps_last_op_per_key() {
        let out = consolidate_upsert(vec![upsert("1", "v1"), upsert("2", "x"), upsert("1", "v2")]);
        // Key 1 keeps its latest value; key 2 unchanged; first-seen key order.
        assert_eq!(out, vec![upsert("1", "v2"), upsert("2", "x")]);
    }

    #[test]
    fn upsert_consolidation_collapses_upsert_then_delete() {
        let out = consolidate_upsert(vec![upsert("1", "v"), delete("1")]);
        assert_eq!(out, vec![delete("1")]);
    }

    #[test]
    fn release_holds_data_at_or_above_the_frontier() {
        let mut buf = ReleaseBuffer::new(Envelope::Diff);
        buf.push_data(5, diff("a", 1)).unwrap();
        buf.push_data(6, diff("b", 1)).unwrap();
        // Releasing below 6 closes t=5 only.
        assert_eq!(buf.release_below(6), vec![diff("a", 1)]);
        // t=6 is still buffered until a higher frontier.
        assert_eq!(buf.release_below(7), vec![diff("b", 1)]);
    }

    #[test]
    fn release_consolidates_across_the_window() {
        let mut buf = ReleaseBuffer::new(Envelope::Diff);
        buf.push_data(5, diff("a", 1)).unwrap();
        buf.push_data(6, diff("a", -1)).unwrap();
        buf.observe_progress(7).unwrap();
        // Insert then retract of the same row across two timestamps nets to
        // nothing in one batch.
        assert!(buf.release_below(7).is_empty());
    }

    #[test]
    fn data_below_the_frontier_is_rejected() {
        let mut buf = ReleaseBuffer::new(Envelope::Diff);
        buf.observe_progress(10).unwrap();
        assert!(buf.push_data(5, diff("late", 1)).is_err());
    }

    #[test]
    fn regressing_frontier_is_rejected() {
        let mut buf = ReleaseBuffer::new(Envelope::Diff);
        buf.observe_progress(10).unwrap();
        assert!(buf.observe_progress(9).is_err());
    }
}
