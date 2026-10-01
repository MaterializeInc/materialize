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

//! The progress-driven batcher: the state machine that turns a stream of
//! changes and progress markers into *consistent, closed* batches.
//!
//! This is the part every hand-rolled subscribe consumer gets wrong. The rule
//! it enforces:
//!
//! * Buffer changes as they arrive.
//! * When a progress marker advances the frontier to `F`, every change with a
//!   timestamp strictly below `F` is now final. Emit exactly those as one
//!   batch, tagged with frontier `F` and a resume token for `F`.
//! * Never emit a change before its timestamp is closed, and never emit the
//!   same timestamp twice.
//!
//! A consumer that applies each emitted batch and persists its token atomically
//! gets exactly-once *state*: after any crash, resuming from the last token
//! neither drops nor duplicates data.
//!
//! A single view is the one-member case of the [`crate::cohort`] engine: the
//! release frontier is the view's own progress frontier. Both share the
//! [`ReleaseBuffer`] buffer-and-consolidate core.

use crate::engine::ReleaseBuffer;
use crate::envelope::{Change, Envelope, StreamMessage};
use crate::error::SubscribeError;
use crate::token::ResumeToken;

/// A batch of changes for a contiguous, now-closed range of timestamps.
///
/// Every change in `updates` has a timestamp strictly below `frontier`, and no
/// future batch will contain a timestamp below `frontier`. `resume_token`
/// checkpoints exactly this position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsistentBatch {
    /// The finalized changes, consolidated to their net effect within this
    /// batch and ordered by first appearance.
    pub updates: Vec<Change>,
    /// The closed frontier: everything below it is present in this or an
    /// earlier batch.
    pub frontier: u64,
    /// The checkpoint for this position. Persist it atomically with the effects
    /// of `updates` to get exactly-once state.
    pub resume_token: ResumeToken,
    /// Whether this batch carries the initial snapshot. Only ever true once,
    /// and only for a subscription started with a snapshot.
    pub is_snapshot: bool,
}

impl ConsistentBatch {
    /// Whether the batch carries no changes. Empty batches still advance the
    /// frontier and carry a fresh token, which lets a consumer keep its
    /// checkpoint moving during idle periods so it does not age out of the
    /// source's retained history.
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}

/// Accumulates [`StreamMessage`]s and emits [`ConsistentBatch`]es as timestamps
/// close.
///
/// Feed every decoded message to [`Batcher::push`]. It returns `Some(batch)`
/// when a progress marker closes one or more timestamps, and `None` otherwise.
#[derive(Debug)]
pub struct Batcher {
    fingerprint: String,
    with_snapshot: bool,
    /// The buffer-and-consolidate core, shared with the cohort engine.
    buffer: ReleaseBuffer,
    /// The last frontier emitted, or `None` before the first progress marker.
    last_frontier: Option<u64>,
    /// The first frontier seen, which is the subscription's effective `AS OF`.
    /// The snapshot is the data closed just after this point.
    first_frontier: Option<u64>,
    /// Whether the snapshot batch has been emitted yet.
    snapshot_emitted: bool,
}

impl Batcher {
    /// Creates a batcher for a subscription with the given query `fingerprint`
    /// and `envelope` (which selects how a batch is consolidated).
    /// `with_snapshot` must match the subscription: `true` for an initial
    /// subscribe, `false` for a resume.
    pub fn new(fingerprint: impl Into<String>, envelope: Envelope, with_snapshot: bool) -> Self {
        Batcher {
            fingerprint: fingerprint.into(),
            with_snapshot,
            buffer: ReleaseBuffer::new(envelope),
            last_frontier: None,
            first_frontier: None,
            snapshot_emitted: false,
        }
    }

    /// Feeds one decoded message. Returns a batch when the frontier advances.
    pub fn push(
        &mut self,
        message: StreamMessage,
    ) -> Result<Option<ConsistentBatch>, SubscribeError> {
        match message {
            StreamMessage::Data { timestamp, change } => {
                self.buffer.push_data(timestamp, change)?;
                Ok(None)
            }
            StreamMessage::Progress { frontier } => self.advance(frontier),
        }
    }

    fn advance(&mut self, frontier: u64) -> Result<Option<ConsistentBatch>, SubscribeError> {
        if self.last_frontier == Some(frontier) {
            // No advance: nothing new is closed. A regression, by contrast, is
            // caught by `observe_progress` below.
            return Ok(None);
        }
        self.buffer.observe_progress(frontier)?;

        if self.first_frontier.is_none() {
            self.first_frontier = Some(frontier);
        }

        let updates = self.buffer.release_below(frontier);

        // The snapshot is the data closed once we advance past the first
        // frontier (the effective `AS OF`). Mark the first such batch.
        //
        // NOTE: this relies on the server emitting a leading `Progress(as_of)`
        // before any snapshot rows (it does, unconditionally, when `PROGRESS`
        // is set). That progress sets `first_frontier`; the snapshot rows sit
        // at `as_of` and are closed by the next, higher progress. A transport
        // that delivered snapshot data before any progress would mistag it.
        let is_snapshot =
            self.with_snapshot && !self.snapshot_emitted && Some(frontier) != self.first_frontier;
        if is_snapshot {
            self.snapshot_emitted = true;
        }

        self.last_frontier = Some(frontier);

        Ok(Some(ConsistentBatch {
            updates,
            frontier,
            resume_token: ResumeToken::new(frontier, self.fingerprint.clone()),
            is_snapshot,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(id: &str) -> Change {
        Change::Diff {
            row: vec![Some(id.to_string())],
            diff: 1,
        }
    }

    fn data(timestamp: u64, change: Change) -> StreamMessage {
        StreamMessage::Data { timestamp, change }
    }

    fn progress(frontier: u64) -> StreamMessage {
        StreamMessage::Progress { frontier }
    }

    #[test]
    fn buffers_until_the_timestamp_closes() {
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        // Data at t=5 is buffered, not emitted.
        assert_eq!(b.push(data(5, insert("a"))).unwrap(), None);
        assert_eq!(b.push(data(5, insert("b"))).unwrap(), None);
        // Progress to 6 closes t=5: both changes emitted together.
        let batch = b.push(progress(6)).unwrap().expect("batch");
        assert_eq!(batch.updates, vec![insert("a"), insert("b")]);
        assert_eq!(batch.frontier, 6);
        assert_eq!(batch.resume_token.frontier(), 6);
    }

    #[test]
    fn closes_a_timestamp_with_mixed_retractions_and_multiplicities() {
        // A single timestamp can hold inserts, retractions, and multiplicities
        // beyond one. Distinct rows all survive consolidation, in first-seen
        // order.
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        let insert3 = Change::Diff {
            row: vec![Some("a".into())],
            diff: 3,
        };
        let retract = Change::Diff {
            row: vec![Some("b".into())],
            diff: -1,
        };
        b.push(data(5, insert3.clone())).unwrap();
        b.push(data(5, retract.clone())).unwrap();
        b.push(data(5, insert("c"))).unwrap();
        let batch = b.push(progress(6)).unwrap().expect("batch");
        assert_eq!(batch.updates, vec![insert3, retract, insert("c")]);
    }

    #[test]
    fn batch_is_consolidated_to_its_net_effect() {
        // Repeated changes to the same row within a batch collapse: two inserts
        // and a retract of `a` net to +1, and `b` inserted then retracted
        // disappears entirely.
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        b.push(data(5, insert("a"))).unwrap();
        b.push(data(5, insert("a"))).unwrap();
        b.push(data(6, insert("b"))).unwrap();
        b.push(data(
            6,
            Change::Diff {
                row: vec![Some("a".into())],
                diff: -1,
            },
        ))
        .unwrap();
        b.push(data(
            6,
            Change::Diff {
                row: vec![Some("b".into())],
                diff: -1,
            },
        ))
        .unwrap();
        let batch = b.push(progress(7)).unwrap().expect("batch");
        assert_eq!(
            batch.updates,
            vec![Change::Diff {
                row: vec![Some("a".into())],
                diff: 1,
            }]
        );
    }

    #[test]
    fn data_at_the_frontier_stays_open() {
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        b.push(data(5, insert("a"))).unwrap();
        b.push(data(6, insert("b"))).unwrap();
        // Progress to 6 closes t=5 only; the t=6 change is still open.
        let batch = b.push(progress(6)).unwrap().expect("batch");
        assert_eq!(batch.updates, vec![insert("a")]);
        // Progress to 7 now closes t=6.
        let batch = b.push(progress(7)).unwrap().expect("batch");
        assert_eq!(batch.updates, vec![insert("b")]);
    }

    #[test]
    fn empty_batches_still_advance_the_token() {
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        let batch = b.push(progress(10)).unwrap().expect("batch");
        assert!(batch.is_empty());
        assert_eq!(batch.frontier, 10);
        assert_eq!(batch.resume_token.frontier(), 10);
    }

    #[test]
    fn duplicate_frontier_emits_nothing() {
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        assert!(b.push(progress(10)).unwrap().is_some());
        // A repeated progress at the same frontier is not an advance.
        assert_eq!(b.push(progress(10)).unwrap(), None);
    }

    #[test]
    fn snapshot_batch_is_the_first_past_the_initial_frontier() {
        // Initial subscribe: progress(as_of) then snapshot data at as_of, then
        // progress advances past it.
        let mut b = Batcher::new("fp", Envelope::Diff, true);
        // First progress reveals as_of = 100; leading batch is not the snapshot.
        let leading = b.push(progress(100)).unwrap().expect("batch");
        assert!(!leading.is_snapshot);
        assert!(leading.is_empty());
        // Snapshot rows arrive at t=100.
        b.push(data(100, insert("snap1"))).unwrap();
        b.push(data(100, insert("snap2"))).unwrap();
        // Advancing past 100 closes the snapshot; this batch is the snapshot.
        let snap = b.push(progress(101)).unwrap().expect("batch");
        assert!(snap.is_snapshot);
        assert_eq!(snap.updates, vec![insert("snap1"), insert("snap2")]);
        // Later batches are not snapshots.
        b.push(data(101, insert("live"))).unwrap();
        let live = b.push(progress(102)).unwrap().expect("batch");
        assert!(!live.is_snapshot);
    }

    #[test]
    fn resume_never_marks_a_snapshot() {
        // A resumed subscription (with_snapshot = false) never flags a snapshot.
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        b.push(progress(100)).unwrap();
        b.push(data(100, insert("x"))).unwrap();
        let batch = b.push(progress(101)).unwrap().expect("batch");
        assert!(!batch.is_snapshot);
    }

    #[test]
    fn late_data_below_the_frontier_is_a_protocol_error() {
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        b.push(progress(10)).unwrap();
        let err = b.push(data(5, insert("late"))).unwrap_err();
        assert!(matches!(err, SubscribeError::Protocol(_)), "{err:?}");
    }

    #[test]
    fn regressing_frontier_is_a_protocol_error() {
        let mut b = Batcher::new("fp", Envelope::Diff, false);
        b.push(progress(10)).unwrap();
        let err = b.push(progress(9)).unwrap_err();
        assert!(matches!(err, SubscribeError::Protocol(_)), "{err:?}");
    }

    #[test]
    fn token_carries_the_fingerprint() {
        let mut b = Batcher::new("my-fingerprint", Envelope::Diff, false);
        let batch = b.push(progress(1)).unwrap().expect("batch");
        assert_eq!(batch.resume_token.fingerprint(), "my-fingerprint");
    }
}
