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

//! The parts of the sink contract that every language shares: what a sink
//! declares about its target, and the checkpoint writes the SDK hands it.
//!
//! The sink interface itself belongs to each language package. Sink code
//! treats the checkpoint text as opaque, so every fact it needs from a write
//! travels in [`WriteKind`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::checkpoint::{Checkpoint, CheckpointError, CommitConflict};
use crate::error::UsageError;
use crate::frontier::Frontier;
use crate::identity::Fingerprint;
use crate::wire;

/// What a sink's target can do, which decides the delivery guarantee.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SinkType {
    /// Each write applies its batch and its checkpoint write together or not
    /// at all, and the checkpoint condition sees the latest committed
    /// checkpoint. State is exactly once.
    Transactional,
    /// A batch applies before its checkpoint write, so it can apply more than
    /// once. A target whose transactions cannot meet the `Transactional`
    /// requirements is this type.
    NonTransactional,
}

/// A compare-and-set of the checkpoint text: the write replaces `expected`
/// with `new`, only if the stored text is still `expected`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CheckpointWrite {
    /// The text the write expects to find.
    pub expected: String,
    /// The text the write leaves. Equal to `expected` for a write that commits
    /// no frontier, which only checks the condition.
    pub new: String,
    /// What the write does.
    #[serde(flatten)]
    pub kind: WriteKind,
}

/// What a checkpoint write does, with what a sink needs to know about it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WriteKind {
    /// A starting worker takes the checkpoint over from its owner.
    TakeOver {
        /// The frontier the worker read, which it already knows is committed.
        frontier: Option<Frontier>,
    },
    /// A commit of everything below `frontier`.
    Commit {
        /// The new frontier.
        frontier: Frontier,
    },
    /// The start of a snapshot, before its first write.
    StartSnapshot {
        /// The snapshot's `AS OF`.
        #[serde(with = "wire::u64_string")]
        as_of: u64,
        /// The generation before this snapshot attempt.
        #[serde(with = "wire::u64_string")]
        old_generation: u64,
        /// The generation the snapshot writes.
        #[serde(with = "wire::u64_string")]
        generation: u64,
    },
    /// Part of a snapshot, which commits no frontier.
    SnapshotWrite {
        /// The generation the snapshot writes.
        #[serde(with = "wire::u64_string")]
        generation: u64,
    },
    /// The commit that completes a snapshot, after which readers see
    /// `generation`.
    CompleteSnapshot {
        /// The new frontier.
        frontier: Frontier,
        /// The generation the snapshot wrote.
        #[serde(with = "wire::u64_string")]
        generation: u64,
    },
    /// New subscription fingerprints, as after a replaced object.
    Fingerprints,
}

/// Why a checkpoint write could not be built.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WriteError {
    /// The new checkpoint is invalid or too large.
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    /// The commit's range is wrong. See [`Checkpoint::check_commit`].
    #[error(transparent)]
    Conflict(#[from] CommitConflict),
    /// The write does not fit the checkpoint's state.
    #[error(transparent)]
    Usage(#[from] UsageError),
}

impl CheckpointWrite {
    /// The write a starting worker sends to take over `stored`, and the
    /// checkpoint it leaves. See [`Checkpoint::take_over`].
    pub fn take_over(stored: &Checkpoint) -> Result<(CheckpointWrite, Checkpoint), WriteError> {
        let next = stored.take_over()?;
        let kind = WriteKind::TakeOver {
            frontier: stored.frontier,
        };
        Ok((CheckpointWrite::between(stored, &next, kind)?, next))
    }

    /// The write that commits everything below `upper`, and the checkpoint it
    /// leaves. The first commit after [`CheckpointWrite::start_snapshot`]
    /// completes the snapshot.
    pub fn commit(
        current: &Checkpoint,
        upper: Frontier,
    ) -> Result<(CheckpointWrite, Checkpoint), WriteError> {
        if current.frontier.is_none() && current.snapshot_as_of.is_none() {
            return Err(UsageError::new("no snapshot has started").into());
        }
        // Against itself, this checks only the range.
        current.check_commit(current, upper)?;
        let next = current.committed(upper);
        let kind = match current.snapshot_as_of {
            Some(_) => WriteKind::CompleteSnapshot {
                frontier: upper,
                generation: current.generation,
            },
            None => WriteKind::Commit { frontier: upper },
        };
        Ok((CheckpointWrite::between(current, &next, kind)?, next))
    }

    /// The write that starts a snapshot at `as_of` of `subscriptions`, and the
    /// checkpoint it leaves. See [`Checkpoint::restart`].
    pub fn start_snapshot(
        current: &Checkpoint,
        subscriptions: BTreeMap<String, Fingerprint>,
        as_of: u64,
    ) -> Result<(CheckpointWrite, Checkpoint), WriteError> {
        let next = current.restart(subscriptions, as_of)?;
        let kind = WriteKind::StartSnapshot {
            as_of,
            old_generation: current.generation,
            generation: next.generation,
        };
        Ok((CheckpointWrite::between(current, &next, kind)?, next))
    }

    /// The write that sends part of the snapshot in progress. It leaves the
    /// checkpoint unchanged.
    pub fn snapshot_write(current: &Checkpoint) -> Result<CheckpointWrite, WriteError> {
        if current.snapshot_as_of.is_none() {
            return Err(UsageError::new("no snapshot is in progress").into());
        }
        let kind = WriteKind::SnapshotWrite {
            generation: current.generation,
        };
        CheckpointWrite::between(current, current, kind)
    }

    /// The write that records new `subscriptions`, and the checkpoint it
    /// leaves. See [`Checkpoint::with_subscriptions`].
    pub fn fingerprints(
        current: &Checkpoint,
        subscriptions: BTreeMap<String, Fingerprint>,
    ) -> Result<(CheckpointWrite, Checkpoint), WriteError> {
        let next = current.with_subscriptions(subscriptions);
        Ok((
            CheckpointWrite::between(current, &next, WriteKind::Fingerprints)?,
            next,
        ))
    }

    fn between(
        expected: &Checkpoint,
        new: &Checkpoint,
        kind: WriteKind,
    ) -> Result<CheckpointWrite, WriteError> {
        Ok(CheckpointWrite {
            expected: expected.encode()?,
            new: new.encode()?,
            kind,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontier(f: u64) -> Frontier {
        Frontier::new(f).unwrap()
    }

    fn committed(at: u64) -> Checkpoint {
        Checkpoint::new(BTreeMap::new()).committed(frontier(at))
    }

    #[test]
    fn a_write_expects_the_current_text_and_leaves_the_next() {
        let current = committed(10);
        let (write, next) = CheckpointWrite::commit(&current, frontier(20)).unwrap();
        assert_eq!(Checkpoint::decode(&write.expected).unwrap(), current);
        assert_eq!(Checkpoint::decode(&write.new).unwrap(), next);
        assert_eq!(next.frontier, Some(frontier(20)));
        assert_eq!(
            write.kind,
            WriteKind::Commit {
                frontier: frontier(20)
            }
        );
    }

    #[test]
    fn the_first_commit_after_a_restart_completes_the_snapshot() {
        let (start, snapshot) =
            CheckpointWrite::start_snapshot(&committed(10), BTreeMap::new(), 40).unwrap();
        assert_eq!(
            start.kind,
            WriteKind::StartSnapshot {
                as_of: 40,
                old_generation: 1,
                generation: 2
            }
        );
        let (complete, done) = CheckpointWrite::commit(&snapshot, Frontier::after(40)).unwrap();
        assert_eq!(
            complete.kind,
            WriteKind::CompleteSnapshot {
                frontier: Frontier::after(40),
                generation: 2
            }
        );
        assert_eq!(done.snapshot_as_of, None);
        let (later, _) = CheckpointWrite::commit(&done, Frontier::after(50)).unwrap();
        assert!(matches!(later.kind, WriteKind::Commit { .. }));
    }

    #[test]
    fn a_snapshot_write_only_checks_the_condition() {
        let (_, snapshot) =
            CheckpointWrite::start_snapshot(&committed(10), BTreeMap::new(), 40).unwrap();
        let write = CheckpointWrite::snapshot_write(&snapshot).unwrap();
        assert_eq!(write.expected, write.new);
        assert_eq!(write.kind, WriteKind::SnapshotWrite { generation: 2 });
        assert!(matches!(
            CheckpointWrite::snapshot_write(&committed(10)),
            Err(WriteError::Usage(_))
        ));
    }

    #[test]
    fn a_commit_with_a_wrong_range_is_not_built() {
        assert!(matches!(
            CheckpointWrite::commit(&committed(10), frontier(10)),
            Err(WriteError::Conflict(CommitConflict::InvalidRange { .. }))
        ));
        let (_, snapshot) =
            CheckpointWrite::start_snapshot(&committed(10), BTreeMap::new(), 40).unwrap();
        assert!(matches!(
            CheckpointWrite::commit(&snapshot, frontier(40)),
            Err(WriteError::Conflict(
                CommitConflict::SnapshotNotClosed { .. }
            ))
        ));
    }

    #[test]
    fn a_commit_needs_a_frontier_or_a_snapshot_in_progress() {
        assert!(matches!(
            CheckpointWrite::commit(&Checkpoint::new(BTreeMap::new()), frontier(10)),
            Err(WriteError::Usage(_))
        ));
    }

    #[test]
    fn a_take_over_carries_the_frontier_it_read() {
        let (write, next) = CheckpointWrite::take_over(&committed(10)).unwrap();
        assert_eq!(
            write.kind,
            WriteKind::TakeOver {
                frontier: Some(frontier(10))
            }
        );
        assert_eq!(next.epoch, 2);
    }
}
