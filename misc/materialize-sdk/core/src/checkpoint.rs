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

//! The checkpoint a durable subscription stores, and the rules that fence
//! stale writers.
//!
//! A store keeps one checkpoint per sink id, as the text [`Checkpoint::encode`]
//! returns. The encoding is deterministic, so every write is a compare-and-set
//! on that text:
//!
//! - [`Checkpoint::new`] is written only if the sink has no checkpoint.
//! - Every other write replaces the text read, only if the stored text is
//!   still that text, and fails if no checkpoint exists.
//!
//! A store with transactions makes the write of the checkpoint part of the
//! transaction that writes the data, so a failed condition rolls the data back
//! too. Such a store can read the checkpoint inside the transaction first, and
//! [`Checkpoint::check_commit`] or [`Checkpoint::check_write`] decides whether
//! the write may go ahead. After a write that failed or whose result is
//! unknown, the writer reads the checkpoint back and [`Checkpoint::settle`]
//! tells what happened.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::frontier::Frontier;
use crate::identity::Fingerprint;
use crate::wire;

/// The checkpoint format this release reads and writes.
///
/// Decoding refuses unknown fields at every level, so an older release cannot
/// read a newer checkpoint and write it back without the fields it does not
/// know. Adding a field therefore raises the format, and a migration from the
/// older formats goes into [`Checkpoint::decode`].
pub const FORMAT: u32 = 1;

/// A frontier for a message, or `none`.
fn shown(frontier: &Option<Frontier>) -> String {
    frontier.map_or_else(|| "none".into(), |frontier| frontier.to_string())
}

/// `value` with every object's keys in sorted order, also where the JSON
/// library keeps insertion order.
fn sorted(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let entries: BTreeMap<String, serde_json::Value> = map.into_iter().collect();
            serde_json::Value::Object(entries.into_iter().map(|(k, v)| (k, sorted(v))).collect())
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(sorted).collect())
        }
        other => other,
    }
}

/// The progress of a durable subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    /// The epoch of the worker that owns the checkpoint.
    pub epoch: u64,
    /// The snapshot the target is being written from. Raised by
    /// [`Checkpoint::restart`], so that writes for an earlier snapshot fail.
    pub generation: u64,
    /// Everything below it is in the target, or `None` before the snapshot's
    /// first batch commits.
    pub frontier: Option<Frontier>,
    /// The `AS OF` of the snapshot being written, set by
    /// [`Checkpoint::restart`] and cleared by the first commit. Never set
    /// together with `frontier`.
    pub snapshot_as_of: Option<u64>,
    /// What each subscription read, by the name the sink gave it. A name, not
    /// a position, so reordering a sink's subscriptions keeps each one's
    /// identity.
    pub subscriptions: BTreeMap<String, Fingerprint>,
}

/// A worker's epoch and the generation it writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Writer {
    /// The worker's epoch.
    #[serde(with = "wire::u64_string")]
    pub epoch: u64,
    /// The generation the worker writes.
    #[serde(with = "wire::u64_string")]
    pub generation: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    format: u32,
    #[serde(with = "wire::u64_string")]
    epoch: u64,
    #[serde(with = "wire::u64_string")]
    generation: u64,
    // Required like every other field, where serde would read a missing
    // `Option` as `None`.
    #[serde(deserialize_with = "Option::deserialize")]
    frontier: Option<Frontier>,
    #[serde(with = "wire::opt_u64_string")]
    snapshot_as_of: Option<u64>,
    subscriptions: BTreeMap<String, Fingerprint>,
}

#[derive(Deserialize)]
struct Format {
    format: u32,
}

/// The largest checkpoint text written, in UTF-8 bytes. The checkpoint grows
/// with the number of subscriptions and their columns.
pub const MAX_BYTES: usize = 1_048_576;

/// A checkpoint could not be read, changed, or written.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CheckpointError {
    /// The text is not a checkpoint this release reads, or a change would
    /// overflow a counter.
    #[error("invalid checkpoint: {0}")]
    Invalid(String),
    /// The encoded checkpoint is larger than the store accepts.
    #[error(
        "the checkpoint is {size} bytes, over the limit of {limit}. Subscribe to fewer \
         views, or to views with fewer columns"
    )]
    TooLarge {
        /// The size of the encoded text.
        size: usize,
        /// The limit.
        limit: usize,
    },
}

/// What to do with a batch whose commit [`Checkpoint::check_commit`] allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitCheck {
    /// Write the batch and [`Checkpoint::committed`].
    Apply,
    /// The stored frontier is already the batch's upper, so an earlier attempt
    /// of this commit succeeded and only its response was lost. Write nothing,
    /// and treat the batch as committed.
    AlreadyCommitted,
}

/// Why a write must not be applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "conflict", rename_all = "snake_case")]
#[non_exhaustive]
pub enum CommitConflict {
    /// A newer worker took the checkpoint over. This worker must stop.
    #[error("fenced: epoch {stored} took over from epoch {epoch}")]
    Fenced {
        /// The stored epoch.
        #[serde(with = "wire::u64_string")]
        stored: u64,
        /// This worker's epoch.
        #[serde(with = "wire::u64_string")]
        epoch: u64,
    },
    /// The owner restarted the snapshot, so a write for an earlier generation
    /// belongs to a stream that was abandoned.
    #[error("superseded: generation {stored} replaced generation {generation}")]
    Superseded {
        /// The stored generation.
        #[serde(with = "wire::u64_string")]
        stored: u64,
        /// The generation of the write.
        #[serde(with = "wire::u64_string")]
        generation: u64,
    },
    /// The range does not advance: `upper` is not above `lower`. The caller
    /// passed the wrong frontiers.
    #[error("the range from {lower} to {upper} does not advance")]
    InvalidRange {
        /// The frontier the range starts at.
        lower: Frontier,
        /// The frontier the range ends at.
        upper: Frontier,
    },
    /// The snapshot's updates are at its `AS OF`, and `upper` does not close
    /// it. The caller passed the wrong frontier.
    #[error("the frontier {upper} does not close the snapshot as of {as_of}")]
    SnapshotNotClosed {
        /// The snapshot's `AS OF`.
        #[serde(with = "wire::u64_string")]
        as_of: u64,
        /// The frontier the commit ends at.
        upper: Frontier,
    },
    /// The stored checkpoint is not one this writer's own writes could have
    /// left, so something outside the protocol changed it.
    #[error(
        "the stored checkpoint (epoch {}, generation {}, frontier {}{}) does not follow from \
         epoch {}, generation {} at frontier {}",
        stored.epoch,
        stored.generation,
        shown(stored_frontier),
        if *same_subscriptions { "" } else { ", other subscriptions" },
        writer.epoch,
        writer.generation,
        shown(lower)
    )]
    Diverged {
        /// The stored epoch and generation.
        stored: Writer,
        /// The stored frontier.
        stored_frontier: Option<Frontier>,
        /// Whether the stored subscriptions are the expected ones.
        same_subscriptions: bool,
        /// The writer.
        writer: Writer,
        /// The frontier the writer expected.
        lower: Option<Frontier>,
    },
}

/// What a write that failed or whose result is unknown came to. See
/// [`Checkpoint::settle`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "settlement", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Settlement {
    /// The write applied.
    Applied,
    /// Send the same write again.
    SendAgain,
    /// Take over again from the checkpoint read back.
    TakeOverAgain,
    /// Another write came in between, for this reason.
    Conflict {
        /// Why.
        conflict: CommitConflict,
    },
}

impl Checkpoint {
    /// The checkpoint of a new sink, owned by epoch 1 at generation 1, with no
    /// frontier.
    pub fn new(subscriptions: BTreeMap<String, Fingerprint>) -> Checkpoint {
        Checkpoint {
            epoch: 1,
            generation: 1,
            frontier: None,
            snapshot_as_of: None,
            subscriptions,
        }
    }

    /// The owner's epoch and generation.
    pub fn writer(&self) -> Writer {
        Writer {
            epoch: self.epoch,
            generation: self.generation,
        }
    }

    /// The checkpoint a starting worker writes to take over from the current
    /// owner, whose writes then fail with [`CommitConflict::Fenced`]. See
    /// [`Checkpoint::settle`] for a take-over whose outcome is unknown.
    pub fn take_over(&self) -> Result<Checkpoint, CheckpointError> {
        let epoch = self
            .epoch
            .checked_add(1)
            .ok_or_else(|| CheckpointError::Invalid("the epoch is at its maximum".into()))?;
        Ok(Checkpoint {
            epoch,
            ..self.clone()
        })
    }

    /// The checkpoint the owner writes before a new snapshot's first write,
    /// including the first snapshot and a snapshot that replaces the target's
    /// state, as after [`crate::IdentityChange::Swapped`] or lost history. It
    /// raises the generation, records `subscriptions` and the snapshot's
    /// `as_of`, and clears the frontier, which is where the snapshot's first
    /// batch starts.
    pub fn restart(
        &self,
        subscriptions: BTreeMap<String, Fingerprint>,
        as_of: u64,
    ) -> Result<Checkpoint, CheckpointError> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| CheckpointError::Invalid("the generation is at its maximum".into()))?;
        Ok(Checkpoint {
            epoch: self.epoch,
            generation,
            frontier: None,
            snapshot_as_of: Some(as_of),
            subscriptions,
        })
    }

    /// The checkpoint with `subscriptions` recorded and all else kept, as
    /// after [`crate::IdentityChange::Replaced`].
    pub fn with_subscriptions(&self, subscriptions: BTreeMap<String, Fingerprint>) -> Checkpoint {
        Checkpoint {
            subscriptions,
            ..self.clone()
        }
    }

    /// The checkpoint after a commit up to `upper`.
    pub fn committed(&self, upper: Frontier) -> Checkpoint {
        Checkpoint {
            frontier: Some(upper),
            snapshot_as_of: None,
            ..self.clone()
        }
    }

    /// Decides, before writing, whether a commit up to `upper` by the writer
    /// that read `expected` may go ahead against this stored checkpoint.
    pub fn check_commit(
        &self,
        expected: &Checkpoint,
        upper: Frontier,
    ) -> Result<CommitCheck, CommitConflict> {
        if let Some(lower) = expected.frontier
            && lower >= upper
        {
            return Err(CommitConflict::InvalidRange { lower, upper });
        }
        if let Some(as_of) = expected.snapshot_as_of
            && !upper.contains(as_of)
        {
            return Err(CommitConflict::SnapshotNotClosed { as_of, upper });
        }
        self.check_owner(expected.writer())?;
        if self == expected {
            Ok(CommitCheck::Apply)
        } else if *self == expected.committed(upper) {
            Ok(CommitCheck::AlreadyCommitted)
        } else {
            Err(self.diverged(expected))
        }
    }

    /// Decides, before writing, whether a write that commits no frontier, such
    /// as part of a snapshot, by the writer that read `expected` may go ahead
    /// against this stored checkpoint.
    pub fn check_write(&self, expected: &Checkpoint) -> Result<(), CommitConflict> {
        self.check_owner(expected.writer())?;
        if self == expected {
            Ok(())
        } else {
            Err(self.diverged(expected))
        }
    }

    fn check_owner(&self, writer: Writer) -> Result<(), CommitConflict> {
        if self.epoch > writer.epoch {
            return Err(CommitConflict::Fenced {
                stored: self.epoch,
                epoch: writer.epoch,
            });
        }
        if self.epoch == writer.epoch && self.generation > writer.generation {
            return Err(CommitConflict::Superseded {
                stored: self.generation,
                generation: writer.generation,
            });
        }
        Ok(())
    }

    fn diverged(&self, expected: &Checkpoint) -> CommitConflict {
        CommitConflict::Diverged {
            stored: self.writer(),
            stored_frontier: self.frontier,
            same_subscriptions: self.subscriptions == expected.subscriptions,
            writer: expected.writer(),
            lower: expected.frontier,
        }
    }

    /// Settles a write from `expected` to `new` that failed or whose result is
    /// unknown, such as one whose response was lost, against this checkpoint
    /// read back after it.
    ///
    /// A read cannot prove that a write in flight will not still apply, so
    /// [`Settlement::SendAgain`] sends the same write. For a write that changes
    /// the text, only one of the two can apply. A write that changes none, such
    /// as a snapshot write, must be safe to apply twice.
    pub fn settle(&self, expected: &Checkpoint, new: &Checkpoint) -> Settlement {
        // Two workers that take over from the same checkpoint write the same
        // text, so a read cannot tell whose take-over landed, and another
        // worker may have taken over in between. Only a take-over that is
        // known to apply settles it, and a second one fences any other.
        if new.epoch != expected.epoch {
            return Settlement::TakeOverAgain;
        }
        if self == expected {
            return Settlement::SendAgain;
        }
        // An earlier copy of this write landed, and the copy that failed
        // found it.
        if self == new {
            return Settlement::Applied;
        }
        let conflict = self
            .check_owner(expected.writer())
            .err()
            .unwrap_or_else(|| self.diverged(expected));
        Settlement::Conflict { conflict }
    }

    /// Encodes the checkpoint as JSON, at most [`MAX_BYTES`] long. The same
    /// checkpoint always encodes to the same text.
    pub fn encode(&self) -> Result<String, CheckpointError> {
        self.encode_within(MAX_BYTES)
    }

    /// Encodes the checkpoint as JSON, for a store that holds at most `limit`
    /// bytes. A limit above [`MAX_BYTES`] counts as [`MAX_BYTES`].
    pub fn encode_within(&self, limit: usize) -> Result<String, CheckpointError> {
        self.validate()?;
        let limit = limit.min(MAX_BYTES);
        let text = self.text()?;
        if text.len() > limit {
            return Err(CheckpointError::TooLarge {
                size: text.len(),
                limit,
            });
        }
        Ok(text)
    }

    /// The checkpoint's canonical text: compact JSON with every object's keys
    /// in sorted order, which any language can produce and compare.
    fn text(&self) -> Result<String, CheckpointError> {
        let wire = Wire {
            format: FORMAT,
            epoch: self.epoch,
            generation: self.generation,
            frontier: self.frontier,
            snapshot_as_of: self.snapshot_as_of,
            subscriptions: self.subscriptions.clone(),
        };
        let value =
            serde_json::to_value(&wire).map_err(|e| CheckpointError::Invalid(e.to_string()))?;
        Ok(sorted(value).to_string())
    }

    /// Decodes a checkpoint written by [`Checkpoint::encode`]. Refuses any
    /// other text for the same checkpoint, because every write compares the
    /// stored text with the exact encoding.
    pub fn decode(encoded: &str) -> Result<Checkpoint, CheckpointError> {
        let Format { format } = serde_json::from_str(encoded)
            .map_err(|e| CheckpointError::Invalid(format!("no format version: {e}")))?;
        if format != FORMAT {
            return Err(CheckpointError::Invalid(format!(
                "format {format} is not supported, this release reads format {FORMAT}"
            )));
        }
        let wire: Wire =
            serde_json::from_str(encoded).map_err(|e| CheckpointError::Invalid(e.to_string()))?;
        let checkpoint = Checkpoint {
            epoch: wire.epoch,
            generation: wire.generation,
            frontier: wire.frontier,
            snapshot_as_of: wire.snapshot_as_of,
            subscriptions: wire.subscriptions,
        };
        checkpoint.validate()?;
        if checkpoint.text()? != encoded {
            return Err(CheckpointError::Invalid(
                "the text is not the checkpoint's exact encoding, so the store changed it: keep \
                 it byte for byte"
                    .into(),
            ));
        }
        Ok(checkpoint)
    }

    /// Refuses a checkpoint that no sequence of writes can leave, so that
    /// nothing is written that a later decode refuses.
    fn validate(&self) -> Result<(), CheckpointError> {
        if self.frontier.is_some() && self.snapshot_as_of.is_some() {
            return Err(CheckpointError::Invalid(
                "a checkpoint with a frontier has no snapshot in progress".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::Envelope;
    use crate::identity::Column;
    use crate::statement::ObjectRef;

    fn frontier(t: u64) -> Frontier {
        Frontier::new(t).unwrap()
    }

    fn checkpoint(epoch: u64, generation: u64, at: Option<u64>) -> Checkpoint {
        Checkpoint {
            epoch,
            generation,
            frontier: at.map(frontier),
            snapshot_as_of: None,
            subscriptions: BTreeMap::new(),
        }
    }

    fn writer(epoch: u64, generation: u64) -> Writer {
        Writer { epoch, generation }
    }

    fn fingerprint(id: &str) -> Fingerprint {
        Fingerprint {
            object: ObjectRef {
                id: id.into(),
                database: Some("db".into()),
                schema: "public".into(),
                name: "orders".into(),
            },
            shard: "s123".into(),
            envelope: Envelope::Diff,
            columns: vec![Column {
                name: "id".into(),
                type_oid: 20,
                type_mod: -1,
                nullable: false,
            }],
        }
    }

    #[test]
    fn a_commit_applies_from_the_expected_frontier() {
        let stored = checkpoint(3, 1, Some(10));
        assert_eq!(
            stored.check_commit(&checkpoint(3, 1, Some(10)), frontier(20)),
            Ok(CommitCheck::Apply)
        );
        assert_eq!(stored.committed(frontier(20)).frontier, Some(frontier(20)));
        assert_eq!(
            checkpoint(1, 1, None).check_commit(&checkpoint(1, 1, None), frontier(5)),
            Ok(CommitCheck::Apply)
        );
    }

    #[test]
    fn a_retried_commit_is_recognized() {
        let stored = checkpoint(3, 1, Some(20));
        assert_eq!(
            stored.check_commit(&checkpoint(3, 1, Some(10)), frontier(20)),
            Ok(CommitCheck::AlreadyCommitted)
        );
    }

    #[test]
    fn a_stale_worker_is_fenced() {
        let stored = checkpoint(4, 1, Some(10));
        let fenced = Err(CommitConflict::Fenced {
            stored: 4,
            epoch: 3,
        });
        assert_eq!(
            stored.check_commit(&checkpoint(3, 1, Some(10)), frontier(20)),
            fenced
        );
        assert_eq!(
            stored.check_write(&checkpoint(3, 1, Some(10))),
            Err(fenced.unwrap_err())
        );
    }

    #[test]
    fn a_write_from_before_a_restart_is_superseded() {
        let before = checkpoint(3, 1, None);
        let after = before.restart(BTreeMap::new(), 40).unwrap();
        assert_eq!(after.writer(), writer(3, 2));
        assert_eq!(after.frontier, None);
        assert_eq!(after.snapshot_as_of, Some(40));
        assert_eq!(after.committed(frontier(50)).snapshot_as_of, None);
        let superseded = CommitConflict::Superseded {
            stored: 2,
            generation: 1,
        };
        assert_eq!(
            after.check_commit(&checkpoint(3, 1, None), frontier(50)),
            Err(superseded.clone())
        );
        assert_eq!(after.check_write(&checkpoint(3, 1, None)), Err(superseded));
        assert_eq!(
            after.check_commit(&after, frontier(50)),
            Ok(CommitCheck::Apply)
        );
    }

    #[test]
    fn an_unexplained_checkpoint_diverges() {
        for (stored, epoch) in [
            (checkpoint(3, 1, Some(15)), 3),
            (checkpoint(2, 1, Some(10)), 3),
        ] {
            assert!(
                matches!(
                    stored.check_commit(&checkpoint(epoch, 1, Some(10)), frontier(20)),
                    Err(CommitConflict::Diverged { .. })
                ),
                "{stored:?}"
            );
        }
        assert!(matches!(
            checkpoint(3, 1, None).check_write(&checkpoint(3, 2, None)),
            Err(CommitConflict::Diverged { .. })
        ));
    }

    #[test]
    fn a_change_to_only_the_subscriptions_diverges() {
        let expected = checkpoint(3, 1, Some(10));
        let subscriptions = BTreeMap::from([("orders".to_string(), fingerprint("u8"))]);
        let diverged = Err(CommitConflict::Diverged {
            stored: writer(3, 1),
            stored_frontier: Some(frontier(10)),
            same_subscriptions: false,
            writer: writer(3, 1),
            lower: Some(frontier(10)),
        });
        let stored = expected.with_subscriptions(subscriptions.clone());
        assert_eq!(stored.check_commit(&expected, frontier(20)), diverged);
        assert_eq!(stored.check_write(&expected), diverged.map(|_| ()));
        // A commit that landed is recognized only with the expected subscriptions.
        let landed = expected.committed(frontier(20));
        assert_eq!(
            landed.check_commit(&expected, frontier(20)),
            Ok(CommitCheck::AlreadyCommitted)
        );
        assert!(matches!(
            landed
                .with_subscriptions(subscriptions)
                .check_commit(&expected, frontier(20)),
            Err(CommitConflict::Diverged {
                same_subscriptions: false,
                ..
            })
        ));
        let message = stored.check_write(&expected).unwrap_err().to_string();
        assert!(message.contains("other subscriptions"), "{message}");
    }

    #[test]
    fn an_unknown_write_is_settled_from_the_checkpoint_read_back() {
        let expected = checkpoint(3, 1, Some(10));
        let new = expected.committed(frontier(20));
        assert_eq!(new.settle(&expected, &new), Settlement::Applied);
        assert_eq!(expected.settle(&expected, &new), Settlement::SendAgain);
        // A write that changes no text cannot be seen in a read, so it is sent
        // again while the expected text is stored, and stops once fenced.
        assert_eq!(expected.settle(&expected, &expected), Settlement::SendAgain);
        assert_eq!(
            checkpoint(4, 1, Some(10)).settle(&expected, &expected),
            Settlement::Conflict {
                conflict: CommitConflict::Fenced {
                    stored: 4,
                    epoch: 3
                }
            }
        );
        assert_eq!(
            checkpoint(4, 1, Some(10)).settle(&expected, &new),
            Settlement::Conflict {
                conflict: CommitConflict::Fenced {
                    stored: 4,
                    epoch: 3
                }
            }
        );
        assert!(matches!(
            checkpoint(3, 1, Some(15)).settle(&expected, &new),
            Settlement::Conflict {
                conflict: CommitConflict::Diverged { .. }
            }
        ));
    }

    #[test]
    fn a_restart_that_landed_is_applied_not_superseded() {
        let expected = checkpoint(3, 1, Some(10));
        let new = expected.restart(BTreeMap::new(), 40).unwrap();
        assert_eq!(new.settle(&expected, &new), Settlement::Applied);
    }

    #[test]
    fn an_unknown_take_over_is_always_taken_over_again() {
        let expected = checkpoint(3, 1, Some(10));
        let new = expected.take_over().unwrap();
        // The same text can be another worker's take-over.
        assert_eq!(new.settle(&expected, &new), Settlement::TakeOverAgain);
        assert_eq!(expected.settle(&expected, &new), Settlement::TakeOverAgain);
    }

    #[test]
    fn a_checkpoint_over_the_limit_is_refused() {
        let stored = checkpoint(1, 1, None);
        let text = stored.encode().unwrap();
        let size = text.len();
        assert_eq!(stored.encode_within(size).unwrap(), text);
        assert_eq!(stored.encode_within(usize::MAX).unwrap(), text);
        assert_eq!(
            stored.encode_within(size - 1),
            Err(CheckpointError::TooLarge {
                size,
                limit: size - 1
            })
        );
    }

    #[test]
    fn a_checkpoint_is_capped_at_max_bytes() {
        let sized = |len: usize| Checkpoint {
            subscriptions: BTreeMap::from([("x".repeat(len), fingerprint("u1"))]),
            ..checkpoint(1, 1, None)
        };
        let base = sized(0).encode().unwrap().len();
        assert_eq!(sized(MAX_BYTES - base).encode().unwrap().len(), MAX_BYTES);
        let over = sized(MAX_BYTES - base + 1);
        let too_large = Err(CheckpointError::TooLarge {
            size: MAX_BYTES + 1,
            limit: MAX_BYTES,
        });
        assert_eq!(over.encode(), too_large);
        assert_eq!(over.encode_within(usize::MAX), too_large);
    }

    #[test]
    fn a_checkpoint_decode_would_refuse_is_never_encoded() {
        let impossible = Checkpoint {
            snapshot_as_of: Some(4),
            ..checkpoint(1, 2, Some(5))
        };
        assert!(matches!(
            impossible.encode(),
            Err(CheckpointError::Invalid(_))
        ));
    }

    #[test]
    fn a_range_that_does_not_advance_is_refused() {
        let stored = checkpoint(1, 1, Some(10));
        for at in [10, 20] {
            assert_eq!(
                stored.check_commit(&checkpoint(1, 1, Some(at)), frontier(10)),
                Err(CommitConflict::InvalidRange {
                    lower: frontier(at),
                    upper: frontier(10)
                })
            );
        }
    }

    #[test]
    fn a_snapshot_commit_must_close_its_as_of() {
        for as_of in [0, 40, u64::MAX] {
            let snapshot = Checkpoint {
                snapshot_as_of: Some(as_of),
                ..checkpoint(1, 1, None)
            };
            let closed = Frontier::after(as_of);
            assert_eq!(
                snapshot.check_commit(&snapshot, closed),
                Ok(CommitCheck::Apply),
                "as of {as_of}"
            );
            assert_eq!(
                snapshot.committed(closed).check_commit(&snapshot, closed),
                Ok(CommitCheck::AlreadyCommitted),
                "as of {as_of}"
            );
            if let Some(open) = Frontier::new(as_of) {
                assert_eq!(
                    snapshot.check_commit(&snapshot, open),
                    Err(CommitConflict::SnapshotNotClosed { as_of, upper: open })
                );
            }
        }
    }

    #[test]
    fn take_over_raises_the_epoch_and_keeps_the_rest() {
        let stored = checkpoint(3, 2, Some(10));
        let next = stored.take_over().unwrap();
        assert_eq!(next.writer(), writer(4, 2));
        assert_eq!(next.frontier, stored.frontier);
        assert!(checkpoint(u64::MAX, 1, None).take_over().is_err());
        assert!(
            checkpoint(1, u64::MAX, None)
                .restart(BTreeMap::new(), 40)
                .is_err()
        );
    }

    #[test]
    fn with_subscriptions_keeps_the_rest() {
        let stored = checkpoint(3, 2, Some(10));
        let subscriptions = BTreeMap::from([("orders".to_string(), fingerprint("u8"))]);
        let next = stored.with_subscriptions(subscriptions.clone());
        assert_eq!(next.writer(), stored.writer());
        assert_eq!(next.frontier, stored.frontier);
        assert_eq!(next.subscriptions, subscriptions);
    }

    #[test]
    fn encoding_round_trips_and_is_stable() {
        let stored = Checkpoint {
            epoch: 2,
            generation: 1,
            frontier: Some(frontier(1_700_000_000_000)),
            snapshot_as_of: None,
            subscriptions: BTreeMap::from([("orders".to_string(), fingerprint("u7"))]),
        };
        let encoded = stored.encode().unwrap();
        assert_eq!(
            encoded,
            concat!(
                r#"{"epoch":"2","format":1,"frontier":"1700000000000","generation":"1","#,
                r#""snapshot_as_of":null,"#,
                r#""subscriptions":{"orders":"#,
                r#"{"columns":[{"name":"id","nullable":false,"type_mod":-1,"type_oid":20}],"#,
                r#""envelope":{"kind":"diff"},"#,
                r#""object":{"database":"db","id":"u7","name":"orders","schema":"public"},"#,
                r#""shard":"s123"}}}"#,
            )
        );
        assert_eq!(Checkpoint::decode(&encoded), Ok(stored));
        // The same checkpoint in another key order or with spaces is refused,
        // because a write compares the exact text.
        let reordered =
            encoded.replacen(r#""epoch":"2","format":1"#, r#""format":1,"epoch":"2""#, 1);
        assert!(Checkpoint::decode(&reordered).is_err());
        assert!(Checkpoint::decode(&encoded.replace(',', ", ")).is_err());
    }

    #[test]
    fn decoding_refuses_other_formats_and_unknown_fields() {
        let base = r#"{"epoch":"1","format":1,"frontier":null,"generation":"1","snapshot_as_of":null,"subscriptions":{}}"#;
        assert!(Checkpoint::decode(base).is_ok());
        let other = base.replace(r#""format":1"#, r#""format":2"#);
        assert!(
            Checkpoint::decode(&other)
                .unwrap_err()
                .to_string()
                .contains("format 2")
        );
        let extra = base.replace(r#""subscriptions":{}"#, r#""subscriptions":{},"x":"1""#);
        assert!(Checkpoint::decode(&extra).is_err());
        let mut nested = Checkpoint::new(BTreeMap::from([("o".to_string(), fingerprint("u1"))]))
            .encode()
            .unwrap();
        nested = nested.replace(r#""shard":"s123""#, r#""shard":"s123","future":"1""#);
        assert!(
            Checkpoint::decode(&nested).is_err(),
            "a nested unknown field"
        );
        assert!(Checkpoint::decode("not json").is_err());
        let zero = base.replace(r#""frontier":null"#, r#""frontier":"0""#);
        assert!(Checkpoint::decode(&zero).is_err());
        let both = base
            .replace(r#""frontier":null"#, r#""frontier":"5""#)
            .replace(r#""snapshot_as_of":null"#, r#""snapshot_as_of":"4""#);
        assert!(
            Checkpoint::decode(&both).is_err(),
            "a frontier and a snapshot in progress"
        );
    }
}
