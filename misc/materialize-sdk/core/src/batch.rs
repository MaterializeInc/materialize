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

//! Holding updates until their timestamps close, and releasing them as
//! batches.
//!
//! The batcher relies on two promises from the `SUBSCRIBE` documentation
//! (`doc/user/content/sql/subscribe.md`, "PROGRESS"): the first row is a
//! progress row at the `AS OF`, and updates arrive in non-decreasing timestamp
//! order. It checks both, along with every other bound it relies on, and fails
//! with a [`ProtocolError`] instead of releasing a frontier it cannot prove.
//!
//! A timestamp closes when a progress row passes it, or when an update at a
//! later timestamp arrives, because the server sends all updates of a time
//! range together, sorted by time and consolidated per timestamp
//! (`ActiveSubscribe::process_response` in
//! `src/adapter/src/active_compute_sink.rs`). The batcher releases at progress
//! rows, and at a later update only when its memory limit forces it to. It
//! passes updates on as they arrive and does not consolidate.

use std::collections::BTreeMap;
use std::mem;

use serde::{Deserialize, Serialize};

use crate::decode::{Change, Message, Row, Update};
use crate::error::{ProtocolError, TimestampTooLarge, UsageError};
use crate::frontier::Frontier;
use crate::wire;

/// Where a subscription starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Start {
    /// Take a snapshot, at `as_of` if given and otherwise at a timestamp the
    /// server picks.
    Snapshot {
        /// The `AS OF` timestamp.
        #[serde(
            default,
            with = "wire::opt_u64_string",
            skip_serializing_if = "Option::is_none"
        )]
        as_of: Option<u64>,
    },
    /// Continue from a committed frontier, without a snapshot.
    Resume {
        /// The frontier to continue from.
        frontier: Frontier,
    },
}

/// Refuses a pending limit at or above the timestamp limit. The snapshot counts
/// as one timestamp, and one larger than the timestamp limit passes only in
/// the chunks that the pending limit releases.
pub fn check_limits(
    max_pending_bytes: Option<usize>,
    max_timestamp_bytes: Option<usize>,
) -> Result<(), UsageError> {
    match (max_pending_bytes, max_timestamp_bytes) {
        (Some(pending), Some(timestamp)) if pending >= timestamp => Err(UsageError::new(
            "max_pending_bytes must be below max_timestamp_bytes",
        )),
        _ => Ok(()),
    }
}

/// What the batcher hands to the consumer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// Part of the initial snapshot, released early to bound memory. The
    /// snapshot completes with the next batch, which is marked as the
    /// snapshot. A chunk carries no frontier, so nothing can
    /// be committed until that batch arrives, and a restart before it delivers
    /// the whole snapshot again. A target therefore writes chunks where the
    /// next snapshot can replace them.
    SnapshotChunk {
        /// The updates, all at the snapshot timestamp.
        updates: Vec<Update>,
    },
    /// A closed batch.
    Batch(Batch),
}

impl Event {
    /// An estimate of the memory the event's updates hold, in the unit of the
    /// batcher's memory limit. The estimate of two appended batches is the
    /// sum of theirs.
    pub fn approx_bytes(&self) -> usize {
        let updates = match self {
            Event::SnapshotChunk { updates } => updates,
            Event::Batch(batch) => &batch.updates,
        };
        updates
            .iter()
            .map(Update::approx_bytes)
            .fold(0, usize::saturating_add)
    }
}

/// Every update at a time in `[lower, upper)` that was not in an earlier
/// event, in timestamp order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Batch {
    lower: Option<Frontier>,
    upper: Frontier,
    snapshot: bool,
    updates: Vec<Update>,
}

impl Batch {
    /// The frontier this batch continues from, or `None` for the batch that
    /// completes the initial snapshot.
    pub fn lower(&self) -> Option<Frontier> {
        self.lower
    }

    /// The frontier this batch closes. Everything below it has been released.
    pub fn upper(&self) -> Frontier {
        self.upper
    }

    /// Whether this batch completes the initial snapshot. Such a batch holds
    /// the snapshot updates, at the `AS OF`, after any
    /// [`Event::SnapshotChunk`] that preceded it, and may also hold updates at
    /// later timestamps.
    pub fn is_snapshot(&self) -> bool {
        self.snapshot
    }

    /// Whether the batch has no updates. An empty batch still advances the
    /// frontier, which keeps a checkpoint moving while the object is idle.
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    /// The updates, in timestamp order.
    pub fn updates(&self) -> &[Update] {
        &self.updates
    }

    /// Takes the updates, in timestamp order.
    pub fn into_updates(self) -> Vec<Update> {
        self.updates
    }

    /// Extends this batch with `later`, the batch that continues from its
    /// upper, so that one batch carries both. Returns `later` unchanged if it
    /// does not continue from this batch, or if this batch completes the
    /// snapshot, so that changes after the snapshot stay apart from it.
    pub fn append(&mut self, later: Batch) -> Result<(), Batch> {
        if self.snapshot || later.lower != Some(self.upper) {
            return Err(later);
        }
        self.updates.extend(later.updates);
        self.upper = later.upper;
        Ok(())
    }

    /// The latest change to each key, for targets that keep only the current
    /// state of each key. Keys are ordered by their cells as text, with `NULL`
    /// first.
    ///
    /// Each change keeps its timestamp. A key with a [`Change::KeyViolation`]
    /// anywhere in the batch yields its latest violation: the upsert envelope
    /// reads each timestamp on its own, so a later change to a key that is not
    /// unique can be a delete while other rows with that key remain. The diff
    /// envelope has no key, so its updates are returned unchanged.
    pub fn into_latest_by_key(self) -> Vec<Update> {
        let mut latest: BTreeMap<Row, Update> = BTreeMap::new();
        let mut unkeyed = Vec::new();
        for update in self.updates {
            match &update.change {
                Change::Upsert { key, .. }
                | Change::Delete { key }
                | Change::KeyViolation { key } => {
                    let violated = latest
                        .get(key)
                        .is_some_and(|kept| matches!(kept.change, Change::KeyViolation { .. }));
                    if !violated || matches!(update.change, Change::KeyViolation { .. }) {
                        latest.insert(key.clone(), update);
                    }
                }
                Change::Diff { .. } => unkeyed.push(update),
            }
        }
        unkeyed.extend(latest.into_values());
        unkeyed
    }
}

/// Why the batcher failed. After either error, every later call fails with it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BatchError {
    /// The server broke the subscribe protocol.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// One timestamp exceeded [`Batcher::with_max_timestamp_bytes`].
    #[error(transparent)]
    TimestampTooLarge(#[from] TimestampTooLarge),
}

/// Turns the messages of one cursor into [`Event`]s.
#[derive(Clone, Debug)]
pub struct Batcher {
    start: Start,
    up_to: Option<u64>,
    max_pending_bytes: Option<usize>,
    max_timestamp_bytes: Option<usize>,
    /// The first progress row's frontier, which is the `AS OF`.
    as_of: Option<u64>,
    /// The latest progress row's frontier.
    progress: Option<u64>,
    /// The timestamp of the latest update.
    last_update: Option<u64>,
    released: Option<Frontier>,
    /// Updates not yet released, in timestamp order.
    pending: Vec<Update>,
    pending_bytes: usize,
    /// The bytes of the pending updates at `last_update`. A snapshot chunk
    /// releases them and the count starts again.
    open_bytes: usize,
    finished: bool,
    /// Whether the server reported the end of the stream, after which no row
    /// arrives, not even progress past `UP TO`.
    ended: bool,
    /// The first error, returned by every later call. Past a protocol error a
    /// release could skip data that never arrived, and past
    /// [`TimestampTooLarge`] the timestamp cannot be released whole.
    failed: Option<BatchError>,
}

impl Batcher {
    /// Returns a batcher for a subscription that starts at `start` and stops
    /// before `up_to`.
    pub fn new(start: Start, up_to: Option<u64>) -> Batcher {
        let released = match start {
            Start::Snapshot { .. } => None,
            Start::Resume { frontier } => Some(frontier),
        };
        Batcher {
            start,
            up_to,
            max_pending_bytes: None,
            max_timestamp_bytes: None,
            as_of: None,
            progress: None,
            last_update: None,
            released,
            pending: Vec::new(),
            pending_bytes: 0,
            open_bytes: 0,
            finished: false,
            ended: false,
            failed: None,
        }
    }

    /// Bounds the bytes held between progress rows, counted as the text of
    /// each cell plus a fixed charge for each update and each cell.
    ///
    /// Above the limit, the batcher releases every complete timestamp at once,
    /// or a [`Event::SnapshotChunk`] when the only pending timestamp is the
    /// snapshot. Any other single timestamp cannot be split, so the batcher can
    /// exceed the limit by one timestamp.
    pub fn with_max_pending_bytes(mut self, limit: usize) -> Batcher {
        self.max_pending_bytes = Some(limit);
        self
    }

    /// Fails the stream with [`TimestampTooLarge`] once the batcher holds more
    /// than `limit` bytes for one timestamp, counted as for
    /// [`Batcher::with_max_pending_bytes`].
    ///
    /// The check runs as each update is added, so it also catches a timestamp
    /// that the same `FETCH` closes. The snapshot counts as one timestamp. One
    /// larger than the limit passes only in chunks, and never fails while the
    /// chunk limit plus its largest update is at most the limit.
    pub fn with_max_timestamp_bytes(mut self, limit: usize) -> Batcher {
        self.max_timestamp_bytes = Some(limit);
        self
    }

    /// The bytes of the updates not yet released, counted as for
    /// [`Batcher::with_max_pending_bytes`].
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// The upper of the latest batch, or the resume frontier before the first
    /// batch of a resumed subscription.
    pub fn released(&self) -> Option<Frontier> {
        self.released
    }

    /// Whether the stream has ended: a batch reached `UP TO`, or
    /// [`Batcher::finish`] was called.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// The frontier of the latest progress row, or `None` before the first.
    pub fn progress(&self) -> Option<u64> {
        self.progress
    }

    /// The stream's `AS OF`, from its first progress row, which arrives before
    /// any update. A snapshot's updates are at this time.
    pub fn as_of(&self) -> Option<u64> {
        self.as_of
    }

    /// Accepts the next message of the cursor.
    ///
    /// After an error, every call fails with the same error.
    pub fn push(&mut self, message: Message) -> Result<Option<Event>, BatchError> {
        self.check_failed()?;
        let result = self.accept(message);
        self.record(result)
    }

    fn accept(&mut self, message: Message) -> Result<Option<Event>, BatchError> {
        if self.finished {
            // With several workers or replicas, more progress rows past
            // `UP TO` can follow the one that reached it, because the
            // controller passes on each advance of the lowest frontier
            // (`absorb_subscribe_response` in `src/compute-client/src/service.rs`).
            // They close nothing new. An update cannot arrive at or past
            // `UP TO`, and nothing follows the end of the stream.
            let at_up_to = !self.ended
                && self.released.is_some()
                && self.released == self.up_to.and_then(Frontier::new);
            if let Message::Progress { frontier } = message {
                if at_up_to && self.progress.is_some_and(|previous| frontier >= previous) {
                    self.progress = Some(frontier);
                    return Ok(None);
                }
            }
            return Err(ProtocolError::new("a row arrived after the stream ended").into());
        }
        match message {
            Message::Progress { frontier } => Ok(self.on_progress(frontier)?),
            Message::Update(update) => {
                self.on_update(update)?;
                // Before a snapshot chunk releases the bytes, so that the
                // batcher never holds more than the limit.
                self.check_timestamp_bytes()?;
                Ok(self.relieve_memory())
            }
        }
    }

    /// Releases what remains once the server has reported the end of the
    /// stream, which an empty `FETCH` with a nonzero count and no timeout does.
    ///
    /// A stream with `UP TO` can end without a progress row at or past
    /// `UP TO` (SQL-528), so its last updates are released here. An unbounded
    /// stream ends only once its upper is empty
    /// (`ActiveSubscribe::process_response`), so every update has arrived.
    pub fn finish(&mut self) -> Result<Option<Batch>, BatchError> {
        self.check_failed()?;
        self.ended = true;
        let result = self.end().map_err(BatchError::from);
        self.record(result)
    }

    fn check_failed(&self) -> Result<(), BatchError> {
        match &self.failed {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn record<T>(&mut self, result: Result<T, BatchError>) -> Result<T, BatchError> {
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        result
    }

    fn end(&mut self) -> Result<Option<Batch>, ProtocolError> {
        if self.finished {
            return Ok(None);
        }
        let Some(progress) = self.progress else {
            return Err(ProtocolError::new(
                "the stream ended before its first progress row",
            ));
        };
        let upper = match self.up_to {
            Some(up_to) => Frontier::new(up_to),
            // The upper is empty, so every timestamp is complete, including
            // the latest progress row's.
            None => Some(Frontier::after(
                self.last_update.map_or(progress, |last| last.max(progress)),
            )),
        };
        self.finished = true;
        let batch = upper.and_then(|upper| self.release(upper));
        // `on_update` refuses updates at or past `UP TO`, so nothing pending
        // can sit beyond `upper`.
        if batch.is_none() && !self.pending.is_empty() {
            return Err(ProtocolError::new(
                "the stream ended with updates its end cannot release",
            ));
        }
        Ok(batch)
    }

    fn on_progress(&mut self, frontier: u64) -> Result<Option<Event>, ProtocolError> {
        // Progress follows the updates below it, so it is above every update.
        if let Some(last) = self.last_update.filter(|last| frontier <= *last) {
            return Err(ProtocolError::new(format!(
                "progress {frontier} arrived after an update at {last}"
            )));
        }
        match self.progress {
            None => self.check_as_of(frontier)?,
            Some(previous) if frontier < previous => {
                return Err(ProtocolError::new(format!(
                    "progress went back from {previous} to {frontier}"
                )));
            }
            Some(_) => {}
        }
        self.progress = Some(frontier);
        // The server sends progress rows past `UP TO`, but no updates at or
        // after it, so the stream closes at `UP TO`.
        let upper = self.up_to.map_or(frontier, |up_to| frontier.min(up_to));
        Ok(Frontier::new(upper)
            .and_then(|upper| self.release(upper))
            .map(Event::Batch))
    }

    /// Checks the first progress row, which is the `AS OF`.
    fn check_as_of(&mut self, as_of: u64) -> Result<(), ProtocolError> {
        match self.start {
            Start::Snapshot {
                as_of: Some(expected),
            } if as_of != expected => {
                return Err(ProtocolError::new(format!(
                    "the stream started at {as_of}, not at the requested AS OF {expected}"
                )));
            }
            // A resume runs `AS OF F - 1`. A later start skips updates at `F`,
            // and an earlier one repeats updates already released.
            Start::Resume { frontier } if as_of != frontier.resume_as_of() => {
                return Err(ProtocolError::new(format!(
                    "the stream started at {as_of}, not at the resume AS OF {}",
                    frontier.resume_as_of()
                )));
            }
            _ => {}
        }
        self.as_of = Some(as_of);
        Ok(())
    }

    fn on_update(&mut self, update: Update) -> Result<(), ProtocolError> {
        let time = update.timestamp;
        let Some(progress) = self.progress else {
            return Err(ProtocolError::new(format!(
                "an update at {time} arrived before the first progress row"
            )));
        };
        if time < progress {
            return Err(ProtocolError::new(format!(
                "an update at {time} arrived after progress reached {progress}"
            )));
        }
        if let Some(last) = self.last_update.filter(|last| time < *last) {
            return Err(ProtocolError::new(format!(
                "an update at {time} arrived after an update at {last}"
            )));
        }
        if let Some(released) = self.released.filter(|released| released.contains(time)) {
            return Err(ProtocolError::new(format!(
                "an update at {time} arrived after frontier {released} was released"
            )));
        }
        if let Some(up_to) = self.up_to.filter(|up_to| time >= *up_to) {
            return Err(ProtocolError::new(format!(
                "an update at {time} arrived at or after UP TO {up_to}"
            )));
        }
        let bytes = update.approx_bytes();
        self.open_bytes = if self.last_update == Some(time) {
            self.open_bytes.saturating_add(bytes)
        } else {
            bytes
        };
        self.last_update = Some(time);
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.pending.push(update);
        Ok(())
    }

    fn check_timestamp_bytes(&self) -> Result<(), TimestampTooLarge> {
        match (self.max_timestamp_bytes, self.last_update) {
            (Some(limit), Some(timestamp)) if self.open_bytes > limit => {
                Err(TimestampTooLarge { timestamp, limit })
            }
            _ => Ok(()),
        }
    }

    fn relieve_memory(&mut self) -> Option<Event> {
        let limit = self.max_pending_bytes?;
        if self.pending_bytes <= limit {
            return None;
        }
        let oldest = self.pending.first()?.timestamp;
        let latest = self.last_update?;
        if oldest < latest {
            return Frontier::new(latest)
                .and_then(|upper| self.release(upper))
                .map(Event::Batch);
        }
        if self.released.is_none() && Some(oldest) == self.as_of {
            self.pending_bytes = 0;
            self.open_bytes = 0;
            let updates = mem::take(&mut self.pending);
            return Some(Event::SnapshotChunk { updates });
        }
        None
    }

    /// Releases the pending updates below `upper`, if `upper` is past what was
    /// already released.
    fn release(&mut self, upper: Frontier) -> Option<Batch> {
        let advances = match self.released {
            Some(released) => upper > released,
            // A snapshot is the state at the `AS OF`, so nothing can be
            // released until the `AS OF` itself closes.
            None => upper.contains(self.as_of?),
        };
        if !advances {
            return None;
        }
        let split = self
            .pending
            .partition_point(|u| upper.contains(u.timestamp));
        let rest = self.pending.split_off(split);
        let updates = mem::replace(&mut self.pending, rest);
        let released_bytes = updates
            .iter()
            .map(Update::approx_bytes)
            .fold(0, usize::saturating_add);
        self.pending_bytes = self.pending_bytes.saturating_sub(released_bytes);
        if self.pending.is_empty() {
            self.open_bytes = 0;
        }
        let batch = Batch {
            lower: self.released,
            upper,
            snapshot: self.released.is_none(),
            updates,
        };
        self.released = Some(upper);
        if self.up_to.and_then(Frontier::new) == Some(upper) {
            self.finished = true;
        }
        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontier(t: u64) -> Frontier {
        Frontier::new(t).unwrap()
    }

    fn snapshot() -> Batcher {
        Batcher::new(Start::Snapshot { as_of: None }, None)
    }

    fn resume(t: u64) -> Batcher {
        Batcher::new(
            Start::Resume {
                frontier: frontier(t),
            },
            None,
        )
    }

    fn insert(timestamp: u64, value: &str) -> Update {
        Update {
            timestamp,
            change: Change::Diff {
                row: vec![Some(value.to_string())],
                diff: 1,
            },
        }
    }

    fn upsert(timestamp: u64, key: &str, value: &str) -> Update {
        Update {
            timestamp,
            change: Change::Upsert {
                key: vec![Some(key.to_string())],
                value: vec![Some(value.to_string())],
            },
        }
    }

    fn delete(timestamp: u64, key: &str) -> Update {
        Update {
            timestamp,
            change: Change::Delete {
                key: vec![Some(key.to_string())],
            },
        }
    }

    fn progress(b: &mut Batcher, t: u64) -> Option<Event> {
        b.push(Message::Progress { frontier: t }).unwrap()
    }

    fn update(b: &mut Batcher, u: Update) -> Option<Event> {
        b.push(Message::Update(u)).unwrap()
    }

    fn batch(event: Option<Event>) -> Batch {
        match event {
            Some(Event::Batch(batch)) => batch,
            other => panic!("expected a batch, got {other:?}"),
        }
    }

    #[test]
    fn a_batch_appends_only_the_batch_that_continues_it() {
        let mut b = snapshot();
        progress(&mut b, 10);
        update(&mut b, insert(10, "a"));
        let mut snapshot = batch(progress(&mut b, 11));
        update(&mut b, insert(11, "b"));
        let mut first = batch(progress(&mut b, 12));
        update(&mut b, insert(12, "c"));
        let second = batch(progress(&mut b, 13));
        let third = batch(progress(&mut b, 14));
        let bytes = |batch: &Batch| Event::Batch(batch.clone()).approx_bytes();
        let expected = bytes(&first) + bytes(&second);

        assert_eq!(
            snapshot.append(first.clone()),
            Err(first.clone()),
            "the batch that completes the snapshot"
        );
        assert_eq!(first.append(third.clone()), Err(third.clone()), "a gap");
        first.append(second).unwrap();
        assert_eq!(first.lower(), Some(frontier(11)));
        assert_eq!(first.upper(), frontier(13));
        assert!(!first.is_snapshot());
        assert_eq!(first.updates(), [insert(11, "b"), insert(12, "c")]);
        assert_eq!(bytes(&first), expected);
        first.append(third).unwrap();
        assert_eq!(first.upper(), frontier(14));
    }

    #[test]
    fn a_snapshot_is_released_when_its_timestamp_closes() {
        let mut b = snapshot();
        assert_eq!(progress(&mut b, 10), None, "the AS OF releases nothing");
        assert_eq!(update(&mut b, insert(10, "a")), None);
        let first = batch(progress(&mut b, 11));
        assert_eq!(first.lower(), None);
        assert_eq!(first.upper(), frontier(11));
        assert!(first.is_snapshot());
        assert_eq!(first.updates(), [insert(10, "a")]);

        assert_eq!(update(&mut b, insert(12, "b")), None);
        let second = batch(progress(&mut b, 13));
        assert_eq!(second.lower(), Some(frontier(11)));
        assert!(!second.is_snapshot());
        assert_eq!(second.updates(), [insert(12, "b")]);
    }

    #[test]
    fn a_resume_continues_from_its_frontier() {
        let mut b = resume(10);
        assert_eq!(progress(&mut b, 9), None, "AS OF F - 1 releases nothing");
        update(&mut b, insert(10, "a"));
        let first = batch(progress(&mut b, 11));
        assert_eq!(first.lower(), Some(frontier(10)));
        assert!(!first.is_snapshot());
        assert_eq!(first.updates(), [insert(10, "a")]);
    }

    #[test]
    fn an_advance_without_updates_is_an_empty_batch() {
        let mut b = resume(10);
        progress(&mut b, 9);
        let empty = batch(progress(&mut b, 20));
        assert!(empty.is_empty());
        assert_eq!(empty.upper(), frontier(20));
        assert_eq!(
            progress(&mut b, 20),
            None,
            "a repeated frontier releases nothing"
        );
    }

    #[test]
    fn a_resume_must_start_at_one_below_its_frontier() {
        for as_of in [8, 10, 11] {
            let mut b = resume(10);
            let err = b.push(Message::Progress { frontier: as_of }).unwrap_err();
            assert!(
                err.to_string().contains("not at the resume AS OF 9"),
                "{err}"
            );
        }
    }

    #[test]
    fn progress_must_be_above_every_update() {
        for frontier in [11, 12] {
            let mut b = resume(10);
            progress(&mut b, 9);
            update(&mut b, insert(12, "a"));
            assert!(
                b.push(Message::Progress { frontier }).is_err(),
                "progress {frontier} after an update at 12"
            );
        }
    }

    #[test]
    fn a_snapshot_at_another_as_of_is_refused() {
        let mut b = Batcher::new(Start::Snapshot { as_of: Some(5) }, None);
        assert!(b.push(Message::Progress { frontier: 6 }).is_err());
    }

    #[test]
    fn the_as_of_is_known_from_the_first_progress_row() {
        let mut b = snapshot();
        assert_eq!(b.as_of(), None);
        progress(&mut b, 10);
        assert_eq!(b.as_of(), Some(10));
        progress(&mut b, 12);
        assert_eq!(b.as_of(), Some(10));
    }

    #[test]
    fn updates_that_break_the_protocol_are_refused() {
        let mut b = snapshot();
        assert!(
            b.push(Message::Update(insert(1, "a"))).is_err(),
            "an update before the first progress row"
        );

        let mut b = snapshot();
        progress(&mut b, 10);
        progress(&mut b, 12);
        assert!(
            b.push(Message::Update(insert(11, "a"))).is_err(),
            "an update below progress"
        );

        let mut b = snapshot();
        progress(&mut b, 10);
        update(&mut b, insert(11, "a"));
        assert!(
            b.push(Message::Update(insert(10, "b"))).is_err(),
            "an update out of order"
        );

        let mut b = resume(10);
        progress(&mut b, 9);
        assert!(
            b.push(Message::Update(insert(9, "a"))).is_err(),
            "an update below the resume frontier"
        );

        let mut b = Batcher::new(Start::Snapshot { as_of: None }, Some(20));
        progress(&mut b, 10);
        assert!(
            b.push(Message::Update(insert(20, "a"))).is_err(),
            "an update at UP TO"
        );

        let mut b = snapshot();
        progress(&mut b, 10);
        assert!(
            b.push(Message::Progress { frontier: 9 }).is_err(),
            "progress going back"
        );
    }

    #[test]
    fn up_to_bounds_the_released_frontier() {
        let mut b = Batcher::new(Start::Snapshot { as_of: None }, Some(15));
        progress(&mut b, 10);
        update(&mut b, insert(10, "a"));
        update(&mut b, insert(14, "b"));
        let last = batch(progress(&mut b, 100));
        assert_eq!(last.upper(), frontier(15));
        assert_eq!(last.updates(), [insert(10, "a"), insert(14, "b")]);
        assert!(b.is_finished());
        assert_eq!(b.finish().unwrap(), None);
        assert!(
            b.push(Message::Progress { frontier: 200 }).is_err(),
            "nothing follows the end"
        );
        assert!(b.finish().is_err(), "an error fails every later call");
    }

    #[test]
    fn finish_releases_a_bounded_stream_up_to_its_end() {
        let mut b = Batcher::new(
            Start::Resume {
                frontier: frontier(10),
            },
            Some(20),
        );
        progress(&mut b, 9);
        update(&mut b, insert(15, "a"));
        progress(&mut b, 16);
        update(&mut b, insert(19, "b"));
        let last = b.finish().unwrap().unwrap();
        assert_eq!(last.lower(), Some(frontier(16)));
        assert_eq!(last.upper(), frontier(20));
        assert_eq!(last.updates(), [insert(19, "b")]);
        assert!(b.is_finished());
    }

    #[test]
    fn finish_releases_every_update_of_an_unbounded_stream() {
        let mut b = snapshot();
        progress(&mut b, 10);
        update(&mut b, insert(10, "a"));
        update(&mut b, insert(13, "b"));
        let last = b.finish().unwrap().unwrap();
        assert_eq!(last.upper(), frontier(14));
        assert!(last.is_snapshot());
        assert_eq!(last.updates(), [insert(10, "a"), insert(13, "b")]);
    }

    #[test]
    fn a_constant_query_completes_at_the_largest_timestamp() {
        let mut b = snapshot();
        progress(&mut b, u64::MAX);
        update(&mut b, insert(u64::MAX, "a"));
        let last = b.finish().unwrap().unwrap();
        assert_eq!(last.upper(), Frontier::after(u64::MAX));
        assert!(last.is_snapshot());
        assert_eq!(last.updates(), [insert(u64::MAX, "a")]);
    }

    #[test]
    fn finish_before_any_progress_is_refused() {
        assert!(snapshot().finish().is_err());
    }

    #[test]
    fn finish_with_up_to_at_the_as_of_releases_nothing() {
        let mut b = Batcher::new(Start::Snapshot { as_of: None }, Some(10));
        progress(&mut b, 10);
        assert_eq!(b.finish().unwrap(), None);
        assert!(b.is_finished());
    }

    #[test]
    fn the_memory_limit_releases_complete_timestamps_early() {
        let one = insert(10, "a").approx_bytes();
        let mut b = resume(10).with_max_pending_bytes(2 * one);
        progress(&mut b, 9);
        assert_eq!(update(&mut b, insert(10, "a")), None);
        assert_eq!(update(&mut b, insert(11, "b")), None);
        let early = batch(update(&mut b, insert(12, "c")));
        assert_eq!(early.upper(), frontier(12));
        assert_eq!(early.updates(), [insert(10, "a"), insert(11, "b")]);
        assert_eq!(b.pending_bytes(), one);
        let rest = batch(progress(&mut b, 13));
        assert_eq!(rest.lower(), Some(frontier(12)));
        assert_eq!(rest.updates(), [insert(12, "c")]);
    }

    #[test]
    fn the_memory_limit_chunks_the_snapshot() {
        let one = insert(10, "a").approx_bytes();
        let mut b = snapshot().with_max_pending_bytes(one);
        progress(&mut b, 10);
        assert_eq!(update(&mut b, insert(10, "a")), None);
        assert_eq!(
            update(&mut b, insert(10, "b")),
            Some(Event::SnapshotChunk {
                updates: vec![insert(10, "a"), insert(10, "b")]
            })
        );
        update(&mut b, insert(10, "c"));
        let last = batch(progress(&mut b, 11));
        assert!(last.is_snapshot());
        assert_eq!(last.lower(), None);
        assert_eq!(last.updates(), [insert(10, "c")]);
    }

    #[test]
    fn the_memory_limit_counts_updates_without_text() {
        let mut b = resume(10).with_max_pending_bytes(1);
        progress(&mut b, 9);
        let empty = |t| Update {
            timestamp: t,
            change: Change::Diff {
                row: vec![None],
                diff: 1,
            },
        };
        assert_eq!(update(&mut b, empty(10)), None);
        assert_eq!(batch(update(&mut b, empty(11))).updates(), [empty(10)]);
    }

    #[test]
    fn the_memory_limit_never_splits_a_timestamp_after_the_snapshot() {
        let mut b = resume(10).with_max_pending_bytes(1);
        progress(&mut b, 9);
        for value in ["a", "b", "c"] {
            assert_eq!(update(&mut b, insert(10, value)), None);
        }
        assert_eq!(b.pending_bytes(), 3 * insert(10, "a").approx_bytes());
        assert_eq!(batch(progress(&mut b, 11)).updates().len(), 3);
    }

    #[test]
    fn one_timestamp_over_the_hard_limit_fails_as_it_is_added() {
        let one = insert(10, "a").approx_bytes();
        let mut b = resume(10).with_max_timestamp_bytes(2 * one);
        progress(&mut b, 9);
        update(&mut b, insert(10, "a"));
        update(&mut b, insert(10, "b"));
        let too_large = BatchError::TimestampTooLarge(TimestampTooLarge {
            timestamp: 10,
            limit: 2 * one,
        });
        // The progress row that would close the timestamp never gets the chance.
        assert_eq!(
            b.push(Message::Update(insert(10, "c"))),
            Err(too_large.clone())
        );
        assert_eq!(
            b.push(Message::Progress { frontier: 11 }),
            Err(too_large.clone())
        );
        assert_eq!(b.finish(), Err(too_large));
    }

    #[test]
    fn the_hard_limit_is_checked_before_a_snapshot_chunk_is_released() {
        let one = insert(10, "a").approx_bytes();
        let mut b = snapshot()
            .with_max_pending_bytes(1)
            .with_max_timestamp_bytes(one - 1);
        progress(&mut b, 10);
        assert_eq!(
            b.push(Message::Update(insert(10, "a"))),
            Err(BatchError::TimestampTooLarge(TimestampTooLarge {
                timestamp: 10,
                limit: one - 1,
            }))
        );
    }

    proptest::proptest! {
        #[test]
        fn the_hard_limit_fails_at_the_first_update_over_it(
            steps in proptest::collection::vec((0u64..=1, "[a-c]{0,8}"), 1..40),
            limit in 0usize..400,
        ) {
            let mut b = resume(10).with_max_timestamp_bytes(limit);
            progress(&mut b, 9);
            let (mut time, mut held, mut over) = (10, 0, None);
            for (advance, value) in &steps {
                if *advance == 1 {
                    time += 1;
                    held = 0;
                }
                let update = insert(time, value);
                held += update.approx_bytes();
                if over.is_none() && held > limit {
                    over = Some(time);
                }
                let result = b.push(Message::Update(update));
                match over {
                    Some(timestamp) => proptest::prop_assert_eq!(
                        result,
                        Err(BatchError::TimestampTooLarge(TimestampTooLarge { timestamp, limit }))
                    ),
                    None => proptest::prop_assert_eq!(result, Ok(None)),
                }
            }
        }
    }

    #[test]
    fn the_hard_limit_restarts_with_each_timestamp() {
        let one = insert(10, "a").approx_bytes();
        let mut b = resume(10).with_max_timestamp_bytes(one);
        progress(&mut b, 9);
        for t in 10..13 {
            assert_eq!(update(&mut b, insert(t, "a")), None);
        }
        assert_eq!(batch(progress(&mut b, 13)).updates().len(), 3);
        assert_eq!(update(&mut b, insert(13, "a")), None);
    }

    #[test]
    fn latest_by_key_keeps_the_last_change_with_its_timestamp() {
        let mut b = resume(10);
        progress(&mut b, 9);
        for u in [
            upsert(10, "k2", "x"),
            upsert(10, "k1", "a"),
            upsert(11, "k1", "b"),
            delete(12, "k2"),
        ] {
            update(&mut b, u);
        }
        let latest = batch(progress(&mut b, 13)).into_latest_by_key();
        assert_eq!(latest, [upsert(11, "k1", "b"), delete(12, "k2")]);
    }

    #[test]
    fn latest_by_key_keeps_a_key_violation() {
        let violation = Update {
            timestamp: 10,
            change: Change::KeyViolation {
                key: vec![Some("k".into())],
            },
        };
        let mut b = resume(10);
        progress(&mut b, 9);
        update(&mut b, violation.clone());
        update(&mut b, delete(11, "k"));
        let latest = batch(progress(&mut b, 12)).into_latest_by_key();
        assert_eq!(latest, [violation]);
    }

    #[test]
    fn batch_json_is_stable() {
        let mut b = resume(10);
        progress(&mut b, 9);
        update(&mut b, insert(10, "a"));
        let json = serde_json::to_value(progress(&mut b, 11)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "event": "batch",
                "lower": "10",
                "upper": "11",
                "snapshot": false,
                "updates": [{"timestamp": "10", "op": "diff", "row": ["a"], "diff": "1"}],
            })
        );
    }
}
