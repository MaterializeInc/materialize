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
//! The server sends all updates of a time range together, sorted by time and
//! consolidated per timestamp (`ActiveSubscribe::process_response` in
//! `src/adapter/src/active_compute_sink.rs`). The batcher releases updates when
//! a progress row passes their timestamps, and passes them on as they arrive
//! without consolidating.

use std::mem;

use serde::{Deserialize, Serialize};

use crate::decode::{Message, Update};
use crate::error::ProtocolError;
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

/// What the batcher hands to the consumer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// A closed batch.
    Batch(Batch),
}

/// Every update at a time in `[lower, upper)` that was not in an earlier
/// event, in timestamp order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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
    /// the snapshot updates, at the `AS OF`, and may also hold updates at later
    /// timestamps.
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
}

/// Turns the messages of one cursor into [`Event`]s.
#[derive(Clone, Debug)]
pub struct Batcher {
    start: Start,
    up_to: Option<u64>,
    /// The first progress row's frontier, which is the `AS OF`.
    as_of: Option<u64>,
    /// The latest progress row's frontier.
    progress: Option<u64>,
    /// The timestamp of the latest update.
    last_update: Option<u64>,
    released: Option<Frontier>,
    /// Updates not yet released, in timestamp order.
    pending: Vec<Update>,
    finished: bool,
    /// The first protocol violation, returned by every later call. A refused
    /// update never reaches `pending`, so continuing would release a frontier
    /// past it.
    failed: Option<ProtocolError>,
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
            as_of: None,
            progress: None,
            last_update: None,
            released,
            pending: Vec::new(),
            finished: false,
            failed: None,
        }
    }

    /// The upper of the latest batch, or the resume frontier before the first
    /// batch of a resumed subscription.
    pub fn released(&self) -> Option<Frontier> {
        self.released
    }

    /// Whether the stream has ended, because a batch reached `UP TO`.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// The frontier of the latest progress row, or `None` before the first.
    pub fn progress(&self) -> Option<u64> {
        self.progress
    }

    /// Accepts the next message of the cursor.
    ///
    /// After an error, every call fails with the same error.
    pub fn push(&mut self, message: Message) -> Result<Option<Event>, ProtocolError> {
        self.check_failed()?;
        let result = self.accept(message);
        self.record(result)
    }

    fn accept(&mut self, message: Message) -> Result<Option<Event>, ProtocolError> {
        // The server sends nothing after the progress row that reaches
        // `UP TO`, or after the end of the stream.
        if self.finished {
            return Err(ProtocolError::new("a row arrived after the stream ended"));
        }
        match message {
            Message::Progress { frontier } => self.on_progress(frontier),
            Message::Update(update) => self.on_update(update),
        }
    }

    fn check_failed(&self) -> Result<(), ProtocolError> {
        match &self.failed {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn record<T>(&mut self, result: Result<T, ProtocolError>) -> Result<T, ProtocolError> {
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        result
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

    fn on_update(&mut self, update: Update) -> Result<Option<Event>, ProtocolError> {
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
        self.last_update = Some(time);
        self.pending.push(update);
        Ok(None)
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
    use crate::decode::Change;

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
        assert!(
            b.push(Message::Progress { frontier: 200 }).is_err(),
            "nothing follows the end"
        );
        assert!(
            b.push(Message::Progress { frontier: 300 }).is_err(),
            "an error fails every later call"
        );
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
