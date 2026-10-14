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

//! Properties of the protocol core over generated inputs, which reach orders
//! of messages and values the hand-written vectors do not.

use std::collections::BTreeMap;

use materialize_sdk_core::checkpoint::{Checkpoint, CommitCheck, CommitConflict, Settlement};
use materialize_sdk_core::identity::{Column, Fingerprint};
use materialize_sdk_core::statement::ObjectRef;
use materialize_sdk_core::{
    Batcher, Change, Decoder, Envelope, Event, Frontier, Message, Row, Start, Update,
};
use proptest::prelude::*;
use proptest::strategy::Union;

/// A nonzero diff in `-limit..=limit`.
fn nonzero(limit: i64) -> impl Strategy<Value = i64> {
    Union::new([(-limit..=-1).boxed(), (1..=limit).boxed()])
}

/// One step of a generated stream: an update at the current time, a move of
/// the current time, or a progress row.
#[derive(Clone, Debug)]
enum Step {
    Update { value: String, diff: i64 },
    Advance(u64),
    Progress(u64),
}

fn step() -> impl Strategy<Value = Step> {
    Union::new_weighted(vec![
        (
            4,
            ("[a-c]{0,3}", nonzero(3))
                .prop_map(|(value, diff)| Step::Update { value, diff })
                .boxed(),
        ),
        (1, (1u64..=3).prop_map(Step::Advance).boxed()),
        (2, (0u64..=2).prop_map(Step::Progress).boxed()),
    ])
}

/// A stream that meets the batcher's preconditions: updates in timestamp
/// order, progress rows above every earlier update and never going back,
/// nothing at or past `UP TO`, and nothing after a progress row reaches it.
/// The server would also consolidate equal rows at one timestamp, which the
/// batcher does not rely on.
#[derive(Clone, Debug)]
struct Stream {
    start: Start,
    up_to: Option<u64>,
    as_of: u64,
    messages: Vec<Message>,
    updates: Vec<Update>,
}

fn stream() -> impl Strategy<Value = Stream> {
    (
        0u8..3,
        1u64..1000,
        prop::option::of(0u64..40),
        prop::collection::vec(step(), 0..60),
    )
        .prop_map(|(start, base, up_to, steps)| {
            let as_of = base;
            let (start, first) = match start {
                // A resume reads `AS OF F - 1` and has no updates before `F`.
                0 => (
                    Start::Resume {
                        frontier: Frontier::new(base + 1).unwrap(),
                    },
                    base + 1,
                ),
                1 => (Start::Snapshot { as_of: Some(base) }, base),
                _ => (Start::Snapshot { as_of: None }, base),
            };
            let up_to = up_to.map(|offset| as_of + 1 + offset);
            let mut messages = vec![Message::Progress { frontier: as_of }];
            let mut updates = Vec::new();
            let (mut now, mut progress, mut last_update) = (first, as_of, None);
            for step in steps {
                if up_to.is_some_and(|up_to| progress >= up_to) {
                    break;
                }
                match step {
                    Step::Update { value, diff } => {
                        if up_to.is_some_and(|up_to| now >= up_to) {
                            continue;
                        }
                        let update = Update {
                            timestamp: now,
                            change: Change::Diff {
                                row: vec![Some(value)],
                                diff,
                            },
                        };
                        last_update = Some(now);
                        updates.push(update.clone());
                        messages.push(Message::Update(update));
                    }
                    Step::Advance(by) => now += by,
                    Step::Progress(by) => {
                        let floor = last_update.map_or(progress, |t| progress.max(t + 1));
                        progress = floor + by;
                        now = now.max(progress);
                        messages.push(Message::Progress { frontier: progress });
                    }
                }
            }
            Stream {
                start,
                up_to,
                as_of,
                messages,
                updates,
            }
        })
}

/// The frontier a stream has proven after `message`: progress, or the
/// timestamp of a later update, which arrive in order, capped at `UP TO`.
fn proven(proven: u64, message: &Message, up_to: Option<u64>) -> u64 {
    let proven = match message {
        Message::Progress { frontier } => proven.max(*frontier),
        Message::Update(update) => proven.max(update.timestamp),
    };
    up_to.map_or(proven, |up_to| proven.min(up_to))
}

proptest! {
    #[test]
    fn a_stream_is_released_once_in_order_and_no_further_than_proven(
        stream in stream(),
        limit in prop::option::of(0usize..400),
    ) {
        let mut batcher = Batcher::new(stream.start, stream.up_to);
        if let Some(limit) = limit {
            batcher = batcher.with_max_pending_bytes(limit);
        }
        let mut released: Vec<Update> = Vec::new();
        let mut frontier = match stream.start {
            Start::Snapshot { .. } => None,
            Start::Resume { frontier } => Some(frontier),
        };
        // A resume starts with its frontier already proven.
        let mut bound = frontier.map_or(0, |f| f.resume_as_of() + 1);
        let mut pushed = 0;
        let mut events = Vec::new();
        for message in &stream.messages {
            bound = proven(bound, message, stream.up_to);
            if matches!(message, Message::Update(_)) {
                pushed += 1;
            }
            events.extend(batcher.push(message.clone()).unwrap());
            for event in events.drain(..) {
                apply(&stream, event, &mut frontier, &mut released)?;
            }
            if let Some(frontier) = frontier {
                prop_assert!(frontier <= Frontier::new(bound.max(1)).unwrap(), "released past what the stream proved");
            }
            // A progress row releases everything below it.
            if let Message::Progress { frontier: p } = message {
                let below = stream.updates.iter().filter(|u| u.timestamp < *p).count();
                prop_assert!(released.len() >= below.min(pushed));
            }
            // Above the memory limit, only the latest timestamp may wait.
            if let Some(limit) = limit.filter(|limit| batcher.pending_bytes() > *limit) {
                let pending = &stream.updates[released.len()..pushed];
                let latest = pending.last().map(|u| u.timestamp);
                prop_assert!(pending.iter().all(|u| Some(u.timestamp) == latest), "limit {}", limit);
                let snapshot_open = frontier.is_none();
                prop_assert!(!(snapshot_open && latest == Some(stream.as_of)), "the snapshot was not chunked");
            }
        }
        if let Some(event) = batcher.finish().unwrap() {
            apply(&stream, Event::Batch(event), &mut frontier, &mut released)?;
        }
        prop_assert!(batcher.is_finished());
        if let Some(up_to) = stream.up_to {
            prop_assert_eq!(frontier, Frontier::new(up_to), "a bounded stream ends at UP TO");
        }
        prop_assert_eq!(released, stream.updates);
    }

    #[test]
    fn the_first_error_sticks_and_nothing_panics(
        messages in prop::collection::vec(
            Union::new([
                (0u64..20).prop_map(|t| Message::Progress { frontier: t }).boxed(),
                (0u64..20, nonzero(2)).prop_map(|(timestamp, diff)| Message::Update(Update {
                    timestamp,
                    change: Change::Diff { row: vec![None], diff },
                })).boxed(),
            ]),
            0..30,
        ),
        resume in any::<bool>(),
        up_to in prop::option::of(1u64..25),
    ) {
        let start = if resume {
            Start::Resume { frontier: Frontier::new(5).unwrap() }
        } else {
            Start::Snapshot { as_of: None }
        };
        let mut batcher = Batcher::new(start, up_to).with_max_pending_bytes(100);
        let mut first = None;
        for message in messages {
            let result = batcher.push(message);
            match (&first, result) {
                (None, Err(error)) => first = Some(error),
                (Some(error), result) => prop_assert_eq!(result, Err(error.clone())),
                (None, Ok(_)) => {}
            }
        }
        if let Some(error) = first {
            prop_assert_eq!(batcher.finish(), Err(error));
        }
    }

    #[test]
    fn the_decoder_never_panics(
        rows in prop::collection::vec(prop::collection::vec(prop::option::of(cell()), 0..7), 0..20),
    ) {
        for row in rows {
            let _ = diff_decoder().decode(row.clone());
            let _ = upsert_decoder().decode(row);
        }
    }

    #[test]
    fn the_decoder_reads_back_valid_rows(
        timestamp in timestamp(),
        diff in Union::new([
            Just(i64::MIN).boxed(),
            Just(i64::MAX).boxed(),
            nonzero(i64::MAX).boxed(),
        ]),
        value in prop::option::of(".*"),
        key in prop::option::of(".*"),
        state in 0u8..3,
    ) {
        let progress = vec![Some(timestamp.to_string()), Some("t".into()), None, None];
        prop_assert_eq!(
            diff_decoder().decode(progress).unwrap(),
            Message::Progress { frontier: timestamp }
        );
        let row: Row = vec![
            Some(timestamp.to_string()),
            Some("f".into()),
            Some(diff.to_string()),
            value.clone(),
        ];
        let decoded = diff_decoder().decode(row).unwrap();
        prop_assert_eq!(
            &decoded,
            &Message::Update(Update { timestamp, change: Change::Diff { row: vec![value.clone()], diff } })
        );
        prop_assert_eq!(&round_trip(&decoded), &decoded);

        let (state, cell, change) = match state {
            0 => ("upsert", value.clone(), Change::Upsert { key: vec![key.clone()], value: vec![value] }),
            1 => ("delete", None, Change::Delete { key: vec![key.clone()] }),
            _ => ("key_violation", None, Change::KeyViolation { key: vec![key.clone()] }),
        };
        let row = vec![Some(timestamp.to_string()), Some("f".into()), Some(state.into()), key, cell];
        let decoded = upsert_decoder().decode(row).unwrap();
        prop_assert_eq!(&decoded, &Message::Update(Update { timestamp, change }));
        prop_assert_eq!(&round_trip(&decoded), &decoded);
    }

    #[test]
    fn the_latest_change_per_key_wins_unless_the_key_was_violated(
        changes in prop::collection::vec((0u8..3, 0u8..3, "[a-c]{0,2}"), 0..30),
    ) {
        let mut batcher = Batcher::new(Start::Resume { frontier: Frontier::new(10).unwrap() }, None);
        batcher.push(Message::Progress { frontier: 9 }).unwrap();
        let mut updates = Vec::new();
        let mut time = 9;
        for (key, kind, value) in changes {
            time += 1;
            let key = vec![Some(key.to_string())];
            let change = match kind {
                0 => Change::Upsert { key, value: vec![Some(value)] },
                1 => Change::Delete { key },
                _ => Change::KeyViolation { key },
            };
            let update = Update { timestamp: time, change };
            batcher.push(Message::Update(update.clone())).unwrap();
            updates.push(update);
        }
        let Some(batch) = batcher.finish().unwrap() else {
            prop_assert!(updates.is_empty());
            return Ok(());
        };
        let mut expected: BTreeMap<Row, Update> = BTreeMap::new();
        for update in &updates {
            let key = key_of(&update.change).clone();
            let violated = updates
                .iter()
                .any(|u| key_of(&u.change) == &key && matches!(u.change, Change::KeyViolation { .. }));
            if !violated || matches!(update.change, Change::KeyViolation { .. }) {
                expected.insert(key, update.clone());
            }
        }
        prop_assert_eq!(batch.into_latest_by_key(), expected.into_values().collect::<Vec<_>>());
    }

    #[test]
    fn a_frontier_resumes_one_below_itself(t in 1u64..=u64::MAX) {
        let frontier = Frontier::new(t).unwrap();
        prop_assert_eq!(frontier.resume_as_of(), t - 1);
        let json = serde_json::to_string(&frontier).unwrap();
        prop_assert_eq!(serde_json::from_str::<Frontier>(&json).unwrap(), frontier);
    }

    #[test]
    fn a_checkpoint_round_trips_through_its_text(checkpoint in checkpoint(u64::MAX)) {
        let text = checkpoint.encode().unwrap();
        let decoded = Checkpoint::decode(&text).unwrap();
        prop_assert_eq!(&decoded, &checkpoint);
        prop_assert_eq!(decoded.encode().unwrap(), text, "the text is deterministic");
    }

    #[test]
    fn settle_follows_its_rules_for_every_kind_of_write(
        expected in checkpoint(u64::MAX - 2),
        upper in 2u64..u64::MAX,
        as_of in any::<u64>(),
    ) {
        let upper = Frontier::new(upper).unwrap();
        let mut writes = vec![
            expected.restart(BTreeMap::new(), as_of).unwrap(),
            expected.with_subscriptions(BTreeMap::new()),
        ];
        if expected.frontier < Some(upper) {
            writes.push(expected.committed(upper));
        }
        let fenced = Settlement::Conflict {
            conflict: CommitConflict::Fenced { stored: expected.epoch + 1, epoch: expected.epoch },
        };
        for new in &writes {
            prop_assert_eq!(expected.settle(&expected, new), Settlement::SendAgain);
            if *new != expected {
                prop_assert_eq!(new.settle(&expected, new), Settlement::Applied);
            }
            prop_assert_eq!(expected.take_over().unwrap().settle(&expected, new), fenced.clone());
            let restarted = expected.restart(BTreeMap::new(), as_of).unwrap();
            if *new != restarted {
                prop_assert_eq!(
                    restarted.settle(&expected, new),
                    Settlement::Conflict {
                        conflict: CommitConflict::Superseded {
                            stored: expected.generation + 1,
                            generation: expected.generation,
                        },
                    }
                );
            }
        }
        // A commit that another write of the same worker cannot explain.
        let lower = Frontier::new(1).unwrap();
        let other = Checkpoint { frontier: Some(lower), snapshot_as_of: None, ..expected.clone() };
        if other != expected && expected.frontier < Some(upper) {
            let new = expected.committed(upper);
            let diverged = matches!(
                other.settle(&expected, &new),
                Settlement::Conflict {
                    conflict: CommitConflict::Diverged { .. }
                }
            );
            prop_assert!(diverged);
        }
        let take_over = expected.take_over().unwrap();
        let again = take_over.take_over().unwrap();
        for stored in [&expected, &take_over, &again] {
            prop_assert_eq!(stored.settle(&expected, &take_over), Settlement::TakeOverAgain);
        }
    }

    #[test]
    fn check_commit_agrees_with_settle((expected, upper) in commit_case()) {
        let checked = expected.check_commit(&expected, upper);
        // As numbers, a commit's frontier must be above the stored frontier
        // and above the snapshot's `AS OF`.
        if let Some(lower) = expected.frontier
            && lower.get() >= upper.get()
        {
            prop_assert_eq!(checked, Err(CommitConflict::InvalidRange { lower, upper }));
            return Ok(());
        }
        if let Some(as_of) = expected.snapshot_as_of
            && u128::from(as_of) >= upper.get()
        {
            prop_assert_eq!(checked, Err(CommitConflict::SnapshotNotClosed { as_of, upper }));
            return Ok(());
        }
        prop_assert_eq!(checked, Ok(CommitCheck::Apply));
        let new = expected.committed(upper);
        prop_assert_eq!(expected.settle(&expected, &new), Settlement::SendAgain);
        prop_assert_eq!(new.check_commit(&expected, upper), Ok(CommitCheck::AlreadyCommitted));
        prop_assert_eq!(new.settle(&expected, &new), Settlement::Applied);
    }
}

/// Applies one released event, checking it continues the release so far.
fn apply(
    stream: &Stream,
    event: Event,
    frontier: &mut Option<Frontier>,
    released: &mut Vec<Update>,
) -> Result<(), TestCaseError> {
    match event {
        Event::SnapshotChunk { updates } => {
            prop_assert!(frontier.is_none(), "a chunk after the snapshot completed");
            prop_assert!(updates.iter().all(|u| u.timestamp == stream.as_of));
            released.extend(updates);
        }
        Event::Batch(batch) => {
            prop_assert_eq!(batch.lower(), *frontier, "batches chain without gaps");
            prop_assert_eq!(batch.is_snapshot(), batch.lower().is_none());
            if batch.is_snapshot() {
                prop_assert!(batch.upper().contains(stream.as_of));
            }
            prop_assert!(Some(batch.upper()) > *frontier);
            for update in batch.updates() {
                prop_assert!(batch.upper().contains(update.timestamp));
                if let Some(lower) = batch.lower() {
                    prop_assert!(!lower.contains(update.timestamp));
                }
            }
            *frontier = Some(batch.upper());
            released.extend(batch.into_updates());
        }
    }
    Ok(())
}

fn key_of(change: &Change) -> &Row {
    match change {
        Change::Upsert { key, .. } | Change::Delete { key } | Change::KeyViolation { key } => key,
        Change::Diff { .. } => panic!("an upsert-envelope change"),
    }
}

fn round_trip(message: &Message) -> Message {
    serde_json::from_str(&serde_json::to_string(message).unwrap()).unwrap()
}

fn diff_decoder() -> Decoder {
    Decoder::new(
        &Envelope::Diff,
        &["mz_timestamp", "mz_progressed", "mz_diff", "v"],
    )
    .unwrap()
}

fn upsert_decoder() -> Decoder {
    Decoder::new(
        &Envelope::Upsert {
            key: vec!["k".into()],
        },
        &["mz_timestamp", "mz_progressed", "mz_state", "k", "v"],
    )
    .unwrap()
}

fn timestamp() -> impl Strategy<Value = u64> {
    Union::new([
        Just(0).boxed(),
        Just(u64::MAX).boxed(),
        any::<u64>().boxed(),
    ])
}

/// A cell: the tokens the decoder looks for, numbers at the edges of their
/// range, or any short text.
fn cell() -> impl Strategy<Value = String> {
    const TOKENS: [&str; 13] = [
        "t",
        "f",
        "upsert",
        "delete",
        "key_violation",
        "0",
        "-0",
        "01",
        "+1",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775808",
        "-9223372036854775809",
    ];
    Union::new_weighted(vec![
        (
            2,
            prop::sample::select(&TOKENS[..])
                .prop_map(str::to_string)
                .boxed(),
        ),
        (1, ".{0,12}".boxed()),
    ])
}

fn checkpoint(max_counter: u64) -> impl Strategy<Value = Checkpoint> {
    let column = (".*", any::<u32>(), any::<i32>(), any::<bool>()).prop_map(
        |(name, type_oid, type_mod, nullable)| Column {
            name,
            type_oid,
            type_mod,
            nullable,
        },
    );
    let fingerprint = (
        "u[0-9]{1,4}",
        prop::option::of(".*"),
        ".*",
        ".*",
        ".*",
        prop::collection::vec(column, 0..3),
        prop::option::of(prop::collection::vec(".+", 1..3)),
    )
        .prop_map(
            |(id, database, schema, name, shard, columns, key)| Fingerprint {
                object: ObjectRef {
                    id,
                    database,
                    schema,
                    name,
                },
                shard,
                envelope: match key {
                    Some(key) => Envelope::Upsert { key },
                    None => Envelope::Diff,
                },
                columns,
            },
        );
    (
        1..=max_counter,
        1..=max_counter,
        Union::new([
            edge_u64()
                .prop_map(|t| (Some(Frontier::after(t)), None))
                .boxed(),
            prop::option::of(edge_u64())
                .prop_map(|as_of| (None, as_of))
                .boxed(),
        ]),
        prop::collection::btree_map(".*", fingerprint, 0..3),
    )
        .prop_map(
            |(epoch, generation, (frontier, snapshot_as_of), subscriptions)| Checkpoint {
                epoch,
                generation,
                frontier,
                snapshot_as_of,
                subscriptions,
            },
        )
}

/// Any `u64`, with the edges as likely as the rest.
fn edge_u64() -> impl Strategy<Value = u64> {
    Union::new_weighted(vec![
        (2, any::<u64>().boxed()),
        (1, Just(0).boxed()),
        (1, Just(u64::MAX).boxed()),
    ])
}

/// A checkpoint and a commit upper, often just around the checkpoint's
/// frontier or snapshot `AS OF`, where the range rules change.
fn commit_case() -> impl Strategy<Value = (Checkpoint, Frontier)> {
    checkpoint(u64::MAX - 1).prop_flat_map(|expected| {
        let near = match (expected.frontier, expected.snapshot_as_of) {
            (Some(frontier), _) => frontier.resume_as_of(),
            (None, as_of) => as_of.unwrap_or(0),
        };
        let upper = Union::new([
            edge_u64().boxed(),
            (-1i64..=1)
                .prop_map(move |offset| near.saturating_add_signed(offset))
                .boxed(),
        ])
        .prop_map(Frontier::after);
        (Just(expected), upper)
    })
}
