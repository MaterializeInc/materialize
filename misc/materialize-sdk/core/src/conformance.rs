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

//! The runner of the conformance vectors in `conformance/`, shared by every
//! package so that each runs the same cases through its binding.
//!
//! A malformed vector is a mistake in the test, not in the code under test,
//! so the runner panics on one.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::time::Duration;

use crate::Frontier;
use crate::batch::{Batch, BatchError, Batcher, Event, Start};
use crate::binding;
use crate::catalog::{
    CatalogObject, ObjectName, indexes_sql, lookup_sql, parse_indexes, parse_lookup,
    parse_read_frontier, read_frontier_sql, resume_margin,
};
use crate::checkpoint::{Checkpoint, CheckpointError};
use crate::classify::{ErrorKind, Phase, classify, resolve_index_check, resolve_object_check};
use crate::decode::{Decoder, Envelope, Message, Row, Update};
use crate::identity::{Fingerprint, IdentityChange, compare};
use crate::sink::{CheckpointWrite, WriteError};
use crate::statement::{Subscribe, fetch_sql};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// Runs `function` of the vectors on `input`, and returns its output in the
/// vectors' JSON form.
pub fn run(function: &str, input: Value) -> Value {
    match function {
        "decode" => decode(input),
        "batch" => batch(input),
        "append" => append(input),
        "latest_by_key" => latest_by_key(input),
        "subscribe_sql" => subscribe_sql(input),
        "fetch_sql" => fetch(input),
        "subscription" => subscription(input),
        "classify" => classify_error(input),
        "compare_identity" => compare_identity(input),
        "resolve_object_check" => resolve(input),
        "resolve_index_check" => resolve_index(input),
        "checkpoint" => checkpoint(input),
        "checkpoint_write" => checkpoint_write(input),
        "catalog" => catalog(input),
        other => panic!("unknown function {other:?}"),
    }
}

/// Reads a typed input, refusing unknown keys so a typo cannot change what a
/// case tests.
fn input<T: DeserializeOwned>(input: Value) -> T {
    serde_json::from_value(input).unwrap_or_else(|e| panic!("invalid input: {e}"))
}

fn error(kind: &str) -> Value {
    json!({ "error": kind })
}

fn u64_of(s: &str) -> u64 {
    canonical(s)
}

/// Parses a decimal string in the vectors' JSON form, refusing a sign or
/// leading zeros, so that a vector another runner would refuse fails here too.
fn canonical<T>(s: &str) -> T
where
    T: std::str::FromStr + ToString,
    T::Err: std::fmt::Debug,
{
    let n: T = s.parse().unwrap();
    assert_eq!(n.to_string(), s, "{s:?} is not a canonical decimal string");
    n
}

/// Decodes each row on its own.
fn decode(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        envelope: Envelope,
        columns: Vec<String>,
        rows: Vec<Row>,
    }
    let Input {
        envelope,
        columns,
        rows,
    } = input(value);
    let Ok(decoder) = Decoder::new(&envelope, &columns) else {
        return error("protocol");
    };
    let results: Vec<Value> = rows
        .into_iter()
        .map(|row| match decoder.decode(row) {
            Ok(message) => json!(message),
            Err(_) => error("protocol"),
        })
        .collect();
    json!({ "messages": results })
}

/// Feeds steps to a batcher. A step is a message, or `"finish"`.
fn batch(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        start: Start,
        up_to: Option<String>,
        max_pending_bytes: Option<String>,
        max_timestamp_bytes: Option<String>,
        steps: Vec<Value>,
    }
    let Input {
        start,
        up_to,
        max_pending_bytes,
        max_timestamp_bytes,
        steps,
    } = input(value);
    let mut batcher = Batcher::new(start, up_to.as_deref().map(u64_of));
    if let Some(limit) = max_pending_bytes {
        batcher = batcher.with_max_pending_bytes(canonical(&limit));
    }
    if let Some(limit) = max_timestamp_bytes {
        batcher = batcher.with_max_timestamp_bytes(canonical(&limit));
    }
    let outputs: Vec<Value> = steps
        .into_iter()
        .map(|step| {
            let result = if step == "finish" {
                batcher.finish().map(|batch| batch.map(Event::Batch))
            } else {
                let message: Message = serde_json::from_value(step.clone()).unwrap();
                // Reading the step back refuses unknown keys and other spellings.
                assert_eq!(json!(message), step, "a step in the core's JSON form");
                batcher.push(message)
            };
            match result {
                Ok(event) => json!(event.into_iter().collect::<Vec<_>>()),
                Err(BatchError::Protocol(_)) => error("protocol"),
                Err(BatchError::TimestampTooLarge(_)) => error("timestamp_too_large"),
            }
        })
        .collect();
    json!({ "outputs": outputs, "finished": batcher.is_finished() })
}

/// Appends every batch a stream releases onto the first.
fn append(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        start: Start,
        steps: Vec<Message>,
    }
    let Input { start, steps } = input(value);
    let mut batcher = Batcher::new(start, None);
    let mut appended: Option<Batch> = None;
    for step in steps {
        if let Some(Event::Batch(batch)) = batcher.push(step).unwrap() {
            match &mut appended {
                None => appended = Some(batch),
                Some(first) => {
                    if first.append(batch).is_err() {
                        return error("refused");
                    }
                }
            }
        }
    }
    json!(appended.map(Event::Batch))
}

/// Releases `updates`, at timestamps of one or more and in order, as one batch
/// and keeps the latest change per key.
fn latest_by_key(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        updates: Vec<Value>,
    }
    let Input { updates } = input(value);
    let updates: Vec<Update> = updates
        .into_iter()
        .map(|value| {
            let update: Update = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(json!(update), value, "an update in the core's JSON form");
            update
        })
        .collect();
    let mut batcher = Batcher::new(
        Start::Resume {
            frontier: Frontier::new(1).unwrap(),
        },
        None,
    );
    batcher.push(Message::Progress { frontier: 0 }).unwrap();
    for update in updates {
        batcher.push(Message::Update(update)).unwrap();
    }
    let batch = batcher.finish().unwrap().unwrap();
    json!(batch.into_latest_by_key())
}

fn subscribe_sql(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        subscribe: Subscribe,
        start: Start,
    }
    let Input { subscribe, start } = input(value);
    match subscribe.sql(start) {
        Ok(sql) => json!({ "sql": sql }),
        Err(_) => error("usage"),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchInput {
    max_rows: String,
    timeout_ms: Option<String>,
}

impl FetchInput {
    fn timeout(&self) -> Option<Duration> {
        self.timeout_ms
            .as_deref()
            .map(|ms| Duration::from_millis(u64_of(ms)))
    }
}

fn fetch(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        cursor: String,
        max_rows: String,
        timeout_ms: Option<String>,
    }
    let Input {
        cursor,
        max_rows,
        timeout_ms,
    } = input(value);
    let fetch = FetchInput {
        max_rows,
        timeout_ms,
    };
    match fetch_sql(&cursor, u64_of(&fetch.max_rows), fetch.timeout()) {
        Ok(sql) => json!({ "sql": sql }),
        Err(_) => error("usage"),
    }
}

/// Drives a subscription: declares its cursor, then runs the steps. A step
/// `{"fetch": {"max_rows", "timeout_ms"}}` yields the statement and its
/// timeout. A step `{"push": {"columns", "rows"}}` passes those to the latest
/// fetch and yields the events and whether the stream has finished.
fn subscription(value: Value) -> Value {
    let Value::Object(mut options) = value else {
        panic!("invalid input: {value}")
    };
    let steps = options.remove("steps").expect("steps");
    let Value::Array(steps) = steps else {
        panic!("invalid steps: {steps}")
    };
    let mut sub = match binding::Subscription::new(&Value::Object(options).to_string()) {
        Ok(sub) => sub,
        Err(error) => return error_of(&error),
    };
    let mut outputs = vec![json!({ "declare": sub.declare_sql().unwrap() })];
    for step in steps {
        let Value::Object(step) = step else {
            panic!("invalid step: {step}")
        };
        let [(name, input)]: [(String, Value); 1] = step
            .into_iter()
            .collect::<Vec<_>>()
            .try_into()
            .expect("a step has one key");
        let input = input.to_string();
        let output = match name.as_str() {
            "fetch" => sub.fetch(&input),
            "push" => sub.push(&input),
            "classify" => sub.classify(&input),
            other => panic!("unknown step {other:?}"),
        };
        outputs.push(match output {
            Ok(text) => serde_json::from_str(&text).unwrap(),
            Err(error) => error_of(&error),
        });
    }
    json!(outputs)
}

fn error_of(failure: &binding::Error) -> Value {
    error(failure.kind())
}

fn classify_error(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        sqlstate: Option<String>,
        message: String,
        phase: Phase,
    }
    let Input {
        sqlstate,
        message,
        phase,
    } = input(value);
    json!(classify(sqlstate.as_deref(), &message, phase))
}

fn compare_identity(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        stored: Fingerprint,
        current: Option<Fingerprint>,
    }
    let Input { stored, current } = input(value);
    json!(compare(&stored, current.as_ref()))
}

fn resolve(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        otherwise: ErrorKind,
        change: IdentityChange,
    }
    let Input { otherwise, change } = input(value);
    json!(resolve_object_check(otherwise, change))
}

fn resolve_index(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        indexes: Vec<String>,
    }
    let Input { indexes } = input(value);
    json!({ "kind": resolve_index_check(&indexes) })
}

/// Decodes the stored checkpoint, then applies the operation. A new checkpoint
/// is compared by its decoded JSON, and its exact text separately as `text`.
fn checkpoint(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        stored: Value,
        operation: Operation,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "snake_case")]
    enum Operation {
        Encode,
        TakeOver,
        EncodeWithin {
            limit: String,
        },
        Restart {
            subscriptions: BTreeMap<String, Fingerprint>,
            as_of: String,
        },
        WithSubscriptions {
            subscriptions: BTreeMap<String, Fingerprint>,
        },
        Committed {
            upper: Frontier,
        },
        CheckCommit {
            expected: Value,
            upper: Frontier,
        },
        CheckWrite {
            expected: Value,
        },
        Settle {
            expected: Value,
            new: Value,
        },
    }
    let Input { stored, operation } = input(value);
    // A store holds the exact encoding, so the JSON form is written out with
    // its keys sorted, as the encoding writes them.
    let decode = |value: Value| Checkpoint::decode(&sorted(value).to_string());
    let stored = match decode(stored) {
        Ok(stored) => stored,
        Err(_) => return error("checkpoint"),
    };
    let written = |text: String| -> Value {
        json!({ "checkpoint": serde_json::from_str::<Value>(&text).unwrap(), "text": text })
    };
    let encoded = |checkpoint: Checkpoint| match checkpoint.encode() {
        Ok(text) => written(text),
        Err(CheckpointError::TooLarge { .. }) => error("checkpoint_too_large"),
        Err(_) => error("checkpoint"),
    };
    match operation {
        Operation::Encode => encoded(stored),
        Operation::TakeOver => match stored.take_over() {
            Ok(next) => encoded(next),
            Err(_) => error("checkpoint"),
        },
        Operation::EncodeWithin { limit } => match stored.encode_within(canonical(&limit)) {
            Ok(text) => written(text),
            Err(CheckpointError::TooLarge { .. }) => error("checkpoint_too_large"),
            Err(_) => error("checkpoint"),
        },
        Operation::Restart {
            subscriptions,
            as_of,
        } => match stored.restart(subscriptions, u64_of(&as_of)) {
            Ok(next) => encoded(next),
            Err(_) => error("checkpoint"),
        },
        Operation::WithSubscriptions { subscriptions } => {
            encoded(stored.with_subscriptions(subscriptions))
        }
        Operation::Committed { upper } => encoded(stored.committed(upper)),
        Operation::CheckCommit { expected, upper } => match decode(expected) {
            Ok(expected) => match stored.check_commit(&expected, upper) {
                Ok(check) => json!({ "commit": check }),
                Err(conflict) => json!(conflict),
            },
            Err(_) => error("checkpoint"),
        },
        Operation::Settle { expected, new } => match (decode(expected), decode(new)) {
            (Ok(expected), Ok(new)) => json!(stored.settle(&expected, &new)),
            _ => error("checkpoint"),
        },
        Operation::CheckWrite { expected } => match decode(expected) {
            Ok(expected) => match stored.check_write(&expected) {
                Ok(()) => json!({ "write": "ok" }),
                Err(conflict) => json!(conflict),
            },
            Err(_) => error("checkpoint"),
        },
    }
}

/// Builds one checkpoint write from the stored checkpoint.
fn checkpoint_write(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        current: Value,
        operation: Operation,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "snake_case")]
    enum Operation {
        TakeOver,
        Commit {
            upper: Frontier,
        },
        StartSnapshot {
            subscriptions: BTreeMap<String, Fingerprint>,
            as_of: String,
        },
        SnapshotWrite,
        Fingerprints {
            subscriptions: BTreeMap<String, Fingerprint>,
        },
    }
    let Input { current, operation } = input(value);
    let Ok(current) = Checkpoint::decode(&sorted(current).to_string()) else {
        return error("checkpoint");
    };
    let built = match operation {
        Operation::TakeOver => CheckpointWrite::take_over(&current).map(|(write, _)| write),
        Operation::Commit { upper } => {
            CheckpointWrite::commit(&current, upper).map(|(write, _)| write)
        }
        Operation::StartSnapshot {
            subscriptions,
            as_of,
        } => CheckpointWrite::start_snapshot(&current, subscriptions, u64_of(&as_of))
            .map(|(write, _)| write),
        Operation::SnapshotWrite => CheckpointWrite::snapshot_write(&current),
        Operation::Fingerprints { subscriptions } => {
            CheckpointWrite::fingerprints(&current, subscriptions).map(|(write, _)| write)
        }
    };
    match built {
        Ok(write) => json!({ "write": write }),
        Err(WriteError::Checkpoint(CheckpointError::TooLarge { .. })) => {
            error("checkpoint_too_large")
        }
        Err(WriteError::Conflict(conflict)) => json!(conflict),
        Err(WriteError::Usage(_)) => error("usage"),
        Err(_) => error("checkpoint"),
    }
}

/// Runs one catalog operation: build a query, read a query's result, or
/// compute the resume margin.
fn catalog(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "snake_case")]
    enum Operation {
        LookupSql {
            name: ObjectName,
        },
        ParseLookup {
            columns: Vec<String>,
            rows: Vec<Row>,
        },
        IndexesSql {
            object_id: String,
            cluster: String,
        },
        ParseIndexes {
            columns: Vec<String>,
            rows: Vec<Row>,
        },
        ReadFrontierSql {
            object_id: String,
        },
        ParseReadFrontier {
            columns: Vec<String>,
            rows: Vec<Row>,
        },
        ResumeMargin {
            frontier: Frontier,
            read_frontier: String,
        },
        Fingerprint {
            object: CatalogObject,
            envelope: Envelope,
        },
    }
    match input(value) {
        Operation::LookupSql { name } => json!({ "sql": lookup_sql(&name) }),
        Operation::ParseLookup { columns, rows } => match parse_lookup(&columns, rows) {
            Ok(object) => json!({ "object": object }),
            Err(_) => error("protocol"),
        },
        Operation::IndexesSql { object_id, cluster } => {
            json!({ "sql": indexes_sql(&object_id, &cluster) })
        }
        Operation::ParseIndexes { columns, rows } => match parse_indexes(&columns, rows) {
            Ok(indexes) => json!({ "indexes": indexes }),
            Err(_) => error("protocol"),
        },
        Operation::ReadFrontierSql { object_id } => {
            json!({ "sql": read_frontier_sql(&object_id) })
        }
        Operation::ParseReadFrontier { columns, rows } => {
            match parse_read_frontier(&columns, rows) {
                Ok(read_frontier) => {
                    json!({ "read_frontier": read_frontier.map(|t| t.to_string()) })
                }
                Err(_) => error("protocol"),
            }
        }
        Operation::ResumeMargin {
            frontier,
            read_frontier,
        } => json!({
            "margin_ms": resume_margin(frontier, u64_of(&read_frontier))
                .map(|margin| margin.as_millis().to_string()),
        }),
        Operation::Fingerprint { object, envelope } => match object.fingerprint(envelope) {
            Ok(fingerprint) => json!({ "fingerprint": fingerprint }),
            Err(_) => error("usage"),
        },
    }
}

/// `value` with every object's keys in sorted order.
fn sorted(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let entries: std::collections::BTreeMap<String, Value> = map.into_iter().collect();
            Value::Object(entries.into_iter().map(|(k, v)| (k, sorted(v))).collect())
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
        other => other,
    }
}
