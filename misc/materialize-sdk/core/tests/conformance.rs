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

//! Runs the conformance vectors in `misc/materialize-sdk/conformance` against
//! the protocol core. The README there defines each function's input and
//! output.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use materialize_sdk_core::Frontier;
use materialize_sdk_core::batch::{BatchError, Batcher, Event, Start};
use materialize_sdk_core::checkpoint::{Checkpoint, CheckpointError};
use materialize_sdk_core::classify::{
    ErrorKind, Phase, classify, resolve_index_check, resolve_object_check,
};
use materialize_sdk_core::decode::{Decoder, Envelope, Message, Row, Update};
use materialize_sdk_core::identity::{Fingerprint, IdentityChange, compare};
use materialize_sdk_core::statement::{Subscribe, fetch_sql};
use materialize_sdk_core::subscription::{Fetch, PushError, Subscription};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorFile {
    function: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    input: Value,
    expect: Value,
}

#[test]
fn conformance_vectors() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance");
    let mut paths: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no vectors in {}", dir.display());

    let mut failures = Vec::new();
    let mut count = 0;
    for path in paths {
        let file: VectorFile = serde_json::from_str(&fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for case in file.cases {
            count += 1;
            let actual = run(&file.function, case.input);
            if actual != case.expect {
                failures.push(format!(
                    "{} / {}\n  expected: {}\n  actual:   {}",
                    path.file_name().unwrap().to_string_lossy(),
                    case.name,
                    case.expect,
                    actual,
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {count} cases failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn run(function: &str, input: Value) -> Value {
    match function {
        "decode" => decode(input),
        "batch" => batch(input),
        "latest_by_key" => latest_by_key(input),
        "subscribe_sql" => subscribe_sql(input),
        "fetch_sql" => fetch(input),
        "subscription" => subscription(input),
        "classify" => classify_error(input),
        "compare_identity" => compare_identity(input),
        "resolve_object_check" => resolve(input),
        "resolve_index_check" => resolve_index(input),
        "checkpoint" => checkpoint(input),
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
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        subscribe: Subscribe,
        start: Start,
        max_pending_bytes: Option<String>,
        max_timestamp_bytes: Option<String>,
        steps: Vec<Step>,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "snake_case")]
    enum Step {
        Fetch(FetchInput),
        Push {
            columns: Vec<String>,
            rows: Vec<Row>,
        },
        Classify {
            sqlstate: Option<String>,
            message: String,
        },
    }
    let Input {
        subscribe,
        start,
        max_pending_bytes,
        max_timestamp_bytes,
        steps,
    } = input(value);
    let mut sub = match Subscription::new(subscribe, start) {
        Ok(sub) => sub,
        Err(_) => return error("usage"),
    };
    if let Some(limit) = max_pending_bytes {
        sub = sub.with_max_pending_bytes(canonical(&limit));
    }
    if let Some(limit) = max_timestamp_bytes {
        sub = sub.with_max_timestamp_bytes(canonical(&limit));
    }
    let mut outputs = vec![json!({ "declare": sub.declare_sql().unwrap() })];
    let mut pending: Option<Fetch> = None;
    for step in steps {
        let output = match step {
            Step::Fetch(fetch) => match sub.fetch(u64_of(&fetch.max_rows), fetch.timeout()) {
                Ok(fetch) => {
                    let output = json!({
                        "sql": fetch.sql(),
                        "timeout_ms": fetch.timeout().map(|t| t.as_millis().to_string()),
                    });
                    pending = Some(fetch);
                    output
                }
                Err(_) => error("usage"),
            },
            Step::Push { columns, rows } => {
                let fetch = pending.take().expect("a push follows a fetch");
                match sub.push(fetch, &columns, rows) {
                    Ok(events) => json!({ "events": events, "finished": sub.is_finished() }),
                    Err(PushError::Protocol(_)) => error("protocol"),
                    Err(PushError::TimestampTooLarge(_)) => error("timestamp_too_large"),
                    Err(PushError::Usage(_)) => error("usage"),
                }
            }
            Step::Classify { sqlstate, message } => {
                json!(sub.classify(sqlstate.as_deref(), &message))
            }
        };
        outputs.push(output);
    }
    json!(outputs)
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
