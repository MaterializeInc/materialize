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

use std::fs;
use std::path::Path;

use materialize_sdk_core::batch::{Batcher, Start};
use materialize_sdk_core::decode::{Decoder, Envelope, Message, Row};
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

/// Feeds messages to a batcher.
fn batch(value: Value) -> Value {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        start: Start,
        up_to: Option<String>,
        steps: Vec<Value>,
    }
    let Input {
        start,
        up_to,
        steps,
    } = input(value);
    let mut batcher = Batcher::new(start, up_to.as_deref().map(u64_of));
    let outputs: Vec<Value> = steps
        .into_iter()
        .map(|step| {
            let message: Message = serde_json::from_value(step.clone()).unwrap();
            // Reading the step back refuses unknown keys and other spellings.
            assert_eq!(json!(message), step, "a step in the core's JSON form");
            let result = batcher.push(message);
            match result {
                Ok(event) => json!(event.into_iter().collect::<Vec<_>>()),
                Err(_) => error("protocol"),
            }
        })
        .collect();
    json!({ "outputs": outputs, "finished": batcher.is_finished() })
}
