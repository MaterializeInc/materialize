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

#![allow(clippy::unwrap_used, clippy::panic)]

use std::path::Path;

use materialize_sdk_codegen::{Seen, files, model, schema};
use serde_json::{Map, Value, json};

#[test]
fn the_generated_files_are_current() {
    for (path, expected) in files() {
        let found = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            found == expected,
            "{} is stale: run `cargo run -p materialize-sdk-codegen`",
            path.display()
        );
    }
}

/// Every object in `value`, depth first.
fn objects<'a>(value: &'a Value, found: &mut Vec<&'a Map<String, Value>>) {
    match value {
        Value::Object(object) => {
            found.push(object);
            object.values().for_each(|v| objects(v, found));
        }
        Value::Array(items) => items.iter().for_each(|v| objects(v, found)),
        _ => {}
    }
}

#[test]
fn every_value_in_the_vectors_matches_its_schema() {
    let model = model(&schema());
    // A value is known by its tag field. A decoded message also has `op`, and
    // is not an `Update`.
    let tags = [
        ("Event", "event"),
        ("Update", "op"),
        ("Classification", "action"),
    ];
    let mut seen = Seen::new();
    let vectors = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance");
    for entry in std::fs::read_dir(vectors).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let file: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let function = file["function"].as_str().unwrap();
        for case in file["cases"].as_array().unwrap() {
            let mut check = |name: &str, value: &Value| {
                model
                    .check_recording(name, value, &mut seen)
                    .unwrap_or_else(|e| panic!("{}: {}: {e}", path.display(), case["name"]));
            };
            // These resolve to an error kind, which the runner wraps.
            if function == "resolve_index_check" || function == "resolve_object_check" {
                if let Some(kind) = case["expect"].get("kind") {
                    check("ErrorKind", kind);
                }
                continue;
            }
            let mut found = Vec::new();
            objects(&case["expect"], &mut found);
            for object in found.into_iter().filter(|o| !o.contains_key("message")) {
                for (name, tag) in tags {
                    if object.contains_key(tag) {
                        check(name, &Value::Object(object.clone()));
                    }
                }
            }
        }
    }
    // Every variant and enum value appears, so none of them is left unchecked.
    assert_eq!(seen, model.all_values());
}

#[test]
fn the_check_refuses_what_the_core_does_not_print() {
    let model = model(&schema());
    let update = |timestamp: Value, diff: Value| json!({"timestamp": timestamp, "op": "diff", "row": ["a", null], "diff": diff});
    let batch = |lower: Value, upper: &str| json!({"event": "batch", "lower": lower, "upper": upper, "snapshot": false, "updates": []});
    assert_eq!(
        model.check("Update", &update(json!("5"), json!("-1"))),
        Ok(())
    );
    for (bad, why) in [
        (update(json!(5), json!("1")), "a number"),
        (update(json!("05"), json!("1")), "a leading zero"),
        (update(json!("5"), json!("+1")), "a sign"),
        (
            update(json!("18446744073709551616"), json!("1")),
            "past u64",
        ),
        (
            json!({"timestamp": "5", "op": "diff", "row": []}),
            "a missing field",
        ),
        (
            json!({"timestamp": "5", "op": "insert", "row": [], "diff": "1"}),
            "an unknown variant",
        ),
    ] {
        assert!(model.check("Update", &bad).is_err(), "{why} passed");
    }
    assert_eq!(
        model.check("Event", &batch(json!(null), "18446744073709551616")),
        Ok(())
    );
    let mut extra = batch(json!("1"), "2");
    extra["more"] = json!(true);
    for (bad, why) in [
        (batch(json!("1"), "0"), "frontier 0"),
        (batch(json!("1"), "18446744073709551617"), "past 2^64"),
        (extra, "an extra field"),
    ] {
        assert!(model.check("Event", &bad).is_err(), "{why} passed");
    }
}
