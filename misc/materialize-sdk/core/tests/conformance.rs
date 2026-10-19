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

use materialize_sdk_core::conformance::run;
use serde::Deserialize;
use serde_json::Value;

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
