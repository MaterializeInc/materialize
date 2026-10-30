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

#![cfg(feature = "antithesis")]

use std::fs;
use std::process::Command;

use serde_json::{Value, json};

#[mz_ore::test]
fn coverage_does_not_assert_product_failures() {
    const CHILD: &str = "MZ_ANTITHESIS_OBSERVATION_PROBE";
    if std::env::var_os(CHILD).is_some() {
        mz_ore::antithesis::init();
        mz_ore::antithesis_sometimes!(false, "probe coverage condition", &json!({}));
        mz_ore::antithesis_reachable!("probe coverage milestone", &json!({}));
        mz_ore::antithesis_reachable!(
            concat!("probe per-site ", file!(), ":", line!()),
            &json!({})
        );
        mz_ore::antithesis::expect_panic();
        mz_ore::antithesis::panicked("probe.rs:1", "expected expired lease");
        mz_ore::antithesis::panicked("probe.rs:2", "unexpected panic");
        mz_ore::antithesis::halted(
            "probe.rs:3",
            "fenced out old deployment; rebooting as leader",
        );
        mz_ore::antithesis::halted("probe.rs:4", "an unreviewed halt reason");
        mz_ore::antithesis_always!(false, "probe product invariant", &json!({}));
        return;
    }

    // The SDK selects its local output when the process initializes it.
    let directory = tempfile::tempdir().expect("create SDK output directory");
    let output = directory.path().join("events.jsonl");
    let result = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "coverage_does_not_assert_product_failures"])
        .env(CHILD, "1")
        .env("ANTITHESIS_SDK_LOCAL_OUTPUT", &output)
        .output()
        .expect("run SDK probe");
    assert!(result.status.success(), "{result:?}");

    let events = fs::read_to_string(output)
        .expect("read SDK output")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("parse SDK event"))
        .collect::<Vec<_>>();
    let assertions = events
        .iter()
        .filter_map(|event| event.get("antithesis_assert"))
        .collect::<Vec<_>>();
    for observation in [
        "probe coverage condition",
        "probe coverage milestone",
        "Materialize process panicked by design",
        "Materialize process halted by design",
    ] {
        assert!(
            assertions
                .iter()
                .all(|event| event["message"] != observation),
            "coverage became an assertion: {observation}"
        );
        assert!(
            events.iter().any(|event| {
                event
                    .get("workload_coverage")
                    .is_some_and(|coverage| coverage["message"] == observation)
            }),
            "coverage was not recorded: {observation}"
        );
    }
    assert!(events.iter().any(|event| {
        event.get("antithesis_guidance").is_some_and(|guidance| {
            guidance["message"] == "workload coverage: probe coverage condition"
                && guidance["guidance_data"]["observed"] == false
        })
    }));
    for failure in [
        "probe product invariant",
        "Materialize process panicked",
        "Materialize process halted for an unexpected reason",
    ] {
        assert!(
            assertions.iter().any(|event| {
                event["message"] == failure && event["hit"] == true && event["condition"] == false
            }),
            "a real violation was lost: {failure}"
        );
    }
    assert!(assertions.iter().all(|event| event["must_hit"] == false));
}
