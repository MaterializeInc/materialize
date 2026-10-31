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

#[cfg(any(
    feature = "antithesis",
    all(feature = "process", feature = "tracing", target_os = "linux")
))]
use std::{fs, process::Command};

#[cfg(feature = "antithesis")]
use serde_json::{Value, json};

#[cfg(feature = "antithesis")]
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

#[cfg(all(feature = "process", feature = "tracing", target_os = "linux"))]
#[mz_ore::test]
fn halt_records_full_message_only_in_antithesis_builds() {
    const CHILD: &str = "MZ_ANTITHESIS_HALT_PROBE";
    if let Some(mode) = std::env::var_os(CHILD) {
        #[cfg(feature = "antithesis")]
        mz_ore::antithesis::init();
        match mode.to_str().expect("probe mode") {
            "designed" | "absent" | "symlink" | "append" => {
                mz_ore::halt!("fenced out old deployment; rebooting as leader")
            }
            "unknown" => mz_ore::halt!("unreviewed halt with Unicode \u{2603} and a newline\nEND"),
            "oversize" => mz_ore::halt!("{}", "x".repeat(4096)),
            "restart" => mz_ore::restart_in_place!("probe restart"),
            "lease" => mz_ore::process::halt_with_code(168, format_args!("probe expired lease")),
            "direct" => mz_ore::process::exit_thread_safe(167),
            mode => panic!("unknown probe mode: {mode}"),
        }
    }

    let directory = tempfile::tempdir().expect("create halt output directory");
    for (mode, code) in [
        ("designed", 166),
        ("unknown", 166),
        ("oversize", 166),
        ("restart", 75),
        ("lease", 168),
        ("direct", 167),
        ("absent", 166),
        ("symlink", 166),
        ("append", 166),
    ] {
        let output = directory.path().join(format!("{mode}.termination"));
        let sdk = directory.path().join(format!("{mode}.sdk"));
        match mode {
            "absent" => {}
            "symlink" => {
                let target = directory.path().join("symlink-target");
                fs::write(&target, b"sentinel").expect("create symlink target");
                std::os::unix::fs::symlink(target, &output).expect("create symlink");
            }
            "append" => fs::write(&output, b"existing\n").expect("create nonempty file"),
            _ => fs::write(&output, b"").expect("create existing termination file"),
        }
        let result = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "halt_records_full_message_only_in_antithesis_builds",
            ])
            .env(CHILD, mode)
            .env("MZ_ANTITHESIS_TERMINATION_LOG", &output)
            .env("ANTITHESIS_SDK_LOCAL_OUTPUT", &sdk)
            .output()
            .expect("run halt probe");
        assert_eq!(result.status.code(), Some(code), "{mode}: {result:?}");
        if mode == "absent" {
            assert!(
                !output.exists(),
                "halt recorder created its own evidence file"
            );
            continue;
        }
        let record = fs::read_to_string(&output).expect("read termination file");
        if mode == "symlink" {
            assert_eq!(record, "sentinel", "halt recorder followed a symlink");
            continue;
        }
        if mode == "append" {
            if cfg!(feature = "antithesis") {
                assert!(record.starts_with("existing\nMZHALT1 0 166 "), "{record:?}");
            } else {
                assert_eq!(record, "existing\n");
            }
            continue;
        }
        if !cfg!(feature = "antithesis") || code != 166 {
            assert!(record.is_empty(), "unexpected termination I/O: {mode}");
            continue;
        }
        assert!(
            record.len() < 4096 && record.ends_with(" END\n"),
            "{mode}: {record:?}"
        );
        if mode == "oversize" {
            assert!(record.contains(" INVALID oversize END\n"), "{record:?}");
            continue;
        }
        let fields = record.split_ascii_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 9, "{record:?}");
        assert_eq!(&fields[..3], &["MZHALT1", "0", "166"]);
        assert!(fields[3].parse::<u128>().expect("nanosecond timestamp") > 0);
        let decode = |hex: &str| {
            let bytes = hex
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    u8::from_str_radix(std::str::from_utf8(pair).expect("hex text"), 16)
                        .expect("hex byte")
                })
                .collect::<Vec<_>>();
            String::from_utf8(bytes).expect("UTF8 field")
        };
        let location = decode(fields[6]);
        let message = decode(fields[7]);
        assert_eq!(
            location.len(),
            fields[4].parse::<usize>().expect("location length")
        );
        assert_eq!(
            message.len(),
            fields[5].parse::<usize>().expect("message length")
        );
        assert!(
            location.starts_with("src/ore/tests/antithesis.rs:"),
            "{location}"
        );
        assert_eq!(
            message,
            if mode == "designed" {
                "fenced out old deployment; rebooting as leader"
            } else {
                "unreviewed halt with Unicode \u{2603} and a newline\nEND"
            }
        );
        #[cfg(feature = "antithesis")]
        if mode == "unknown" {
            assert!(
                fs::read_to_string(sdk)
                    .expect("read SDK events")
                    .lines()
                    .any(|line| {
                        let event: Value = serde_json::from_str(line).expect("parse SDK event");
                        event.get("antithesis_assert").is_some_and(|assertion| {
                            assertion["message"]
                                == "Materialize process halted for an unexpected reason"
                                && assertion["hit"] == true
                                && assertion["condition"] == false
                        })
                    }),
                "unknown halt lost its product assertion"
            );
        }
    }
}
