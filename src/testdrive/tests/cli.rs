// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    Command::cargo_bin("testdrive").unwrap()
}

#[test]
fn test_missing_file() {
    cmd()
        .arg("noexist")
        .assert()
        .failure()
        .stderr(predicate::str::starts_with(
            r#"error: opening noexist: No such file or directory"#,
        ));
}

#[test]
fn test_bad_file() {
    cmd()
        .arg("tests")
        .assert()
        .failure()
        .stderr(predicate::str::starts_with(
            "error: reading tests: Is a directory",
        ));
}

#[test]
fn test_leading_input() {
    cmd()
        .write_stdin("leading input")
        .assert()
        .failure()
        .stderr(
            r#"<stdin>:1:1: error: unexpected input line at beginning of file
     |
   1 | leading input
     | ^
"#,
        );
}

#[test]
fn test_cmd_missing_name() {
    cmd().write_stdin("$   ").assert().failure().stderr(concat!(
        r#"<stdin>:1:1: error: command line is missing command name
     |
   1 | $   "#, // separated to preserve trailing spaces
        r#"
     | ^
"#,
    ));
}

#[test]
fn test_cmd_arg_missing_value() {
    cmd().write_stdin("$ cmd badarg").assert().failure().stderr(
        r#"<stdin>:1:7: error: command argument is not in required key=value format
     |
   1 | $ cmd badarg
     |       ^
"#,
    );
}

#[test]
fn test_cmd_arg_bad_nesting_close() {
    cmd()
        .write_stdin("$ cmd arg={}}")
        .assert()
        .failure()
        .stderr(
            r#"<stdin>:1:13: error: command argument has unbalanced close brace
     |
   1 | $ cmd arg={}}
     |             ^
"#,
        );
}

#[test]
fn test_cmd_arg_bad_nesting_open() {
    cmd()
        .write_stdin("$ cmd arg={{one} two three")
        .assert()
        .failure()
        .stderr(
            r#"<stdin>:1:26: error: command argument has unterminated open brace
     |
   1 | $ cmd arg={{one} two three
     |                          ^
"#,
        );
}
