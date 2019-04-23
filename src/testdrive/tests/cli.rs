// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

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
            "error: opening noexist: No such file or directory",
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
        .with_stdin()
        .buffer("leading input")
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
    cmd()
        .with_stdin()
        .buffer("$   ")
        .assert()
        .failure()
        .stderr(concat!(
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
    cmd()
        .with_stdin()
        .buffer("$ cmd badarg")
        .assert()
        .failure()
        .stderr(
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
        .with_stdin()
        .buffer("$ cmd arg={}}")
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
        .with_stdin()
        .buffer("$ cmd arg={{one} two three")
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
