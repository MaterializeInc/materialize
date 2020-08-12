// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;

use protoc::Protoc;

const SIMPLE_PROTO: &str = "message Simple {
    required int32 i = 1;
}
";

fn assert_error<T, E, R>(res: R, message: &str)
where
    E: fmt::Display,
    R: Borrow<Result<T, E>>,
{
    match res.borrow() {
        Ok(_) => panic!("expected error but got ok"),
        Err(e) if !e.to_string().contains(message) => {
            panic!(
                "incorrect error message\nerror: {}\nexpected message: {}",
                e, message
            );
        }
        Err(_) => (),
    }
}

fn build_workspace(proto_contents: &str) -> anyhow::Result<(TempDir, PathBuf)> {
    let temp_dir = tempfile::tempdir()?;
    let proto_path = temp_dir.path().join("input.proto");
    fs::write(&proto_path, proto_contents)?;
    Ok((temp_dir, proto_path))
}

#[test]
fn bad_out_dir() -> anyhow::Result<()> {
    let (temp_dir, proto_path) = build_workspace(SIMPLE_PROTO)?;
    let res = Protoc::new()
        .include(temp_dir.path())
        .input(proto_path)
        .compile_into(&temp_dir.path().join("noexist"));
    assert_error(res, "out directory for protobuf generation does not exist");
    Ok(())
}

#[test]
fn missing_input_file() -> anyhow::Result<()> {
    let (temp_dir, _proto_path) = build_workspace(SIMPLE_PROTO)?;
    let res = Protoc::new()
        .include(temp_dir.path())
        .input(temp_dir.path().join("noexist"))
        .compile_into(temp_dir.path());
    assert_error(res, "input protobuf file does not exist");
    Ok(())
}

#[test]
fn bad_input_file() -> anyhow::Result<()> {
    let (temp_dir, proto_path) = build_workspace("bad syntax")?;
    let res = Protoc::new()
        .include(temp_dir.path())
        .input(proto_path)
        .compile_into(&temp_dir.path());
    assert_error(&res, "input.proto");
    assert_error(&res, "ParserError");
    Ok(())
}

#[test]
fn simple_success() -> anyhow::Result<()> {
    let (temp_dir, proto_path) = build_workspace(SIMPLE_PROTO)?;
    Protoc::new()
        .include(temp_dir.path())
        .input(proto_path)
        .compile_into(&temp_dir.path())?;
    Ok(())
}

#[test]
fn well_known_types() -> anyhow::Result<()> {
    let (temp_dir, proto_path) = build_workspace(
        r#"import "google/protobuf/timestamp.proto";

    message HasWellKnownType {
        required google.protobuf.Timestamp ts = 1;
    }"#,
    )?;
    Protoc::new()
        .include(temp_dir.path())
        .input(proto_path)
        .compile_into(&temp_dir.path())?;
    Ok(())
}
