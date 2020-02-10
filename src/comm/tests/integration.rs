// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert_cmd::assert::Assert;
use assert_cmd::cargo::CommandCargoExt;
use rand::Rng;
use std::error::Error;
use std::ffi::OsStr;
use std::process::{Child, Command, Stdio};

#[test]
fn test_pingpong() -> Result<(), Box<dyn Error>> {
    fn spawn<I>(args: I) -> Result<Child, Box<dyn Error>>
    where
        I: IntoIterator,
        I::Item: AsRef<OsStr>,
    {
        Ok(Command::cargo_bin("examples/pingpong")?
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?)
    }
    let n = rand::thread_rng().gen::<u8>().to_string();
    let p0 = spawn(&["-p0", &n])?;
    let p1 = spawn(&["-p1"])?;
    let p2 = spawn(&["-p2"])?;
    let stdout_pred = predicates::str::contains(format!("magic number: {}", n));
    Assert::new(p0.wait_with_output()?).success();
    Assert::new(p1.wait_with_output()?)
        .success()
        .stdout(stdout_pred.clone());
    Assert::new(p2.wait_with_output()?)
        .success()
        .stdout(stdout_pred);
    Ok(())
}
