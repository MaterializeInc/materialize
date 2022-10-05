// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use anyhow::Context;
use rand::Rng;

use crate::action::ControlFlow;
use crate::parser::BuiltinCommand;

pub fn run_sleep(cmd: BuiltinCommand) -> Result<ControlFlow, anyhow::Error> {
    run_sleep_inner(cmd, false)
}

pub fn run_random_sleep(cmd: BuiltinCommand) -> Result<ControlFlow, anyhow::Error> {
    run_sleep_inner(cmd, true)
}

fn run_sleep_inner(mut cmd: BuiltinCommand, random: bool) -> Result<ControlFlow, anyhow::Error> {
    let arg = cmd.args.string("duration")?;
    cmd.args.done()?;
    let duration = humantime::parse_duration(&arg).context("parsing duration")?;
    let sleep = if random {
        let mut rng = rand::thread_rng();
        rng.gen_range(Duration::from_secs(0)..duration)
    } else {
        duration
    };
    println!("Sleeping for {:?}", sleep);
    thread::sleep(sleep);
    Ok(ControlFlow::Continue)
}
