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

use crate::action::{ControlFlow, State, SyncAction};
use crate::parser::BuiltinCommand;

pub struct SleepAction {
    duration: Duration,
    random: bool,
}

pub fn build_random_sleep(mut cmd: BuiltinCommand) -> Result<SleepAction, anyhow::Error> {
    let arg = cmd.args.string("duration")?;
    let duration = mz_repr::util::parse_duration(&arg).context("parsing duration")?;
    Ok(SleepAction {
        duration,
        random: true,
    })
}

pub fn build_sleep(mut cmd: BuiltinCommand) -> Result<SleepAction, anyhow::Error> {
    let arg = cmd.args.string("duration")?;
    let duration = mz_repr::util::parse_duration(&arg).context("parsing duration")?;
    Ok(SleepAction {
        duration,
        random: false,
    })
}

impl SyncAction for SleepAction {
    fn run(&self, _: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let sleep = if self.random {
            let mut rng = rand::thread_rng();
            rng.gen_range(Duration::from_secs(0)..self.duration)
        } else {
            self.duration
        };
        println!("Sleeping for {:?}", sleep);
        thread::sleep(sleep);
        Ok(ControlFlow::Continue)
    }
}
