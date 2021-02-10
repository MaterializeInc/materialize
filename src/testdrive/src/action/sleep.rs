// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use rand::Rng;

use crate::action::{State, SyncAction};
use crate::parser::BuiltinCommand;

pub struct SleepAction {
    duration: Duration,
    random: bool,
}

pub fn build_random_sleep(mut cmd: BuiltinCommand) -> Result<SleepAction, String> {
    let arg = cmd.args.string("duration")?;
    let duration = parse_duration::parse(&arg).map_err(|e| e.to_string())?;
    Ok(SleepAction {
        duration,
        random: true,
    })
}

pub fn build_sleep(mut cmd: BuiltinCommand) -> Result<SleepAction, String> {
    let arg = cmd.args.string("duration")?;
    let duration = parse_duration::parse(&arg).map_err(|e| e.to_string())?;
    Ok(SleepAction {
        duration,
        random: false,
    })
}

impl SyncAction for SleepAction {
    fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, _: &mut State) -> Result<(), String> {
        let sleep = if self.random {
            let mut rng = rand::thread_rng();
            rng.gen_range(Duration::from_secs(0)..self.duration)
        } else {
            self.duration
        };
        println!("Sleeping for {:?}", sleep);
        thread::sleep(sleep);
        Ok(())
    }
}
