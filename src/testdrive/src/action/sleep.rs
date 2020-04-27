// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use rand::Rng;
use rdkafka::util::duration_to_millis;

use crate::action::{State, SyncAction};
use crate::parser::BuiltinCommand;

pub struct SleepAction {
    time: Duration,
}

pub fn build_sleep(mut cmd: BuiltinCommand) -> Result<SleepAction, String> {
    let arg = cmd.args.string("duration").map_err(|e| e.to_string())?;
    let time = parse_duration::parse(&arg).map_err(|e| e.to_string())?;
    Ok(SleepAction { time })
}

impl SyncAction for SleepAction {
    fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, _: &mut State) -> Result<(), String> {
        let mut rng = rand::thread_rng();
        let sleep = Duration::from_millis(rng.gen_range(0, duration_to_millis(self.time)));
        println!("Sleeping for {:?}", sleep);
        std::thread::sleep(sleep);
        Ok(())
    }
}
