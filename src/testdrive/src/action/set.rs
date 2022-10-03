// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;

use anyhow::Context;
use regex::Regex;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub const DEFAULT_REGEX_REPLACEMENT: &str = "<regex_match>";

pub fn run_regex_set(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let regex: Regex = cmd.args.parse("match")?;
    let replacement = cmd
        .args
        .opt_string("replacement")
        .unwrap_or_else(|| DEFAULT_REGEX_REPLACEMENT.into());
    cmd.args.done()?;

    state.regex = Some(regex);
    state.regex_replacement = replacement;
    Ok(ControlFlow::Continue)
}

pub fn run_regex_unset(
    cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    cmd.args.done()?;
    state.regex = None;
    state.regex_replacement = DEFAULT_REGEX_REPLACEMENT.to_string();
    Ok(ControlFlow::Continue)
}

pub fn run_sql_timeout(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let duration = cmd.args.string("duration")?;
    let duration = if duration.to_lowercase() == "default" {
        None
    } else {
        Some(mz_repr::util::parse_duration(&duration).context("parsing duration")?)
    };
    let force = cmd.args.opt_bool("force")?.unwrap_or(false);
    cmd.args.done()?;
    state.timeout = duration.unwrap_or(state.default_timeout);
    if !force {
        // Bump the timeout to be at least the default timeout unless the
        // timeout has been forced.
        state.timeout = cmp::max(state.timeout, state.default_timeout);
    }
    Ok(ControlFlow::Continue)
}

pub fn run_max_tries(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let max_tries = cmd.args.string("max-tries")?;
    cmd.args.done()?;
    state.max_tries = max_tries.parse::<usize>()?;
    Ok(ControlFlow::Continue)
}

pub fn set_vars(cmd: BuiltinCommand, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
    for (key, val) in cmd.args {
        if val.is_empty() {
            state.cmd_vars.insert(key, cmd.input.join("\n"));
        } else {
            state.cmd_vars.insert(key, val);
        }
    }

    Ok(ControlFlow::Continue)
}
