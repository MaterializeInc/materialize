// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use regex::Regex;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub const DEFAULT_REGEX_REPLACEMENT: &str = "<regex_match>";

pub enum RegexAction {
    Set { regex: Regex, replacement: String },
    Unset,
}

pub fn build_regex_set(mut cmd: BuiltinCommand) -> Result<RegexAction, anyhow::Error> {
    let regex = cmd.args.parse("match")?;
    let replacement = cmd
        .args
        .opt_string("replacement")
        .unwrap_or_else(|| DEFAULT_REGEX_REPLACEMENT.into());
    cmd.args.done()?;
    Ok(RegexAction::Set { regex, replacement })
}

pub fn build_regex_unset(_cmd: BuiltinCommand) -> Result<RegexAction, anyhow::Error> {
    Ok(RegexAction::Unset)
}

#[async_trait]
impl Action for RegexAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        match self {
            RegexAction::Set { regex, replacement } => {
                state.regex = Some(regex.clone());
                state.regex_replacement = replacement.clone();
            }
            RegexAction::Unset => {
                state.regex = None;
            }
        }
        Ok(ControlFlow::Continue)
    }
}

pub struct SqlTimeoutAction {
    duration: Option<Duration>,
    force: bool,
}

pub fn build_sql_timeout(mut cmd: BuiltinCommand) -> Result<SqlTimeoutAction, anyhow::Error> {
    let duration = cmd.args.string("duration")?;
    let duration = if duration.to_lowercase() == "default" {
        None
    } else {
        Some(mz_repr::util::parse_duration(&duration).context("parsing duration")?)
    };
    let force = cmd.args.opt_bool("force")?.unwrap_or(false);
    cmd.args.done()?;
    Ok(SqlTimeoutAction { duration, force })
}

#[async_trait]
impl Action for SqlTimeoutAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        state.timeout = self.duration.unwrap_or(state.default_timeout);
        if !self.force {
            // Bump the timeout to be at least the default timeout unless the
            // timeout has been forced.
            state.timeout = cmp::max(state.timeout, state.default_timeout);
        }
        Ok(ControlFlow::Continue)
    }
}

pub struct MaxTriesAction {
    max_tries: Option<usize>,
}

pub fn build_max_tries(mut cmd: BuiltinCommand) -> Result<MaxTriesAction, anyhow::Error> {
    let max_tries = cmd.args.string("max-tries")?;
    Ok(MaxTriesAction {
        max_tries: Some(max_tries.parse::<usize>()?),
    })
}

#[async_trait]
impl Action for MaxTriesAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        state.max_tries = self.max_tries.unwrap_or(state.default_max_tries);
        Ok(ControlFlow::Continue)
    }
}
