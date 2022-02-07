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

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub const DEFAULT_REGEX_REPLACEMENT: &str = "<regex_match>";

pub struct RegexAction {
    regex: Regex,
    replacement: String,
}

pub fn build_regex(cmd: BuiltinCommand) -> Result<RegexAction, anyhow::Error> {
    let regex = cmd.args.parse("match")?;
    let replacement = cmd
        .args
        .opt_string("replacement")
        .unwrap_or_else(|| DEFAULT_REGEX_REPLACEMENT.into());
    cmd.args.done()?;
    Ok(RegexAction { regex, replacement })
}

#[async_trait]
impl Action for RegexAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        state.regex = Some(self.regex.clone());
        state.regex_replacement = self.replacement.clone();
        Ok(())
    }
}

pub struct SqlTimeoutAction {
    duration: Option<Duration>,
}

pub fn build_sql_timeout(cmd: BuiltinCommand) -> Result<SqlTimeoutAction, anyhow::Error> {
    let duration = cmd.args.string("duration")?;
    let duration = if duration.to_lowercase() == "default" {
        None
    } else {
        Some(repr::util::parse_duration(&duration).context("parsing duration")?)
    };
    cmd.args.done()?;
    Ok(SqlTimeoutAction { duration })
}

#[async_trait]
impl Action for SqlTimeoutAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        state.timeout = cmp::max(
            self.duration.unwrap_or(state.default_timeout),
            state.default_timeout,
        );
        Ok(())
    }
}
