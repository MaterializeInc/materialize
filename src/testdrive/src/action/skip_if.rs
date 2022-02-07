// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Context as _};
use async_trait::async_trait;

use crate::action::{Action, Context, State};
use crate::parser::BuiltinCommand;

pub struct SkipIfAction {
    query: String,
    context: Context,
}

pub fn build_publish(cmd: BuiltinCommand, context: Context) -> Result<SkipIfAction, anyhow::Error> {
    Ok(SkipIfAction {
        query: cmd.input.join(" "),
        context,
    })
}

#[async_trait]
impl Action for SkipIfAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        let stmt = state
            .pgclient
            .prepare(&self.query)
            .await
            .context("failed to prepare skip-if query")?;

        let actual: Vec<_> = state
            .pgclient
            .query(&stmt, &[])
            .await
            .context("executing query failed")?
            .into_iter()
            .map(|row| crate::action::sql::decode_row(row, self.context.clone()))
            .collect::<Result<_, _>>()?;

        if vec![vec!["true".to_string()]] == actual {
            println!("skip-if query returned true; skipping rest of file");
            state.skip_rest = true;
        } else if vec![vec!["false".to_string()]] == actual {
            println!("skip-if query returned false; continuing");
        } else {
            bail!(
                "skip-if query did not return `true` or `false`: {:?}",
                actual
            );
        }

        Ok(())
    }
}
