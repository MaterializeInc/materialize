// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use async_trait::async_trait;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::postgres::postgres_client;

pub struct ConnectAction {
    name: String,
    url: String,
}

pub fn build_connect(mut cmd: BuiltinCommand) -> Result<ConnectAction, anyhow::Error> {
    let name = cmd.args.string("name")?;
    if name.starts_with("postgres://") {
        bail!("connection name can not be url");
    }

    let url = cmd.args.string("url")?;
    cmd.args.done()?;
    Ok(ConnectAction { name, url })
}

#[async_trait]
impl Action for ConnectAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let (client, _) = postgres_client(&self.url).await?;
        state.postgres_clients.insert(self.name.clone(), client);
        Ok(ControlFlow::Continue)
    }
}
