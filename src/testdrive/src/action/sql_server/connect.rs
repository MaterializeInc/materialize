// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;

use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct ConnectAction {
    name: String,
    config: tiberius::Config,
}

pub fn build_connect(mut cmd: BuiltinCommand) -> Result<ConnectAction, String> {
    let name = cmd.args.string("name")?;
    cmd.args.done()?;

    let ado_string = cmd.input.join("\n");

    let config =
        Config::from_ado_string(&ado_string).map_err(|e| format!("parsing ADO string: {}", e))?;

    Ok(ConnectAction { name, config })
}

#[async_trait]
impl Action for ConnectAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let tcp = TcpStream::connect(self.config.get_addr())
            .await
            .map_err(|e| format!("Connecting to SQL Server: {}", e))?;

        tcp.set_nodelay(true)
            .map_err(|e| format!("Setting NODELAY socket option: {}", e))?;

        // To be able to use Tokio's tcp, we're using the `compat_write` from
        // the `TokioAsyncWriteCompatExt` to get a stream compatible with the
        // traits from the `futures` crate.
        let client = Client::connect(self.config.clone(), tcp.compat_write())
            .await
            .map_err(|e| format!("connecting to SQL Server: {}", e))?;

        state.sql_server_clients.insert(self.name.clone(), client);
        Ok(())
    }
}
