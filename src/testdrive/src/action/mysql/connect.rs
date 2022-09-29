// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;

use anyhow::anyhow;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct ConnectAction {
    name: String,
    url: String,
    password: Option<String>,
}

pub fn build_connect(mut cmd: BuiltinCommand) -> Result<ConnectAction, anyhow::Error> {
    let name = cmd.args.string("name")?;
    let url = cmd.args.string("url")?;
    // We allow the password to be specified outside of the URL
    // in case it contains special characters
    let password = cmd.args.opt_string("password");
    cmd.args.done()?;

    Ok(ConnectAction {
        name,
        url,
        password,
    })
}

#[async_trait]
impl Action for ConnectAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let opts_url = mysql_async::Opts::from_url(&self.url)
            .map_err(|_| anyhow!("Unable to parse MySQL URL {}", self.url))?;
        let opts = mysql_async::OptsBuilder::from_opts(opts_url).pass(self.password.clone());
        let pool = mysql_async::Pool::new(opts);
        let conn = pool
            .get_conn()
            .await
            .map_err(|_| anyhow!("Unable to connect to MySQL server at {}", self.url))?;

        state.mysql_clients.insert(self.name.clone(), conn);
        Ok(ControlFlow::Continue)
    }
}
