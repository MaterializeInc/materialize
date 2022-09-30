// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::postgres::postgres_client;

pub async fn run_connect(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    if name.starts_with("postgres://") {
        bail!("connection name can not be url");
    }

    let url = cmd.args.string("url")?;
    cmd.args.done()?;

    let (client, _) = postgres_client(&url).await?;
    state.postgres_clients.insert(name.clone(), client);
    Ok(ControlFlow::Continue)
}
