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

pub async fn run_append(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let database = cmd
        .args
        .opt_string("database")
        .unwrap_or_else(|| "materialize".to_string());
    let schema = cmd
        .args
        .opt_string("schema")
        .unwrap_or_else(|| "public".to_string());
    let name = cmd.args.string("name")?;

    let status_code = cmd.args.opt_parse::<u16>("status")?;
    // Interpret the remaining arguments as headers.
    let headers: Vec<(String, String)> = cmd.args.into_iter().collect();

    let body = cmd.input.join("\n");

    println!("$ webhook-append {database}.{schema}.{name}\n{body}\n{headers:?}");

    let client = reqwest::Client::new();
    let url = format!(
        "http://{}/api/webhook/{database}/{schema}/{name}",
        state.materialize_http_addr
    );
    let mut builder = client.post(url).body(body);

    // Append all of our headers.
    for (name, value) in headers {
        builder = builder.header(name, value);
    }

    let response = builder.send().await?;
    let status = response.status();

    println!("{}\n{}", status, response.text().await?);

    let expected_status = status_code.unwrap_or(200);
    if status.as_u16() == expected_status {
        Ok(ControlFlow::Continue)
    } else {
        bail!("webhook append returned unexpected status: {}", status);
    }
}
