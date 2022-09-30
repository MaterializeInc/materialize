// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use reqwest::Method;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

use reqwest::header::CONTENT_TYPE;

pub async fn run_request(
    mut cmd: BuiltinCommand,
    _: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let url = cmd.args.string("url")?;
    let method: Method = cmd.args.parse("method")?;
    let content_type = cmd.args.opt_string("content-type");
    let body = cmd.input.join("\n");

    println!("$ http-request {} {}\n{}", method, url, body);

    let client = reqwest::Client::new();

    let mut request = client.request(method, &url).body(body);

    if let Some(value) = &content_type {
        request = request.header(CONTENT_TYPE, value);
    }

    let response = request.send().await?;
    let status = response.status();

    println!("{}\n{}", status, response.text().await?);

    if status.is_success() {
        Ok(ControlFlow::Continue)
    } else {
        bail!("http request returned failing status: {}", status)
    }
}
