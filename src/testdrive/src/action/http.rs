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
use reqwest::Method;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

use reqwest::header::CONTENT_TYPE;

pub struct RequestAction {
    url: String,
    method: Method,
    content_type: Option<String>,
    body: String,
}

pub fn build_request(mut cmd: BuiltinCommand) -> Result<RequestAction, anyhow::Error> {
    Ok(RequestAction {
        url: cmd.args.string("url")?,
        method: cmd.args.parse("method")?,
        content_type: cmd.args.opt_string("content-type"),
        body: cmd.input.join("\n"),
    })
}

#[async_trait]
impl Action for RequestAction {
    async fn run(&self, _: &mut State) -> Result<ControlFlow, anyhow::Error> {
        println!("$ http-request {} {}\n{}", self.method, self.url, self.body);

        let client = reqwest::Client::new();

        let mut request = client
            .request(self.method.clone(), &self.url)
            .body(self.body.clone());

        if let Some(value) = &self.content_type {
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
}
