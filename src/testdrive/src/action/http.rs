// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

use reqwest::header::CONTENT_TYPE;

pub struct RequestAction {
    url: String,
    method: String,
    content_type: Option<String>,
    body: String,
}

pub fn build_request(mut cmd: BuiltinCommand) -> Result<RequestAction, String> {
    Ok(RequestAction {
        url: cmd.args.string("url")?,
        method: cmd.args.string("method")?.to_ascii_uppercase(),
        content_type: cmd.args.opt_parse("content-type")?,
        body: cmd.input.join("\n"),
    })
}

#[async_trait]
impl Action for RequestAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, _: &mut State) -> Result<(), String> {
        println!("$ http-request {} {}\n{}", self.method, self.url, self.body);

        let client = reqwest::Client::new();

        let method = self
            .method
            .parse()
            .or_else(|_| Err(format!("Unknown http method type: {}", self.method)))?;

        let mut request = client.request(method, &self.url).body(self.body.clone());

        if let Some(value) = &self.content_type {
            request = request.header(CONTENT_TYPE, value);
        }

        let response = request.send().await.map_err(|e| e.to_string())?;
        let status = response.status();

        println!(
            "{}\n{}",
            status,
            response.text().await.map_err(|e| e.to_string())?
        );

        if status.is_success() {
            Ok(())
        } else {
            Err(format!("http-request returned code: {}", status))
        }
    }
}
