// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::default::Default;

use async_trait::async_trait;
use rusoto_s3::{PutObjectRequest, S3};

use crate::action::{Action, State, CI_S3_BUCKET};
use crate::parser::BuiltinCommand;

pub struct PutObjectAction {
    key: String,
    contents: String,
}

pub fn build_put_object(mut cmd: BuiltinCommand) -> Result<PutObjectAction, String> {
    Ok(PutObjectAction {
        key: cmd.args.string("key")?,
        contents: cmd.input.join("\n"),
    })
}

#[async_trait]
impl Action for PutObjectAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        println!(
            "Putting S3 object: {}/{} ({} bytes)",
            CI_S3_BUCKET,
            self.key,
            self.contents.len()
        );
        state
            .s3_client
            .as_ref()?
            .put_object(PutObjectRequest {
                bucket: CI_S3_BUCKET.to_string(),
                key: self.key.clone(),
                body: Some(self.contents.clone().into_bytes().into()),
                ..Default::default()
            })
            .await
            .map(|_| {
                state.s3_objects_created.insert(self.key.clone());
            })
            .map_err(|e| format!("putting s3 object: {}", e))
    }
}
