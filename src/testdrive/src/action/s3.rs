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
use rusoto_core::RusotoError;
use rusoto_s3::{
    CreateBucketConfiguration, CreateBucketError, CreateBucketRequest, PutObjectRequest, S3,
};

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct CreateBucketAction {
    bucket: String,
}

pub fn build_create_bucket(mut cmd: BuiltinCommand) -> Result<CreateBucketAction, String> {
    Ok(CreateBucketAction {
        bucket: cmd.args.string("bucket")?,
    })
}

#[async_trait]
impl Action for CreateBucketAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        println!("Creating S3 Bucket {}", self.bucket);

        match state
            .s3_client
            .create_bucket(CreateBucketRequest {
                bucket: self.bucket.clone(),
                create_bucket_configuration: Some(CreateBucketConfiguration {
                    location_constraint: Some(state.aws_region.name().to_string()),
                }),
                ..Default::default()
            })
            .await
        {
            Ok(_) | Err(RusotoError::Service(CreateBucketError::BucketAlreadyOwnedByYou(_))) => {
                state.s3_buckets_created.insert(self.bucket.clone());
                Ok(())
            }
            Err(e) => Err(format!("creating bucket: {}", e)),
        }
    }
}

pub struct PutObjectAction {
    bucket: String,
    key: String,
    contents: String,
}

pub fn build_put_object(mut cmd: BuiltinCommand) -> Result<PutObjectAction, String> {
    Ok(PutObjectAction {
        bucket: cmd.args.string("bucket")?,
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
        println!("Creating S3 Bucket {}", self.bucket);

        state
            .s3_client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                body: Some(self.contents.clone().into_bytes().into()),
                key: self.key.clone(),
                ..Default::default()
            })
            .await
            .map(|_| ())
            .map_err(|e| format!("putting s3 object: {}", e))
    }
}
