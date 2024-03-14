// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;
use anyhow::bail;
use aws_credential_types::Credentials;
use aws_types::region::Region;
use aws_types::sdk_config::SdkConfig;
use aws_types::sdk_config::SharedCredentialsProvider;
use std::str;

pub async fn run_verify_data(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let expected_body = cmd.input.join("\n") + "\n";
    let bucket: String = cmd.args.parse("bucket")?;
    let key: String = cmd.args.parse("key")?;
    cmd.args.done()?;

    println!("Verifying contents of S3 bucket {bucket} key {key}...");

    let client = mz_aws_util::s3::new_client(&state.aws_config);
    let file = client
        .get_object()
        .bucket(bucket)
        .key(format!("{}/part-0001.csv", key))
        .send()
        .await?;
    let bytes = file.body.collect().await?.into_bytes();
    let actual_body = str::from_utf8(bytes.as_ref())?;
    if *actual_body != *expected_body {
        bail!(
            "content did not match\nexpected:\n{:?}\n\nactual:\n{:?}",
            expected_body,
            actual_body
        );
    }

    Ok(ControlFlow::Continue)
}
