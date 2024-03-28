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
use std::str;

pub async fn run_verify_data(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let mut expected_body = cmd.input;
    let bucket: String = cmd.args.parse("bucket")?;
    let key: String = cmd.args.parse("key")?;
    let sort_rows = cmd.args.opt_bool("sort-rows")?.unwrap_or(false);
    cmd.args.done()?;

    println!("Verifying contents of S3 bucket {bucket} key {key}...");

    let client = mz_aws_util::s3::new_client(&state.aws_config);
    let files = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&format!("{}/", key))
        .send()
        .await?;
    if files.contents.is_none() {
        bail!("no files found in bucket {bucket} key {key}");
    }

    let mut rows = vec![];
    for obj in files.contents.unwrap().iter() {
        let file = client
            .get_object()
            .bucket(&bucket)
            .key(obj.key().unwrap())
            .send()
            .await?;
        let bytes = file.body.collect().await?.into_bytes();
        let actual_body = str::from_utf8(bytes.as_ref())?;
        rows.extend(actual_body.lines().map(|l| l.to_string()));
    }
    if sort_rows {
        expected_body.sort();
        rows.sort();
    }
    if rows != expected_body {
        bail!(
            "content did not match\nexpected:\n{:?}\n\nactual:\n{:?}",
            expected_body,
            rows
        );
    }

    Ok(ControlFlow::Continue)
}
