// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str;
use std::thread;
use std::time::Duration;

use anyhow::bail;
use regex::Regex;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

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
        if obj.key().map_or(false, |key| key.contains("INCOMPLETE")) {
            bail!("found incomplete sentinel file in path: {obj:?}")
        }

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

pub async fn run_verify_keys(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let bucket: String = cmd.args.parse("bucket")?;
    let prefix_path: String = cmd.args.parse("prefix-path")?;
    let key_pattern: Regex = cmd.args.parse("key-pattern")?;
    let num_attempts = cmd.args.opt_parse("num-attempts")?.unwrap_or(30);
    cmd.args.done()?;

    println!("Verifying {key_pattern} in S3 bucket {bucket} path {prefix_path}...");

    let client = mz_aws_util::s3::new_client(&state.aws_config);

    let mut attempts = 0;
    while attempts <= num_attempts {
        attempts += 1;
        let files = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&format!("{}/", prefix_path))
            .send()
            .await?;
        match files.contents {
            Some(files) => {
                let files: Vec<_> = files
                    .iter()
                    .filter(|obj| key_pattern.is_match(obj.key().unwrap()))
                    .map(|obj| obj.key().unwrap())
                    .collect();
                if !files.is_empty() {
                    println!("Found matching files: {files:?}");
                    return Ok(ControlFlow::Continue);
                }
            }
            _ => thread::sleep(Duration::from_secs(1)),
        }
    }

    bail!("Did not find matching files in bucket {bucket} prefix {prefix_path}");
}
