// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::{bail, Context};

use mz_ccsr::{SchemaReference, SchemaType};
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;

use crate::action::{ControlFlow, State};
use crate::format::avro;
use crate::parser::BuiltinCommand;

pub async fn run_publish(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    // Parse arguments.
    let subject = cmd.args.string("subject")?;
    let schema_type = match cmd.args.string("schema-type")?.as_str() {
        "avro" => SchemaType::Avro,
        "json" => SchemaType::Json,
        "protobuf" => SchemaType::Protobuf,
        s => bail!("unknown schema type: {}", s),
    };
    let references_in = match cmd.args.opt_string("references") {
        None => vec![],
        Some(s) => s.split(',').map(|s| s.to_string()).collect(),
    };
    cmd.args.done()?;
    let schema = cmd.input.join("\n");

    // Run action.
    println!(
        "Publishing schema for subject {} to the schema registry...",
        subject.quoted(),
    );
    let mut references = vec![];
    for reference in references_in {
        let subject = state
            .ccsr_client
            .get_subject(&reference)
            .await
            .with_context(|| format!("fetching reference {}", reference))?;
        references.push(SchemaReference {
            name: subject.name,
            subject: reference.to_string(),
            version: subject.version,
        })
    }
    state
        .ccsr_client
        .publish_schema(&subject, &schema, schema_type, &references)
        .await
        .context("publishing schema")?;
    Ok(ControlFlow::Continue)
}

pub async fn run_verify(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    // Parse arguments.
    let subject = cmd.args.string("subject")?;
    match cmd.args.string("schema-type")?.as_str() {
        "avro" => (),
        f => bail!("unknown format: {}", f),
    };
    cmd.args.done()?;
    let expected_schema = match &cmd.input[..] {
        [expected_schema] => {
            avro::parse_schema(expected_schema).context("parsing expected avro schema")?
        }
        _ => bail!("unable to read expected schema input"),
    };

    // Run action.
    println!(
        "Verifying contents of latest schema for subject {} in the schema registry...",
        subject.quoted(),
    );
    let actual_schema = state
        .ccsr_client
        .get_schema_by_subject(&subject)
        .await
        .context("fetching schema")?
        .raw;
    let actual_schema = avro::parse_schema(&actual_schema).context("parsing actual avro schema")?;
    if expected_schema != actual_schema {
        bail!(
            "schema did not match\nexpected:\n{:?}\n\nactual:\n{:?}",
            expected_schema,
            actual_schema,
        );
    }
    Ok(ControlFlow::Continue)
}

pub async fn run_wait(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    // Parse arguments.
    let subject = cmd.args.string("subject")?;
    cmd.args.done()?;
    cmd.assert_no_input()?;

    // Run action.
    println!(
        "Waiting for schema for subject {} to become available in the schema registry...",
        subject.quoted(),
    );
    Retry::default()
        .initial_backoff(Duration::from_millis(50))
        .factor(1.5)
        .max_duration(state.timeout)
        .retry_async_canceling(|_| async {
            state
                .ccsr_client
                .get_schema_by_subject(&subject)
                .await
                .context("fetching schema")
                .and(Ok(()))
        })
        .await?;
    Ok(ControlFlow::Continue)
}
